package app

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"time"

	"feedvalve/internal/mirror"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// ShardOf maps a unit onto one of n shards (Vol 1 section 10.4, HLD section
// 6.5).
//
// General units -- those with FlagGeneral -- go to generalShard per FR8,
// REGARDLESS of any Token field the message happens to carry. That is INV-SCOPE
// and it is not a special case to be tidied away: bible section 4.5's market
// status messages carry a Token field that names a contract, while the row they
// write is keyed by MarketType and is market-wide. Routing one of those by its
// Token would put the same MarketType row on four different shards, which
// breaks ASM-15's single-writer-per-row and produces intermittent lost updates
// with no error anywhere.
//
// For token-scoped units the token is passed through an integer finaliser
// before the modulo. The mix is NOT decoration: exchange tokens are allocated
// in contiguous blocks, so a plain token % n correlates shard assignment with
// contract family and puts a whole expiry series on one shard.
//
// The mix MUST be deterministic and stable for the life of the deployment: it
// decides which shard owns a row, so changing it re-keys every mirror. That is
// also why runtime.shards and runtime.generalShard are startup-only (Vol 0
// section 7.2, contradictions C3/C4).
func ShardOf(u *model.UpdateUnit, n int, generalShard int) model.ShardID {
	if u.General() {
		return model.ShardID(generalShard)
	}
	return model.ShardID(mix32(uint32(u.Token)) % uint32(n))
}

// mix32 is a 32->32 bit avalanche of the fmix32 shape: two xor-shifts and two
// odd multiplies, about four instructions, no allocation, no branch, no
// dependency.
//
// THE PROPERTIES ARE NORMATIVE, NOT THE CONSTANTS (Vol 1 section 10.4). Any
// stable finaliser satisfying these three would do:
//
//  1. deterministic across runs and architectures, so -selfcheck's ASM-15 check
//     and the running process agree about which shard owns a row;
//  2. avalanche in the LOW bits, because % n with a small n uses only those --
//     a mix that scrambled only the high bits would leave token % 4 exactly
//     where it started;
//  3. allocation-free and branch-free, because this runs once per unit on the
//     path G-ALLOC-1 measures at zero.
//
// Both multipliers are odd, which makes each multiply a bijection on uint32 and
// is what keeps the whole function one-to-one: two distinct tokens can collide
// on a shard, but never because the mix threw information away.
func mix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// allocShardQueues allocates Q1: one channel and one done channel per shard.
//
// A method rather than four lines inside Build, because the sub-packet tests
// construct a Graph directly -- they exercise the decode path without a
// lifecycle -- and a fan-out into a nil slice is an index panic rather than a
// useful failure. One allocator means the fixture and the real graph cannot
// disagree about the shape of the thing being fanned into.
func (g *Graph) allocShardQueues(shards, capacity int) {
	g.shardCh = make([]chan model.UpdateUnit, shards)
	g.shardDrainDone = make([]chan struct{}, shards)
	for k := range g.shardCh {
		g.shardCh[k] = make(chan model.UpdateUnit, capacity)
		g.shardDrainDone[k] = make(chan struct{})
	}
}

// buildShards is the mirror's half of startup step 12 (Vol 2 section 1.3):
// every shard with its arenas, indexes, rings and batches, and the channels
// that join them. Allocation only -- nothing starts until Run.
//
// The capacities are the design, not tuning. batchQ holds shards x m, so a
// shard's non-blocking send cannot find it full while the shard owns a free
// batch (Vol 2 section 7.4). completionCh[k] holds m, so every batch out when
// shard k returns at the drain deadline still fits, and no writer ever blocks
// on a shard that has stopped reading (V2 section 2.6, gate G-LIFE-4).
func (g *Graph) buildShards(sz mirror.Sizing) {
	n, m := sz.Shards, sz.MaxInFlightPerShard
	g.bus = mirror.NewFlushBus(n)
	g.batchQ = make(chan *model.Batch, n*m)
	g.completionCh = make([]chan *model.Batch, n)
	g.tickCh = make([]chan struct{}, n)
	g.shards = make([]*mirror.Shard, n)

	// One load of the published snapshot per call, which is what puts the
	// Params inside Runtime rather than beside it (ruling D1).
	params := func() *mirror.Params { return g.loadRuntime().Mirror }
	for k := range n {
		g.completionCh[k] = make(chan *model.Batch, m)
		g.tickCh[k] = make(chan struct{}, 1)
		g.shards[k] = mirror.NewShard(model.ShardID(k), sz, params, g.deps.Dests, g.guards, g.bus,
			&g.deps.Inst.mirrorShards[k], g.deps.Log, func(err error) { g.shardReport(k, err) },
			mirror.Edges{
				In:           g.shardCh[k],
				Completion:   g.completionCh[k],
				Tick:         g.tickCh[k],
				BatchQ:       g.batchQ,
				DrainExpired: g.drainExpired,
			})
	}
}

// startShards starts one goroutine per shard, the sole consumer of its
// shardCh[k] (Vol 4 startup step 14). The stage keeps the name the Phase 2
// discard consumers had, "shard-consumers", so the ordering gates keep meaning
// what they say.
//
// Started BEFORE the decoder that feeds them -- consumers before producers;
// starting a producer first burns queue slack for nothing.
func (g *Graph) startShards() {
	g.shardConsumersStarted = true
	g.mark(&g.acquired, "shard-consumers")
	for k, s := range g.shards {
		go g.runShard(k, s.Run)
	}
}

// runShard is shard k's goroutine: run the loop, and map how it ended onto the
// fatal path (phase 3 ruling D9). It closes shardDrainDone[k] however it ends,
// so teardown's join cannot hang on a shard that died.
//
// A panic is recovered ONCE and not resumed: an arena may be mid-mutation, so
// the shard's state is no longer trustworthy (F26). The stack is logged, since
// F26's runbook is "file a bug with the stack".
func (g *Graph) runShard(k int, run func() error) {
	defer close(g.shardDrainDone[k])
	defer func() {
		if r := recover(); r != nil {
			g.deps.Inst.panicsRecoveredShard.Add(1)
			detail := "shard=" + strconv.Itoa(k) + " panic=" + fmt.Sprint(r)
			// Not behind Allow, like stallFatal's line: it runs once, on the
			// way out, and is the operator's only account of why.
			g.deps.Log.Errorf(obs.ClassPanicRecovered,
				"shard goroutine panicked; its arena may be mid-mutation, the process stops: %s\n%s",
				detail, debug.Stack())
			g.Fatal(FatalShardPanic, detail)
		}
	}()
	if err := run(); err != nil {
		reason := FatalShardInvariant
		if errors.Is(err, mirror.ErrBatchQSendFailed) {
			reason = FatalBatchQSendFail // S1
		}
		detail := "shard=" + strconv.Itoa(k) + " counter=" +
			shardDefectCounter(&g.deps.Inst.mirrorShards[k], reason) + " err=" + err.Error()
		// Not behind Allow: a shard returns at most once, and this line is the
		// operator's account of the stop. Its own class rather than the
		// startup one, which is for failed startup steps and is also the
		// UDP read-error bucket.
		g.deps.Log.Errorf(obs.ClassShardDefect, "shard stopped on a construction defect: %s", detail)
		g.Fatal(reason, detail)
	}
}

// shardDefectCounter names the counter that recorded the defect a shard
// returned, so the shutdown line says which invariant broke (ruling D9). The
// shard counts before it returns, on this goroutine, so the read is ordered.
// Step 0a -- a misrouted or twice-returned batch -- has no counter of its own.
func shardDefectCounter(in *mirror.ShardInstruments, reason FatalReason) string {
	switch {
	case reason == FatalBatchQSendFail:
		return "flush_deferred"
	case in.InflightDesync.Load() > 0:
		return "inflight_desync"
	case in.CohortNotEmpty.Load() > 0:
		return "cohort_not_empty"
	}
	return "none"
}

// shardReport is shard k's report function: defect S3, the one Run survives
// (ruling D9). The rows were requeued, so the shard keeps running into the
// normal drain and can still write them; the process stops through F27.
// Fatal is one-shot, so a writer returning several such batches stops it once.
//
// The line has its own class, shard_defect. Under the startup class it would
// share a bucket with the UDP read-error lines, and a burst of those could
// suppress F27's explanation. It stays behind Allow because, unlike the stop
// itself, a writer can report this once per bad batch.
func (g *Graph) shardReport(k int, err error) {
	if g.deps.Log.Allow(obs.ClassShardDefect) {
		g.deps.Log.Errorf(obs.ClassShardDefect,
			"a writer returned a batch it never adjudicated; the rows are requeued, the shard "+
				"drains and the process stops: shard=%d %v", k, err)
	}
	g.Fatal(FatalBatchRowUnreported, "shard="+strconv.Itoa(k)+" counter=batch_row_unreported err="+err.Error())
}

// startFlushTicker runs the one W ticker (Vol 2 section 2.4): every
// flush.windowMs, a NON-BLOCKING send on each tickCh[k]. A full tickCh means
// shard k has not returned to its select within W -- counted as
// tick_send_dropped{k} (A13, the ticker's) and never waited for: one slow
// shard must not delay the others' ticks.
//
// The period is re-read at each tick, and the ticker reset when it changed:
// Vol 4 section 2.4's ticker row, "at each tick, to reset its period", so a
// reload of flush.windowMs takes effect within one old window. The stats row's
// "the sleeping goroutine keeps the old period" is a different row and does
// not govern this timer.
//
// Startup step 15 (Vol 4 section 1.4), beside the stats goroutine: after the
// shards, so the first tick cannot land on a shard that does not exist.
func (g *Graph) startFlushTicker() {
	g.tickerStarted = true
	g.mark(&g.acquired, "flush-ticker")
	window := g.loadRuntime().Mirror.Window
	go func() {
		defer close(g.tickerDone)
		t := time.NewTicker(window)
		defer t.Stop()
		for {
			select {
			case <-g.tickerStop:
				return
			case <-t.C:
				if w := g.loadRuntime().Mirror.Window; w != window {
					window = w
					t.Reset(w)
				}
				g.sendTicks()
			}
		}
	}()
}

// sendTicks is one tick: a non-blocking send per shard.
func (g *Graph) sendTicks() {
	for k, ch := range g.tickCh {
		select {
		case ch <- struct{}{}:
		default:
			g.deps.Inst.mirrorShards[k].TickSendDropped.Add(1)
		}
	}
}

// stopFlushTicker stops the ticker. Called once the shards have returned; its
// sends are non-blocking, so the order is not load-bearing.
func (g *Graph) stopFlushTicker() {
	if !g.tickerStarted {
		return
	}
	close(g.tickerStop)
	<-g.tickerDone
	g.mark(&g.released, "flush-ticker")
}

// armDrainDeadline arms the one shared drain deadline (Vol 4 section 1.5 step
// 7, ruling D4): flush.drainTimeoutMs after t_shutdown, close drainExpired.
// Once only. logShutdownInitiated arms it at t_shutdown; teardown arms it on
// the error paths that never reach that line, so every drain is bounded.
//
// Each shard selects on drainExpired only once it has seen its shardCh closed,
// so a deadline that has already passed -- drainTimeoutMs = 0 -- still cannot
// pre-empt units buffered in Q1.
func (g *Graph) armDrainDeadline() {
	g.drainArm.Do(func() {
		g.drainTimer = time.AfterFunc(g.loadRuntime().Mirror.DrainTimeout, func() { close(g.drainExpired) })
	})
}

// stopShardConsumers is shutdown step 6: close every shardCh[k] and wait for
// its consumer to finish what is already queued.
//
// CLOSED, NOT ABANDONED, and the ordering is what makes that safe: this runs
// only after the decoder goroutine has returned, so the sole producer is gone
// and no send can race the close. Closing while the decoder still held the
// channel would panic on send -- which is why this is a separate function
// called from one place in teardown rather than a defer that could be
// registered anywhere.
//
// With live shards the wait IS step 7: each shard drains under the shared
// deadline and returns, and step 8 -- counting what the deadline abandoned --
// happens inside the shard (Vol 2 section 2.6).
func (g *Graph) stopShardConsumers() {
	if !g.shardConsumersStarted {
		return
	}
	for k := range g.shardCh {
		close(g.shardCh[k])
	}
	g.mark(&g.released, "shard-channels")

	for k := range g.shardDrainDone {
		<-g.shardDrainDone[k]
	}
	g.mark(&g.released, "shard-consumers")
}

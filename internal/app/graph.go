package app

import (
	"context"
	"hash/maphash"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/framing"
	"feedvalve/internal/health"
	"feedvalve/internal/mirror"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/writer"
)

// FatalReason enumerates the terminal conditions of Vol 4 section 1.2 exit
// code 4 -- eight of them, Vol 4's five plus V7, F27 and FatalShardInvariant
// (phase 3 ruling D9) -- so the shutdown line and the exit path cannot disagree
// about which one fired.
//
// They are an enumeration rather than an error string because the shutdown
// summary, the exit code and the log line all have to name the same condition,
// and a formatted string would let them drift.
type FatalReason uint8

const (
	// FatalQ1Overflow is F9 -- a shard queue send that exhausted its budget.
	// Vol 0 section 9.2, HLD section 6.6.
	FatalQ1Overflow FatalReason = iota
	// FatalDBFailFast is F13 -- the writer pool entered FailFast.
	FatalDBFailFast
	// FatalWriterPoolStop is F20 -- the two-writer statement rule stopped the
	// pool. HLD section 9.2.
	FatalWriterPoolStop
	// FatalBatchQSendFail is F21 -- a batchQ send that cannot fail by
	// construction, failed anyway. HLD section 7.3.
	FatalBatchQSendFail
	// FatalShardPanic is F26 -- a panic with an arena mid-mutation, which is
	// the one panic that cannot be contained and resumed.
	FatalShardPanic
	// FatalUDPReadFailure is Vol 1's V-7: maxConsecutiveReadErrors reads failed
	// in a row with the context still alive.
	//
	// poc logs and continues unconditionally, which on an unrecoverable socket
	// is a tight spin logging at full CPU -- worse on a 4-CPU capped partition
	// than stopping. Vol 1 section 3.7 makes it terminal instead, and terminal
	// here means the drain runs: the shards still owe their dirty rows to the
	// database, which is exactly what an os.Exit would skip.
	FatalUDPReadFailure
	// FatalBatchRowUnreported is F27 -- a writer returned a batch with a row
	// it never adjudicated (Vol 2 section 2.7, S3). The shard requeued the rows
	// and keeps running into the drain, so they can still be written on the
	// way down (phase 3 ruling D9).
	FatalBatchRowUnreported
	// FatalShardInvariant is a shard construction defect that is not a panic:
	// S2 (a row in two batches), cohort_not_empty, or a misrouted or twice
	// returned batch (step 0a). Not F26, whose runbook is "file a bug with the
	// stack" and whose observable is panics_recovered{stage=shard} -- neither
	// holds for a returned error (ruling D9).
	FatalShardInvariant

	numFatalReasons
)

var fatalReasonNames = [numFatalReasons]string{
	FatalQ1Overflow:         "F9-q1-overflow",
	FatalDBFailFast:         "F13-db-fail-fast",
	FatalWriterPoolStop:     "F20-writer-pool-stop",
	FatalBatchQSendFail:     "F21-batchq-send-failed",
	FatalShardPanic:         "F26-shard-panic",
	FatalUDPReadFailure:     "V7-udp-read-failure",
	FatalBatchRowUnreported: "F27-batch-row-unreported",
	FatalShardInvariant:     "shard-invariant",
}

func (r FatalReason) String() string {
	if r < numFatalReasons {
		return fatalReasonNames[r]
	}
	return "unknown-terminal-condition"
}

// fatalEvent is one terminal condition, with the detail that goes on the log
// line.
type fatalEvent struct {
	reason FatalReason
	detail string
}

// Deps is everything Build needs that the startup steps before it produced.
//
// Vol 4 section 1.7 sketches Build as taking four positional parameters; this
// is the same set plus the two loggers and the destination table, which the
// sketch omits because it predates P1-07. A struct rather than seven positional
// arguments, so a future addition cannot silently reorder them.
type Deps struct {
	Cfg   *config.Config
	Store *config.Store
	// DB is nil when the configuration declares no db object. It is the
	// oradb wrapper (section 1.7's *oradb.DB), which owns the redactor and
	// hands out sessions; app owns its Close.
	DB    *oradb.DB
	Reg   obs.Registry
	Log   *obs.AppLogger
	Debug *obs.DebugLogger
	Dests *destinations.Set

	// Stderr is where the emission goes. Vol 4 section 6.7: "the entire
	// observability surface is stderr" -- there is no file, no socket and no
	// endpoint. Injected rather than taken from os.Stderr so the whole
	// lifecycle is testable in-process, which is what makes the startup and
	// drain gates possible at all.
	Stderr io.Writer

	// Inst are the instrument handles resolved at startup step 9, before the
	// registry was frozen.
	Inst instruments

	// Decode is called by the decoder goroutine for every datagram drained
	// from QD, in arrival order, on that one goroutine.
	//
	// This is the seam Phase 2 filled: framing's envelope split, the decoder
	// registry, and the fan-out onto shardCh[k] under the fan-out budget all
	// hang off it, and in production it is nil so receiver.decodeDatagram
	// runs. A non-nil value is the TEST seam -- it is how the lifecycle suite
	// drives the drain with a callback that blocks or counts -- and the
	// lifecycle owns only that the consumer exists, is the only one, and
	// drains to empty on close; it does not own what the bytes mean.
	//
	// Returning a non-nil error stops the drain and fails Run, which is the
	// right shape for a decode path whose errors are either per-packet (and so
	// handled inside, by Reject) or terminal.
	Decode func(*model.Datagram) error

	// ConfigPath is the file SIGHUP re-reads. Empty means the process runs on
	// built-in defaults and there is nothing to reload (D7).
	ConfigPath string
}

// Graph is the constructed process graph.
//
// No field is exported, because nothing outside app may hold a reference to the
// socket, the ring or a goroutine's done channel. What the tests need instead
// is the acquisition and release ORDER, which is recorded rather than exposed --
// see stages.
type Graph struct {
	deps Deps

	// QD, allocated at startup step 12 and drained at shutdown step 5a.
	qdProducer *framing.Producer
	qdConsumer *framing.Consumer

	// Q1: one channel per shard, allocated at startup step 12 and closed at
	// shutdown step 6. Vol 0 section 9.2 -- model.UpdateUnit BY VALUE, 360
	// bytes, capacity runtime.shardQueueCapacity.
	//
	// By value and not by pointer, and that is ADR-006 rather than a
	// micro-optimisation: a pointer here would hand a shard a window onto
	// fanoutBuf, which the decoder overwrites on the very next sub-packet.
	// model.UpdateUnit is pointer-free and slice-free precisely so this copy is
	// the whole transfer.
	//
	// The producer is the decoder goroutine, sole; the consumer is shard k,
	// sole -- which is what makes INV-ORDER (Vol 1 section 10.7) hold
	// structurally, with no sequence number involved.
	shardCh []chan model.UpdateUnit

	// shardDrainDone is closed by each shard consumer when its channel is
	// drained and closed, so teardown can join them rather than assume.
	shardDrainDone        []chan struct{}
	shardConsumersStarted bool

	// The mirror (Vol 2), allocated by Build at startup step 12 and run by
	// startShards. shards[k] is the sole consumer of shardCh[k].
	//
	// batchQ is Q2, shared by every shard, capacity shards x
	// maxInFlightPerShard, so a shard's non-blocking send can never find it
	// full (Vol 2 section 7.4). completionCh[k] is Q2c, capacity
	// maxInFlightPerShard -- LOAD BEARING (V2 section 2.6, gate G-LIFE-4): a
	// shard returning at the drain deadline with all m batches out leaves room
	// for every one of them, so no writer blocks on a shard that stopped
	// reading. tickCh[k] is cap 1, fed by the flush ticker with a non-blocking
	// send. The writer pool (Run starts it when a database is configured)
	// is batchQ's consumer; teardown closes batchQ (step 9) once the shards have
	// joined, then joins the pool (step 10).
	shards       []*mirror.Shard
	batchQ       chan *model.Batch
	completionCh []chan *model.Batch
	tickCh       []chan struct{}
	bus          *mirror.FlushBus

	// pool is the writer pool: built by Build when Deps.DB is non-nil, started
	// by Run BEFORE the shards (V4 section 1.4 step 13), nil otherwise. See
	// writers_wiring.go. poolCancel releases the pool-owned context (D13);
	// poolWatch* run the goroutine that turns Pool.Stopped into Fatal.
	pool           *writer.Pool
	poolCancel     context.CancelFunc
	writersStarted bool
	poolWatchStop  chan struct{}
	poolWatchDone  chan struct{}

	// guards is the per-transcode guard state every shard shares (ruling D2),
	// and guardSnap the evaluator's own state, touched only by the stats
	// goroutine under emitMu. guardEvalOverride is a test seam: when non-zero
	// it replaces the LIVE ordering.guardEvalIntervalSec (which is reloadable),
	// so a test can shorten it below the configuration's 1 s floor before Run
	// without a new key. Production leaves it zero.
	guards            *mirror.GuardTable
	guardSnap         *mirror.GuardSnapshot
	guardEvalOverride time.Duration

	// drainExpired is the ONE shared drain deadline (Vol 4 section 1.5 step 7,
	// ruling D4), closed by drainTimer flush.drainTimeoutMs after t_shutdown.
	// Armed once (drainArm), stopped if every shard is quiescent first.
	drainExpired chan struct{}
	drainArm     sync.Once
	drainTimer   *time.Timer

	// The flush ticker: one time.Ticker at flush.windowMs, a non-blocking send
	// per tickCh[k].
	tickerStarted bool
	tickerStop    chan struct{}
	tickerDone    chan struct{}

	// conn is the UDP listener, bound by Run as the LAST resource.
	//
	// Guarded by mu, because bind writes it on the lifecycle goroutine while
	// the receiver, the teardown and LocalAddr read it from others. The race
	// detector found this; nothing about it is visible in behaviour, which is
	// the whole argument for having fixed the toolchain so -race could run.
	//
	// THE CONCRETE TYPE, not net.PacketConn, and both reasons are load-bearing
	// (Vol 1 section 3.1): SetReadBuffer is not on the interface, and neither
	// is ReadFromUDPAddrPort. PacketConn.ReadFrom returns a net.Addr, which for
	// UDP is a heap-allocated *net.UDPAddr carrying a []byte IP -- per
	// datagram, 2,400 times a second, on the one goroutine ADR-006 keeps
	// allocation-free.
	conn *net.UDPConn

	// runtime is the published configuration-and-registry snapshot (ADR-L11),
	// loaded once per datagram by the decoder goroutine. Written by Build
	// before anything starts, and afterwards by the SIGHUP reload path (reload.go).
	runtime atomic.Pointer[Runtime]

	// readerErr is the receiver's terminal error, if it had one: written by
	// the reader goroutine immediately before it closes readerDone, read by
	// wait and by nothing else. The channel close is the happens-before, which
	// is why this needs no lock (Vol 1 section 3.4: readLoop "returns a
	// non-nil error on a fatal condition").
	readerErr error

	// fatal carries the first terminal condition. Capacity 1 and guarded by
	// fatalOnce, so a goroutine reporting a terminal condition never blocks
	// even if the lifecycle has already begun draining for another reason.
	fatal     chan fatalEvent
	fatalOnce sync.Once

	readerDone  chan struct{}
	decoderDone chan struct{}
	statsDone   chan struct{}
	statsStop   chan struct{}

	// Whether each goroutine actually started, so teardown waits only on
	// channels something will close. A bind failure leaves the reader
	// unstarted, and waiting on its done channel would hang the error path --
	// which is the shape gate G-LIFE-3 exercises.
	readerStarted  bool
	decoderStarted bool
	statsStarted   bool

	teardownOnce sync.Once

	// reloadMu serialises SIGHUP reloads (D12) and guards reloadClosed, which
	// stopHUP and teardown set so that a signal arriving after the lifecycle
	// has left the running state is a logged no-op. Holding it across a whole
	// reload is what makes stopHUP wait for an in-flight one.
	reloadMu     sync.Mutex
	reloadClosed bool

	// emitMu serialises the two emitters: the stats goroutine's tick and
	// logShutdownInitiated on the Run goroutine. They overlap because
	// teardown stops stats LAST, after the shutdown snapshot has been emitted.
	// Held around sampleGauges and Reg.Emit together (emitStats), because
	// both halves are unsafe for two callers: Reg.Emit's snapshot writes the
	// slab its render pass reads, and sampleGauges read-modify-writes
	// lastParks, which two callers would turn into a doubled qd_wake_parked.
	//
	// Here and not inside obs, which owns no goroutine and has exactly one
	// caller per emission by design; the lifecycle is what has two.
	emitMu sync.Mutex

	// lastParks converts the ring's cumulative park count into the delta
	// qd_wake_parked wants. The ring exposes a plain number because the
	// ledger assigns that instrument to the decoder, not to framing.
	// Guarded by emitMu.
	lastParks uint64

	// stages records every resource acquisition and release, in order.
	//
	// This exists because the P1-09 card asks for "every deferred cleanup
	// observed to run (assert it, don't eyeball it)", and because gate
	// G-LIFE-1 is an assertion about ORDER -- that the socket is bound after
	// the pre-allocation and after the goroutines start. Neither is checkable
	// from the outside without a record, and a test that inspected real defers
	// would have to be the process.
	//
	// The record is in addition to real defers, never instead of them: a panic
	// must still unwind the resources, which only a defer does.
	mu       sync.Mutex
	acquired []string
	released []string

	drainStarted time.Time

	// circuit is the liveness machine, owned by the stats goroutine (P5-11).
	circuit circuitMonitor

	// sessionCh is channel H (Vol 4 section 4.5): market-status observations
	// from the decoder to the stats goroutine, by value, capacity fixed at
	// startup. sessions is the machine it feeds, owned by the stats goroutine.
	sessionCh chan model.SessionEvent
	sessions  health.Sessions

	// statsNow replaces the stats goroutine's clock in tests (clockNow). Nil in
	// production. Set before Run.
	statsNow func() (wall time.Time, mono time.Duration)
}

// Build performs the startup steps that allocate but do not yet run: Vol 4
// section 1.4 step 10 (immutable derived state) and step 12 (pre-allocation).
//
// It deliberately starts NO goroutine and binds NO socket. The sketch in
// section 1.7 has Build starting the writer and shard goroutines; keeping Build
// allocation-only means a failure between Build and Run has nothing running to
// stop, which is what makes the unwind in gate G-LIFE-3 simple enough to be
// obviously correct. So Build allocates the shards and Run starts them, where
// the teardown that must stop them also lives -- and the writers the
// same way.
//
// Step 11 -- OpenDB and its deferred Close -- happens in run before this,
// because the one-shot -dbcheck mode needs the connection without a graph.
func Build(d Deps) (*Graph, error) {
	if d.Cfg == nil {
		return nil, exitf(ExitStartup, "internal: Build with no configuration")
	}
	if d.Reg == nil {
		return nil, exitf(ExitStartup, "internal: Build with no instrument registry; "+
			"startup step 9 must resolve every instrument before the graph is built")
	}
	if d.Log == nil {
		return nil, exitf(ExitStartup, "internal: Build with no logger")
	}

	g := &Graph{
		deps:         d,
		fatal:        make(chan fatalEvent, 1),
		readerDone:   make(chan struct{}),
		decoderDone:  make(chan struct{}),
		statsDone:    make(chan struct{}),
		statsStop:    make(chan struct{}),
		sessionCh:    make(chan model.SessionEvent, sessionChCap),
		tickerStop:   make(chan struct{}),
		tickerDone:   make(chan struct{}),
		drainExpired: make(chan struct{}),
	}

	// Step 10: the immutable derived state. The destination table, the ColMask
	// bit assignment, the image offsets and the contributor slots were all
	// resolved by loadAll and validated together with the configuration; this
	// records the dependency rather than redoing it. The decoder registry and
	// the policy map join it in Phase 2.
	if d.Dests == nil || d.Dests.Len() == 0 {
		return nil, exitf(ExitStartup, "internal: Build with no destination table")
	}

	// The decoder registry: level 2 of Vol 1 section 8.1, resolved against the
	// catalogue and the destination table. This is where rules V-T1 and V-T6
	// are evaluated -- the two config.Unchecked() named as needing the
	// registry since P1-06 -- so a configuration naming a transcode this build
	// cannot decode fails HERE, at startup, rather than by storing nothing for
	// it in production.
	// The decoders' counter sink, constructed BEFORE the registry because
	// BuildRegistry needs it to construct the decoders, and given the registry
	// immediately after -- which is the only order that works and the reason
	// setRegistry exists.
	d.Inst.log = d.Log
	cnt := newDecodeCounters(&d.Inst)
	reg, err := decode.BuildRegistry(transcodeSpecs(d.Cfg), d.Dests, cnt)
	if err != nil {
		return nil, exitf(ExitStartup, "building the decoder registry: %w", registryError(err))
	}

	// The per-transcode counters were sized from the CONFIGURED inventory at
	// step 9, which is what obs sized its {code} dimension from (obsOptions) --
	// not from reg.Len(), even though a successful build makes them equal.
	// They must be the same number or a per-transcode counter would index a
	// slot obs never allocated, and the two are derived from the same
	// configuration by different paths, so the agreement is asserted here,
	// where the decoder registry finally exists.
	//
	// ASSERTED, NOT RESOLVED. Resolving here is what this check used to sit
	// beside, and it could not work: Build runs after reg.Freeze(), so the
	// accessor panics by gate G-OBS-1's design (see resolveInstruments).
	if reg.Len() != len(d.Cfg.Transcodes) {
		return nil, exitf(ExitStartup, "internal: the registry resolved %d of %d configured "+
			"transcodes; every per-transcode counter is indexed by that position",
			reg.Len(), len(d.Cfg.Transcodes))
	}
	cnt.setRegistry(reg)
	g.deps = d

	// The ordering guard's per-transcode table, zipped HERE from the handles
	// resolved before Freeze and the registry's codes -- the same configured
	// positions the assertion above just checked agree (ruling D2).
	mt := &g.deps.Inst.mirrorTranscode
	g.guards = mirror.NewGuardTable(reg.Codes(), mt.GuardOffered, mt.GuardSuperseded)
	g.guardSnap = mirror.NewGuardSnapshot(g.guards, mt.GuardDisabled)

	// Published BEFORE any goroutine exists, which is what makes the decoder's
	// load unconditional rather than a nil check it has to repeat (ADR-L11).
	// The mirror's reloadable parameters travel in the same snapshot (ruling
	// D1): one pointer, so no reader sees a new Config beside old Params.
	rt, err := newRuntime(d.Cfg, reg, d.Dests)
	if err != nil {
		// Unreachable on a validated Config (V-SRC1 uses the same parser); an
		// error here means an unvalidated Config reached Build, and refusing
		// to start is better than a source filter nobody realises is broken.
		return nil, exitf(ExitStartup, "parsing udp.sourceAllowList: %w", err)
	}
	g.setRuntime(rt)
	g.mark(&g.acquired, "derived-state")

	// Step 12: pre-allocation, BEFORE any socket exists. A
	// first-datagram-triggered allocation of tens of megabytes would blow the
	// stall budget on packet one at market open, and an out-of-memory
	// condition must be a startup failure rather than a 10:00:00 failure.
	//
	// QD first: one allocation of capacity x 1040 bytes, pointer-free. Q1
	// and the shards -- arenas, indexes, rings, batch free lists -- follow.
	p, c, err := framing.NewRing(d.Cfg.Runtime.DatagramQueueCapacity)
	if err != nil {
		return nil, exitf(ExitStartup, "allocating the datagram ring at capacity %d: %w",
			d.Cfg.Runtime.DatagramQueueCapacity, err)
	}
	g.qdProducer, g.qdConsumer = p, c
	g.mark(&g.acquired, "qd-ring")

	// Q1, the other half of step 12: shards x shardQueueCapacity x 360 bytes,
	// 5.6 MB at the defaults. Allocated HERE, before any socket exists, for the
	// reason the step exists at all -- a first-datagram-triggered allocation of
	// megabytes would blow the stall budget on packet one at market open, and
	// an out-of-memory condition must be a startup failure rather than a
	// 10:00:00 one.
	//
	// config has already validated both numbers (V-RANGE over 1..64 shards,
	// and V-X4 that generalShard indexes one of them), so there is no bound
	// check here that would duplicate one the configuration cannot get past.
	g.allocShardQueues(d.Cfg.Runtime.Shards, d.Cfg.Runtime.ShardQueueCapacity)
	g.mark(&g.acquired, "shard-channels")

	// The rest of step 12: the shards themselves -- arenas, indexes, rings and
	// batches -- with their channels and the one seed (Vol 2 section 1.3).
	g.buildShards(mirrorSizing(d.Cfg, maphash.MakeSeed()))
	g.mark(&g.acquired, "shards")

	// The writer pool, allocation only like everything in Build: no goroutine
	// and no session until Run. Absent without a database, which is what keeps
	// the fake-writer tests honest -- and there is no stub writer in the binary.
	if err := g.buildPool(); err != nil {
		return nil, err
	}

	return g, nil
}

// Fatal is how any goroutine reports a terminal condition.
//
// One-shot: the first caller wins, later callers are no-ops, and the reason of
// the first is what reaches the shutdown line and the exit code. That matters
// because one condition commonly causes another -- a database fail-fast stops
// the writer pool -- and the operator needs the condition that started it.
//
// It NEVER calls os.Exit. os.Exit skips every defer, which would leave the
// Oracle pool open and the drain unrun; this is the regression CLAUDE.md is
// written about. Fatal delivers to a channel and lets Run drain and return.
func (g *Graph) Fatal(reason FatalReason, detail string) {
	g.fatalOnce.Do(func() {
		g.fatal <- fatalEvent{reason: reason, detail: detail}
	})
}

// mark records a stage transition. The caller passes the slice to append to so
// acquisition and release share one lock and one ordering.
func (g *Graph) mark(into *[]string, name string) {
	g.mu.Lock()
	*into = append(*into, name)
	g.mu.Unlock()
}

// Acquired returns the resources the graph took, in order. For the ordering
// gate; not part of the running process's behaviour.
func (g *Graph) Acquired() []string { return g.snapshot(&g.acquired) }

// Released returns the resources the graph gave back, in order.
func (g *Graph) Released() []string { return g.snapshot(&g.released) }

func (g *Graph) snapshot(from *[]string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), (*from)...)
}

// inst returns the lifecycle's instrument handles, resolved before Freeze and
// carried in Deps.
func (g *Graph) inst() *instruments { return &g.deps.Inst }

// LocalAddr is the address the listener actually bound. It differs from the
// configured one whenever the configuration asks for port 0, which is how the
// tests get a free port without racing another process for a fixed one.
func (g *Graph) LocalAddr() net.Addr {
	c := g.getConn()
	if c == nil {
		return nil
	}
	return c.LocalAddr()
}

// setConn and getConn keep every access to the listener under one lock. The
// listener is written once, by bind, and read by the receiver, the teardown and
// LocalAddr -- three goroutines, so "written once" is not on its own enough.
func (g *Graph) setConn(c *net.UDPConn) {
	g.mu.Lock()
	g.conn = c
	g.mu.Unlock()
}

func (g *Graph) getConn() *net.UDPConn {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.conn
}

// gapReports is the gap detector's sink. decodeCounters implements both it and
// decode.Counters, because app holds every handle either needs.
func (g *Graph) gapReports() decode.Reports {
	g.deps.Inst.log = g.deps.Log
	return newDecodeCounters(&g.deps.Inst)
}

// unknownSlot is obs's dynamic code table, reached through the one accessor
// that returns an INDEX rather than a counter (Vol 1 section 7.7).
func (g *Graph) unknownSlot(code int) (int, bool) {
	type slotResolver interface {
		UnknownTranscodeSlot(code int) (int, bool)
	}
	if r, ok := g.deps.Reg.(slotResolver); ok {
		return r.UnknownTranscodeSlot(code)
	}
	return -1, false
}

// Len is the number of datagrams currently queued in QD. For diagnostics and
// for the drain assertion; the ring's own Len is exact only from the producer
// or the consumer goroutine.
func (g *Graph) Len() int {
	if g.qdConsumer == nil {
		return 0
	}
	return g.qdConsumer.Len()
}

package app

import (
	"context"
	"encoding/binary"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/mirror"
	"feedvalve/internal/model"
)

// The P3-13 wiring tests: live shards inside a running graph, the flush ticker,
// the shared drain deadline, the fatal mapping of phase 3 ruling D9, and the
// guard evaluator's ticker. The shard's own behaviour is internal/mirror's;
// what is tested here is that app connects it the way Vol 2 and Vol 4 say.

// configureTranscodes declares codes as rate-limited, each contributing every
// column of every destination the field map says it feeds -- the mapping's own
// view, as newDiffFixture takes it, rather than a table here that could
// disagree with it.
func configureTranscodes(t *testing.T, cfg *config.Config, codes ...int) {
	t.Helper()
	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	for _, code := range codes {
		tc := config.TranscodeConfig{Code: code, Class: "rate-limited"}
		for _, d := range dests.All() {
			if !d.Contributes(model.TransCode(code)) {
				continue
			}
			cols := make([]string, 0, len(d.Columns))
			for i := range d.Columns {
				cols = append(cols, d.Columns[i].Name())
			}
			tc.Contributions = append(tc.Contributions,
				config.ContributionConfig{Destination: d.Name, Columns: cols})
		}
		cfg.Transcodes = append(cfg.Transcodes, tc)
	}
}

// wiringGraph is testGraph with 7208 configured, so a real datagram decodes into
// units the shards merge, and with the given tune applied after.
func wiringGraph(t *testing.T, tune func(*config.Config)) (*Graph, *syncBuf) {
	t.Helper()
	return testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		configureTranscodes(t, c, allocGateCode)
		if tune != nil {
			tune(c)
		}
	})
}

// mbpPayloadTokens is mbp7208Payload with the record tokens chosen by the
// caller (at most two: 7208 carries mbpMaxRecords), so a test can put rows on
// the shard it wants. Offsets are the same transcribed absolute ones.
func mbpPayloadTokens(tokens ...uint32) []byte {
	p := bcastPayload(allocGateCode, 470, mbpAbsPayloadLen)
	binary.BigEndian.PutUint16(p[mbpAbsNoOfRecords:mbpAbsNoOfRecords+2], uint16(len(tokens)))
	for rec, tok := range tokens {
		base := mbpAbsRecord0 + rec*mbpAbsStride
		binary.BigEndian.PutUint32(p[base+mbpRecTokenOff:], tok)
		binary.BigEndian.PutUint16(p[base+mbpRecBookTypeOff:], 1)
	}
	return p
}

// tokensPerShard finds per tokens that ShardOf routes to each of n shards.
func tokensPerShard(n, per int) [][]uint32 {
	out := make([][]uint32, n)
	full := 0
	for tok := uint32(90001); full < n; tok++ {
		k := mix32(tok) % uint32(n)
		if len(out[k]) < per {
			out[k] = append(out[k], tok)
			if len(out[k]) == per {
				full++
			}
		}
	}
	return out
}

// runGraph starts g.Run and returns a stop function that cancels it (the
// SIGTERM path) and returns Run's error, plus the channel Run's result arrives
// on for tests where a terminal condition ends it first. Cleanup cancels too.
func runGraph(t *testing.T, g *Graph) (cancel func(), done <-chan error) {
	t.Helper()
	ctx, cancelCtx := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- g.Run(ctx) }()
	t.Cleanup(cancelCtx)
	waitUntil(t, "the listener to be bound", func() bool {
		return slices.Contains(g.Acquired(), "udp-listener")
	})
	return cancelCtx, ch
}

// awaitRun waits for Run's result, bounded so a hung drain fails the test.
func awaitRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// sendUntil writes every payload to the graph's listener until cond holds.
// Loopback UDP may drop, so it repeats; each payload is idempotent for the
// counters the callers wait on.
func sendUntil(t *testing.T, g *Graph, payloads [][]byte, what string, cond func() bool) {
	t.Helper()
	conn, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialling the listener: %v", err)
	}
	defer conn.Close()
	for deadline := time.Now().Add(5 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		for _, p := range payloads {
			d := datagramOf(t, uncompressedSub(p))
			if _, err := conn.Write(d.Buf[:d.N]); err != nil {
				t.Fatalf("writing a datagram: %v", err)
			}
		}
		for i := 0; i < 10 && !cond(); i++ {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// commitAll is B-V3's every-row verdict: Committed, no error code.
func commitAll(b *model.Batch) {
	for i := range b.Active() {
		b.Rows[i].Outcome, b.Rows[i].ErrCode = model.OutcomeCommitted, 0
	}
}

// fakeWriter consumes batchQ in place of the real pool (a graph without a database) and obeys the shard
// side of the boundary: it writes only Outcome, ErrCode and Err, then returns
// the batch on completionCh[b.Shard] and never touches it again. committed
// counts the rows it reported Committed.
type fakeWriter struct {
	stop      chan struct{}
	done      chan struct{}
	committed atomic.Int64
	batches   atomic.Int64
}

// allCommitted starts a fake writer that commits every row of every batch.
func allCommitted(t *testing.T, g *Graph) *fakeWriter {
	return startFakeWriter(t, g, func(_ int64, b *model.Batch) { commitAll(b) })
}

// startFakeWriter runs adjudicate(n, b) on the n-th batch (from 1) it takes.
func startFakeWriter(t *testing.T, g *Graph, adjudicate func(n int64, b *model.Batch)) *fakeWriter {
	t.Helper()
	w := &fakeWriter{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			select {
			case <-w.stop:
				return
			case b, ok := <-g.batchQ:
				if !ok { // teardown step 9 closed it
					return
				}
				adjudicate(w.batches.Add(1), b)
				for i := range b.Active() {
					if b.Rows[i].Outcome == model.OutcomeCommitted {
						w.committed.Add(1)
					}
				}
				g.completionCh[b.Shard] <- b
			}
		}
	}()
	t.Cleanup(w.close)
	return w
}

func (w *fakeWriter) close() {
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	<-w.done
}

// abandoned is drain_abandoned_rows summed over the destinations. The
// {destination} handles are shared by every shard's set, so shard 0's are all.
func abandoned(g *Graph) uint64 {
	var n uint64
	for _, c := range g.inst().mirrorShards[0].DrainAbandonedRows {
		n += c.Load()
	}
	return n
}

// TestG_DRAIN_1_ShardHalf_TheDrainWritesEveryDirtyRow is the shard half of gate
// G-DRAIN-1.
//
// SETUP: four live shards, a fake writer committing every row, and a window of
// a minute so no tick flushes anything before shutdown. STIMULUS: datagrams
// dirtying two rows on every shard, then SIGTERM (context cancel). ASSERTION:
// Run returns nil; drain_abandoned_rows is 0 for every destination; the writer
// committed exactly the rows dirtied -- all of them by the drain, since nothing
// else could have opened an epoch -- and every shard logged a quiescent
// "shard drained", none a drain deadline.
//
// WOULD HAVE CAUGHT: a teardown that closes shardCh but does not wait for the
// shards (they would be abandoned mid-drain), a shard drain that returns on the
// channel close instead of at quiescence, and the discard consumer left in the
// built graph (nothing would ever be dirtied or committed -- the non-vacuity
// check below).
func TestG_DRAIN_1_ShardHalf_TheDrainWritesEveryDirtyRow(t *testing.T) {
	g, buf := wiringGraph(t, func(c *config.Config) { c.Flush.WindowMs = 60000 })
	cancel, done := runGraph(t, g)
	w := allCommitted(t, g)

	shards := g.deps.Cfg.Runtime.Shards
	var payloads [][]byte
	for _, toks := range tokensPerShard(shards, 2) {
		payloads = append(payloads, mbpPayloadTokens(toks...))
	}
	want := uint64(2 * shards)
	dirtied := g.inst().mirrorShards[0].RowsDirtied
	sendUntil(t, g, payloads, "every row to be dirtied", func() bool { return dirtied.Load() >= want })

	// Non-vacuity: the rows are dirty and NOT yet written, so whatever the
	// writer commits below, the drain wrote.
	if n := w.committed.Load(); n != 0 {
		t.Fatalf("the writer committed %d rows before shutdown with a one-minute window; "+
			"the drain would not be what wrote them", n)
	}

	start := time.Now()
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run = %v, want nil for a requested shutdown", err)
	}
	took := time.Since(start)

	if n := abandoned(g); n != 0 {
		t.Errorf("drain_abandoned_rows = %d, want 0: a drain with a working writer "+
			"abandoned rows\n%s", n, buf.String())
	}
	if got := uint64(w.committed.Load()); got != dirtied.Load() || got != want {
		t.Errorf("the writer committed %d rows; %d were dirtied (want %d) -- the drain did "+
			"not write every dirty row", got, dirtied.Load(), want)
	}
	log := buf.String()
	if n := strings.Count(log, "shard drained:"); n != shards {
		t.Errorf("%d shards logged a quiescent drain, want %d:\n%s", n, shards, log)
	}
	if strings.Contains(log, "drain deadline") {
		t.Errorf("a shard hit the drain deadline with a working writer:\n%s", log)
	}
	if took >= g.loadRuntime().Mirror.DrainTimeout {
		t.Errorf("the drain took %v, at or past drainTimeoutMs -- it ran to the deadline "+
			"rather than to quiescence", took)
	}
}

// TestG_DRAIN_2_ShardHalf_OneSharedDeadline is the shard half of gate G-DRAIN-2,
// and G-LIFE-4's shard half after it.
//
// SETUP: four shards, flush.maxBatchSize 1, a one-minute window, two rows per
// shard dirtied BEFORE any tick, then one tick sent by the test -- so each
// shard's single epoch holds both rows and sends m = 2 batches. Nothing
// consumes batchQ (this graph has no database, hence no pool; the real writers' version is the end-to-end G-DRAIN-2), so every shard holds both
// and cannot reach quiescence. drainTimeoutMs = 200. The decoder is then
// stalled through the Deps.Decode seam on one more datagram, and released
// stallMs = 400 after t_shutdown -- so the shards see their channels close
// 400 ms into the drain, after the deadline has already passed. STIMULUS:
// SIGTERM. ASSERTION: Run returns nil in about max(400, 200) = 400 ms, not
// 400 + 200. Then batchq_depth samples shards x m, and, with every shard
// returned and nobody reading, all m batches per shard are sent on
// completionCh[k] and none blocks.
//
// WOULD HAVE CAUGHT: the deadline armed when the shard channels close (in
// teardown, or by each shard as it sees its close) rather than at t_shutdown
// -- the pre-close stall would then be added to it, about 600 ms; a deadline
// that never fires (the test's own timeout); batchq_depth not sampled; and a
// completionCh sized below maxInFlightPerShard -- the writer pool would hang
// on a shard that had stopped reading, and the whole shutdown with it (Vol 2
// section 2.6).
//
// The tick is the test's, not the ticker's, so no tick can fall between the
// two rows of one datagram: an epoch opened over one row sends one batch that
// never completes, and the shard would never reach m.
func TestG_DRAIN_2_ShardHalf_OneSharedDeadline(t *testing.T) {
	const drainMs, stallMs = 200, 400

	var (
		g        *Graph
		rcv      *receiver
		stalling atomic.Bool
		stalled  atomic.Bool
		release  = make(chan struct{})
	)
	// The real decode path, except while stalling: then one datagram blocks
	// the decoder -- the stage BEFORE the shard channels close -- until
	// release. The receiver is built on the decoder goroutine, its only user.
	decode := func(d *model.Datagram) error {
		if stalling.Load() {
			stalled.Store(true)
			<-release
			return nil
		}
		if rcv == nil {
			rcv = newReceiver(g)
		}
		return rcv.decodeDatagram(d)
	}
	g, _ = testGraphWith(t, "127.0.0.1:0", func(c *config.Config) {
		configureTranscodes(t, c, allocGateCode)
		c.Flush.WindowMs = 60000
		c.Flush.MaxBatchSize = 1
		c.Flush.DrainTimeoutMs = drainMs
	}, decode)
	cancel, done := runGraph(t, g)

	shards := g.deps.Cfg.Runtime.Shards
	m := g.deps.Cfg.Runtime.MaxInFlightPerShard
	var payloads [][]byte
	for _, toks := range tokensPerShard(shards, m) {
		payloads = append(payloads, mbpPayloadTokens(toks...))
	}
	dirtied := g.inst().mirrorShards[0].RowsDirtied
	sendUntil(t, g, payloads, "every row to be dirtied",
		func() bool { return dirtied.Load() >= uint64(shards*m) })

	g.sendTicks()
	waitUntil(t, "every shard to hold m batches", func() bool {
		for k := range shards {
			if g.inst().mirrorShards[k].EpochBatches.Load() < uint64(m) {
				return false
			}
		}
		return true
	})

	stalling.Store(true)
	sendUntil(t, g, payloads[:1], "the decoder to stall", stalled.Load)

	start := time.Now()
	cancel()
	time.AfterFunc(stallMs*time.Millisecond, func() { close(release) })
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run = %v, want nil: a drain deadline is a designed outcome (ADR-L37)", err)
	}
	took := time.Since(start)
	t.Logf("drain with %d shards each holding %d batches, decoder stalled %d ms, took %v "+
		"(deadline %d ms)", shards, m, stallMs, took, drainMs)

	if took < stallMs*time.Millisecond {
		t.Errorf("Run returned after %v, before the %d ms decoder stall ended -- the drain "+
			"did not wait for the decoder", took, stallMs)
	}
	if limit := (stallMs + drainMs) * time.Millisecond; took >= limit {
		t.Errorf("the drain took %v, at or past stall + deadline = %v: the deadline was "+
			"armed when the shard channels closed, not at t_shutdown (Vol 4 section 1.5 "+
			"step 7, ruling D4)", took, limit)
	}

	// batchq_depth, sampled at emission: shards x m batches are queued and
	// nothing reads them.
	g.emitMu.Lock()
	g.sampleGauges()
	g.emitMu.Unlock()
	if got := g.inst().batchqDepth.Load(); got != int64(shards*m) {
		t.Errorf("batchq_depth = %d, want %d (len(batchQ) at emission)", got, shards*m)
	}

	// G-LIFE-4, shard half. Every shard has returned; nobody reads
	// completionCh. Each batch still out must fit.
	if n := len(g.batchQ); n != shards*m {
		t.Fatalf("batchQ holds %d batches, want %d (m per shard) -- the setup did not "+
			"reach every shard's in-flight limit", n, shards*m)
	}
	for range shards * m {
		b := <-g.batchQ
		commitAll(b)
		select {
		case g.completionCh[b.Shard] <- b:
		default:
			t.Fatalf("returning batch of shard %d blocked with the shard gone: completionCh "+
				"cap = %d, maxInFlightPerShard = %d", b.Shard, cap(g.completionCh[b.Shard]), m)
		}
	}
	for k := range shards {
		if c, l := cap(g.completionCh[k]), len(g.completionCh[k]); c != m || l != m {
			t.Errorf("completionCh[%d]: cap %d, len %d; want both %d", k, c, l, m)
		}
	}
}

// riggedRun runs a graph whose shards were rigged before Run and returns Run's
// error once a terminal condition has ended it.
func riggedRun(t *testing.T, g *Graph) error {
	t.Helper()
	_, done := runGraph(t, g)
	return awaitRun(t, done)
}

// TestShardFatalPathsMapPerD9 rigs each way a shard goroutine can end the
// process and checks the reason it reaches the exit (phase 3 ruling D9).
//
// SETUP: a live graph, one shard's rings rigged through their exported
// fields BEFORE Run (no goroutine yet, so no race), a 20 ms window so a tick
// opens the epoch. STIMULUS: the tick. ASSERTION: exit 4 with the reason and
// the counter named, and for the panic the stack in the log and
// panics_recovered{stage=shard}.
//
// WOULD HAVE CAUGHT: a recover missing from the shard goroutine (the process
// dies with exit 2 and no drain), F26 overloaded for a returned error, S1
// mapped to the generic invariant, and a shard that dies without closing
// shardDrainDone (teardown hangs: the test's own timeout).
func TestShardFatalPathsMapPerD9(t *testing.T) {
	tune := func(c *config.Config) { c.Flush.WindowMs = 20 }

	t.Run("panic is F26 with a stack", func(t *testing.T) {
		g, buf := wiringGraph(t, tune)
		// A slot past every arena chunk: fillRow's entry lookup panics with
		// the epoch half-built -- the arena-mid-mutation case F26 exists for.
		g.shards[0].Accum[0].PushBack(1 << 30)

		err := riggedRun(t, g)
		if got := ExitCode(err); got != ExitTerminal {
			t.Fatalf("exit code = %d (%v), want %d", got, err, ExitTerminal)
		}
		if !strings.Contains(err.Error(), FatalShardPanic.String()) {
			t.Errorf("the error names %v, want %v", err, FatalShardPanic)
		}
		log := buf.String()
		if !strings.Contains(log, "shard goroutine panicked") || !strings.Contains(log, "fillRow") {
			t.Errorf("the log has no panic line with a stack through fillRow:\n%s", log)
		}
		if n := g.inst().panicsRecoveredShard.Load(); n != 1 {
			t.Errorf("panics_recovered{stage=shard} = %d, want 1", n)
		}
	})

	t.Run("cohort_not_empty is FatalShardInvariant", func(t *testing.T) {
		g, _ := wiringGraph(t, tune)
		// A row left in the cohort, and work in the accumulator so the next
		// tick opens an epoch over it: HLD 7.12 risk 10's detector.
		g.shards[1].Cohort[0].PushBack(0)
		g.shards[1].Accum[0].PushBack(0)

		err := riggedRun(t, g)
		if got := ExitCode(err); got != ExitTerminal {
			t.Fatalf("exit code = %d (%v), want %d", got, err, ExitTerminal)
		}
		if !strings.Contains(err.Error(), FatalShardInvariant.String()) ||
			!strings.Contains(err.Error(), "counter=cohort_not_empty") {
			t.Errorf("the error is %v, want %v naming cohort_not_empty", err, FatalShardInvariant)
		}
		if n := g.inst().mirrorShards[1].CohortNotEmpty.Load(); n != 1 {
			t.Errorf("cohort_not_empty{1} = %d, want 1", n)
		}
		if n := g.inst().panicsRecoveredShard.Load(); n != 0 {
			t.Errorf("panics_recovered{stage=shard} = %d for a returned error, want 0", n)
		}
	})

	t.Run("S1 is FatalBatchQSendFail", func(t *testing.T) {
		g, _ := wiringGraph(t, tune)
		// batchQ full before any shard runs: the send section 7.4 proves
		// cannot fail, failing. Slot 0 is a real (zero) entry, so fillRow
		// succeeds and the send is what fails.
		for range cap(g.batchQ) {
			g.batchQ <- &model.Batch{}
		}
		g.shards[2].Accum[0].PushBack(0)

		err := riggedRun(t, g)
		if got := ExitCode(err); got != ExitTerminal {
			t.Fatalf("exit code = %d (%v), want %d", got, err, ExitTerminal)
		}
		if !strings.Contains(err.Error(), FatalBatchQSendFail.String()) ||
			!strings.Contains(err.Error(), "counter=flush_deferred") {
			t.Errorf("the error is %v, want %v naming flush_deferred", err, FatalBatchQSendFail)
		}
	})
}

// TestG_ROW_3_UnreportedRowIsF27AndTheShardDrains is the lifecycle half of
// G-ROW-3 under ruling D9.
//
// SETUP: a fake writer whose FIRST batch comes back with Err nil and row 0
// still Pending, and which commits every row afterwards; two rows on one
// shard. STIMULUS: the first tick's epoch. ASSERTION: exit 4 naming F27;
// batch_row_unreported is 1; and the shard did NOT stop -- it drained to
// quiescence, so the requeued row was written on the way down: the writer
// committed every dirtied row and nothing was abandoned.
//
// WOULD HAVE CAUGHT: S3 returned from Run like the other defects (the shard
// stops, the requeued row is never written, abandoned would be 0 only because
// the shard was no longer counting -- and "shard drained" would be missing),
// and S3 mapped to anything but F27.
func TestG_ROW_3_UnreportedRowIsF27AndTheShardDrains(t *testing.T) {
	g, buf := wiringGraph(t, func(c *config.Config) { c.Flush.WindowMs = 20 })
	_, done := runGraph(t, g)
	w := startFakeWriter(t, g, func(n int64, b *model.Batch) {
		commitAll(b)
		if n == 1 {
			b.Rows[0].Outcome = model.OutcomePending // never adjudicated, Err nil
		}
	})

	toks := tokensPerShard(g.deps.Cfg.Runtime.Shards, 2)[0]
	dirtied := g.inst().mirrorShards[0].RowsDirtied
	sendUntil(t, g, [][]byte{mbpPayloadTokens(toks...)}, "both rows to be dirtied",
		func() bool { return dirtied.Load() >= 2 })

	err := awaitRun(t, done)
	if got := ExitCode(err); got != ExitTerminal {
		t.Fatalf("exit code = %d (%v), want %d", got, err, ExitTerminal)
	}
	if !strings.Contains(err.Error(), FatalBatchRowUnreported.String()) {
		t.Errorf("the error names %v, want %v", err, FatalBatchRowUnreported)
	}
	var unreported uint64
	for k := range g.inst().mirrorShards {
		unreported += g.inst().mirrorShards[k].BatchRowUnreported.Load()
	}
	if unreported != 1 {
		t.Errorf("batch_row_unreported = %d, want 1", unreported)
	}
	if got := uint64(w.committed.Load()); got != dirtied.Load() {
		t.Errorf("the writer committed %d of %d dirtied rows: the requeued row was not "+
			"written by the drain\n%s", got, dirtied.Load(), buf.String())
	}
	if n := abandoned(g); n != 0 {
		t.Errorf("drain_abandoned_rows = %d, want 0", n)
	}
	if !strings.Contains(buf.String(), "shard drained: shard=0") {
		t.Errorf("shard 0 did not drain to quiescence after S3:\n%s", buf.String())
	}
}

// TestG_ORD_2_TheGuardEvaluatorRunsInTheGraph is G-ORD-2's lifecycle half: the
// stats goroutine's second ticker drives EvaluateGuards on a running graph.
//
// SETUP: two configured transcodes; the evaluation interval Build derived
// overwritten to 50 ms before Run (ordering.guardEvalIntervalSec has a 1 s
// floor, so this is the only way to reach 50 ms -- no new key, YAGNI). The
// stats interval stays at 60 s, so nothing but the guard ticker can act.
// STIMULUS: transcode 0's guard counters show every comparison superseded;
// transcode 1's none, both past guardMinSamples. These are the handles the
// shards' merges add to; the per-unit counting is P3-07's and the decision
// P3-12's, so what is under test here is the wiring. ASSERTION: within a second,
// ordering_guard_disabled is 1 for transcode 0 only, with its ERROR logged.
//
// WOULD HAVE CAUGHT: no guard ticker (nothing ever disables in a running
// graph), the evaluator run on the 60 s stats tick, and Params not reaching it
// through Runtime (a nil dereference on the stats goroutine).
func TestG_ORD_2_TheGuardEvaluatorRunsInTheGraph(t *testing.T) {
	g, buf := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		configureTranscodes(t, c, 7208, 7305)
	})
	if got := g.loadRuntime().Mirror.GuardEvalInterval; got != 5*time.Second {
		t.Fatalf("Build derived a guard interval of %v, want the configured 5 s", got)
	}
	g.guardEvalOverride = 50 * time.Millisecond

	mt := &g.inst().mirrorTranscode
	samples := uint64(g.loadRuntime().Mirror.GuardMinSamples) * 2
	mt.GuardOffered[0].Add(samples)
	mt.GuardSuperseded[0].Add(samples)
	mt.GuardOffered[1].Add(samples)

	cancel, done := runGraph(t, g)
	deadline := time.Now().Add(time.Second)
	for mt.GuardDisabled[0].Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("transcode 7208's guard did not disable within a second of a 50 ms "+
				"evaluation interval:\n%s", buf.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if v := mt.GuardDisabled[1].Load(); v != 0 {
		t.Errorf("ordering_guard_disabled{7305} = %d, want 0: the healthy transcode was "+
			"switched off too", v)
	}
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run = %v", err)
	}
	// Read after Run has joined the stats goroutine: the evaluator stores the
	// gauge BEFORE it logs, so the line may trail the poll above.
	if !strings.Contains(buf.String(), "ordering guard disabled") {
		t.Errorf("no guard_disabled ERROR was logged:\n%s", buf.String())
	}
}

// TestFlushTickerNeverBlocks is A13's: the ticker's send is non-blocking and a
// full tickCh[k] is counted against k, never waited for (Vol 2 section 2.4).
//
// SETUP: a built graph, nothing running, tickCh[0] already holding a tick (a
// shard that has not returned to its select). STIMULUS: one tick. ASSERTION:
// the tick returns, tick_send_dropped{0} is 1 and the other shards' are 0 with
// their ticks delivered. Non-vacuity: a second tick drops for every shard.
//
// WOULD HAVE CAUGHT: a blocking send (the tick never returns: one slow shard
// stops every shard's ticks) and a drop counted against the wrong shard.
func TestFlushTickerNeverBlocks(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", nil)
	g.tickCh[0] <- struct{}{}

	tick := func() {
		ret := make(chan struct{})
		go func() { g.sendTicks(); close(ret) }()
		select {
		case <-ret:
		case <-time.After(5 * time.Second):
			t.Fatal("the flush ticker blocked on a full tickCh")
		}
	}

	tick()
	for k := range g.tickCh {
		want := uint64(0)
		if k == 0 {
			want = 1
		}
		if got := g.inst().mirrorShards[k].TickSendDropped.Load(); got != want {
			t.Errorf("tick_send_dropped{%d} = %d, want %d", k, got, want)
		}
		if n := len(g.tickCh[k]); n != 1 {
			t.Errorf("tickCh[%d] holds %d ticks, want 1", k, n)
		}
	}

	tick()
	for k := range g.tickCh {
		if got := g.inst().mirrorShards[k].TickSendDropped.Load(); got < 1 {
			t.Errorf("tick_send_dropped{%d} = %d after a tick into a full channel", k, got)
		}
	}
}

// TestBuildWiresTheMirror pins the step-12 shapes Vol 2 makes load-bearing,
// and the eight fatal reasons G-LIFE-5 loops over.
func TestBuildWiresTheMirror(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", nil)
	cfg := g.deps.Cfg
	n, m := cfg.Runtime.Shards, cfg.Runtime.MaxInFlightPerShard

	if len(g.shards) != n {
		t.Errorf("%d shards built, want %d", len(g.shards), n)
	}
	if cap(g.batchQ) != n*m {
		t.Errorf("batchQ cap = %d, want shards x maxInFlightPerShard = %d", cap(g.batchQ), n*m)
	}
	for k := range n {
		if c := cap(g.completionCh[k]); c != m {
			t.Errorf("completionCh[%d] cap = %d, want maxInFlightPerShard = %d", k, c, m)
		}
		if c := cap(g.tickCh[k]); c != 1 {
			t.Errorf("tickCh[%d] cap = %d, want 1", k, c)
		}
	}
	p := g.loadRuntime().Mirror
	if p == nil || p.Gen != cfg.Gen || p.Window != cfg.Window() || p.DrainTimeout != cfg.DrainTimeout() {
		t.Errorf("Runtime.Mirror = %+v does not project the configuration (gen %d)", p, cfg.Gen)
	}
	if numFatalReasons != 8 {
		t.Errorf("numFatalReasons = %d, want 8 (ruling D9: six, plus F27 and FatalShardInvariant)",
			numFatalReasons)
	}
	seen := map[string]bool{}
	for r := FatalReason(0); r < numFatalReasons; r++ {
		if s := r.String(); s == "" || s == "unknown-terminal-condition" || seen[s] {
			t.Errorf("FatalReason %d has name %q", r, s)
		} else {
			seen[s] = true
		}
	}
}

// TestFatalDuringRequestedDrainIsExit4 is ruling D9 meeting a SIGTERM: a
// terminal condition raised INSIDE a requested drain still reaches the exit
// code.
//
// SETUP: a one-minute window, so nothing is written before shutdown; a fake
// writer whose FIRST batch comes back with row 0 never adjudicated (S3), and
// which commits everything afterwards; two rows dirtied on shard 0. STIMULUS:
// SIGTERM -- the drain's own epoch is the first, so F27 fires during the drain,
// after wait() has already returned on ctx.Done(). ASSERTION: Run returns exit
// 4 naming F27, not nil; the log says the shutdown began as a signal and was
// escalated; and the shard still drained the requeued row.
//
// WOULD HAVE CAUGHT: Run deciding the exit code from wait() alone -- the F27 is
// logged and then lost, the process exits 0 and the log says reason=signal.
func TestFatalDuringRequestedDrainIsExit4(t *testing.T) {
	g, buf := wiringGraph(t, func(c *config.Config) { c.Flush.WindowMs = 60000 })
	cancel, done := runGraph(t, g)
	w := startFakeWriter(t, g, func(n int64, b *model.Batch) {
		commitAll(b)
		if n == 1 {
			b.Rows[0].Outcome = model.OutcomePending // never adjudicated, Err nil
		}
	})

	toks := tokensPerShard(g.deps.Cfg.Runtime.Shards, 2)[0]
	dirtied := g.inst().mirrorShards[0].RowsDirtied
	sendUntil(t, g, [][]byte{mbpPayloadTokens(toks...)}, "both rows to be dirtied",
		func() bool { return dirtied.Load() >= 2 })
	if n := w.batches.Load(); n != 0 {
		t.Fatalf("the writer took %d batches before shutdown with a one-minute window; F27 "+
			"would not be raised inside the drain", n)
	}

	cancel()
	err := awaitRun(t, done)
	if got := ExitCode(err); got != ExitTerminal {
		t.Fatalf("exit code = %d (%v), want %d: a terminal condition raised during a "+
			"requested drain was lost\n%s", got, err, ExitTerminal, buf.String())
	}
	if !strings.Contains(err.Error(), FatalBatchRowUnreported.String()) {
		t.Errorf("the error names %v, want %v", err, FatalBatchRowUnreported)
	}
	log := buf.String()
	for _, want := range []string{
		"shutdown initiated reason=signal",
		"shutdown escalated reason=" + FatalBatchRowUnreported.String(),
		"shard drained: shard=0",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("the log does not carry %q:\n%s", want, log)
		}
	}
	if got := uint64(w.committed.Load()); got != dirtied.Load() {
		t.Errorf("the writer committed %d of %d dirtied rows", got, dirtied.Load())
	}
}

// TestDrainDeadlineIsArmedAtTShutdown pins WHERE the shared deadline is armed:
// in logShutdownInitiated, at t_shutdown (Vol 4 section 1.5 step 7, ruling D4).
//
// SETUP: a built graph that never runs, drainTimeoutMs = 50. STIMULUS:
// logShutdownInitiated alone -- no teardown. ASSERTION: drainExpired closes,
// no sooner than the timeout after the call.
//
// WOULD HAVE CAUGHT: the arm removed from logShutdownInitiated. teardown arms
// it too, for the error paths that never log the line, and on the normal path
// does so only milliseconds later -- so an end-to-end drain without a
// pre-close stall cannot tell the two apart. This calls only the first.
func TestDrainDeadlineIsArmedAtTShutdown(t *testing.T) {
	const drainMs = 50
	g, _ := testGraph(t, "127.0.0.1:0", func(c *config.Config) { c.Flush.DrainTimeoutMs = drainMs })

	start := time.Now()
	g.logShutdownInitiated(fatalEvent{}, false)
	select {
	case <-g.drainExpired:
	case <-time.After(5 * time.Second):
		t.Fatal("drainExpired never closed: logShutdownInitiated did not arm the drain deadline")
	}
	if took := time.Since(start); took < drainMs*time.Millisecond {
		t.Errorf("drainExpired closed %v after t_shutdown, before the %d ms timeout", took, drainMs)
	}
}

// TestFlushTickerRereadsItsPeriod is Vol 4 section 2.4's ticker row: the
// ticker picks up a new flush.windowMs "at each tick, to reset its period".
//
// SETUP: a built graph, the ticker alone at a 20 ms window. STIMULUS: a new
// Runtime published with a one-hour window. ASSERTION: within the next 400 ms
// at most two more ticks arrive -- one possibly already buffered, and the one
// on which the ticker notices -- rather than about twenty.
//
// WOULD HAVE CAUGHT: a ticker that reads its period once at start, the
// behaviour section 2.4 gives the stats goroutine and not this one.
func TestFlushTickerRereadsItsPeriod(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", func(c *config.Config) { c.Flush.WindowMs = 20 })
	g.startFlushTicker()
	t.Cleanup(g.stopFlushTicker)

	select {
	case <-g.tickCh[0]:
	case <-time.After(5 * time.Second):
		t.Fatal("the ticker never ticked at a 20 ms window")
	}

	old := g.loadRuntime()
	params := *old.Mirror
	params.Window = time.Hour
	rt := *old
	rt.Mirror = &params
	g.setRuntime(&rt)

	ticks := 0
	for end := time.After(400 * time.Millisecond); ; {
		select {
		case <-g.tickCh[0]:
			ticks++
			continue
		case <-end:
		}
		break
	}
	if ticks > 2 {
		t.Errorf("%d ticks arrived in 400 ms after the window became one hour: the ticker "+
			"did not reset its period at a tick (Vol 4 section 2.4)", ticks)
	}
}

// TestParseGuardMapsEveryMode pins ordering.guard's projection. GuardAuto is
// the enum's zero value, so a swapped or missing case shows no obvious garbage
// value -- and "on" silently becoming off bypasses the guard.
func TestParseGuardMapsEveryMode(t *testing.T) {
	if mirror.GuardOn == mirror.GuardOff || mirror.GuardOn == mirror.GuardAuto ||
		mirror.GuardOff == mirror.GuardAuto {
		t.Fatal("the three guard modes are not distinct, so the mapping below proves nothing")
	}
	for in, want := range map[string]mirror.GuardMode{
		"on":   mirror.GuardOn,
		"off":  mirror.GuardOff,
		"auto": mirror.GuardAuto,
	} {
		if got := parseGuard(in); got != want {
			t.Errorf("parseGuard(%q) = %d, want %d", in, got, want)
		}
	}
}

// TestMirrorParamsProjectsEveryField compares mirrorParams against a Params
// written out by hand from a configuration where every projected key is off
// its default -- so a field dropped, swapped or converted in the wrong unit
// shows, which a check of three fields could not.
func TestMirrorParamsProjectsEveryField(t *testing.T) {
	cfg := config.Defaults()
	cfg.Gen = 7
	cfg.Flush.WindowMs = 123
	cfg.Flush.MaxBatchSize = 77
	cfg.Flush.EpochStallWarnMs = 4321
	cfg.Flush.DrainTimeoutMs = 9876
	cfg.Ordering.Guard = "on"
	cfg.Ordering.GuardSuppressionRatio = 0.375
	cfg.Ordering.GuardMinSamples = 555
	cfg.Ordering.GuardEvalIntervalSec = 13

	want := mirror.Params{
		Gen:                   7,
		Window:                123 * time.Millisecond,
		MaxBatchSize:          77,
		EpochStallWarn:        4321 * time.Millisecond,
		DrainTimeout:          9876 * time.Millisecond,
		Guard:                 mirror.GuardOn,
		GuardSuppressionRatio: 0.375,
		GuardMinSamples:       555,
		GuardEvalInterval:     13 * time.Second,
	}
	def := config.Defaults()
	for _, same := range []struct {
		name string
		eq   bool
	}{
		{"flush.windowMs", def.Flush.WindowMs == cfg.Flush.WindowMs},
		{"flush.maxBatchSize", def.Flush.MaxBatchSize == cfg.Flush.MaxBatchSize},
		{"flush.epochStallWarnMs", def.Flush.EpochStallWarnMs == cfg.Flush.EpochStallWarnMs},
		{"flush.drainTimeoutMs", def.Flush.DrainTimeoutMs == cfg.Flush.DrainTimeoutMs},
		{"ordering.guard", def.Ordering.Guard == cfg.Ordering.Guard},
		{"ordering.guardSuppressionRatio", def.Ordering.GuardSuppressionRatio == cfg.Ordering.GuardSuppressionRatio},
		{"ordering.guardMinSamples", def.Ordering.GuardMinSamples == cfg.Ordering.GuardMinSamples},
		{"ordering.guardEvalIntervalSec", def.Ordering.GuardEvalIntervalSec == cfg.Ordering.GuardEvalIntervalSec},
	} {
		if same.eq {
			t.Errorf("%s is at its default here, so a projection that ignored it would pass", same.name)
		}
	}
	if got := mirrorParams(cfg); *got != want {
		t.Errorf("mirrorParams =\n%+v\nwant\n%+v", *got, want)
	}
}

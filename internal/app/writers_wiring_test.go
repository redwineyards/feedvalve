package app

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
	"feedvalve/internal/writer"
)

// The P4-10 wiring tests: the writer pool inside a running graph. The pool's own
// behaviour is internal/writer's; what is tested here is that app connects it
// the way Vol 4 section 1.4 says -- handles before Freeze, started before the
// shards and the socket, only with a database, and a stop turned into exit 4.

// dbGraph is testGraph with a database: an oratest fake opened through
// oradb.OpenDriver and carried in Deps.DB, 7208 configured, a 20 ms window.
// Cleanup closes the pool (after the test's own cancel, LIFO) and the database.
func dbGraph(t *testing.T, tune func(*config.Config)) (*Graph, *syncBuf, *oratest.Fake) {
	t.Helper()
	t.Setenv(oradb.PasswordEnv, "")
	f := oratest.New()
	t.Cleanup(f.Close)

	cfg := config.Defaults()
	cfg.UDP.Listen = "127.0.0.1:0"
	cfg.Runtime.ExpectedRows = 1024
	cfg.Runtime.ArenaChunkRows = 256
	cfg.Flush.WindowMs = 20
	configureTranscodes(t, cfg, allocGateCode)
	if tune != nil {
		tune(cfg)
	}
	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	db, err := oradb.OpenDriver(context.Background(), oratest.DriverName, f.DSN(), oradb.DBSettings{
		Host: "h", Port: 1521, Service: "S", User: "u",
		ConnectTimeoutSec: 10, QueryTimeoutSec: 30, ConnMaxLifetimeMinutes: 30,
		MaxOpenConns: cfg.Writers.Count + 1, MaxIdleConns: cfg.Writers.Count + 1,
	})
	if err != nil {
		t.Fatalf("OpenDriver: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	buf := &syncBuf{}
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg, dests)
	reg.SetIntervalSec(cfg.Stats.IntervalSec)
	reg.Freeze()
	lg := obs.NewAppLogger(buf, obs.ParseLevel(cfg.Stats.LogLevel), 10000, reg)
	dbg := obs.NewDebugLogger(buf, cfg.Debug.TraceTranscodes, cfg.Debug.HexDump)
	g, err := Build(Deps{
		Cfg: cfg, Store: storeOf(cfg), DB: db, Reg: reg,
		Log: lg, Debug: dbg, Dests: dests, Stderr: buf, Inst: inst,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// The pool is not part of Run's teardown until P4-11, so the test closes it.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if g.pool != nil {
			if err := g.pool.Close(ctx); err != nil {
				t.Errorf("closing the writer pool: %v", err)
			}
		}
		if g.poolCancel != nil {
			g.poolCancel()
		}
	})
	return g, buf, f
}

// feedUntilDone sends fresh-token 7208 datagrams (new rows every time, so each
// flush has something to write) until done delivers Run's result.
func feedUntilDone(t *testing.T, g *Graph, done <-chan error) error {
	t.Helper()
	var tok atomic.Uint32
	tok.Store(100000)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			return err
		default:
		}
		n := tok.Add(2)
		sendOnce(t, g, mbpPayloadTokens(n, n+1))
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Run did not end")
	return nil
}

// TestG_OBS_1_WriterHandlesAreResolvedBeforeFreeze: every handle the pool
// writes comes from resolveInstruments, which runs before reg.Freeze.
//
// SETUP: the default configuration (3 destinations). STIMULUS:
// resolveInstruments, then Freeze, as run.go does. ASSERTION: the writer set,
// its run set and each pool-level handle are non-nil; the per-destination
// handles are non-nil for every destination. Non-vacuity: the same resolution
// on the frozen registry panics, so the check cannot pass by the resolver
// having become lazy.
//
// WOULD HAVE CAUGHT: a handle added to the writer and resolved in Build or
// Start (the first increment panics in production, inside a recover boundary);
// resolveInstruments forgetting the writer altogether (nil dereference on the
// first batch).
func TestG_OBS_1_WriterHandlesAreResolvedBeforeFreeze(t *testing.T) {
	cfg := config.Defaults()
	dests, err := destinations.Default()
	if err != nil {
		t.Fatal(err)
	}
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg, dests)
	reg.Freeze()

	w := inst.writer
	if w == nil || w.Run == nil {
		t.Fatalf("writer instruments not resolved: %+v", w)
	}
	for name, h := range map[string]bool{
		"WritersConfigured": w.WritersConfigured != nil, "WritersRunning": w.WritersRunning != nil,
		"WritersStopped": w.WritersStopped != nil, "HealthState": w.HealthState != nil,
		"WritesAttempted": w.WritesAttempted != nil, "WritesSucceeded": w.WritesSucceeded != nil,
		"WritesFailed": w.WritesFailed != nil, "BatchesWritten": w.BatchesWritten != nil,
		"OracleError": w.OracleError != nil, "Run.Panics": w.Run.Panics != nil,
		"Run.Busy": w.Run.Busy != nil, "Run.FallbackPerrow": w.Run.FallbackPerrow != nil,
	} {
		if !h {
			t.Errorf("%s is nil", name)
		}
	}
	for d := range dests.Len() {
		if w.Run.RowsWritten[d] == nil || w.Run.RowRejected[d] == nil || w.Run.RowsAffectedMismatch[d] == nil {
			t.Errorf("destination %d: a run handle is nil", d)
		}
		if w.BindNonfinite[d][0] == nil || w.BindBadTime[d][0] == nil {
			t.Errorf("destination %d: a bind handle is nil", d)
		}
	}
	for i := range w.ConnRecycled {
		if w.ConnRecycled[i] == nil {
			t.Errorf("ConnRecycled[%d] is nil", i)
		}
	}
	for i := range w.Probe {
		if w.Probe[i] == nil {
			t.Errorf("Probe[%d] is nil", i)
		}
	}
	for i := range w.WriteRetry {
		if w.WriteRetry[i] == nil {
			t.Errorf("WriteRetry[%d] is nil", i)
		}
	}

	defer func() {
		if recover() == nil {
			t.Error("resolving writer handles on a frozen registry did not panic; the check above is blind")
		}
	}()
	var late instruments
	late.resolveWriterInstruments(reg, dests.Len())
}

// TestG_OBS_3_WriterNamesAreEmitted: the writer's metrics reach the output
// under the names the operator's runbook uses.
//
// SETUP: a registry resolved by resolveInstruments. STIMULUS: every writer
// handle incremented once, one emission. ASSERTION: each name of a
// hand-written list appears. (The ledger-wide two-way parity is obs's own
// TestG_OBS_3; this pins that APP resolves the writer's half of it.)
//
// WOULD HAVE CAUGHT: a writer instrument resolved with a neighbour's ID (its
// own name never emitted), or left out of resolveWriterInstruments.
func TestG_OBS_3_WriterNamesAreEmitted(t *testing.T) {
	cfg := config.Defaults()
	dests, err := destinations.Default()
	if err != nil {
		t.Fatal(err)
	}
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg, dests)
	reg.Freeze()

	w := inst.writer
	w.WritersConfigured.Store(1)
	w.WritersRunning.Store(1)
	w.HealthState.Store(1)
	w.Run.Busy.Store(1)
	for _, c := range []*obs.Counter{w.WritersStopped, w.WritesAttempted, w.WritesSucceeded,
		w.WritesFailed, w.BatchesWritten, w.Run.Panics, w.Run.FallbackPerrow,
		w.ConnRecycled[0], w.Probe[0], w.WriteRetry[0], w.Run.RowsWritten[0],
		w.Run.RowRejected[0], w.Run.RowsAffectedMismatch[0], w.BindNonfinite[0][0], w.BindBadTime[0][0]} {
		c.Add(1)
	}
	w.OracleError(1)

	var out bytes.Buffer
	if err := reg.Emit(&out); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"writers_configured", "writers_running", "writers_stopped", "writer_health_state",
		"writers_busy", "writes_attempted", "writes_succeeded", "writes_failed",
		"batches_written", "rows_written", "row_rejected", "rows_affected_mismatch",
		"batch_fallback_perrow", "writer_conn_recycled", "write_probe", "write_retry",
		"oracle_error", "bind_nonfinite", "bind_bad_time",
		"stmt_cache_uncached", "batch_row_empty_mask", // stmt_cache_entries{writer} is sparse: absent at 0
	} {
		// Whole names: "rows_written" must not pass on "rows_written_total".
		if !emittedNames(out.String())[name] {
			t.Errorf("%s is not in the emission", name)
		}
	}
}

// emittedNames returns every metric name in an emission, label and value
// stripped: "write_retry{session}=0(+0)" gives "write_retry".
func emittedNames(emission string) map[string]bool {
	names := map[string]bool{}
	for _, line := range strings.Split(emission, "\n") {
		for _, f := range strings.Fields(line) {
			if eq := strings.IndexByte(f, '='); eq > 0 {
				name := f[:eq]
				if br := strings.IndexByte(name, '{'); br > 0 {
					name = name[:br]
				}
				names[name] = true
			}
		}
	}
	return names
}

// groupKeys returns the metric keys (label kept, value stripped) of one
// emission group line, in emission order.
func groupKeys(emission, group string) []string {
	for _, line := range strings.Split(emission, "\n") {
		_, rest, ok := strings.Cut(line, " group="+group)
		if !ok {
			continue
		}
		var keys []string
		for _, f := range strings.Fields(rest) {
			if eq := strings.IndexByte(f, '='); eq > 0 {
				keys = append(keys, f[:eq])
			}
		}
		return keys
	}
	return nil
}

// TestWriterMetricSetGolden pins the writer's emitted metric set and its order
// in the two groups it owns, against a hand-written list.
//
// SETUP: the default configuration (3 destinations), a registry resolved by
// resolveInstruments, oracle_error{1} bumped once (the sparse series appears
// only after its first increment). STIMULUS: one emission. ASSERTION: the keys
// of group=write_pool and group=writes equal the lists below, in order. A
// third check requires the lists to be non-empty, so an emission whose groups
// went missing cannot pass by both sides being empty.
//
// WOULD HAVE CAUGHT: a writer instrument dropped from or added to
// resolveWriterInstruments, resolved under another group, or reordered (the
// operator's grep-by-position tooling and the runbook table shift silently).
func TestWriterMetricSetGolden(t *testing.T) {
	cfg := config.Defaults()
	dests, err := destinations.Default()
	if err != nil {
		t.Fatal(err)
	}
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg, dests)
	reg.Freeze()
	inst.writer.OracleError(1)

	var out bytes.Buffer
	if err := reg.Emit(&out); err != nil {
		t.Fatal(err)
	}
	wantPool := []string{
		"writers_configured", "writers_busy", "writers_stopped",
		"rows_inflight{0}", "rows_inflight{1}", "rows_inflight{2}", "rows_inflight{3}",
		"redirty_total",
		"inflight_desync{0}", "inflight_desync{1}", "inflight_desync{2}", "inflight_desync{3}",
		"writers_running", "stmt_cache_uncached",
		"writer_conn_recycled{hold}", "writer_conn_recycled{idle}", "writer_conn_recycled{error}",
		"write_probe{ok}", "write_probe{fail}", "writer_health_state",
	}
	wantWrites := []string{
		"writes_attempted", "writes_succeeded", "writes_failed", "batches_written",
		"rows_written{MD_TOKEN_QUOTE}", "rows_written{MD_SECURITY_MASTER}", "rows_written{MD_MARKET_STATUS}",
		"row_rejected{MD_TOKEN_QUOTE}", "row_rejected{MD_SECURITY_MASTER}", "row_rejected{MD_MARKET_STATUS}",
		"write_retry{session}", "write_retry{deadlock}", "write_retry{space}", "write_retry{unknown}",
		"commit_duration_max_ns", "writer_batch_duration_max_ns",
		"array_bind_rows_max{MD_TOKEN_QUOTE}", "array_bind_rows_max{MD_SECURITY_MASTER}", "array_bind_rows_max{MD_MARKET_STATUS}",
		"batch_fallback_perrow",
		"rows_affected_mismatch{MD_TOKEN_QUOTE}", "rows_affected_mismatch{MD_SECURITY_MASTER}", "rows_affected_mismatch{MD_MARKET_STATUS}",
		"batch_row_empty_mask{MD_TOKEN_QUOTE}", "batch_row_empty_mask{MD_SECURITY_MASTER}", "batch_row_empty_mask{MD_MARKET_STATUS}",
		"oracle_error{1}",
	}
	for group, want := range map[string][]string{"write_pool": wantPool, "writes": wantWrites} {
		got := groupKeys(out.String(), group)
		if len(got) == 0 {
			t.Errorf("group %s is missing from the emission", group)
			continue
		}
		if !slices.Equal(got, want) {
			t.Errorf("group %s keys differ\n got: %q\nwant: %q", group, got, want)
		}
	}
}

// TestPoolExistsOnlyWithADatabase: Build allocates a pool iff Deps.DB is set,
// and starts nothing either way.
//
// WOULD HAVE CAUGHT: a stub writer in the no-database graph (the fake writers
// of the shard tests would then race it for batchQ), and a pool started in
// Build (the G-LIFE-3 unwind would have goroutines to stop).
func TestPoolExistsOnlyWithADatabase(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", nil)
	if g.pool != nil {
		t.Error("a graph without a database built a pool")
	}
	if err := g.startWriters(); err != nil || slices.Contains(g.Acquired(), "writers") {
		t.Errorf("startWriters with no pool: err=%v acquired=%v", err, g.Acquired())
	}

	g2, _, _ := dbGraph(t, nil)
	if g2.pool == nil {
		t.Fatal("a graph with a database built no pool")
	}
	if slices.Contains(g2.Acquired(), "writers") {
		t.Errorf("Build started the pool: %v", g2.Acquired())
	}
	select {
	case <-g2.pool.Stopped():
		t.Error("a pool that was never started reports Stopped")
	default:
	}
}

// TestG_LIFE_1_WritersStartBeforeShardsAndSocket is the writer part of G-LIFE-1.
//
// SETUP: a graph with a database. STIMULUS: Run, then cancel. ASSERTION: the
// recorded stage marks put "writers" before "shard-consumers" before
// "udp-listener". Non-vacuity: the same ordering check rejects the reversed
// slice.
//
// WOULD HAVE CAUGHT: startWriters placed after startShards (a first batch with
// no consumer) or after the bind (a datagram accepted with nowhere to write).
func TestG_LIFE_1_WritersStartBeforeShardsAndSocket(t *testing.T) {
	ordered := func(s []string) bool {
		w, sh, u := slices.Index(s, "writers"), slices.Index(s, "shard-consumers"), slices.Index(s, "udp-listener")
		return w >= 0 && w < sh && sh < u
	}
	if ordered([]string{"udp-listener", "shard-consumers", "writers"}) {
		t.Fatal("the ordering check accepts the reversed order")
	}
	g, _, _ := dbGraph(t, nil)
	cancel, done := runGraph(t, g)
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !ordered(g.Acquired()) {
		t.Errorf("acquisition order = %v", g.Acquired())
	}
}

// TestG_ROW_3_RealPool_ShardToWriterToCompletion: batches flow from the shards
// through the real pool over oratest and back, every row adjudicated.
//
// SETUP: a graph with an oratest database, 2 writers, a 20 ms window.
// STIMULUS: datagrams dirtying two rows on every shard (a repeat
// re-dirties, so rows_dirtied outruns rows_written and is no yardstick), then
// wait until rows_written covers all eight, then SIGTERM. ASSERTION: Run
// returns nil; batch_row_unreported is 0 on every shard; abandoned rows are 0;
// the fake saw at least one commit and batches_written > 0. Non-vacuity: rows
// were actually dirtied and the fake actually executed statements.
//
// WOULD HAVE CAUGHT: completion channels converted from the wrong slice (a
// batch returned to another shard's channel), Start called with the wrong
// batchQ, and a pool whose writers never adjudicate a row (F27 and the
// unreported counter).
func TestG_ROW_3_RealPool_ShardToWriterToCompletion(t *testing.T) {
	g, buf, f := dbGraph(t, func(c *config.Config) { c.Writers.Count = 2 })
	cancel, done := runGraph(t, g)

	shards := g.deps.Cfg.Runtime.Shards
	var payloads [][]byte
	for _, toks := range tokensPerShard(shards, 2) {
		payloads = append(payloads, mbpPayloadTokens(toks...))
	}
	rowsWritten := func() (n uint64) {
		for _, c := range g.inst().writer.Run.RowsWritten {
			if c != nil {
				n += c.Load()
			}
		}
		return n
	}
	dirtied := func() (n uint64) {
		for k := range g.inst().mirrorShards {
			n += g.inst().mirrorShards[k].RowsDirtied.Load()
		}
		return n
	}
	defer func() {
		if t.Failed() {
			w := g.inst().writer
			t.Logf("attempted=%d ok=%d failed=%d written=%d commits=%d rollbacks=%d", w.WritesAttempted.Load(), w.WritesSucceeded.Load(), w.WritesFailed.Load(), w.BatchesWritten.Load(), f.Counts().Commits, f.Counts().Rollbacks)
			t.Logf("rows dirtied=%d written=%d\n%s", dirtied(), rowsWritten(), buf.String())
		}
	}()
	sendUntil(t, g, payloads, "the two rows per shard to be written",
		func() bool { return rowsWritten() >= uint64(2*shards) })
	// The pool's own gauges, set by the real pool and not by the test: a pool
	// that never updated them would pass every other check here.
	if got := g.inst().writer.WritersConfigured.Load(); got != 2 {
		t.Errorf("writers_configured = %d, want 2", got)
	}
	if got := g.inst().writer.WritersRunning.Load(); got < 1 {
		t.Errorf("writers_running = %d while Run is live, want >= 1", got)
	}
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run: %v\n%s", err, buf.String())
	}

	if dirtied() == 0 || f.Counts().Commits == 0 {
		t.Fatalf("vacuous: dirtied=%d commits=%d", dirtied(), f.Counts().Commits)
	}
	for k := range g.inst().mirrorShards {
		if n := g.inst().mirrorShards[k].BatchRowUnreported.Load(); n != 0 {
			t.Errorf("shard %d: batch_row_unreported = %d, want 0", k, n)
		}
	}
	if n := abandoned(g); n != 0 {
		t.Errorf("drain_abandoned_rows = %d, want 0", n)
	}
	if g.inst().writer.BatchesWritten.Load() == 0 {
		t.Error("batches_written = 0")
	}
}

// TestG_STMT_1_EndToEnd_StatementFailureIsF20Exit4: every statement fails with
// ORA-00942 on a pool of three writers.
//
// SETUP: oratest scripted to fail any statement (not session operations) with
// ORA-00942, writers.count 3, statementFailWriters 2. STIMULUS: fresh rows until
// the pool stops. ASSERTION: Run returns an ExitError with code 4 that names
// F20 and not F13.
//
// WOULD HAVE CAUGHT: the watcher not started (Run never ends), StopStatement
// mapped to F13, and the pool stop not reaching Fatal (exit 0 on a dead
// writer pool).
func TestG_STMT_1_EndToEnd_StatementFailureIsF20Exit4(t *testing.T) {
	g, _, f := dbGraph(t, func(c *config.Config) {
		c.Writers.Count = 3
		c.Writers.StatementFailWriters = 2
		c.Writers.RetryInitialMs, c.Writers.RetryMaxMs = 5, 10
	})
	f.SetScript(func(c oratest.Call) oratest.Result {
		if strings.HasPrefix(c.SQL, "<") {
			return oratest.Result{}
		}
		return oratest.Result{Err: oratest.ORA(942)}
	})
	_, done := runGraph(t, g)
	err := feedUntilDone(t, g, done)
	if got := ExitCode(err); got != ExitTerminal {
		t.Fatalf("exit code = %d (%v), want %d", got, err, ExitTerminal)
	}
	if !strings.Contains(err.Error(), FatalWriterPoolStop.String()) ||
		strings.Contains(err.Error(), FatalDBFailFast.String()) {
		t.Errorf("error = %v, want %v", err, FatalWriterPoolStop)
	}
	var se *writer.StopError
	if !errors.As(g.pool.StopReason(), &se) || se.Kind != writer.StopStatement {
		t.Errorf("pool stop reason = %v, want a statement stop", g.pool.StopReason())
	}
}

// TestG_STMT_1_EndToEnd_DeadDatabaseIsF13Exit4: every session operation fails.
//
// SETUP: the fake opened healthy, then scripted so every operation fails with
// ORA-03113 and drops the connection; writers.failFastSeconds 1 (the pool's
// clock is real; this is the smallest configurable window, standing for the
// design's 30 s), the count threshold set out of reach so only the clock can
// fire. STIMULUS: fresh rows. ASSERTION: exit 4 naming F13, pool stop kind
// fail-fast.
//
// WOULD HAVE CAUGHT: FailFast mapped to F20, the time rule not evaluated while
// the writers sit in backoff, and an unreachable database leaving Run
// running forever.
func TestG_STMT_1_EndToEnd_DeadDatabaseIsF13Exit4(t *testing.T) {
	g, _, f := dbGraph(t, func(c *config.Config) {
		c.Writers.Count = 2
		c.Writers.FailFastSeconds = 1
		c.Writers.FailFastWrites = 1 << 30
		c.Writers.RetryInitialMs, c.Writers.RetryMaxMs = 5, 20
	})
	f.SetScript(func(oratest.Call) oratest.Result {
		return oratest.Result{Err: oratest.ORA(3113), Drop: true}
	})
	_, done := runGraph(t, g)
	err := feedUntilDone(t, g, done)
	if got := ExitCode(err); got != ExitTerminal {
		t.Fatalf("exit code = %d (%v), want %d", got, err, ExitTerminal)
	}
	if !strings.Contains(err.Error(), FatalDBFailFast.String()) {
		t.Errorf("error = %v, want %v", err, FatalDBFailFast)
	}
	var se *writer.StopError
	if !errors.As(g.pool.StopReason(), &se) || se.Kind != writer.StopFailFast {
		t.Errorf("pool stop reason = %v, want fail-fast", g.pool.StopReason())
	}
}

// TestPoolStopReasonMapping pins the D3 mapping with a hand-written table.
func TestPoolStopReasonMapping(t *testing.T) {
	for _, c := range []struct {
		err  error
		want FatalReason
	}{
		{&writer.StopError{Kind: writer.StopFailFast}, FatalDBFailFast},
		{&writer.StopError{Kind: writer.StopStatement, Identity: "d0/m1"}, FatalWriterPoolStop},
		{&writer.StopError{Kind: writer.StopNoWriters}, FatalWriterPoolStop},
		{nil, FatalWriterPoolStop},
	} {
		if got, _ := poolStopReason(c.err); got != c.want {
			t.Errorf("poolStopReason(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

// TestWriterParamsProjectsEveryField: unit conversions, from distinct values.
func TestWriterParamsProjectsEveryField(t *testing.T) {
	cfg := config.Defaults()
	cfg.DB.QueryTimeoutSec, cfg.DB.ConnMaxLifetimeMinutes = 7, 11
	w := &cfg.Writers
	w.RetryInitialMs, w.RetryMaxMs, w.ConnIdleReleaseSec, w.StmtCacheSize = 13, 17, 19, 23
	w.FailFastSeconds, w.FailFastWrites, w.StatementFailWriters, w.StatementFailWindowSec = 29, 31, 37, 41
	cfg.Runtime.Time1980UnixOffsetSec = 315552600
	want := writer.Params{
		Time1980UnixOffsetSec: 315552600,
		QueryTimeout: 7 * time.Second, RetryInitial: 13 * time.Millisecond, RetryMax: 17 * time.Millisecond,
		ConnIdleRelease: 19 * time.Second, ConnHold: 11 * time.Minute, StmtCacheSize: 23,
		FailFastWindow: 29 * time.Second, FailFastWrites: 31, StatementFailWriters: 37,
		StatementFailWindow: 41 * time.Second,
	}
	if got := *writerParams(cfg); got != want {
		t.Errorf("writerParams = %+v, want %+v", got, want)
	}
}

// sendOnce writes one payload to the graph's listener as a datagram.
func sendOnce(t *testing.T, g *Graph, payload []byte) {
	t.Helper()
	conn, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialling the listener: %v", err)
	}
	defer conn.Close()
	d := datagramOf(t, uncompressedSub(payload))
	if _, err := conn.Write(d.Buf[:d.N]); err != nil {
		t.Fatalf("writing a datagram: %v", err)
	}
}

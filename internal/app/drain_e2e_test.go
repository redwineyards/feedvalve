package app

import (
	"context"
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
)

// The P4-11 end-to-end gates: the whole graph (decoder, shards, the real writer
// pool) over a healthy or stalled oratest database, through SIGTERM.

// perDest copies a per-destination counter set (shared by every shard and
// writer) into a fixed array.
func perDest(counters []*obs.Counter) (out [3]uint64) {
	for d, c := range counters {
		if c != nil && d < len(out) {
			out[d] = c.Load()
		}
	}
	return out
}

// writtenByDest is rows_written{destination}, shared by every writer.
func writtenByDest(g *Graph) [3]uint64 {
	return perDest(g.inst().writer.Run.RowsWritten[:])
}

// createdByDest is rows_created{destination}: the distinct rows the shards have
// ever dirtied, per destination. The handles are shared by every shard.
func createdByDest(g *Graph) [3]uint64 {
	return perDest(g.inst().mirrorShards[0].RowsCreated[:])
}

// abandonedByDest is drain_abandoned_rows{destination}.
func abandonedByDest(g *Graph) [3]uint64 {
	return perDest(g.inst().mirrorShards[0].DrainAbandonedRows[:])
}

// heldUDPAddr returns a loopback UDP address that is already bound for the life
// of the test, so a second bind to it fails deterministically (EADDRINUSE).
func heldUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("holding a UDP port: %v", err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().String()
}

func sum3(a [3]uint64) uint64 { return a[0] + a[1] + a[2] }

// stallStatements makes every statement (not the session operations, whose
// keys start with '<') wait an hour, honouring the caller's context. It counts
// the statements that began.
func stallStatements(f *oratest.Fake) *atomic.Int64 {
	var started atomic.Int64
	f.SetScript(func(c oratest.Call) oratest.Result {
		if strings.HasPrefix(c.SQL, "<") {
			return oratest.Result{}
		}
		started.Add(1)
		return oratest.Result{Delay: time.Hour}
	})
	return &started
}

// dirtyRows sends per fresh rows to every shard (tokens from first on) and
// waits until every one has been created.
func dirtyRows(t *testing.T, g *Graph, per int, first uint32) {
	t.Helper()
	shards := g.deps.Cfg.Runtime.Shards
	before := sum3(createdByDest(g))
	var payloads [][]byte
	for _, toks := range tokensPerShardFrom(shards, per, first) {
		payloads = append(payloads, mbpPayloadTokens(toks...))
	}
	sendUntil(t, g, payloads, "every row to be created", func() bool {
		// 7208 feeds one destination, one row per token: per*shards new rows.
		return sum3(createdByDest(g)) >= before+uint64(per*shards)
	})
}

// tokensPerShardFrom is tokensPerShard starting its search at from, so a test
// can ask for rows it has not used before.
func tokensPerShardFrom(n, per int, from uint32) [][]uint32 {
	out := make([][]uint32, n)
	full := 0
	for tok := from; full < n; tok++ {
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

// TestG_DRAIN_1_EndToEnd_AHealthyDatabaseWritesEveryDirtyRow is gate G-DRAIN-1
// with the real writers.
//
// SETUP: a graph over a healthy oratest database, 2 writers, a one-minute
// window (no tick can flush before shutdown). STIMULUS: two rows dirtied on
// every shard, then SIGTERM. ASSERTION: Run returns nil; rows_written equals
// rows_created per destination (every distinct row was created once and written
// once); drain_abandoned_rows is 0; the fake committed; no drain-deadline line;
// the drain ended before drainTimeoutMs. Non-vacuity: nothing was written, and
// nothing committed, before SIGTERM, so the drain wrote every row.
//
// WOULD HAVE CAUGHT: batchQ never closed or never consumed during the drain (the
// shards wait to the deadline and abandon everything), the pool cancelled on
// SIGTERM (the drain's batches come back Failed), and a pool joined before the
// shards (the last batches have no consumer).
func TestG_DRAIN_1_EndToEnd_AHealthyDatabaseWritesEveryDirtyRow(t *testing.T) {
	g, buf, f := dbGraph(t, func(c *config.Config) {
		c.Writers.Count = 2
		c.Flush.WindowMs = 60000
	})
	cancel, done := runGraph(t, g)
	dirtyRows(t, g, 2, 90001)

	if sum3(writtenByDest(g)) != 0 || f.Counts().Commits != 0 {
		t.Fatalf("non-vacuity: %d rows written, %d commits before SIGTERM with a one-minute window",
			sum3(writtenByDest(g)), f.Counts().Commits)
	}
	created := createdByDest(g)
	if sum3(created) == 0 {
		t.Fatal("non-vacuity: no rows were created")
	}

	start := time.Now()
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run = %v, want nil\n%s", err, buf.String())
	}
	took := time.Since(start)

	if got := writtenByDest(g); got != created {
		t.Errorf("rows_written = %v, rows_created = %v: the drain did not write every row\n%s", got, created, buf.String())
	}
	if got := abandonedByDest(g); got != [3]uint64{} {
		t.Errorf("drain_abandoned_rows = %v, want zeros", got)
	}
	if f.Counts().Commits == 0 {
		t.Error("the fake saw no commit")
	}
	if strings.Contains(buf.String(), "drain deadline") {
		t.Errorf("a drain with a healthy database hit the deadline:\n%s", buf.String())
	}
	if took >= g.loadRuntime().Mirror.DrainTimeout {
		t.Errorf("the drain took %v, at or past drainTimeoutMs: it ran to the deadline", took)
	}
	if n := g.inst().writer.WritersRunning.Load(); n != 0 {
		t.Errorf("writers_running = %d after Run returned, want 0", n)
	}
}

// TestG_DRAIN_2_EndToEnd_AStalledWriterIsBoundedAndCounted is gate G-DRAIN-2
// with the real writers.
//
// SETUP: a healthy database, 2 writers, a one-minute window, drainTimeoutMs
// 400. STAGE 1: rows A dirtied, one tick, written. STAGE 2: every statement now
// stalls for an hour; rows B dirtied and flushed by a tick, so their batches
// are with a stalled writer; rows C dirtied after, so they sit on the
// accumulating rings. STIMULUS: SIGTERM. ASSERTION: Run returns nil within
// drainTimeoutMs plus a loose epsilon, having logged the drain deadline;
// drain_abandoned_rows{d} equals rows_created{d} - rows_written{d} for every
// destination, which is derived from counters the accounting does not write;
// and the total covers at least B and C. Non-vacuity: A was written, a
// statement was actually stalled, and B and C are non-zero.
//
// WOULD HAVE CAUGHT: an outstanding batch left out of the count (only C would
// be abandoned), a pool never cancelled at drainExpired (Run hangs on the
// writers: the stall is an hour), and a per-shard deadline.
func TestG_DRAIN_2_EndToEnd_AStalledWriterIsBoundedAndCounted(t *testing.T) {
	const drainMs = 400
	g, buf, f := dbGraph(t, func(c *config.Config) {
		c.Writers.Count = 2
		c.Flush.WindowMs = 60000
		c.Flush.DrainTimeoutMs = drainMs
	})
	cancel, done := runGraph(t, g)

	dirtyRows(t, g, 2, 90001) // A
	g.sendTicks()
	waitUntil(t, "rows A written", func() bool {
		w := sum3(writtenByDest(g))
		return w > 0 && w == sum3(createdByDest(g)) && g.inst().mirrorShards[0].RowsInflight.Load() == 0
	})
	a := sum3(writtenByDest(g))

	started := stallStatements(f)
	dirtyRows(t, g, 2, 200001) // B
	g.sendTicks()
	waitUntil(t, "a statement to stall", func() bool { return started.Load() > 0 })
	b := sum3(createdByDest(g)) - a
	dirtyRows(t, g, 2, 300001) // C
	c := sum3(createdByDest(g)) - a - b

	start := time.Now()
	cancel()
	err := awaitRun(t, done)
	took := time.Since(start)
	if err != nil {
		t.Fatalf("Run = %v, want nil: a drain deadline is a designed outcome\n%s", err, buf.String())
	}
	if limit := drainMs*time.Millisecond + 1500*time.Millisecond; took >= limit {
		t.Errorf("Run took %v after SIGTERM, drainTimeoutMs is %d: the drain was not bounded", took, drainMs)
	}
	if !strings.Contains(buf.String(), "drain deadline") {
		t.Errorf("no drain-deadline line: the stall did not reach the deadline\n%s", buf.String())
	}

	created, written, abandoned := createdByDest(g), writtenByDest(g), abandonedByDest(g)
	for d := range abandoned {
		if want := created[d] - written[d]; abandoned[d] != want {
			t.Errorf("drain_abandoned_rows{%d} = %d, want rows_created - rows_written = %d", d, abandoned[d], want)
		}
	}
	if a == 0 || b == 0 || c == 0 {
		t.Fatalf("non-vacuity: stage sizes A=%d B=%d C=%d", a, b, c)
	}
	// A was verified written before the stall and nothing is written after it,
	// so exactly the B and C rows are abandoned: no more, no fewer.
	if got := sum3(abandoned); got != b+c {
		t.Errorf("abandoned %d rows, want exactly %d (B %d in a stalled batch + C %d dirty)", got, b+c, b, c)
	}
}

// TestG_LIFE_4_EndToEnd_StalledWritersReturnTheirBatchesAndRunCompletes is gate
// G-LIFE-4 end to end.
//
// SETUP: every shard has maxInFlightPerShard batches (maxBatchSize 1) in front
// of a stalled writer pool (3 writers, so some wait in batchQ), drainTimeoutMs
// 300. STIMULUS: SIGTERM. ASSERTION: the shards leave at the deadline; the
// writers, cancelled at drainExpired, return every batch they held without
// blocking (Run returns, nil); the pool is fully joined by then (writers_running
// 0, Close returns at once); and the batches the writers held are home on their
// shards' completionCh, within its capacity. Non-vacuity: statements stalled,
// rows were abandoned, and at least one batch came home.
//
// WOULD HAVE CAUGHT: completionCh smaller than maxInFlightPerShard (a writer
// blocks returning a batch to a shard that left: Run never returns), the pool
// cancelled only after a Close that waits for the stalled batch (hang), and
// Close called before the shards joined.
func TestG_LIFE_4_EndToEnd_StalledWritersReturnTheirBatchesAndRunCompletes(t *testing.T) {
	g, buf, f := dbGraph(t, func(c *config.Config) {
		c.Writers.Count = 3
		c.Flush.WindowMs = 60000
		c.Flush.MaxBatchSize = 1
		c.Flush.DrainTimeoutMs = 300
	})
	started := stallStatements(f)
	cancel, done := runGraph(t, g)

	shards := g.deps.Cfg.Runtime.Shards
	m := g.deps.Cfg.Runtime.MaxInFlightPerShard
	dirtyRows(t, g, m, 90001)
	g.sendTicks()
	waitUntil(t, "every shard to hold m batches", func() bool {
		for k := range shards {
			if g.inst().mirrorShards[k].EpochBatches.Load() < uint64(m) {
				return false
			}
		}
		return started.Load() > 0
	})

	// The writers are mid-statement (an hour-long stall) when SIGTERM arrives.
	if n := g.inst().writer.WritersRunning.Load(); n == 0 {
		t.Fatal("non-vacuity: no writer was running before SIGTERM")
	}
	start := time.Now()
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run = %v, want nil\n%s", err, buf.String())
	}
	if took, limit := time.Since(start), 300*time.Millisecond+1500*time.Millisecond; took >= limit {
		t.Errorf("Run took %v after SIGTERM, drainTimeoutMs is 300: the writers were cancelled late", took)
	}
	// Run returning is the join: the database closes only after it (run's
	// deferred Close), so no writer can be mid-operation on a closed database.
	if n := g.inst().writer.WritersRunning.Load(); n != 0 {
		t.Errorf("writers_running = %d after Run, want 0: a writer was still mid-statement", n)
	}
	ctx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	if err := g.pool.Close(ctx); err != nil {
		t.Errorf("Pool.Close after Run: %v (writers still running)", err)
	}
	if sum3(abandonedByDest(g)) == 0 {
		t.Error("non-vacuity: nothing was abandoned")
	}
	home := 0
	for k := range shards {
		if l := len(g.completionCh[k]); l > m {
			t.Errorf("completionCh[%d] holds %d, cap %d", k, l, m)
		} else {
			home += l
		}
	}
	if home == 0 {
		t.Error("no batch came home on any completionCh: the writers never returned the batches they held")
	}
}

// TestG_LIFE_3_Ordering_WritersAreJoinedBeforeTheDatabaseCloses is the ordering
// half of G-LIFE-3 and G-LIFE-1's full teardown, through the real relay.
//
// SETUP: relay over an oratest database, the socket bind rigged to fail AFTER
// the writer pool started (startup step 13 precedes step 16). STIMULUS: relay
// returns the startup error. ASSERTION: the shutdown line lists "shard-consumers"
// before "writers" among the released resources; no driver operation began after
// the first connection close (db.Close is the only closer in this run, so the
// database was closed only after the writers had left); every connection opened
// was closed. Non-vacuity: operations were observed and the close happened.
// The "timestamp" is the fake's own connection-close counter sampled by each
// operation, which orders the two without a clock. The writers are idle on this
// path, so the mid-operation half of the ordering (writers joined while a
// statement is in flight, before the database can close) is asserted in
// TestG_LIFE_4_EndToEnd, where a statement is stalled when teardown starts.
//
// WOULD HAVE CAUGHT: a pool never closed on the early-error path, a database
// closed under a running writer (an operation after a close), and a teardown
// that forgot the pool entirely.
func TestG_LIFE_3_Ordering_WritersAreJoinedBeforeTheDatabaseCloses(t *testing.T) {
	fake := oratest.New()
	t.Cleanup(fake.Close)
	old := openDB
	openDB = func(ctx context.Context, s oradb.DBSettings) (*oradb.DB, error) {
		return oradb.OpenDriver(ctx, oratest.DriverName, fake.DSN(), s)
	}
	t.Cleanup(func() { openDB = old })
	t.Setenv(oradb.PasswordEnv, "")

	var ops, afterClose atomic.Int64
	fake.SetScript(func(oratest.Call) oratest.Result {
		ops.Add(1)
		if fake.Counts().Closes > 0 {
			afterClose.Add(1)
		}
		return oratest.Result{}
	})

	cfg := config.Defaults()
	cfg.HasDB = true
	cfg.DB = oradb.DBSettings{
		Host: "h", Port: 1521, Service: "s", User: "u",
		ConnectTimeoutSec: 10, QueryTimeoutSec: 30, ConnMaxLifetimeMinutes: 30,
		MaxOpenConns: cfg.Writers.Count + 1, MaxIdleConns: cfg.Writers.Count + 1,
	}
	cfg.UDP.Listen = heldUDPAddr(t)
	dests, err := destinations.Default()
	if err != nil {
		t.Fatal(err)
	}
	buf := &syncBuf{}
	if runErr := relayWith(t, cfg, dests, buf); runErr == nil || ExitCode(runErr) != ExitStartup {
		t.Fatalf("relay = %v, want a startup error", runErr)
	}

	out := buf.String()
	_, rel, ok := strings.Cut(out, "released=")
	if !ok {
		t.Fatalf("no shutdown summary:\n%s", out)
	}
	released := strings.Fields(strings.Trim(strings.SplitN(rel, "\n", 2)[0], "[]"))
	w, sh := slices.Index(released, "writers"), slices.Index(released, "shard-consumers")
	if w < 0 || sh < 0 || sh > w {
		t.Errorf("released = %v: want shard-consumers before writers", released)
	}
	c := fake.Counts()
	if ops.Load() == 0 || c.Opens == 0 {
		t.Errorf("non-vacuity: %d operations, %d connections opened", ops.Load(), c.Opens)
	}
	if c.Closes != c.Opens {
		t.Errorf("%d connections opened, %d closed: a session leaked", c.Opens, c.Closes)
	}
	if n := afterClose.Load(); n != 0 {
		t.Errorf("%d driver operations began after a connection was closed: the database closed under a writer", n)
	}
}

// TestTeardownIsOnceAndSafeWithNothingStarted: the guards. A graph that never
// ran (pool never started, no shard goroutine, no socket) tears down without
// panicking, a second teardown does nothing (batchQ is closed exactly once),
// and a graph whose Run failed at the bind has stopped the pool it started.
//
// WOULD HAVE CAUGHT: close(batchQ) outside the Once (a double-close panic on
// the deferred second teardown), Pool.Close on a never-started pool blocking,
// and a started pool left running on the early-error path.
func TestTeardownIsOnceAndSafeWithNothingStarted(t *testing.T) {
	t.Run("never started", func(t *testing.T) {
		g, _, _ := dbGraph(t, nil)
		g.teardown()
		g.teardown()
		if slices.Contains(g.Released(), "writers") {
			t.Errorf("released = %v: a pool that never started was reported released", g.Released())
		}
		if _, ok := <-g.batchQ; ok {
			t.Error("batchQ was not closed")
		}
	})
	t.Run("without a database", func(t *testing.T) {
		g, _ := testGraph(t, "127.0.0.1:0", nil)
		g.teardown()
		g.teardown()
	})
	t.Run("started then bind failure", func(t *testing.T) {
		g, _, _ := dbGraph(t, func(c *config.Config) { c.UDP.Listen = heldUDPAddr(t) })
		err := g.Run(context.Background())
		if ExitCode(err) != ExitStartup {
			t.Fatalf("Run = %v, want a startup error", err)
		}
		if !slices.Contains(g.Released(), "writers") {
			t.Errorf("released = %v: the writers started and were not stopped", g.Released())
		}
		if n := g.inst().writer.WritersRunning.Load(); n != 0 {
			t.Errorf("writers_running = %d after the failed Run, want 0", n)
		}
		g.teardown() // the deferred second call
	})
}

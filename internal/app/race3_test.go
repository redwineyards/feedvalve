package app

// P4-14: the whole pipeline over the REAL writer pool, under the scheduler
// stress of G-RACE-3, with an oratest database that keeps its own books.
//
// THE obs.Emit RACE (CLAUDE.md memory "obs.Emit concurrent race"; HLD/Phase 2
// record). stats.intervalSec is 1 in these tests, so the stats goroutine is
// emitting while SIGTERM arrives and the lifecycle emits the terminal
// snapshot: the exact overlap of that defect. RECORDED RESULT, 2026-10-23,
// phase4 worktree, go test -race, CC=/c/msys64/clang64/bin/clang.exe, one
// package at a time: see the "obs.Emit:" paragraph in
// docs/development/phase4-tasks.md section 9 (as built). It is NOT attributed
// to this card; if it surfaces, the report names obs.(*Reg).Emit frames.
//
// What stands in for "the database". oratest keeps no rows, so raceDB models
// the part of Oracle the gates need, from the statements the writer sends:
// a row's value becomes durable at the transaction's COMMIT and not before,
// is dropped at ROLLBACK, and a row held by one connection's open transaction
// must not appear in another's (Oracle would block it; a test cannot, so the
// overlap itself is the finding -- G-INFL-1 seen from the database side).
// last_traded_price is the one column the feed varies, so it is the column
// compared; its bind position is read off the statement text
// (":N AS last_traded_price"), not from the code under test's BindPlan.

import (
	"context"
	"encoding/binary"
	"flag"
	"math/rand"
	"net"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
)

// race3For is the G-RACE-2/3 pipeline run length: V4's figure is 60 s. The
// default is 10 s so go test ./internal/app stays tolerable, -short takes 2 s,
// and the full run is
//
//	go test -race -tags feedvalve_assert -timeout 1h ./internal/app \
//	    -run TestG_RACE_3_WholePipeline -feedvalve.race3 60s
var race3For = flag.Duration("feedvalve.race3", 10*time.Second,
	"how long the real-writer pipeline stress (G-RACE-2/3, P4-14) runs (the gate's figure is 60s)")

func race3Duration() time.Duration {
	if testing.Short() {
		return 2 * time.Second
	}
	return *race3For
}

// quoteTable is destination 0's table: the only one 7208 feeds.
const quoteTable = "MERGE INTO MD_TOKEN_QUOTE "

var ltpBind = regexp.MustCompile(`:(\d+) AS last_traded_price`)

// raceDB is the books of the fake database. All state is under mu.
type raceDB struct {
	mu     sync.Mutex
	rng    *rand.Rand
	poison map[int64]bool

	failEvery int          // 1 in failEvery statements fail (half session errors, half unclassified); 0 never
	sticky    map[int]bool // connection -> its next statement fails ORA-00600 (usually the writer's retry)

	ltpIdx  map[string]int // statement text -> bind position of last_traded_price, -1 if none
	holder  map[int64]int  // key -> connection whose open transaction holds it
	held    map[int][]int64
	pending map[int][]kv
	durable map[int64]int64 // key -> committed last_traded_price

	commitDelay time.Duration // slept by slowScript, outside mu
	commitNanos atomic.Int64
	commitN     atomic.Int64

	overlap       int // a key written by two connections' open transactions at once
	other         int // statements that are not destination 0's MERGE
	noLTP         int // destination 0 statements that do not carry last_traded_price
	transient     int
	poisoned      int
	commits       int
	rollbacks     int
	stmts         int
	committedKeys int
}

type kv struct{ key, ltp int64 }

func newRaceDB(poison map[int64]bool, failEvery int) *raceDB {
	return &raceDB{rng: rand.New(rand.NewSource(7)), poison: poison, failEvery: failEvery,
		ltpIdx: map[string]int{}, holder: map[int64]int{}, held: map[int][]int64{},
		pending: map[int][]kv{}, sticky: map[int]bool{}, durable: map[int64]int64{}}
}

func (r *raceDB) release(conn int) {
	for _, k := range r.held[conn] {
		if r.holder[k] == conn {
			delete(r.holder, k)
		}
	}
	delete(r.held, conn)
	delete(r.pending, conn)
}

// keysOf and valsOf unpack an array bind (phase 1) or a scalar (phase 2).
func int64s(v any) []int64 {
	switch x := v.(type) {
	case []int64:
		return x
	case int64:
		return []int64{x}
	}
	return nil
}

func (r *raceDB) script(c oratest.Call) oratest.Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch c.SQL {
	case oratest.OpCommit:
		for _, w := range r.pending[c.Conn] {
			r.durable[w.key] = w.ltp
			r.committedKeys++
		}
		r.commits++
		r.release(c.Conn)
		return oratest.Result{Delay: 0}
	case oratest.OpRollback:
		r.rollbacks++
		r.release(c.Conn)
		return oratest.Result{}
	}
	if strings.HasPrefix(c.SQL, "<") {
		return oratest.Result{}
	}
	if !strings.HasPrefix(c.SQL, quoteTable) {
		r.other++
		return oratest.Result{}
	}
	r.stmts++
	idx, ok := r.ltpIdx[c.SQL]
	if !ok {
		idx = -1
		if m := ltpBind.FindStringSubmatch(c.SQL); m != nil {
			n, _ := strconv.Atoi(m[1])
			idx = n - 1
		}
		r.ltpIdx[c.SQL] = idx
	}
	keys := int64s(c.Args[0])
	var ltps []int64
	if idx >= 0 && idx < len(c.Args) {
		ltps = int64s(c.Args[idx])
	} else {
		r.noLTP++
	}
	bad := false
	for _, k := range keys {
		if h, held := r.holder[k]; held && h != c.Conn {
			r.overlap++
		}
		r.holder[k] = c.Conn
		r.held[c.Conn] = append(r.held[c.Conn], k)
		bad = bad || r.poison[k]
	}
	switch {
	case bad:
		r.poisoned++
		return oratest.Result{Err: oratest.ORA(1438)} // value larger than precision: a row's own fault
	case r.sticky[c.Conn]:
		// The next statement on this session fails too. That is usually the
		// writer's retry of the unclassified failure, so the writer gives up and
		// the batch comes home Failed (the shard requeues its rows).
		delete(r.sticky, c.Conn)
		r.transient++
		return oratest.Result{Err: oratest.ORA(600)}
	case r.failEvery > 0 && r.rng.Intn(r.failEvery) == 0:
		r.transient++
		if r.rng.Intn(2) == 0 {
			r.sticky[c.Conn] = true
			return oratest.Result{Err: oratest.ORA(600)} // ORA-00600: unclassified
		}
		return oratest.Result{Err: oratest.ORA(3113)}
	}
	for i, k := range keys {
		if i < len(ltps) {
			r.pending[c.Conn] = append(r.pending[c.Conn], kv{k, ltps[i]})
		}
	}
	return oratest.Result{Delay: time.Duration(r.rng.Intn(100)) * time.Microsecond} // 0-100 us
}

// raceGraph is dbGraph with the decode seam, a database that keeps books, and
// a one-second stats interval (see the header).
func raceGraph(t *testing.T, tune func(*config.Config), decode func(*model.Datagram) error,
	rdb *raceDB, latency time.Duration) (*Graph, *syncBuf, *oratest.Fake) {
	t.Helper()
	t.Setenv(oradb.PasswordEnv, "")
	f := oratest.New()
	t.Cleanup(f.Close)
	if rdb != nil {
		f.SetScript(rdb.script)
	}
	f.SetLatency(latency)

	cfg := config.Defaults()
	cfg.UDP.Listen = "127.0.0.1:0"
	cfg.Runtime.ExpectedRows = 1024
	cfg.Runtime.ArenaChunkRows = 256
	cfg.Flush.WindowMs = 20
	cfg.Stats.IntervalSec = 1
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
		Log: lg, Debug: dbg, Dests: dests, Stderr: buf, Inst: inst, Decode: decode,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
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

// TestG_RACE_3_WholePipelineWithTheRealWriterPool is gates G-RACE-2 and
// G-RACE-3 (V4 section 7) for Phase 4, and the end-to-end half of G-MASK-1,
// G-ORD-1, G-INFL-1 and G-ROW-1.
//
// SETUP: the stress_test.go pipeline -- loopback socket, framing, the decoder
// behind Deps.Decode, four live shards, GOMAXPROCS = 2 x shards, flush window
// 5 ms and maxBatchSize 4 -- but with the REAL writer.Pool (4 writers) over an
// oratest database in place of the stress writers. Injected: 0-100 us jitter at
// socket send and QD -> decoder (as G-RACE-3; the shard -> batchQ and return
// boundaries get no explicit jitter, only the database's 0-100 us per-statement
// delay and 300 us latency, which perturb the writers' side of both -- a stated
// limit, recorded in phase4-tasks.md section 9), 300 us statement latency plus a
// 0-100 us delay per statement in the database, 1 statement in 150 failing
// (half ORA-03113, a session error the writer retries; half ORA-00600, unclassified, after which the
// next statement on that session also fails -- usually the retry -- so the batch comes home Failed
// and the shard requeues its rows), and one poison row
// per shard that fails with ORA-01438 every time it is written (a row's own
// fault: phase 2 isolates it, it is Rejected and quarantined). Sixty-four
// tokens, sixteen per shard.
//
// STIMULUS: datagrams of two 7208 records each, every record a new
// last_traded_price, for the run length (race3For), then the decoder is let
// catch up, then SIGTERM and the drain.
//
// ASSERTION, over the accumulated state once Run has returned (all of it from
// the database's books or the mirror, none from the writer's own counters
// except where named):
//   - G-ORD-1: for every row, the last value the decoder saw equals the mirror
//     image AND the last value the database committed; no unit was superseded
//     out of order; the receipt clock never tied;
//   - G-MASK-1: every row's committed last_traded_price equals its mirror
//     image's (no column re-dirtied in flight was lost), its ColMask is 0,
//     Written includes the column;
//   - G-INFL-1: no key was ever in two connections' open transactions at once
//     (the database's view), inflight_desync is 0;
//   - G-ROW-1: the quarantined rows are exactly poison rows, none of them
//     ever committed, every other row committed; batch_row_unreported 0;
//   - G-ROW-2 (end to end, indirect): a batch returned with Err != nil has all
//     its rows Pending -> Failed and requeued at the head. The ORA-00600
//     batches are the ones that fail, so "rows requeued for retry" is
//     non-zero (non-vacuity) and the checks below leave no stranded row:
//     batch_row_unreported 0, nothing Dirty/InFlight at quiescence, and the
//     accounting identity. The per-row Failed assertion is writer-side.
//   - accounting: committed distinct rows + quarantined rows == rows_created,
//     and drain_abandoned_rows is 0, so every accepted row is written,
//     abandoned or still held -- none vanished;
//   - Run returns nil (no FailFast, no F20), a committed transaction exists
//     for every row the writer counted written, nothing is dirty or in
//     flight at quiescence and every Free list is full.
//
// Non-vacuity: transient failures and poison rejections were injected, the
// writer retried and rejected rows, rows were re-dirtied in flight, commits
// and rollbacks both happened, and rows were compared.
//
// WOULD HAVE CAUGHT: any unsynchronised sharing between a writer goroutine and
// a shard under -race; a batch handed to two writers (overlap); a column lost
// when a row re-dirtied during its own write was cleared at completion; a
// quarantined row offered again (it would commit, or be written, counted
// against the books); a writer that returns a batch twice or not at all
// (unreported rows, a Free list short at quiescence); a retry that re-applies
// a stale image over a newer one (G-ORD-1 value mismatch).
func TestG_RACE_3_WholePipelineWithTheRealWriterPool(t *testing.T) {
	prev := runtime.GOMAXPROCS(2 * stressShards)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	calibrateJitter()

	shardToks := tokensPerShard(stressShards, 16)
	var toks []uint32
	poison := map[model.RowKey]bool{}
	poison64 := map[int64]bool{}
	for _, ts := range shardToks {
		toks = append(toks, ts...)
		poison[model.SingleKey(ts[0])] = true
		poison64[int64(ts[0])] = true
	}
	rdb := newRaceDB(poison64, 150)

	var (
		g       *Graph
		rcv     *receiver
		drng    = rand.New(rand.NewSource(1))
		expect  = map[uint32]uint32{}
		seenAt  = map[uint32]int64{}
		ties    uint64
		decoded atomic.Int64
	)
	decode := func(d *model.Datagram) error {
		jitter(drng) // QD -> decoder
		if rcv == nil {
			rcv = newReceiver(g)
		}
		if err := rcv.decodeDatagram(d); err != nil {
			return err
		}
		p := d.Buf[6:d.N]
		for rec := range int(binary.BigEndian.Uint16(p[mbpAbsNoOfRecords:])) {
			base := mbpAbsRecord0 + rec*mbpAbsStride
			tok := binary.BigEndian.Uint32(p[base+mbpRecTokenOff:])
			if d.RecvMono > seenAt[tok] {
				expect[tok] = binary.BigEndian.Uint32(p[base+mbpRecLTPOff:])
				seenAt[tok] = d.RecvMono
			} else {
				ties++
			}
		}
		decoded.Add(1)
		return nil
	}
	g, buf, f := raceGraph(t, func(c *config.Config) {
		c.Runtime.Shards = stressShards
		c.Writers.Count = 4
		c.Flush.WindowMs = 5
		c.Flush.MaxBatchSize = 4
		c.Ordering.Guard = "on"
	}, decode, rdb, 300*time.Microsecond)
	f.SetRecordArgs(false) // the script keeps the books; the exec log would grow without bound

	cancel, done := runGraph(t, g)

	conn, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialling the listener: %v", err)
	}
	defer conn.Close()
	srng := rand.New(rand.NewSource(2))
	env := make([]byte, 4+2+mbpAbsPayloadLen)
	env[0] = 8
	binary.BigEndian.PutUint16(env[2:4], 1)
	var seq uint32
	runFor := race3Duration()
	for end := time.Now().Add(runFor); time.Now().Before(end); {
		i := srng.Intn(len(toks))
		j := (i + 1 + srng.Intn(len(toks)-1)) % len(toks)
		p := mbpPayloadTokens(toks[i], toks[j])
		for rec := range 2 {
			seq++
			base := mbpAbsRecord0 + rec*mbpAbsStride
			binary.BigEndian.PutUint32(p[base+mbpRecLTPOff:], seq)
			p[base+mbpRecNetChangeOff] = '+'
		}
		copy(env[6:], p)
		if _, err := conn.Write(env); err != nil {
			t.Fatalf("writing a datagram: %v", err)
		}
		jitter(srng)
	}
	for last, deadline := int64(-1), time.Now().Add(time.Minute); ; {
		n := decoded.Load()
		if n == last {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the decoder was still decoding a minute after the sender stopped (%d)", n)
		}
		last = n
		time.Sleep(250 * time.Millisecond)
	}

	cancel()
	if err := awaitRunFor(t, done, time.Minute); err != nil {
		t.Fatalf("Run = %v, want nil for a requested shutdown\n%s", err, buf.String())
	}

	in := g.inst()
	wi := in.writer
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	t.Logf("G-RACE-2/3 (real pool): %v, datagrams sent=%d received=%d decoded=%d; db: statements=%d "+
		"commits=%d rollbacks=%d transient=%d poison-rejected-stmts=%d durable rows=%d; writer: "+
		"batches=%d rows_written=%d rejected=%d; redirty=%d",
		runFor, seq/2, in.datagramsReceived.Load(), decoded.Load(), rdb.stmts, rdb.commits, rdb.rollbacks,
		rdb.transient, rdb.poisoned, len(rdb.durable), wi.BatchesWritten.Load(),
		perDest(wi.Run.RowsWritten[:])[0], perDest(wi.Run.RowRejected[:])[0],
		in.mirrorShards[0].RedirtyTotal.Load())

	if n := uint64(decoded.Load()); n != in.datagramsReceived.Load() {
		t.Errorf("datagrams_received = %d but the decoder saw %d", in.datagramsReceived.Load(), n)
	}

	// ---- counters ----
	var desync, unreported, retried, epochs uint64
	for k := range in.mirrorShards {
		desync += in.mirrorShards[k].InflightDesync.Load()
		unreported += in.mirrorShards[k].BatchRowUnreported.Load()
		retried += in.mirrorShards[k].RetryRequeued.Load()
		epochs += in.mirrorShards[k].EpochOpened.Load()
	}
	if desync != 0 || unreported != 0 {
		t.Errorf("inflight_desync = %d, batch_row_unreported = %d; want 0 and 0", desync, unreported)
	}
	if rdb.overlap != 0 {
		t.Errorf("G-INFL-1: %d keys were in two connections' open transactions at once", rdb.overlap)
	}
	if rdb.other != 0 || rdb.noLTP != 0 {
		t.Errorf("the books saw %d statements that are not destination 0's MERGE and %d without "+
			"last_traded_price: the books are blind to them", rdb.other, rdb.noLTP)
	}
	if n := abandoned(g); n != 0 {
		t.Errorf("drain_abandoned_rows = %d, want 0 with working writers\n%s", n, buf.String())
	}
	if n := in.mirrorShards[0].UnitsSupersededOutOfOrder.Load(); n != 0 {
		t.Errorf("G-ORD-1: units_superseded_out_of_order = %d, want 0", n)
	}
	if ties != 0 {
		t.Errorf("seam 9 regressed: the receipt clock tied %d times", ties)
	}
	for i, c := range in.mirrorTranscode.BlobLenMismatch {
		if n := c.Load(); n != 0 {
			t.Errorf("blob_len_mismatch[%d] = %d, want 0", i, n)
		}
	}
	if n := wi.WritersRunning.Load(); n != 0 {
		t.Errorf("writers_running = %d after Run", n)
	}

	// ---- the walk ----
	m := g.deps.Cfg.Runtime.MaxInFlightPerShard
	chunk := int32(g.deps.Cfg.Runtime.ArenaChunkRows)
	ltpCol := g.deps.Dests.ByID(0).Column(0)
	checked, quarantined := 0, 0
	for k, s := range g.shards {
		if len(s.Free) != m {
			t.Errorf("shard %d: Free holds %d of %d batches at quiescence", k, len(s.Free), m)
		}
		for row, slot := range s.Index[0] {
			e := &s.Arena[0][slot/chunk][slot%chunk]
			if e.Dirty || e.InFlight || e.ReDirty {
				t.Errorf("shard %d row %d not quiescent: Dirty %v InFlight %v ReDirty %v",
					k, row, e.Dirty, e.InFlight, e.ReDirty)
				continue
			}
			dbLTP, written := rdb.durable[int64(row.Lo())]
			if e.Quarantined {
				quarantined++
				if !poison[row] || written {
					t.Errorf("G-ROW-1: shard %d row %d is quarantined; poison %v, committed %v -- want true, false",
						k, row, poison[row], written)
				}
				continue
			}
			if poison[row] {
				t.Errorf("G-ROW-1: poison row %d on shard %d is not quarantined", row, k)
			}
			if !written {
				t.Errorf("accounting: shard %d row %d is not quarantined, abandoned or dirty, and the database never committed it", k, row)
				continue
			}
			if e.ColMask != 0 || !e.Written.Has(0) {
				t.Errorf("G-MASK-1: shard %d row %d: ColMask %#x Written %#x", k, row, uint64(e.ColMask), uint64(e.Written))
			}
			mirror := int64(binary.BigEndian.Uint32(e.Image[ltpCol.Offset:]))
			if dbLTP != mirror {
				t.Errorf("G-MASK-1: shard %d row %d: the database committed last_traded_price %d, the mirror holds %d "+
					"-- a column dirtied in flight was lost, or a stale image was written last", k, row, dbLTP, mirror)
			}
			if want, ok := expect[uint32(row.Lo())]; !ok || int64(want) != dbLTP {
				t.Errorf("G-ORD-1: shard %d row %d: the last value the decoder saw was %d (seen %v), the database holds %d",
					k, row, want, ok, dbLTP)
			}
			checked++
		}
	}
	created := perDest(in.mirrorShards[0].RowsCreated[:])[0]
	if got := uint64(len(rdb.durable) + quarantined); got != created {
		t.Errorf("accounting: %d distinct rows committed + %d quarantined != rows_created %d", len(rdb.durable), quarantined, created)
	}
	if created != uint64(len(expect)) {
		t.Errorf("rows_created = %d, the decoder saw %d tokens", created, len(expect))
	}
	if w := perDest(wi.Run.RowsWritten[:])[0]; w < uint64(len(rdb.durable)) {
		t.Errorf("rows_written = %d is below the %d distinct rows the database committed", w, len(rdb.durable))
	}

	// ---- non-vacuity ----
	for _, c := range []struct {
		what string
		n    uint64
	}{
		{"datagrams decoded", uint64(decoded.Load())},
		{"epochs opened", epochs},
		{"transactions committed", uint64(rdb.commits)},
		{"transactions rolled back", uint64(rdb.rollbacks)},
		{"injected session errors", uint64(rdb.transient)},
		{"statements failing on a poison row", uint64(rdb.poisoned)},
		{"rows rejected by the writer", perDest(wi.Run.RowRejected[:])[0]},
		{"rows re-dirtied in flight", in.mirrorShards[0].RedirtyTotal.Load()},
		{"rows requeued for retry", retried},
		{"rows quarantined at the end", uint64(quarantined)},
		{"rows checked against the database", uint64(checked)},
	} {
		if c.n == 0 {
			t.Errorf("non-vacuity: no %s, so part of the gate measured nothing", c.what)
		}
	}
}

// awaitRunFor is awaitRun with the caller's bound: a drain under a long stress
// run has more to flush than the unit tests' 10 s allows for.
func awaitRunFor(t *testing.T, done <-chan error, d time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatal("Run did not return")
		return nil
	}
}

// race3Iteration numbers the runs of the -count test, so consecutive counts
// alternate between a healthy and a stalled database without any flag.
var race3Iteration atomic.Int64

// TestG_LIFE_4_RepeatedShutdownUnderLoadAlwaysCompletes is gate G-LIFE-4 shaped
// for -count=20 (and -race): the shutdown races, run many times.
//
//	go test -race -count=20 -timeout 20m ./internal/app -run TestG_LIFE_4_Repeated
//
// SETUP: two shards, three writers, an oratest database with 200 us statement
// latency and 0-100 us jitter, a continuous feed of fresh rows. Odd iterations
// (counted across -count, from a package counter) add a database that stalls
// every statement for an hour after a random 20-120 ms of healthy running, with
// drainTimeoutMs 300; even iterations stay healthy. STIMULUS: SIGTERM at a
// random moment under load. ASSERTION: Run returns nil within the drain
// bound; writers_running is 0; Pool.Close returns at once; every completionCh
// holds at most its capacity; batch_row_unreported and inflight_desync are 0;
// and no goroutine outlives the run (NumGoroutine returns to within 3 of its
// value before Build once the test's own cleanup has run). Healthy iterations
// additionally assert nothing was abandoned and every row the shards created
// is durable in the database's books; stalled ones that something was
// abandoned (non-vacuity: the stall bit).
//
// Only this test is shaped for -count=20; the rest of V4's G-RACE-2 -count=20
// set (G-FLUSH-*, G-EPOCH-*, G-INFL-*, G-ROW-*, G-CFG-3) is not run by P4-14.
//
// WOULD HAVE CAUGHT: a writer blocked returning a batch to a shard that has
// left (Run hangs, stalled variant); a pool cancelled by SIGTERM instead of
// drainExpired (the healthy drain's batches come back Failed and rows are
// abandoned); a leaked writer, timer or sender goroutine (the count);
// state shared between iterations (the second count fails alone).
func TestG_LIFE_4_RepeatedShutdownUnderLoadAlwaysCompletes(t *testing.T) {
	stalled := race3Iteration.Add(1)%2 == 1
	runtime.GC()
	before := runtime.NumGoroutine()

	rdb := newRaceDB(map[int64]bool{}, 0)
	g, buf, f := raceGraph(t, func(c *config.Config) {
		c.Runtime.Shards = 2
		c.Writers.Count = 3
		c.Flush.WindowMs = 5
		c.Flush.MaxBatchSize = 2
		c.Flush.DrainTimeoutMs = 300
		if !stalled {
			c.Flush.DrainTimeoutMs = 5000
		}
	}, nil, rdb, 200*time.Microsecond)
	f.SetRecordArgs(false)

	var stall atomic.Bool
	books := rdb.script
	f.SetScript(func(c oratest.Call) oratest.Result {
		if stall.Load() && !strings.HasPrefix(c.SQL, "<") {
			return oratest.Result{Delay: time.Hour}
		}
		return books(c)
	})

	cancel, done := runGraph(t, g)
	seed := time.Now().UnixNano()
	t.Logf("seed %d (replay by fixing it here)", seed)
	rng := rand.New(rand.NewSource(seed))
	conn, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var tok atomic.Uint32
	tok.Store(500000)
	stop := make(chan struct{})
	var feeder sync.WaitGroup
	feeder.Add(1)
	go func() {
		defer feeder.Done()
		env := make([]byte, 4+2+mbpAbsPayloadLen)
		env[0] = 8
		binary.BigEndian.PutUint16(env[2:4], 1)
		for {
			select {
			case <-stop:
				return
			default:
			}
			n := tok.Add(2)
			copy(env[6:], mbpPayloadTokens(n, n+1))
			conn.Write(env)
			time.Sleep(200 * time.Microsecond)
		}
	}()
	healthy := time.Duration(20+rng.Intn(100)) * time.Millisecond
	time.Sleep(healthy)
	if stalled {
		stall.Store(true)
		time.Sleep(30 * time.Millisecond) // let statements reach the stall
	}
	if sum3(createdByDest(g)) == 0 {
		close(stop)
		feeder.Wait()
		t.Fatal("non-vacuity: no row was created before SIGTERM")
	}
	start := time.Now()
	cancel()
	err = awaitRunFor(t, done, 20*time.Second)
	took := time.Since(start)
	close(stop)
	feeder.Wait()
	if err != nil {
		t.Fatalf("Run = %v, want nil\n%s", err, buf.String())
	}
	drain := g.loadRuntime().Mirror.DrainTimeout
	if limit := drain + 1500*time.Millisecond; took >= limit {
		t.Errorf("Run took %v after SIGTERM, drainTimeoutMs is %v", took, drain)
	}
	if n := g.inst().writer.WritersRunning.Load(); n != 0 {
		t.Errorf("writers_running = %d after Run", n)
	}
	ctx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	if err := g.pool.Close(ctx); err != nil {
		t.Errorf("Pool.Close after Run: %v", err)
	}
	cancelClose()
	m := g.deps.Cfg.Runtime.MaxInFlightPerShard
	for k := range g.completionCh {
		if l := len(g.completionCh[k]); l > m {
			t.Errorf("completionCh[%d] holds %d, cap %d", k, l, m)
		}
	}
	in := g.inst()
	for k := range in.mirrorShards {
		if n := in.mirrorShards[k].InflightDesync.Load(); n != 0 {
			t.Errorf("shard %d: inflight_desync = %d", k, n)
		}
		if n := in.mirrorShards[k].BatchRowUnreported.Load(); n != 0 {
			t.Errorf("shard %d: batch_row_unreported = %d", k, n)
		}
	}
	created, ab := sum3(createdByDest(g)), sum3(abandonedByDest(g))
	if stalled {
		if ab == 0 {
			t.Error("non-vacuity: nothing was abandoned although every statement stalled")
		}
	} else {
		if ab != 0 {
			t.Errorf("healthy drain abandoned %d rows\n%s", ab, buf.String())
		}
		rdb.mu.Lock()
		durable := uint64(len(rdb.durable))
		rdb.mu.Unlock()
		if durable != created {
			t.Errorf("healthy drain: %d distinct rows durable, %d created: a row was accepted and never written",
				durable, created)
		}
	}

	// No goroutine outlives the run. The graph's own cleanups have not run
	// yet; do their work here (the pool is already closed above).
	g.deps.DB.Close()
	f.Close()
	waitUntil(t, "goroutines to return to the baseline", func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= before+3
	})
}

// slowScript is script plus the commit stub: oratest ignores latency on commit,
// so the cost of a commit (t_commit, V3 section 1.2) is slept here, outside mu
// so concurrent commits overlap as they would on separate sessions, and
// measured, so the expectation below uses what the stub really cost.
func (r *raceDB) slowScript(c oratest.Call) oratest.Result {
	res := r.script(c)
	if c.SQL == oratest.OpCommit && r.commitDelay > 0 {
		t0 := time.Now()
		time.Sleep(r.commitDelay)
		r.commitNanos.Add(int64(time.Since(t0)))
		r.commitN.Add(1)
	}
	return res
}

// TestPoolUtilisationAt5000RowsPerSecond is V3 section 1.2's occupancy
// arithmetic checked against the real pool.
//
// The LLD's figure: 5,000 rows/s in 250-row batches is 20 batches/s; each costs
// bind (~1.4 ms) plus t_commit; "20 x (1.4 + 20) ms = 428 ms/s, over 4 writers,
// is 10.7% busy per writer". The task card says "5 ms commit stub (about 11%)";
// those two do not agree (with 5 ms the same arithmetic gives 3.2%), so both
// are run: the 20 ms commit reproduces the LLD's 10.7%, the 5 ms commit is
// checked against its own arithmetic.
//
// SETUP: ONE shard, four writers, flush window 50 ms, maxBatchSize 250, 250
// tokens (so every window finds all 250 rows dirty: one 250-row batch per
// window); 1.4 ms of statement latency standing in for bind plus execute, and
// a commit stub of 5 or 20 ms. STIMULUS: 2,500 two-record datagrams/s (5,000
// row updates/s) for the run length, paced from the wall clock so a late
// sender catches up; the writers_busy gauge sampled every millisecond after a
// one-second warm-up. ASSERTION, loose: the rows written per second are within
// 0.6-1.4x of 5,000 and the batches per second within 0.6-1.4x of 20 (the
// load was real); the mean busy fraction (mean writers_busy / 4) is within
// 0.5-2.5x of batches/s x (measured mean commit + 1.4 ms) / 4, and, in
// absolute terms, in [1%, 12%] for the 5 ms stub and [5%, 25%] for the 20 ms
// stub (the LLD's ~10.7%). Non-vacuity: the gauge was non-zero in some sample
// and zero in some other, so a stuck or constant gauge cannot pass.
//
// WOULD HAVE CAUGHT: writers_busy not decremented on a path (utilisation
// pinned at 100%), a pool that serialises commits behind one lock (busy
// several times the arithmetic), and a batch ceiling or window that quietly
// changed the batch rate away from the LLD's 20/s.
func TestPoolUtilisationAt5000RowsPerSecond(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commit time.Duration
		lo, hi float64
	}{
		{"commit5ms", 5 * time.Millisecond, 0.01, 0.12},
		{"commit20ms", 20 * time.Millisecond, 0.05, 0.25},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const (
				stmtLatency = 1400 * time.Microsecond
				writers     = 4
				tokens      = 250
			)
			rdb := newRaceDB(map[int64]bool{}, 0)
			rdb.commitDelay = tc.commit
			g, buf, f := raceGraph(t, func(c *config.Config) {
				c.Runtime.Shards = 1
				c.Writers.Count = writers
				c.Flush.WindowMs = 50
				c.Flush.MaxBatchSize = tokens
			}, nil, rdb, stmtLatency)
			f.SetRecordArgs(false)
			f.SetScript(rdb.slowScript)
			cancel, done := runGraph(t, g)
			wi := g.inst().writer

			conn, err := net.Dial("udp", g.LocalAddr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			env := make([]byte, 4+2+mbpAbsPayloadLen)
			env[0] = 8
			binary.BigEndian.PutUint16(env[2:4], 1)

			warm, measure := time.Second, 5*time.Second
			if testing.Short() {
				measure = 3 * time.Second
			}
			var sum, samples, nonzero, zero atomic.Int64
			var sampling atomic.Bool
			stopSample := make(chan struct{})
			var sw sync.WaitGroup
			sw.Add(1)
			go func() {
				defer sw.Done()
				for {
					select {
					case <-stopSample:
						return
					default:
					}
					if sampling.Load() {
						b := wi.Run.Busy.Load()
						sum.Add(b)
						samples.Add(1)
						if b > 0 {
							nonzero.Add(1)
						} else {
							zero.Add(1)
						}
					}
					time.Sleep(time.Millisecond)
				}
			}()

			start := time.Now()
			var sent, tok int
			var w0, b0 uint64
			var t0 time.Time
			began := false
			for end := start.Add(warm + measure); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
				el := time.Since(start)
				if !began && el >= warm {
					began, t0 = true, time.Now()
					w0, b0 = perDest(wi.Run.RowsWritten[:])[0], wi.BatchesWritten.Load()
					rdb.commitNanos.Store(0)
					rdb.commitN.Store(0)
					sampling.Store(true)
				}
				for due := int(el.Seconds() * 2500); sent < due; sent++ {
					a, b := uint32(100000+tok%tokens), uint32(100000+(tok+1)%tokens)
					tok += 2
					pl := mbpPayloadTokens(a, b)
					for rec := range 2 { // a new value each time, or the merge sees no change and dirties nothing
						binary.BigEndian.PutUint32(pl[mbpAbsRecord0+rec*mbpAbsStride+mbpRecLTPOff:], uint32(2*sent+rec+1))
					}
					copy(env[6:], pl)
					conn.Write(env)
				}
			}
			dur := time.Since(t0).Seconds()
			rows := float64(perDest(wi.Run.RowsWritten[:])[0]-w0) / dur
			batches := float64(wi.BatchesWritten.Load()-b0) / dur
			sampling.Store(false)
			close(stopSample)
			sw.Wait()
			meanCommit := time.Duration(rdb.commitNanos.Load() / max(rdb.commitN.Load(), 1))
			cancel()
			if err := awaitRunFor(t, done, time.Minute); err != nil {
				t.Fatalf("Run = %v\n%s", err, buf.String())
			}

			busy := float64(sum.Load()) / float64(max(samples.Load(), 1)) / writers
			want := batches * (meanCommit + stmtLatency).Seconds() / writers
			t.Logf("commit stub %v (measured mean %v): %.0f rows/s written, %.1f batches/s, busy %.1f%% "+
				"(arithmetic %.1f%%, LLD at 20 ms: 10.7%%), %d samples; datagrams sent=%d received=%d", tc.commit, meanCommit, rows, batches,
				100*busy, 100*want, samples.Load(), sent, g.inst().datagramsReceived.Load())

			if rows < 3000 || rows > 7000 {
				t.Errorf("rows written = %.0f/s, want about 5000 (0.6-1.4x): the load was not 5,000 rows/s", rows)
			}
			if batches < 12 || batches > 28 {
				t.Errorf("batches = %.1f/s, want about 20 (0.6-1.4x)", batches)
			}
			if busy < 0.5*want || busy > 2.5*want {
				t.Errorf("busy = %.1f%%, arithmetic %.1f%% (allowed 0.5-2.5x)", 100*busy, 100*want)
			}
			// The absolute bounds are wall-clock based: skip them when the host is
			// starved (-short, few procs); the relative check above is the gate.
			if !testing.Short() && runtime.NumCPU() >= 4 && (busy < tc.lo || busy > tc.hi) {
				t.Errorf("busy = %.1f%%, want within [%.0f%%, %.0f%%]", 100*busy, 100*tc.lo, 100*tc.hi)
			}
			if nonzero.Load() == 0 || zero.Load() == 0 {
				t.Errorf("non-vacuity: writers_busy was non-zero in %d samples and zero in %d", nonzero.Load(), zero.Load())
			}
		})
	}
}

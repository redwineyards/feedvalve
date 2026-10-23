package writer

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
)

// --- the pool rig ---

// fakeTimers stands in for Pool.after: each call is recorded with its duration
// and returns a channel the test fires by hand.
type fakeTimers struct {
	mu   sync.Mutex
	list []*fakeTimer
}

type fakeTimer struct {
	d time.Duration
	c chan time.Time
}

func (ts *fakeTimers) after(d time.Duration) (<-chan time.Time, func()) {
	t := &fakeTimer{d: d, c: make(chan time.Time, 1)}
	ts.mu.Lock()
	ts.list = append(ts.list, t)
	ts.mu.Unlock()
	return t.c, func() {}
}

func (ts *fakeTimers) count(d time.Duration) int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	n := 0
	for _, t := range ts.list {
		if t.d == d {
			n++
		}
	}
	return n
}

func (ts *fakeTimers) latest(d time.Duration) *fakeTimer {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	for i := len(ts.list) - 1; i >= 0; i-- {
		if ts.list[i].d == d {
			return ts.list[i]
		}
	}
	return nil
}

const (
	idleD = 60 * time.Second
	holdD = 30 * time.Minute
)

type rigOpts struct {
	writers, shards, compCap, qCap int
	par                            func(*Params)
}

type poolRig struct {
	t      *testing.T
	db     *oradb.DB
	f      *oratest.Fake
	reg    *obs.Reg
	inst   *Instruments
	p      *Pool
	batchQ chan *model.Batch
	comp   []chan *model.Batch
	timers *fakeTimers
}

func defaultParams() *Params {
	return &Params{
		QueryTimeout: 30 * time.Second, RetryInitial: 100 * time.Millisecond, RetryMax: 5 * time.Second,
		ConnIdleRelease: idleD, ConnHold: holdD, StmtCacheSize: 64,
		FailFastWindow: 30 * time.Second, FailFastWrites: 20,
		StatementFailWriters: 2, StatementFailWindow: 60 * time.Second,
	}
}

// newPoolRig builds a pool over an oratest fake and does not start it, so a
// test can set the Pool's seams first. Cleanup closes the pool, draining
// completions so a blocked send cannot hang it.
func newPoolRig(t *testing.T, o rigOpts) *poolRig {
	t.Helper()
	if o.writers == 0 {
		o.writers = 1
	}
	if o.shards == 0 {
		o.shards = 1
	}
	if o.compCap == 0 {
		o.compCap = 2
	}
	if o.qCap == 0 {
		o.qCap = 64
	}
	db, f := fakeOra(t)
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: o.shards})
	in := NewInstruments(reg, 3)
	in.ResolveCache(reg, 32, 3)
	in.ResolvePool(reg, 3)
	reg.Freeze()

	par := defaultParams()
	if o.par != nil {
		o.par(par)
	}
	p, err := NewPool(db, destSet(t), Config{Writers: o.writers, Ceiling: rigCeiling},
		func() *Params { return par }, in, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := &poolRig{t: t, db: db, f: f, reg: reg, inst: in, p: p, timers: &fakeTimers{},
		batchQ: make(chan *model.Batch, o.qCap)}
	p.after = r.timers.after
	for range o.shards {
		r.comp = append(r.comp, make(chan *model.Batch, o.compCap))
	}
	t.Cleanup(func() {
		stop := make(chan struct{})
		defer close(stop)
		for _, c := range r.comp {
			go func() {
				for {
					select {
					case <-c:
					case <-stop:
						return
					}
				}
			}()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Errorf("cleanup Close: %v", err)
		}
	})
	return r
}

func (r *poolRig) start() {
	r.t.Helper()
	send := make([]chan<- *model.Batch, len(r.comp))
	for i, c := range r.comp {
		send[i] = c
	}
	if err := r.p.Start(context.Background(), r.batchQ, send); err != nil {
		r.t.Fatal(err)
	}
}

func (r *poolRig) batch(shard model.ShardID) *model.Batch {
	b := standard()
	b.Shard = shard
	return b
}

func (r *poolRig) recv(shard int) *model.Batch {
	r.t.Helper()
	select {
	case b := <-r.comp[shard]:
		return b
	case <-time.After(5 * time.Second):
		r.t.Fatalf("no completion on shard %d", shard)
		return nil
	}
}

func (r *poolRig) live() int {
	r.p.mu.Lock()
	defer r.p.mu.Unlock()
	return len(r.p.live)
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func isMerge(c oratest.Call) bool { return strings.HasPrefix(c.SQL, "MERGE") }

func stopped(p *Pool) bool {
	select {
	case <-p.Stopped():
		return true
	default:
		return false
	}
}

// gate blocks every commit until opened, so a test can hold batches in flight.
type gate struct {
	entered chan struct{}
	open    chan struct{}
}

func newGate() *gate { return &gate{entered: make(chan struct{}, 64), open: make(chan struct{})} }

func (g *gate) script(c oratest.Call) oratest.Result {
	if c.SQL == oratest.OpCommit {
		g.entered <- struct{}{}
		<-g.open
	}
	return oratest.Result{}
}

func allCommitted(t *testing.T, b *model.Batch) {
	t.Helper()
	if b.Err != nil {
		t.Fatalf("Err = %v", b.Err)
	}
	if got, want := outcomes(b), all(7, model.OutcomeCommitted); len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("outcomes %v, want all committed", got)
	}
	for i, o := range outcomes(b) {
		if o != model.OutcomeCommitted {
			t.Fatalf("row %d outcome %v, want Committed", i, o)
		}
	}
}

// --- tests ---

func TestPoolCommitsABatchAndReturnsIt(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	r.start()
	r.batchQ <- r.batch(0)
	b := r.recv(0)
	allCommitted(t, b)
	if c := r.f.Counts(); c.Begins != 1 || c.Commits != 1 {
		t.Errorf("begins/commits = %d/%d, want 1/1", c.Begins, c.Commits)
	}
	if got := r.inst.WritesSucceeded.Load(); got != 1 {
		t.Errorf("writes_succeeded = %d, want 1", got)
	}
}

// TestG_STMT_1_SameStatementOnTwoWritersStopsThePool is gate G-STMT-1 as
// Vol 4 words it.
//
// SETUP: two writers, a database that answers every MERGE with ORA-00942.
// STIMULUS: one batch (it goes to one writer), then a second identical one.
// ASSERTION: after the first, that writer retires (writers_stopped 1, one
// still running) and the pool has NOT stopped -- one writer is
// indistinguishable from a session problem; after the second, on the other
// writer, Stopped is closed with a *StopError of kind statement naming the
// hand-written identity d0/m3 (destination 0, lowest mask, which phase 1
// executes first), matching oradb.ErrPoolStopped. Both batches came back with
// an Err and every row Failed with ErrCode 942.
// WOULD HAVE CAUGHT: retiring writers one by one until the pool is empty
// while the process looks healthy; stopping on the first failure; counting the
// same writer twice.
func TestG_STMT_1_SameStatementOnTwoWritersStopsThePool(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 2})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if isMerge(c) {
			return oratest.Result{Err: oratest.ORA(942)}
		}
		return oratest.Result{}
	})
	r.start()

	r.batchQ <- r.batch(0)
	b1 := r.recv(0)
	assertStatementFailure(t, b1)
	eventually(t, "the first writer to retire", func() bool { return r.live() == 1 })
	if stopped(r.p) {
		t.Fatal("pool stopped after one writer saw the failure")
	}
	if got := r.inst.WritersStopped.Load(); got != 1 {
		t.Errorf("writers_stopped = %d, want 1", got)
	}

	r.batchQ <- r.batch(0)
	b2 := r.recv(0)
	assertStatementFailure(t, b2)
	eventually(t, "Stopped", func() bool { return stopped(r.p) })

	var se *StopError
	if !errors.As(r.p.StopReason(), &se) {
		t.Fatalf("StopReason = %v, want *StopError", r.p.StopReason())
	}
	if se.Kind != StopStatement || se.Identity != "d0/m3" {
		t.Errorf("stop = %v identity %q, want statement d0/m3", se.Kind, se.Identity)
	}
	if !errors.Is(se, oradb.ErrPoolStopped) {
		t.Errorf("a statement stop does not match oradb.ErrPoolStopped")
	}
	// Non-vacuity: the first failure alone did not stop it (checked above), and
	// the stop is not the zero-running rule: one writer was still running.
	if se.Kind == StopNoWriters {
		t.Error("stopped by the no-writers rule, not the statement rule")
	}
}

func assertStatementFailure(t *testing.T, b *model.Batch) {
	t.Helper()
	var oe *oradb.Error
	if !errors.As(b.Err, &oe) || oe.Class != oradb.ErrStatement || oe.Code != 942 {
		t.Fatalf("Err = %v, want statement ORA-00942", b.Err)
	}
	for i, row := range b.Active() {
		if row.Outcome != model.OutcomeFailed || row.ErrCode != 942 {
			t.Fatalf("row %d = %v/%d, want Failed/942", i, row.Outcome, row.ErrCode)
		}
	}
}

// TestG_STMT_1_DifferentIdentitiesNeverCombine is the non-vacuity half: two
// writers failing on DIFFERENT statements do not stop the pool.
func TestG_STMT_1_DifferentIdentitiesNeverCombine(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 3})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if isMerge(c) {
			return oratest.Result{Err: oratest.ORA(942)}
		}
		return oratest.Result{}
	})
	r.start()
	b1 := mkBatch([3][][2]uint64{{{3, 1}}, nil, nil})
	b2 := mkBatch([3][][2]uint64{{{7, 1}}, nil, nil})
	r.batchQ <- b1
	r.recv(0)
	eventually(t, "a writer to retire", func() bool { return r.live() == 2 })
	r.batchQ <- b2
	r.recv(0)
	eventually(t, "a second writer to retire", func() bool { return r.live() == 1 })
	time.Sleep(20 * time.Millisecond)
	if stopped(r.p) {
		t.Fatalf("pool stopped on two different identities: %v", r.p.StopReason())
	}
	if r.p.StopReason() != nil {
		t.Errorf("StopReason = %v before the pool stopped", r.p.StopReason())
	}
}

// TestZeroRunningWritersStopsThePool is ruling D4: when the last writer
// retires itself nothing will ever write again, so the pool stops.
//
// SETUP: one writer, default statementFailWriters 2, every MERGE fails with
// ORA-00942.
// STIMULUS: one batch.
// ASSERTION: the writer retires, and Stopped closes with kind no_writers (one
// writer cannot satisfy the two-writer statement rule), matching ErrPoolStopped.
// WOULD HAVE CAUGHT: a pool that retires its only member and then sits "up"
// with batchQ unread.
func TestZeroRunningWritersStopsThePool(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 1})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if isMerge(c) {
			return oratest.Result{Err: oratest.ORA(942)}
		}
		return oratest.Result{}
	})
	r.start()
	r.batchQ <- r.batch(0)
	r.recv(0)
	eventually(t, "Stopped", func() bool { return stopped(r.p) })
	var se *StopError
	if !errors.As(r.p.StopReason(), &se) || se.Kind != StopNoWriters {
		t.Fatalf("StopReason = %v, want no_writers", r.p.StopReason())
	}
	if !errors.Is(se, oradb.ErrPoolStopped) {
		t.Error("a no-writers stop does not match ErrPoolStopped")
	}
}

// TestB_V3b_BatchIsReturnedBeforeTheBackoffSleep is the deterministic form of
// V3 9.2's MUST: a writer never holds a batch across a backoff sleep.
//
// SETUP: one writer, a database that fails every Begin with ORA-03113, a
// two-shard completion layout; the injected sleep records len(completion[1])
// when it is called and returns at once.
// STIMULUS: one batch for shard 1.
// ASSERTION: the sleep ran and saw exactly 1 batch already waiting on
// completion[1], for a duration inside the 100 ms +-20% initial backoff; the
// batch has Err set and every row Failed; the probe that follows restores
// Healthy (last class Session) and counts write_probe ok.
// WOULD HAVE CAUGHT: sleeping before the send, which pins one of the shard's
// two in-flight slots for up to five seconds (HLD 7.12 risk 9).
func TestB_V3b_BatchIsReturnedBeforeTheBackoffSleep(t *testing.T) {
	r := newPoolRig(t, rigOpts{shards: 2})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpBegin {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	slept := make(chan [2]int64, 4)
	r.p.sleep = func(d time.Duration) { slept <- [2]int64{int64(len(r.comp[1])), int64(d)} }
	r.start()

	r.batchQ <- r.batch(1)
	select {
	case got := <-slept:
		if got[0] != 1 {
			t.Errorf("len(completion[1]) at sleep = %d, want 1: the batch was held across the sleep", got[0])
		}
		if d := time.Duration(got[1]); d < 80*time.Millisecond || d > 120*time.Millisecond {
			t.Errorf("first backoff = %v, want 100ms +-20%%", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never slept")
	}
	b := r.recv(1)
	var oe *oradb.Error
	if !errors.As(b.Err, &oe) || oe.Class != oradb.ErrSession {
		t.Fatalf("Err = %v, want a session failure", b.Err)
	}
	for i, row := range b.Active() {
		if row.Outcome != model.OutcomeFailed {
			t.Errorf("row %d outcome %v, want Failed", i, row.Outcome)
		}
	}
	eventually(t, "the probe to restore Healthy", func() bool {
		return r.inst.Probe[obs.ProbeOK].Load() == 1 && r.p.health.State() == HealthHealthy
	})
	if got := r.inst.HealthState.Load(); got != int64(HealthHealthy) {
		t.Errorf("writer_health_state = %d, want Healthy", got)
	}
}

// TestRetriedThenSuccessfulBatchCountsOneRetryAndTouchesNoHealth: D6c.
func TestRetriedThenSuccessfulBatchCountsOneRetryAndTouchesNoHealth(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpBegin && c.N == 1 {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	var sleeps atomic.Int32
	r.p.sleep = func(time.Duration) { sleeps.Add(1) }
	r.start()
	r.batchQ <- r.batch(0)
	allCommitted(t, r.recv(0))
	if got := r.inst.WriteRetry[obs.RetrySession].Load(); got != 1 {
		t.Errorf("write_retry{session} = %d, want 1", got)
	}
	if r.p.health.degraded.Load() || r.p.health.State() != HealthHealthy {
		t.Error("a retried-then-successful batch changed the pool's health")
	}
	if got := r.inst.WritesFailed.Load(); got != 0 {
		t.Errorf("writes_failed = %d, want 0", got)
	}
	if got := r.inst.ConnRecycled[obs.RecycleError].Load(); got != 1 {
		t.Errorf("writer_conn_recycled{error} = %d, want 1 (the session was retired)", got)
	}
	time.Sleep(10 * time.Millisecond)
	if sleeps.Load() != 0 {
		t.Errorf("slept %d times after a successful batch", sleeps.Load())
	}
}

// TestNoPostSendAccess is the -race test for "then no further access": the
// completion consumer rewrites every field of the batch the instant it
// receives it (exactly what a shard does when it recycles one) and sends it
// straight back, 300 times over two writers. A writer that read or wrote the
// batch after its send is a data race, and the detector reports it.
//
// WOULD HAVE CAUGHT: counting b.N or reading b.Err after the send; logging the
// batch's epoch after it.
func TestNoPostSendAccess(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 2, compCap: 4})
	r.start()
	const rounds = 300
	r.batchQ <- r.batch(0)
	r.batchQ <- r.batch(0)
	done := 0
	for done < rounds {
		b := r.recv(0)
		allCommitted(t, b)
		scribble(b) // only meaningful under -race
		done++
		if done+1 < rounds {
			r.batchQ <- b
		}
	}
}

// TestG_CFG_7_ResizeGrowsAndShrinksTheLivePool is gate G-CFG-7 in full via
// Resize.
//
// SETUP: four writers; every commit blocks on a gate so batches stay in
// flight.
// STIMULUS: Resize(8); eight batches, one per writer, all held at commit;
// Resize(4); open the gate; Resize(8) again.
// ASSERTION: after the first Resize eight writers run, database/sql allows
// 9 open connections (writers+1) and writers_configured is 8; after the
// shrink the four leavers finish and return their batches (eight completions,
// none with an Err) and only then exit -- four remain, 5 open connections;
// the regrow brings it back to eight and 9. DBStats exposes no idle limit,
// so MaxIdle (set by the same call as MaxOpen) is not asserted.
// WOULD HAVE CAUGHT: a fixed pool that makes the extra writers block inside
// database/sql; a shrink that abandons a transaction; a regrow that compared
// with the wrong count.
func TestG_CFG_7_ResizeGrowsAndShrinksTheLivePool(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 4, compCap: 16})
	g := newGate()
	r.f.SetScript(g.script)
	r.start()
	if got := r.db.Stats().MaxOpenConnections; got != 5 {
		t.Fatalf("MaxOpen after Start = %d, want 5", got)
	}
	if err := r.p.Resize(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
	if r.live() != 8 || r.db.Stats().MaxOpenConnections != 9 || r.inst.WritersConfigured.Load() != 8 || r.inst.WritersRunning.Load() != 8 {
		t.Fatalf("after Resize(8): live %d, MaxOpen %d, configured %d, running %d; want 8, 9, 8, 8",
			r.live(), r.db.Stats().MaxOpenConnections, r.inst.WritersConfigured.Load(), r.inst.WritersRunning.Load())
	}
	for range 8 {
		r.batchQ <- r.batch(0)
	}
	for range 8 {
		<-g.entered
	}
	if err := r.p.Resize(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	if r.live() != 8 {
		t.Errorf("a shrink removed writers mid-batch: %d live, want 8 until their batches return", r.live())
	}
	if got := r.db.Stats().MaxOpenConnections; got != 5 {
		t.Errorf("MaxOpen after shrink = %d, want 5", got)
	}
	close(g.open)
	for range 8 {
		allCommitted(t, r.recv(0))
	}
	eventually(t, "the four leavers to exit", func() bool { return r.live() == 4 })
	r.p.mu.Lock()
	for id := range 4 { // the highest ids leave first
		if r.p.live[id] == nil {
			t.Errorf("writer %d left on a shrink to 4; the survivors must be 0..3", id)
		}
	}
	r.p.mu.Unlock()

	if err := r.p.Resize(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
	if r.live() != 8 || r.db.Stats().MaxOpenConnections != 9 {
		t.Errorf("after regrow: live %d, MaxOpen %d, want 8 and 9", r.live(), r.db.Stats().MaxOpenConnections)
	}
	if got := r.inst.WritersRunning.Load(); got != 8 {
		t.Errorf("writers_running = %d, want 8", got)
	}
	// The regrown pool still works: every one of eight batches completes.
	for range 8 {
		r.batchQ <- r.batch(0)
	}
	for range 8 {
		allCommitted(t, r.recv(0))
	}
}

func TestResizeRejectsBelowStatementFailWritersAndBeforeStart(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 2})
	if err := r.p.Resize(context.Background(), 3); err == nil {
		t.Error("Resize before Start succeeded")
	}
	r.start()
	if err := r.p.Resize(context.Background(), 1); err == nil {
		t.Error("Resize(1) below statementFailWriters 2 succeeded")
	}
	if r.live() != 2 {
		t.Errorf("a rejected Resize changed the pool: %d live", r.live())
	}
}

// TestG_CFG_7_PoolRegrowsAfterSelfStops: D4's other half. A writer that
// retired itself is replaced by Resize(n) with the same n.
func TestResizeRegrowsAfterSelfStop(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 3})
	var fail atomic.Bool
	fail.Store(true)
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if isMerge(c) && fail.Load() {
			return oratest.Result{Err: oratest.ORA(942)}
		}
		return oratest.Result{}
	})
	r.start()
	r.batchQ <- r.batch(0)
	r.recv(0)
	eventually(t, "a writer to retire", func() bool { return r.live() == 2 })
	fail.Store(false)
	if err := r.p.Resize(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if r.live() != 3 {
		t.Errorf("live = %d after Resize(3), want 3", r.live())
	}
}

// TestG_LIFE_4_WriterHalfEveryWriterReturnsWithNobodyReading is gate G-LIFE-4's
// writer half.
//
// SETUP: two shards, each completion channel buffered to
// maxInFlightPerShard (2), two writers; nobody ever reads a completion.
// STIMULUS: maxInFlightPerShard batches per shard (4 in all), then Close.
// ASSERTION: Close returns nil within its deadline -- every writer sent
// without blocking and left -- and each channel still holds its two batches.
// Non-vacuity: with a channel of capacity 1 and two batches for that shard (an
// in-flight allowance the shard would never exceed) the second send blocks, and
// Close then reports the deadline; reading one completion frees it.
// WOULD HAVE CAUGHT: an unbuffered completion channel, or a send that waits for
// the shard's loop, which would deadlock shutdown against a shard that has
// stopped reading.
func TestG_LIFE_4_WriterHalfEveryWriterReturnsWithNobodyReading(t *testing.T) {
	// inflight mirrors internal/app's maxInFlightPerShard, which this package
	// cannot import; change them together.
	const inflight = 2
	r := newPoolRig(t, rigOpts{writers: 2, shards: 2, compCap: inflight})
	r.start()
	for range inflight {
		r.batchQ <- r.batch(0)
		r.batchQ <- r.batch(1)
	}
	eventually(t, "all four batches to be returned", func() bool {
		return len(r.comp[0]) == inflight && len(r.comp[1]) == inflight
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.p.Close(ctx); err != nil {
		t.Fatalf("Close with nobody reading completions: %v", err)
	}
	if r.live() != 0 {
		t.Errorf("%d writers still live after Close", r.live())
	}

	// Non-vacuity.
	r2 := newPoolRig(t, rigOpts{writers: 1, compCap: 1})
	r2.start()
	r2.batchQ <- r2.batch(0)
	r2.batchQ <- r2.batch(0)
	eventually(t, "the first batch", func() bool { return len(r2.comp[0]) == 1 })
	short, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	// Close signals the writer, which is blocked sending the second batch.
	if err := r2.p.Close(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want the deadline: the send must block when the shard exceeds its allowance", err)
	}
	<-r2.comp[0]
	long, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	if err := r2.p.Close(long); err != nil {
		t.Fatalf("Close after reading one completion: %v", err)
	}
}

func TestStartRejectsAnUnbufferedCompletionChannel(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	err := r.p.Start(context.Background(), r.batchQ, []chan<- *model.Batch{make(chan *model.Batch)})
	if err == nil {
		t.Fatal("Start accepted an unbuffered completion channel")
	}
	if r.live() != 0 {
		t.Error("writers were started despite the rejection")
	}
	if err := r.p.Start(context.Background(), r.batchQ, nil); err == nil {
		t.Error("Start accepted no completion channels")
	}
	r.start() // a rejected Start leaves the pool startable
	if err := r.p.Start(context.Background(), r.batchQ, []chan<- *model.Batch{r.comp[0]}); err == nil {
		t.Error("a second Start succeeded")
	}
}

// TestFailFastKeepsDrainingWithErrSet: D2.
//
// SETUP: failFastWrites 1 and a database whose every Begin fails with ORA-03113.
// STIMULUS: one batch (trips FailFast), then three more.
// ASSERTION: Stopped closes with kind fail_fast (not matching ErrPoolStopped);
// the three later batches come back at once with Err set, every row Failed,
// and the database sees no further Begin; the writer is still running; closing
// batchQ ends it.
// WOULD HAVE CAUGHT: writers exiting at FailFast, which strands the shards'
// in-flight batches and wedges their epochs.
func TestFailFastKeepsDrainingWithErrSet(t *testing.T) {
	r := newPoolRig(t, rigOpts{par: func(p *Params) { p.FailFastWrites = 1 }})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpBegin {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	r.p.sleep = func(time.Duration) {}
	r.start()
	r.batchQ <- r.batch(0)
	r.recv(0)
	eventually(t, "Stopped", func() bool { return stopped(r.p) })
	var se *StopError
	if !errors.As(r.p.StopReason(), &se) || se.Kind != StopFailFast {
		t.Fatalf("StopReason = %v, want fail_fast", r.p.StopReason())
	}
	if errors.Is(se, oradb.ErrPoolStopped) {
		t.Error("a fail-fast stop matches ErrPoolStopped")
	}
	begins := r.f.Counts().Begins
	for range 3 {
		r.batchQ <- r.batch(0)
		b := r.recv(0)
		if b.Err == nil {
			t.Fatal("a batch in FailFast came back with Err nil")
		}
		for i, row := range b.Active() {
			if row.Outcome != model.OutcomeFailed {
				t.Fatalf("row %d outcome %v, want Failed", i, row.Outcome)
			}
		}
	}
	if got := r.f.Counts().Begins; got != begins {
		t.Errorf("FailFast batches reached the database: %d -> %d begins", begins, got)
	}
	if r.live() != 1 {
		t.Errorf("%d writers live in FailFast, want 1 (they keep receiving)", r.live())
	}
	close(r.batchQ)
	eventually(t, "the writer to exit on batchQ close", func() bool { return r.live() == 0 })
	if se2 := r.p.StopReason(); se2 != error(se) {
		t.Errorf("StopReason changed to %v after the writers exited", se2)
	}
}

// TestQueuedBatchesAreAllProcessedBeforeWritersExitOnBatchQClose and the
// goroutine baseline.
func TestQueuedBatchesAreAllProcessedBeforeWritersExitOnBatchQClose(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 3, compCap: 32})
	base := runtime.NumGoroutine()
	const n = 25
	for range n {
		r.batchQ <- r.batch(0)
	}
	close(r.batchQ)
	r.start()
	for range n {
		allCommitted(t, r.recv(0))
	}
	eventually(t, "the writers to exit", func() bool { return r.live() == 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.inst.BatchesWritten.Load(); got != n {
		t.Errorf("batches_written = %d, want %d", got, n)
	}
	if r.p.StopReason() != nil {
		t.Errorf("a clean close set StopReason %v", r.p.StopReason())
	}
	eventually(t, "the goroutine count to return to its baseline", func() bool { return runtime.NumGoroutine() <= base })
}

func TestCloseOnANeverStartedPoolIsANoOp(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.p.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := r.p.Start(context.Background(), r.batchQ, []chan<- *model.Batch{r.comp[0]}); err == nil {
		t.Error("Start succeeded on a closed pool")
	}
}

// TestCloseFinishesWhatIsQueuedThenWritersLeave: Close without closing batchQ.
func TestCloseFinishesWhatIsQueuedThenWritersLeave(t *testing.T) {
	r := newPoolRig(t, rigOpts{compCap: 8})
	g := newGate()
	r.f.SetScript(g.script)
	r.start()
	for range 3 {
		r.batchQ <- r.batch(0)
	}
	<-g.entered // the writer holds batch 1; two are queued
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- r.p.Close(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	close(g.open)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if b := r.recv(0); b.Err != nil {
			t.Fatalf("batch %d: %v", i, b.Err)
		}
	}
}

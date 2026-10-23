package mirror

import (
	"bytes"
	"errors"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// rigOpts shapes a runRig. Zero values take the defaults newRunRig documents.
type rigOpts struct {
	shards    int           // FlushBus width; default 4
	m         int           // maxInFlightPerShard; default 2
	maxBatch  int           // flush.maxBatchSize; default 1000
	inCap     int           // shardCh[0] capacity; default 64
	qCap      int           // batchQ capacity; default 8, and -1 means unbuffered
	writers   int           // fake writers; default 2, and -1 means none
	stallWarn time.Duration // flush.epochStallWarnMs; default 30 s
	mode      fakeMode      // default allCommitted
}

// runRig is shard 0 wired to real channels the way app will wire it (P3-13),
// with fake writers on batchQ: the test drives in, tick and drainExpired
// directly, and Run runs on its own goroutine. While Run runs the test reads
// only atomics (the instruments, the bus) and the writers' record; the shard's
// own fields are read after wait has returned.
type runRig struct {
	t     testing.TB
	s     *Shard
	in    chan model.UpdateUnit
	tick  chan struct{}
	q     chan *model.Batch
	comp  chan *model.Batch // completionCh[0], capacity m
	drain chan struct{}
	w     *fakeWriter
	rec   *batchLog
	log   *bytes.Buffer // written only by the shard; read after wait

	reportMu sync.Mutex
	reports  []error

	done             chan error
	seq              int32
	closeIn, closeDr func()
}

// batchLog is what the writers saw, in the order they received it: each
// batch's epoch and row keys, copied while the writer owned the batch.
type batchLog struct {
	mu      sync.Mutex
	epochs  []uint64
	batches [][]model.RowKey
}

func (l *batchLog) snapshot() ([]uint64, [][]model.RowKey) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.epochs), slices.Clone(l.batches)
}

func (l *batchLog) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.batches)
}

// recording records each batch on arrival, then waits and adjudicates as mode
// does. The writer owns the batch while it reads it.
func recording(l *batchLog, mode fakeMode) fakeMode {
	return fakeMode{
		hold: mode.hold,
		wait: func() {
			if mode.wait != nil {
				mode.wait()
			}
		},
		adjudicate: func(b *model.Batch) {
			l.mu.Lock()
			l.epochs = append(l.epochs, b.Epoch)
			l.batches = append(l.batches, rowsOf(b))
			l.mu.Unlock()
			mode.adjudicate(b)
		},
	}
}

// newRunRig builds the rig; Run is not started (start does that). Cleanup
// closes in and the drain deadline, waits for Run, then stops the writers --
// a test that holds batches registers its own release after this, so it runs
// first.
func newRunRig(t testing.TB, o rigOpts) *runRig {
	t.Helper()
	def := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	def(&o.shards, 4)
	def(&o.m, 2)
	def(&o.maxBatch, 1000)
	def(&o.inCap, 64)
	def(&o.qCap, 8)
	def(&o.writers, 2)
	if o.stallWarn == 0 {
		o.stallWarn = 30 * time.Second
	}
	if o.mode.adjudicate == nil {
		o.mode = allCommitted()
	}

	dst, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	sz := smallSizing(256)
	sz.Shards, sz.MaxInFlightPerShard = o.shards, o.m
	codes := configuredCodes(dst)
	inst := fullInstruments(sz.Shards, len(codes))
	guards := NewGuardTable(codes, inst.Transcode.GuardOffered, inst.Transcode.GuardSuperseded)

	r := &runRig{
		t:     t,
		in:    make(chan model.UpdateUnit, o.inCap),
		tick:  make(chan struct{}, 1),
		q:     make(chan *model.Batch, max(o.qCap, 0)),
		drain: make(chan struct{}),
		rec:   &batchLog{},
		log:   &bytes.Buffer{},
		done:  make(chan error, 1),
	}
	r.closeIn = sync.OnceFunc(func() { close(r.in) })
	r.closeDr = sync.OnceFunc(func() { close(r.drain) })
	p := &Params{Gen: 1, MaxBatchSize: o.maxBatch, EpochStallWarn: o.stallWarn}
	r.comp = make(chan *model.Batch, o.m)
	completion := []chan *model.Batch{r.comp}
	report := func(err error) {
		r.reportMu.Lock()
		r.reports = append(r.reports, err)
		r.reportMu.Unlock()
	}
	r.s = NewShard(0, sz, func() *Params { return p }, dst, guards, NewFlushBus(sz.Shards), inst,
		obs.NewAppLogger(r.log, obs.LevelInfo, 1000, nil), report,
		Edges{In: r.in, Completion: completion[0], Tick: r.tick, BatchQ: r.q, DrainExpired: r.drain})
	if o.writers > 0 {
		r.w = startFakeWriters(o.writers, recording(r.rec, o.mode), r.q, completion)
	}

	t.Cleanup(func() {
		r.closeIn()
		r.closeDr()
		select {
		case <-r.done:
		case <-time.After(5 * time.Second):
			t.Error("cleanup: Run did not return; leaving the writers running")
			return
		}
		if r.w != nil {
			close(r.q)
			r.w.wait()
		}
	})
	return r
}

// start runs Run on its own goroutine.
func (r *runRig) start() {
	go func() { r.done <- r.s.Run() }()
}

// wait is Run's return value, failing the test if it takes over 5 s. Once it
// returns, the shard's fields may be read from the test goroutine. It may be
// called once.
func (r *runRig) wait() error {
	r.t.Helper()
	select {
	case err := <-r.done:
		r.done <- err // for cleanup
		return err
	case <-time.After(5 * time.Second):
		r.t.Fatal("Run did not return")
		return nil
	}
}

// running reports whether Run has not returned.
func (r *runRig) running() bool {
	select {
	case err := <-r.done:
		r.done <- err
		return false
	default:
		return true
	}
}

func (r *runRig) reported() []error {
	r.reportMu.Lock()
	defer r.reportMu.Unlock()
	return slices.Clone(r.reports)
}

// unit is a contribution to destination d for row, from code with flags --
// built on the test goroutine (put reads only the immutable mapping) with a
// rising Seq and RecvMono, so the guard always applies it.
func (r *runRig) unit(d model.DestID, code model.TransCode, flags model.UnitFlags, row model.RowKey, f fv) model.UpdateUnit {
	r.seq++
	u := model.UpdateUnit{Code: code, Flags: flags, Seq: r.seq, LogTime: 100, RecvMono: int64(r.seq)}
	put(r.t, r.s, &u, d, row, slotOf(r.t, r.s, d, code), f)
	return u
}

// quote is a rate-limited 7208 unit: ltp v for row of MD_TOKEN_QUOTE.
func (r *runRig) quote(row model.RowKey, v uint32) model.UpdateUnit {
	return r.unit(tokenQuote, 7208, 0, row, fv{0, be32(v)})
}

// master is a 7305 unit for row of MD_SECURITY_MASTER, non-rate-limited (it
// raises the flush flag) if flush is set.
func (r *runRig) master(row model.RowKey, v uint32, flush bool) model.UpdateUnit {
	var flags model.UnitFlags
	if flush {
		flags = model.FlagNonRateLimited
	}
	return r.unit(securityMaster, 7305, flags, row, fv{3, be32(v)})
}

// eventually polls cond for up to 5 s.
func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// merged waits until n units have been merged. A test sends a tick only after
// it: in and tick both ready is a coin toss in select, and a tick taken first
// finds nothing dirty and is consumed.
func (r *runRig) merged(n uint64) {
	r.t.Helper()
	eventually(r.t, "the units merged", counterIs(r.s.inst.UnitsApplied, n))
}

// counterIs is a condition for eventually: c reads n.
func counterIs(c *obs.Counter, n uint64) func() bool { return func() bool { return c.Load() == n } }

// gated is a mode whose writers each wait for one token on gate, or for open
// to be closed, then adjudicate with adj.
func gated(gate chan struct{}, open <-chan struct{}, adj func(*model.Batch)) fakeMode {
	return fakeMode{
		wait: func() {
			select {
			case <-gate:
			case <-open:
			}
		},
		adjudicate: adj,
	}
}

// TestG_FLUSH_1_RequestOnEmptyRingsOpensOnTheNextUnit is gate G-FLUSH-1
// (V4:1632), through Run.
//
// SETUP: every ring empty; req[0] raised (with its wake token) before Run
// starts; the tick channel is never fed.
//
// STIMULUS: Run takes the wake and finds nothing to flush (epoch_empty 1);
// then one rate-limited unit arrives.
//
// ASSERTION: an epoch opens on that unit's pass -- epoch_opened 1, with no
// tick (flush_on_tick 0) -- and the batch carries the row, so the empty-ring
// pass did not consume the request; req[0] is cleared by the open.
//
// Non-vacuity: epoch_empty is 1 before the unit is sent, so the request really
// met empty rings first.
//
// WOULD HAVE CAUGHT: HLD section 7.3's ordering defect (req cleared before the
// ring test, so the unit waits a full W for a tick that never comes here), and
// a loop whose level check ran only in the wake arm (L1).
func TestG_FLUSH_1_RequestOnEmptyRingsOpensOnTheNextUnit(t *testing.T) {
	r := newRunRig(t, rigOpts{})
	r.s.bus.Trigger(&r.s.busInst)
	r.start()

	eventually(t, "the wake to meet empty rings", counterIs(r.s.inst.EpochEmpty, 1))
	if r.s.inst.EpochOpened.Load() != 0 || !r.s.bus.Pending(0) {
		t.Fatal("setup: an epoch opened on empty rings, or the request was consumed by epoch_empty")
	}

	r.in <- r.quote(7, 100)
	eventually(t, "an epoch on the unit's pass", counterIs(r.s.inst.EpochOpened, 1))
	eventually(t, "the batch", func() bool { return r.rec.len() == 1 })
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if _, b := r.rec.snapshot(); !slices.Equal(b[0], []model.RowKey{7}) {
		t.Fatalf("batch carries %v, want [7]", b[0])
	}
	if n := r.s.inst.FlushOnTick.Load(); n != 0 {
		t.Fatalf("flush_on_tick = %d with no tick ever sent", n)
	}
	if r.s.inst.EpochOpened.Load() != 1 || r.s.bus.Pending(0) {
		t.Fatalf("epoch_opened %d, req[0] %v; want 1 and cleared by the open",
			r.s.inst.EpochOpened.Load(), r.s.bus.Pending(0))
	}
}

// TestG_FLUSH_2_RequestsDuringAFlushCoalesce is gate G-FLUSH-2 (V4:1633, as
// amended by ruling D7).
//
// SETUP: one epoch open on a tick, its one batch held by the writer.
//
// STIMULUS: N non-rate-limited units, each dirtying a fresh row and raising a
// flush request; then the batch is released.
//
// ASSERTION: exactly one follow-up epoch (epoch_opened 2 when the drain
// returns), carrying all N rows; flush_coalesced == N-1 on a one-shard bus and
// (N-1)*S on an S-shard bus whose other shards are idle, because Trigger
// counts per already-set bit and nobody clears the idle shards' bits;
// flush_on_tick 1 -- the first epoch, not the request-driven follow-up.
//
// Non-vacuity: flush_triggered == N, and no epoch opened while the batch was
// held.
//
// WOULD HAVE CAUGHT: the flag turned back into a queue -- N follow-up flushes,
// the A x S commit demand the flag exists to bound (HLD section 6.6).
func TestG_FLUSH_2_RequestsDuringAFlushCoalesce(t *testing.T) {
	const n = 5
	for _, shards := range []int{1, 4} {
		release := make(chan struct{})
		r := newRunRig(t, rigOpts{shards: shards, mode: holdUntil(release)})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		t.Cleanup(releaseOnce)
		r.start()

		r.in <- r.quote(1, 100)
		r.merged(1)
		r.tick <- struct{}{}
		r.w.waitHeld(t, 1)
		var want []model.RowKey
		for i := range model.RowKey(n) {
			r.in <- r.master(100+i, uint32(i), true)
			want = append(want, 100+i)
		}
		eventually(t, "the N units merged", counterIs(r.s.inst.UnitsApplied, 1+n))
		if r.s.inst.EpochOpened.Load() != 1 || r.s.inst.FlushTriggered.Load() != n {
			t.Fatalf("S=%d setup: epoch_opened %d flush_triggered %d; want 1 and %d",
				shards, r.s.inst.EpochOpened.Load(), r.s.inst.FlushTriggered.Load(), n)
		}

		releaseOnce()
		eventually(t, "the follow-up batch", func() bool { return r.rec.len() == 2 })
		r.closeIn()
		if err := r.wait(); err != nil {
			t.Fatalf("S=%d Run: %v", shards, err)
		}
		if got := r.s.inst.EpochOpened.Load(); got != 2 {
			t.Errorf("S=%d: epoch_opened = %d, want 2: exactly one follow-up flush", shards, got)
		}
		if got := r.s.inst.FlushOnTick.Load(); got != 1 {
			t.Errorf("S=%d: flush_on_tick = %d, want 1: only the first epoch was opened by a tick", shards, got)
		}
		if got, want := r.s.inst.FlushCoalesced.Load(), uint64((n-1)*shards); got != want {
			t.Errorf("S=%d: flush_coalesced = %d, want %d", shards, got, want)
		}
		if _, b := r.rec.snapshot(); !slices.Equal(b[1], want) {
			t.Errorf("S=%d: follow-up batch carries %v, want %v", shards, b[1], want)
		}
	}
}

// TestG_FLUSH_3_WakeArmOpensAParkedShard is gate G-FLUSH-3 (V4:1634; the
// "within microseconds" is informational, the V4:1634 erratum).
//
// SETUP: one rate-limited row dirty; no tick ever sent, so the shard parks in
// select with work it has no reason to flush.
//
// STIMULUS: another goroutine calls Trigger.
//
// ASSERTION: the wake arm is taken (flush_wake_received{0} 1) and an epoch
// opens; flush_on_tick 0, so the tick was not the cause. Elapsed time is
// logged, not asserted -- it flakes under -race -count=20.
//
// Non-vacuity: 10 ms after the row merged, no epoch had opened.
//
// WOULD HAVE CAUGHT: defect L1 -- a flag with no wakeup, the shard parked until
// its next tick (forever, here).
func TestG_FLUSH_3_WakeArmOpensAParkedShard(t *testing.T) {
	r := newRunRig(t, rigOpts{})
	r.start()
	r.in <- r.quote(3, 100)
	eventually(t, "the unit merged", counterIs(r.s.inst.UnitsApplied, 1))
	time.Sleep(10 * time.Millisecond)
	if r.s.inst.EpochOpened.Load() != 0 {
		t.Fatal("setup: an epoch opened without a tick or a request")
	}

	began := time.Now()
	go r.s.bus.Trigger(&r.s.busInst)
	eventually(t, "the wake-driven epoch", counterIs(r.s.inst.EpochOpened, 1))
	t.Logf("trigger to epoch open: %v", time.Since(began))
	eventually(t, "the batch", func() bool { return r.rec.len() == 1 })
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := r.s.inst.FlushWakeReceived.Load(); got != 1 {
		t.Errorf("flush_wake_received = %d, want 1: the wake arm was not the cause", got)
	}
	if got := r.s.inst.FlushOnTick.Load(); got != 0 {
		t.Errorf("flush_on_tick = %d with no tick ever sent", got)
	}
}

// TestG_EPOCH_3_AFailingRowNeverHoldsTheEpochOpen is gate G-EPOCH-3 (V4:1637).
//
// SETUP: row A, whose every commit fails (the writer marks it Failed); ticks
// every 10 ms. A's first batch is held while row B is merged, so B is ahead of
// A on the accumulating ring until A's failure comes home.
//
// STIMULUS: A's batch is released; epochs keep running.
//
// ASSERTION: every batch is its own epoch, numbered consecutively (each epoch
// closed before the next opened); A heads every batch, the second being
// [A, B]; epoch_opened keeps rising past 6.
//
// Non-vacuity: B was merged while A was out, so a tail push would have put B
// first.
//
// WOULD HAVE CAUGHT: a permanently failing row holding its epoch open forever
// (no tick then fires, the cadence stops silently), and the head push placed
// after the close (ruling D8) or at the tail.
func TestG_EPOCH_3_AFailingRowNeverHoldsTheEpochOpen(t *testing.T) {
	const a, b = model.RowKey(1), model.RowKey(2)
	var failA atomic.Bool
	failA.Store(true)
	gate, open := make(chan struct{}), make(chan struct{})
	r := newRunRig(t, rigOpts{mode: gated(gate, open, func(bt *model.Batch) {
		commitAll(bt)
		for i := range bt.Active() {
			if bt.Rows[i].Row == a && failA.Load() {
				bt.Rows[i].Outcome = model.OutcomeFailed
			}
		}
	})})
	openOnce := sync.OnceFunc(func() { close(open) })
	t.Cleanup(openOnce)
	r.start()

	r.in <- r.quote(a, 1)
	r.merged(1)
	r.tick <- struct{}{}
	r.w.waitHeld(t, 1)
	r.in <- r.quote(b, 2)
	eventually(t, "B merged while A is out", counterIs(r.s.inst.UnitsApplied, 2))

	stop := make(chan struct{})
	ticker := sync.WaitGroup{}
	ticker.Add(1)
	go func() {
		defer ticker.Done()
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				select {
				case r.tick <- struct{}{}:
				default:
				}
			}
		}
	}()
	openOnce()
	eventually(t, "epochs to keep opening", func() bool { return r.s.inst.EpochOpened.Load() >= 6 })
	failA.Store(false)
	close(stop)
	ticker.Wait()
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	epochs, batches := r.rec.snapshot()
	for i, rows := range batches {
		if epochs[i] != uint64(i+1) {
			t.Fatalf("batch %d is of epoch %d, want %d: an epoch did not close before the next", i, epochs[i], i+1)
		}
		if rows[0] != a {
			t.Fatalf("batch %d = %v: A does not head it", i, rows)
		}
	}
	if !slices.Equal(batches[1], []model.RowKey{a, b}) {
		t.Fatalf("second batch = %v, want [A B]: the failed row did not head the next cohort", batches[1])
	}
	if got := r.s.inst.RetryRequeued.Load(); got < 5 {
		t.Fatalf("retry_requeued = %d, want >= 5", got)
	}
}

// TestG_EPOCH_4_EveryDirtiedRowIsPumped is gate G-EPOCH-4 (V4:1638).
//
// SETUP: m = 2, maxBatchSize 4, two writers committing.
//
// STIMULUS: M = 10 intervals, each 30 random units -- rate-limited 7208 and
// non-rate-limited 7305 over 20 rows each -- then a tick; then the drain.
//
// ASSERTION: Σ epoch_rows >= rows_dirtied, nothing abandoned, Run returns nil.
//
// Non-vacuity: rows_dirtied > M, a property of the workload alone, checked
// first; that more than one epoch opened is checked after the gate's claim, so
// a swapped-rings regression is reported as the claim failing, not as a
// vacuous test.
//
// WOULD HAVE CAUGHT: HLD section 7.12 risk 10, the swapped rings -- pumping
// from Accum, or swapping the wrong way, leaves dirtied rows unpumped, so
// epoch_rows falls short (and the drain runs into its deadline, which this
// test sets at 4 s rather than letting it hang).
func TestG_EPOCH_4_EveryDirtiedRowIsPumped(t *testing.T) {
	const intervals, perInterval, rows = 10, 30, 20
	r := newRunRig(t, rigOpts{maxBatch: 4})
	r.start()
	rng := rand.New(rand.NewSource(4))
	for range intervals {
		for range perInterval {
			row := model.RowKey(rng.Intn(rows))
			if rng.Intn(4) == 0 {
				r.in <- r.master(row, rng.Uint32(), true)
			} else {
				r.in <- r.quote(row, rng.Uint32())
			}
		}
		r.tick <- struct{}{}
	}
	deadline := time.AfterFunc(4*time.Second, r.closeDr)
	defer deadline.Stop()
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	pumped, dirtied := r.s.inst.EpochRows.Load(), r.s.inst.RowsDirtied.Load()
	if dirtied <= intervals {
		t.Fatalf("non-vacuity: rows_dirtied %d, want > %d", dirtied, intervals)
	}
	if pumped < dirtied {
		t.Fatalf("Σ epoch_rows = %d < rows_dirtied = %d", pumped, dirtied)
	}
	if got := r.s.inst.EpochOpened.Load(); got < 2 {
		t.Errorf("non-vacuity: epoch_opened %d, want >= 2", got)
	}
	for d := range model.MaxDestinations {
		if n := r.s.inst.DrainAbandonedRows[d].Load(); n != 0 {
			t.Errorf("drain_abandoned_rows{%d} = %d, want 0", d, n)
		}
	}
}

// drainCohort dirties rows 1..n of MD_TOKEN_QUOTE through Run, opens one
// epoch with a tick, and waits until every row has been committed. It
// returns the wall time from the tick to the last commit.
func drainCohort(t *testing.T, r *runRig, n int) time.Duration {
	t.Helper()
	r.start()
	for i := range n {
		r.in <- r.quote(model.RowKey(i+1), uint32(i))
	}
	eventually(t, "the rows merged", counterIs(r.s.inst.UnitsApplied, uint64(n)))
	began := time.Now()
	r.tick <- struct{}{}
	eventually(t, "the cohort drained", func() bool {
		return r.s.inst.EpochRows.Load() == uint64(n) && r.s.inst.RowsInflight.Load() == 0
	})
	wall := time.Since(began)
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return wall
}

// TestG_INFL_3_OneInFlightKeepsDirtyOrder is gate G-INFL-3 (V4:1641).
//
// SETUP: m = 1, maxBatchSize 2, two writers with 2 ms latency -- enough
// writers and time for overlap if the shard allowed it.
//
// STIMULUS: rows 1..10 dirtied in order, one tick, the cohort drained.
//
// ASSERTION: the writers never held more than one batch; the batches' rows,
// concatenated, are 1..10 -- every row of batch i dirtied before every row of
// batch i+1; five batches.
//
// WOULD HAVE CAUGHT: concurrency where the configuration forbids it, or the
// drain ceasing to be oldest-first (ADR-015).
func TestG_INFL_3_OneInFlightKeepsDirtyOrder(t *testing.T) {
	const n = 10
	r := newRunRig(t, rigOpts{m: 1, maxBatch: 2, mode: latency(2 * time.Millisecond)})
	drainCohort(t, r, n)

	if got := r.w.maxHeld.Load(); got != 1 {
		t.Errorf("writers held %d batches at once, want 1 (m = 1)", got)
	}
	_, batches := r.rec.snapshot()
	var order []model.RowKey
	for _, b := range batches {
		order = append(order, b...)
	}
	want := make([]model.RowKey, n)
	for i := range want {
		want[i] = model.RowKey(i + 1)
	}
	if len(batches) != n/2 || !slices.Equal(order, want) {
		t.Errorf("%d batches carrying %v, want %d carrying %v", len(batches), order, n/2, want)
	}
}

// TestG_INFL_4_MInFlightDrainsConcurrently is gate G-INFL-4 (V4:1642; the
// "1/m of the wall time" is informational, the V4:1642 erratum).
//
// SETUP: the same 10-row cohort, maxBatchSize 1, four writers with 20 ms
// latency, at m = 1 and at m = 2.
//
// STIMULUS: one tick; the cohort drained.
//
// ASSERTION: at m = 2 the writers held 2 batches at once -- m reached, never
// exceeded; at m = 1, 1. The wall times are logged; the only bound is the
// loose one, m = 2 faster than m = 1.
//
// WOULD HAVE CAUGHT: the pump emitting one batch at a time whatever m says --
// the original defect, which no other gate notices.
func TestG_INFL_4_MInFlightDrainsConcurrently(t *testing.T) {
	const n = 10
	var wall [3]time.Duration
	for _, m := range []int{1, 2} {
		r := newRunRig(t, rigOpts{m: m, maxBatch: 1, writers: 4, mode: latency(20 * time.Millisecond)})
		wall[m] = drainCohort(t, r, n)
		if got := r.w.maxHeld.Load(); got != int32(m) {
			t.Errorf("m = %d: writers held at most %d batches at once, want exactly %d", m, got, m)
		}
	}
	t.Logf("cohort of %d: m=1 %v, m=2 %v (%.2fx)", n, wall[1], wall[2], float64(wall[2])/float64(wall[1]))
	if wall[2] >= wall[1] {
		t.Errorf("m = 2 took %v, not less than m = 1's %v", wall[2], wall[1])
	}
}

// TestRunEpochStalledOncePerEpoch is section 2.4's stall detector, on an
// injected clock (configuration forbids epochStallWarnMs below 1 s).
//
// SETUP: epochStallWarn 1 s; the shard's clock is a test counter; epoch 1 open
// at t=0 with its batch held.
//
// STIMULUS: a tick at t=0; ticks at t=2 s twice; then epoch 2 opens at t=2 s,
// a tick there, and one at t=4 s.
//
// ASSERTION: epoch_stalled goes 0, 1, 1, 1, 2 -- one increment and one WARN
// per stalled epoch, however many ticks see it -- while
// tick_skipped_epoch_open counts every tick that lands in an open epoch.
//
// WOULD HAVE CAUGHT: a stall WARN per tick (150 per stalled epoch at W =
// 200 ms), a once-per-process flag never reset at open, or a stall check on
// the wrong clock.
func TestRunEpochStalledOncePerEpoch(t *testing.T) {
	gate, open := make(chan struct{}), make(chan struct{})
	r := newRunRig(t, rigOpts{stallWarn: time.Second, mode: gated(gate, open, commitAll)})
	openOnce := sync.OnceFunc(func() { close(open) })
	t.Cleanup(openOnce)
	var clk atomic.Int64
	r.s.now = clk.Load
	r.start()

	tick := func(skipped, stalled uint64) {
		t.Helper()
		r.tick <- struct{}{}
		eventually(t, "the tick", counterIs(r.s.inst.TickSkippedEpochOpen, skipped))
		// epoch_stalled is counted after tick_skipped_epoch_open in the same
		// arm: wait for it, then make sure it does not overshoot.
		eventually(t, "epoch_stalled", counterIs(r.s.inst.EpochStalled, stalled))
		time.Sleep(2 * time.Millisecond)
		if got := r.s.inst.EpochStalled.Load(); got != stalled {
			t.Fatalf("after tick %d: epoch_stalled = %d, want %d", skipped, got, stalled)
		}
	}
	r.in <- r.quote(1, 1)
	r.merged(1)
	r.tick <- struct{}{}
	r.w.waitHeld(t, 1)
	tick(1, 0) // not yet past the threshold
	clk.Store(int64(2 * time.Second))
	tick(2, 1)
	tick(3, 1)

	r.in <- r.quote(2, 2)
	eventually(t, "row 2 merged", counterIs(r.s.inst.UnitsApplied, 2))
	gate <- struct{}{}
	eventually(t, "epoch 2 open and held", func() bool {
		return r.s.inst.EpochOpened.Load() == 2 && r.w.held.Load() == 1
	})
	tick(4, 1) // epoch 2 opened at t=2 s
	clk.Store(int64(4 * time.Second))
	tick(5, 2)
	openOnce()
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.Count(r.log.String(), "WARN epoch stalled"); got != 2 {
		t.Errorf("%d epoch-stalled WARN lines, want 2:\n%s", got, r.log.String())
	}
}

// TestOnTickRefreshesTheGuardAndNeverOpens is ruling D3 at the tick: one
// Params load, which refreshes ordering.guard for the merge; with no epoch
// open the tick only marks tickPending for stage B.
func TestOnTickRefreshesTheGuardAndNeverOpens(t *testing.T) {
	s, _, set := newEpochShard(t, 8, 1000)
	set(&Params{Gen: 1, MaxBatchSize: 1000, Guard: GuardOff})
	if s.guard == GuardOff {
		t.Fatal("non-vacuity: the guard was already off")
	}
	s.onTick()
	if s.guard != GuardOff || !s.tickPending || s.EpochOpen {
		t.Fatalf("guard %v tickPending %v EpochOpen %v; want GuardOff, true, false", s.guard, s.tickPending, s.EpochOpen)
	}
}

// TestRunDrainReturnsQuiescent is section 2.6's clean exit: in closed with
// rows dirty and batches out, Run drains back to back and returns nil only
// when no epoch is open, nothing is dirty or out, and Free is full. One row is
// written by a tick before the drain, so the "shard drained" line's count is
// the drain's own 10 rows (V2:545), not epoch_rows' 11.
func TestRunDrainReturnsQuiescent(t *testing.T) {
	r := newRunRig(t, rigOpts{maxBatch: 2, mode: latency(time.Millisecond)})
	r.start()
	r.in <- r.quote(9, 9)
	r.merged(1)
	r.tick <- struct{}{}
	eventually(t, "the pre-drain row written", func() bool {
		return r.s.inst.EpochRows.Load() == 1 && r.s.inst.RowsInflight.Load() == 0
	})
	for i := range 5 {
		r.in <- r.quote(model.RowKey(i), uint32(i))
		r.in <- r.master(model.RowKey(i), uint32(i), false)
	}
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	s := r.s
	if !s.quiescent() || len(s.Free) != 2 || s.CohortOut != 0 {
		t.Fatalf("returned with EpochOpen %v CohortOut %d Free %d dirty %d/%d/%d",
			s.EpochOpen, s.CohortOut, len(s.Free), s.Accum[0].Len(), s.Accum[1].Len(), s.Accum[2].Len())
	}
	if got := s.inst.EpochRows.Load(); got != 11 {
		t.Fatalf("epoch_rows = %d, want 11: the pre-drain row and all 10 rows written in the drain", got)
	}
	if !strings.Contains(r.log.String(), "shard drained: shard=0 rows=10 ") {
		t.Errorf("no 'shard drained' INFO counting the drain's 10 rows (V2:545):\n%s", r.log.String())
	}
}

// TestRunDrainDeadlineCountsAbandonedRows is section 2.6's deadline arm.
//
// SETUP: m = 1, maxBatchSize 1; rows A, B, C of MD_TOKEN_QUOTE in one epoch,
// A's batch held for good, B and C still in the cohort; row X of
// MD_SECURITY_MASTER merged meanwhile, on Accum.
//
// STIMULUS: the shared deadline closes, then in closes.
//
// ASSERTION: Run returns nil (a designed outcome, not a failure);
// drain_abandoned_rows = {3, 1, 0} -- cohort plus accumulating plus the row of
// A's batch, which is still with a writer and is counted from rowsOut, never
// by reading the batch (P4-11); one ERROR line.
//
// Non-vacuity: the deadline closed first and Run was still running, so it was
// not reachable before in closed.
func TestRunDrainDeadlineCountsAbandonedRows(t *testing.T) {
	release := make(chan struct{})
	r := newRunRig(t, rigOpts{m: 1, maxBatch: 1, mode: holdUntil(release)})
	t.Cleanup(sync.OnceFunc(func() { close(release) }))
	r.start()
	for i := range 3 {
		r.in <- r.quote(model.RowKey(i), uint32(i))
	}
	r.merged(3)
	r.tick <- struct{}{}
	r.w.waitHeld(t, 1)
	r.in <- r.master(9, 9, false)
	eventually(t, "X merged", counterIs(r.s.inst.UnitsApplied, 4))

	r.closeDr()
	time.Sleep(10 * time.Millisecond)
	if !r.running() {
		t.Fatal("Run returned on the deadline before seeing in closed (ruling D4)")
	}
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v, want nil", err)
	}
	want := [model.MaxDestinations]uint64{3, 1, 0}
	for d := range model.MaxDestinations {
		if got := r.s.inst.DrainAbandonedRows[d].Load(); got != want[d] {
			t.Errorf("drain_abandoned_rows{%d} = %d, want %d", d, got, want[d])
		}
	}
	if r.s.CohortOut != 1 {
		t.Errorf("CohortOut = %d, want 1: the held batch is still out", r.s.CohortOut)
	}
	if got := strings.Count(r.log.String(), "ERROR drain deadline reached"); got != 1 {
		t.Errorf("%d drain-deadline ERROR lines, want 1:\n%s", got, r.log.String())
	}
}

// TestRunAbandonNeverReadsAnOutstandingBatch is the -race half of P4-11's drain
// accounting.
//
// SETUP: the TestRunDrainDeadlineCountsAbandonedRows shape (A held; B, C in the
// cohort; X on Accum), but the stalled writer is hostile: it scribbles on the
// held batch's N, Epoch, DestStart and DestCount for as long as it holds it.
//
// STIMULUS: the deadline closes, then in closes; the writer is released only
// after Run has returned.
//
// ASSERTION: drain_abandoned_rows = {3, 1, 0} -- A's row included, taken from
// the shard's own rowsOut -- and, under -race, no data race report: a shard that
// read the batch to count its rows would race with the scribbling writer and
// would also count 3 + 11 rows.
//
// WOULD HAVE CAUGHT: abandon summing b.DestCount over the batches still out (the
// obvious implementation of "count the stalled batch").
func TestRunAbandonNeverReadsAnOutstandingBatch(t *testing.T) {
	release := make(chan struct{})
	r := newRunRig(t, rigOpts{m: 1, maxBatch: 1, mode: scribbleUntil(release)})
	t.Cleanup(sync.OnceFunc(func() { close(release) }))
	r.start()
	for i := range 3 {
		r.in <- r.quote(model.RowKey(i), uint32(i))
	}
	r.merged(3)
	r.tick <- struct{}{}
	r.w.waitHeld(t, 1)
	r.in <- r.master(9, 9, false)
	eventually(t, "X merged", counterIs(r.s.inst.UnitsApplied, 4))

	r.closeDr()
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v, want nil", err)
	}
	want := [model.MaxDestinations]uint64{3, 1, 0}
	for d := range model.MaxDestinations {
		if got := r.s.inst.DrainAbandonedRows[d].Load(); got != want[d] {
			t.Errorf("drain_abandoned_rows{%d} = %d, want %d", d, got, want[d])
		}
	}
	if r.s.CohortOut != 1 {
		t.Errorf("non-vacuity: CohortOut = %d, want 1: the batch was still out", r.s.CohortOut)
	}
}

// TestRowsOutIsBalancedByCompletions: rowsOut rises at the send and falls at the
// completion, per destination, so a healthy run ends at zero.
//
// SETUP: one quote, a writer that holds its batch until released. STIMULUS: one
// tick, wait until the batch is out, release, wait until it is home, drain.
// ASSERTION: rowsOut is all zeros at return. Non-vacuity: the batch was
// counted out (epoch_batches) and held before the release, and the drained
// row was written (rows_inflight back to 0), so a zero is a decrement, not a
// rowsOut that never rose.
func TestRowsOutIsBalancedByCompletions(t *testing.T) {
	release := make(chan struct{})
	r := newRunRig(t, rigOpts{m: 1, maxBatch: 1, mode: holdUntil(release)})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	r.start()
	r.in <- r.quote(1, 1)
	r.merged(1)
	r.tick <- struct{}{}
	r.w.waitHeld(t, 1)
	eventually(t, "the batch counted out", counterIs(r.s.inst.EpochBatches, 1))
	releaseOnce()
	eventually(t, "the row written", func() bool { return r.s.inst.RowsInflight.Load() == 0 })
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.s.rowsOut != [model.MaxDestinations]int{} {
		t.Errorf("rowsOut = %v after every batch came home, want zeros", r.s.rowsOut)
	}
}

// TestRunZeroDrainTimeoutStillMergesBufferedUnits is ruling D4 with
// drainTimeoutMs = 0: the deadline is already closed when Run starts, and
// shardCh holds 50 units then its close. Every unit is merged -- the deadline
// arm is nil until the close is seen -- and each row is written or counted
// abandoned. The per-pass instruments ride along: the first pass leaves n-1
// units queued, so shard_depth_max is n-1; the queue ends empty, so
// shard_depth is 0; and on a clock that steps on every reading a pass takes
// time, so shard_loop_max_ns is above 0.
//
// WOULD HAVE CAUGHT: arming the deadline from the start: select picks
// uniformly among ready arms, so the chance all 50 units win is 2^-50; and a
// pass that stopped storing or observing its depth and duration.
func TestRunZeroDrainTimeoutStillMergesBufferedUnits(t *testing.T) {
	const n = 50
	r := newRunRig(t, rigOpts{inCap: n})
	var clk atomic.Int64
	r.s.now = func() int64 { return clk.Add(1) }
	for i := range n {
		r.in <- r.quote(model.RowKey(i), uint32(i))
	}
	r.closeIn()
	r.closeDr()
	r.start()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := r.s.inst.UnitsApplied.Load(); got != n {
		t.Fatalf("units_applied = %d, want all %d buffered units merged before the deadline", got, n)
	}
	// EpochRows counts rows SENT; rows still out are in abandoned as well.
	var out uint64
	for d := range r.s.rowsOut {
		out += uint64(r.s.rowsOut[d])
	}
	accounted := r.s.inst.EpochRows.Load() - out
	for d := range model.MaxDestinations {
		accounted += r.s.inst.DrainAbandonedRows[d].Load()
	}
	if accounted != n {
		t.Fatalf("written %d + abandoned = %d, want %d", r.s.inst.EpochRows.Load(), accounted, n)
	}
	if got := r.s.inst.ShardDepthMax.Load(); got != n-1 {
		t.Errorf("shard_depth_max = %d, want %d", got, n-1)
	}
	if got := r.s.inst.ShardDepth.Load(); got != 0 {
		t.Errorf("shard_depth = %d at return, want 0", got)
	}
	if r.s.inst.ShardLoopMaxNs.Load() == 0 {
		t.Error("shard_loop_max_ns = 0 on a clock that steps on every reading")
	}
}

// TestRunPublishesGaugesOnEntry is Run's init (V2:378, Figure 2.2): the depth
// gauges are published before the first select, so a shard parked with
// nothing to do still reports the state it starts from.
//
// SETUP: one row merged directly, before Run, so dirty_depth{0} is stale at 0.
//
// STIMULUS: Run starts; nothing is ever sent.
//
// ASSERTION: dirty_depth{0} becomes 1 while Run is parked, and no epoch opens.
//
// Non-vacuity: the gauge read 0 before Run started.
func TestRunPublishesGaugesOnEntry(t *testing.T) {
	r := newRunRig(t, rigOpts{})
	r.s.merge(r.quote(5, 1))
	if got := r.s.inst.DirtyDepth[tokenQuote].Load(); got != 0 {
		t.Fatalf("non-vacuity: dirty_depth{0} = %d before Run, want 0", got)
	}
	r.start()
	eventually(t, "dirty_depth{0} published on entry", func() bool {
		return r.s.inst.DirtyDepth[tokenQuote].Load() == 1
	})
	if r.s.inst.EpochOpened.Load() != 0 {
		t.Fatal("an epoch opened with no tick, request or drain")
	}
}

// TestRunDrainDeadlineCountsAnOutstandingBatchWithNothingElse is the deadline arm's
// "ERROR only if non-zero" (section 2.6, teardown step 8).
//
// SETUP: m = 1; one row in one epoch, its batch held for good, so the cohort
// is empty and nothing is dirty -- but the epoch is open and the shard is not
// quiescent.
//
// STIMULUS: the deadline closes, then in closes.
//
// ASSERTION: Run returns nil; drain_abandoned_rows is {1, 0, 0}: the row of
// the held batch is unconfirmed and is counted (P4-11), with its ERROR line.
//
// Non-vacuity: CohortOut is still 1 when Run returns, so it left through the
// deadline arm, not a quiescent drain.
func TestRunDrainDeadlineWithNothingUnwrittenIsSilent(t *testing.T) {
	release := make(chan struct{})
	r := newRunRig(t, rigOpts{m: 1, mode: holdUntil(release)})
	t.Cleanup(sync.OnceFunc(func() { close(release) }))
	r.start()
	r.in <- r.quote(1, 1)
	r.merged(1)
	r.tick <- struct{}{}
	r.w.waitHeld(t, 1)

	r.closeDr()
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v, want nil", err)
	}
	if r.s.CohortOut != 1 {
		t.Fatalf("non-vacuity: CohortOut = %d, want 1: Run did not leave through the deadline", r.s.CohortOut)
	}
	want := [model.MaxDestinations]uint64{1, 0, 0}
	for d := range model.MaxDestinations {
		if got := r.s.inst.DrainAbandonedRows[d].Load(); got != want[d] {
			t.Errorf("drain_abandoned_rows{%d} = %d, want %d", d, got, want[d])
		}
	}
	if got := strings.Count(r.log.String(), "ERROR drain deadline"); got != 1 {
		t.Errorf("%d drain-deadline ERROR lines, want 1:\n%s", got, r.log.String())
	}
}

// TestRunTickDuringOpenEpochKeepsTheRequest: a tick that lands mid-epoch sets
// TickMissed and counts tick_skipped_epoch_open, and MUST NOT clear req[0]
// (HLD section 7.3): the request is served when the epoch closes.
func TestRunTickDuringOpenEpochKeepsTheRequest(t *testing.T) {
	release := make(chan struct{})
	r := newRunRig(t, rigOpts{mode: holdUntil(release)})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	r.start()
	r.in <- r.quote(1, 1)
	r.merged(1)
	r.tick <- struct{}{}
	r.w.waitHeld(t, 1)
	r.in <- r.master(2, 2, true)
	eventually(t, "the request raised", counterIs(r.s.inst.FlushTriggered, 1))
	if !r.s.bus.Pending(0) {
		t.Fatal("setup: req[0] not raised")
	}

	r.tick <- struct{}{}
	eventually(t, "the mid-epoch tick", counterIs(r.s.inst.TickSkippedEpochOpen, 1))
	if !r.s.bus.Pending(0) {
		t.Fatal("a tick during an open epoch cleared req[0]")
	}

	releaseOnce()
	eventually(t, "the request served", func() bool { return r.rec.len() == 2 })
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, b := r.rec.snapshot(); !slices.Equal(b[1], []model.RowKey{2}) {
		t.Fatalf("follow-up batch = %v, want [2]", b[1])
	}
}

// TestRunStaleWakeIsOnePassAndNothingElse: a non-rate-limited merge raises the
// request and leaves a wake token; the same pass's stage B opens the epoch and
// takes the request, so the token is stale. Its pass counts
// flush_wake_received and does nothing else: no second epoch, no empty-ring
// attempt.
func TestRunStaleWakeIsOnePassAndNothingElse(t *testing.T) {
	r := newRunRig(t, rigOpts{})
	r.start()
	r.in <- r.master(1, 1, true)
	eventually(t, "the stale wake taken", counterIs(r.s.inst.FlushWakeReceived, 1))
	eventually(t, "the batch committed", func() bool {
		return r.rec.len() == 1 && r.s.inst.RowsInflight.Load() == 0
	})
	time.Sleep(10 * time.Millisecond)
	if r.s.inst.EpochOpened.Load() != 1 || r.s.inst.EpochEmpty.Load() != 0 || r.rec.len() != 1 {
		t.Fatalf("epoch_opened %d epoch_empty %d batches %d; want 1, 0, 1",
			r.s.inst.EpochOpened.Load(), r.s.inst.EpochEmpty.Load(), r.rec.len())
	}
	if r.s.bus.Pending(0) {
		t.Fatal("req[0] still set after the epoch opened")
	}
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestRunS3IsReportedAndRunContinues is ruling D9 inside Run.
//
// SETUP: the writer leaves row 0 of its first batch Pending with Err nil
// (unreportedRow), and commits everything after.
//
// STIMULUS: rows A, B flushed by a tick; later another tick; then in closes.
//
// ASSERTION: report is called exactly once, with errBatchRowUnreported; Run
// does not return; the next epoch's first batch carries A, requeued at the
// head; the close still reaches a quiescent nil return.
func TestRunS3IsReportedAndRunContinues(t *testing.T) {
	var calls atomic.Int32
	mode := fakeMode{adjudicate: func(b *model.Batch) {
		if calls.Add(1) == 1 {
			unreportedRow(0).adjudicate(b)
			return
		}
		commitAll(b)
	}}
	r := newRunRig(t, rigOpts{mode: mode})
	r.start()
	r.in <- r.quote(1, 1)
	r.in <- r.quote(2, 2)
	eventually(t, "the rows merged", counterIs(r.s.inst.UnitsApplied, 2))
	r.tick <- struct{}{}
	eventually(t, "the S3 report", func() bool { return len(r.reported()) == 1 })
	time.Sleep(10 * time.Millisecond)
	if !r.running() {
		t.Fatal("Run returned on S3")
	}

	r.tick <- struct{}{}
	eventually(t, "the retry", func() bool { return r.rec.len() == 2 && r.s.inst.RowsInflight.Load() == 0 })
	r.closeIn()
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reps := r.reported()
	if len(reps) != 1 || !errors.Is(reps[0], errBatchRowUnreported) {
		t.Fatalf("reports = %v, want exactly one errBatchRowUnreported", reps)
	}
	if _, b := r.rec.snapshot(); !slices.Equal(b[1], []model.RowKey{1}) {
		t.Fatalf("next epoch's first batch = %v, want [1]: the unreported row requeued", b[1])
	}
	if got := r.s.inst.BatchRowUnreported.Load(); got != 1 {
		t.Fatalf("batch_row_unreported = %d, want 1", got)
	}
}

// TestRunS1AndS2Return: unlike S3, a failed batchQ send (S1) and a completion
// naming a row not in flight (S2) end Run with their error, and nothing is
// reported.
func TestRunS1AndS2Return(t *testing.T) {
	t.Run("S1", func(t *testing.T) {
		r := newRunRig(t, rigOpts{qCap: -1, writers: -1}) // nobody receives on batchQ
		r.start()
		r.in <- r.quote(1, 1)
		r.merged(1)
		r.tick <- struct{}{}
		if err := r.wait(); err == nil || !strings.Contains(err.Error(), "batchQ send failed") {
			t.Fatalf("Run = %v, want the S1 error", err)
		}
		if r.s.inst.FlushDeferred.Load() != 1 || len(r.reported()) != 0 {
			t.Fatalf("flush_deferred %d, reports %v; want 1 and none",
				r.s.inst.FlushDeferred.Load(), r.reported())
		}
	})
	t.Run("S2", func(t *testing.T) {
		r := newRunRig(t, rigOpts{writers: -1})
		// Driven synchronously before Run starts: one batch out, then its row's
		// InFlight cleared behind the shard's back.
		r.s.merge(r.quote(1, 1))
		if err := r.s.maybeOpenEpoch(); err != nil {
			t.Fatalf("open: %v", err)
		}
		b := next(t, r.q)
		rowEntry(r.s, tokenQuote, 1).InFlight = false
		commitAll(b)
		r.start()
		r.comp <- b
		if err := r.wait(); err == nil || !strings.Contains(err.Error(), "defect S2") {
			t.Fatalf("Run = %v, want the S2 error", err)
		}
		if r.s.inst.InflightDesync.Load() != 1 || len(r.reported()) != 0 {
			t.Fatalf("inflight_desync %d, reports %v; want 1 and none",
				r.s.inst.InflightDesync.Load(), r.reported())
		}
	})
}

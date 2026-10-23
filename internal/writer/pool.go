package writer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

// Pool is the writer pool: writers.count goroutines, their sessions and
// statement caches, and the shared health state the fail-fast and pool-stop
// rules key on (V3 2.6). It holds no mirror state and may not import mirror.
//
// Lifecycle: NewPool allocates and opens nothing. Start launches the writers,
// which acquire sessions lazily. Writers leave when batchQ is closed (after
// processing everything queued), when Close is called, when the context is
// cancelled, when Resize shrinks the pool, or when they retire themselves.
// FailFast does NOT make them leave (ruling D2): they keep receiving and fail
// every batch at once so the shards' in-flight slots keep draining, until
// batchQ closes.
type Pool struct {
	db     *oradb.DB
	dests  *destinations.Set
	cfg    Config
	params func() *Params
	inst   *Instruments
	log    *obs.AppLogger
	health *poolHealth

	// Seams for tests; NewPool installs the real ones.
	now   func() time.Time
	rnd   func() float64
	sleep func(d time.Duration)                            // nil: an interruptible real sleep
	after func(d time.Duration) (<-chan time.Time, func()) // a timer and its stop

	closing chan struct{} // closed by Close
	stopped chan struct{} // closed once, by stopPool
	stopErr atomic.Pointer[StopError]
	stopOne sync.Once

	wg sync.WaitGroup

	mu         sync.Mutex // guards everything below
	started    bool
	closed     bool
	ctx        context.Context
	batchQ     <-chan *model.Batch
	completion []chan<- *model.Batch
	live       map[int]*writer
	configured int
}

// NewPool builds the pool without starting it and without opening a session.
// params is called once here (the health thresholds) and once per batch by each
// writer; it must never return nil. inst and log may be nil.
func NewPool(db *oradb.DB, dests *destinations.Set, cfg Config, params func() *Params, inst *Instruments, log *obs.AppLogger) (*Pool, error) {
	switch {
	case db == nil:
		return nil, errors.New("writer: NewPool: nil database")
	case dests == nil:
		return nil, errors.New("writer: NewPool: nil destinations")
	case params == nil:
		return nil, errors.New("writer: NewPool: nil params")
	case cfg.Writers < 1:
		return nil, fmt.Errorf("writer: NewPool: writers = %d, want at least 1", cfg.Writers)
	case cfg.Ceiling < 1:
		return nil, fmt.Errorf("writer: NewPool: batch ceiling = %d, want at least 1", cfg.Ceiling)
	}
	p0 := params()
	if p0 == nil {
		return nil, errors.New("writer: NewPool: params returned nil")
	}
	if inst == nil {
		inst = &Instruments{}
	}
	p := &Pool{
		db: db, dests: dests, cfg: cfg, params: params, inst: inst, log: log,
		now:     time.Now,
		rnd:     rand.Float64,
		after:   realAfter,
		closing: make(chan struct{}),
		stopped: make(chan struct{}),
		live:    map[int]*writer{},
	}
	p.health = newPoolHealth(healthFrom(p0), func() time.Time { return p.now() })
	return p, nil
}

func healthFrom(p *Params) healthConfig {
	return healthConfig{
		failFastWindow:      p.FailFastWindow,
		failFastWrites:      p.FailFastWrites,
		statementFailNeed:   p.StatementFailWriters,
		statementFailWindow: p.StatementFailWindow,
	}
}

// ReloadHealth re-reads params() and applies the health thresholds, so a
// reloaded writers.failFastSeconds or statementFail* takes effect on the next
// failure. The caller orders it before Resize, so the new statementFailWriters
// floor is in force when the new count is judged. A nil params is ignored.
func (p *Pool) ReloadHealth() {
	if par := p.params(); par != nil {
		p.health.reload(healthFrom(par))
	}
}

func realAfter(d time.Duration) (<-chan time.Time, func()) {
	if d <= 0 {
		return nil, func() {}
	}
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

// Start launches writers.count goroutines. They acquire sessions lazily, so
// Start does not touch the database. Each completion channel must be buffered:
// the writer's send is blocking, and an unbuffered channel would make a writer
// wait for the shard's loop, which is the coupling Vol 0 9.2 forbids. A pool
// can be started once.
func (p *Pool) Start(ctx context.Context, batchQ <-chan *model.Batch, completion []chan<- *model.Batch) error {
	if len(completion) == 0 {
		return errors.New("writer: Start: no completion channels")
	}
	for k, c := range completion {
		if c == nil || cap(c) < 1 {
			return fmt.Errorf("writer: Start: completion channel %d is nil or unbuffered", k)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.closed:
		return errors.New("writer: Start: pool is closed")
	case p.started:
		return errors.New("writer: Start: pool already started")
	}
	p.started, p.ctx, p.batchQ, p.completion = true, ctx, batchQ, completion
	p.configured = p.cfg.Writers
	p.db.SetPoolSize(p.configured+1, p.configured+1)
	setGauge(p.inst.WritersConfigured, int64(p.configured))
	for range p.configured {
		p.spawn()
	}
	return nil
}

// spawn starts one writer in the lowest free slot. Caller holds mu.
func (p *Pool) spawn() {
	id := 0
	for p.live[id] != nil {
		id++
	}
	w := &writer{p: p, id: id, quit: make(chan struct{}), par: p.params()}
	p.live[id] = w
	setGauge(p.inst.WritersRunning, int64(len(p.live)))
	p.wg.Add(1)
	go p.runWriter(w)
}

// Resize changes the number of running writers: it re-applies the database/sql
// limits to n+1 first (Vol 0 D9), then starts goroutines or signals the
// highest-numbered ones to leave after their current batch. It compares with
// the writers actually running, so a pool that lost members to self-stops
// regrows to n (ruling D4). n below statementFailWriters is rejected (C6),
// not clamped. A pool that has stopped (StopReason set, including
// StopNoWriters) or been closed cannot be resized.
func (p *Pool) Resize(_ context.Context, n int) error {
	floor := max(p.params().StatementFailWriters, 1)
	if n < floor {
		return fmt.Errorf("writer: Resize: %d writers is below writers.statementFailWriters (%d)", n, floor)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case !p.started:
		return errors.New("writer: Resize: pool not started")
	case p.closed:
		return errors.New("writer: Resize: pool is closed")
	case p.stopErr.Load() != nil:
		return errors.New("writer: Resize: pool has stopped")
	}
	p.db.SetPoolSize(n+1, n+1)
	p.configured = n
	setGauge(p.inst.WritersConfigured, int64(n))
	active := 0
	for _, w := range p.live {
		if !w.quitting {
			active++
		}
	}
	for ; active < n; active++ {
		p.spawn()
	}
	ids := make([]int, 0, len(p.live))
	for id := range p.live {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for i := len(ids) - 1; i >= 0 && active > n; i-- { // highest ids first
		if w := p.live[ids[i]]; !w.quitting {
			w.quitting = true
			close(w.quit)
			active--
		}
	}
	return nil
}

// Stopped is closed when the pool has stopped itself. app turns it into a
// drain and a non-zero exit; this package never calls os.Exit.
func (p *Pool) Stopped() <-chan struct{} { return p.stopped }

// StopReason returns why Stopped was closed, a *StopError, or nil before it
// closes.
func (p *Pool) StopReason() error {
	if e := p.stopErr.Load(); e != nil {
		return e
	}
	return nil
}

// stopPool records the first reason and closes Stopped. Later calls are no-ops.
func (p *Pool) stopPool(e *StopError) {
	p.stopOne.Do(func() {
		p.stopErr.Store(e)
		close(p.stopped)
	})
}

// Close signals every writer to leave once it has finished the batch it holds
// and whatever is already queued, then waits for them, closing cached
// statements and releasing sessions on the way out. It does not close the
// *sql.DB, which app owns. It never cancels the pool's context: a batch in
// flight finishes (D13). ctx bounds the wait. Close on a pool that was never
// started, or twice, is a no-op.
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.closing)
	}
	started := p.started
	p.mu.Unlock()
	if !started {
		return nil
	}
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runWriter is a writer goroutine's whole life.
func (p *Pool) runWriter(w *writer) {
	defer p.wg.Done()
	kind := p.loop(p.ctx, w)

	p.mu.Lock()
	delete(p.live, w.id)
	left := len(p.live)
	setGauge(p.inst.WritersRunning, int64(left))
	closing := p.closed
	p.mu.Unlock()
	// Every member retired itself: nothing will ever write again (D4). A pool
	// that is being closed or shrunk to this point is not a failure.
	if kind == exitRetired && left == 0 && !closing {
		p.stopPool(&StopError{Kind: StopNoWriters})
	}
}

// pause is the backoff sleep. It is interruptible by shutdown, a shrink or
// Close; the injected sleep replaces the whole wait.
func (p *Pool) pause(ctx context.Context, w *writer, d time.Duration) {
	if d <= 0 {
		return
	}
	if p.sleep != nil {
		p.sleep(d)
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	case <-w.quit:
	case <-p.closing:
	}
}

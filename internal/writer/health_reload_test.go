package writer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/oradb"
)

// TestReloadHealthExtendsTheFailFastBudget is the P5-05 lever test: raising
// writers.failFastSeconds mid-run extends the budget of an episode already open.
//
// SETUP: a pool over the fake with FailFastWindow 30 s, FailFastWrites high so
// only the clock can trip; a Session failure opens a Degraded episode.
// STIMULUS: params() now returns 120 s; ReloadHealth; 60 s pass; a second failure.
// ASSERTION: Continue (inside the new budget). Without ReloadHealth the same
// sequence is StopPool (non-vacuity, run first). After a further 60 s: StopPool.
// WOULD HAVE CAUGHT: a failFastSeconds key that reloads in config but is
// frozen in poolHealth.cfg, the lever the runbook names doing nothing.
func TestReloadHealthExtendsTheFailFastBudget(t *testing.T) {
	run := func(t *testing.T, reload bool) (afterMinute Action, afterTwo Action) {
		r := newPoolRig(t, rigOpts{par: func(p *Params) { p.FailFastWrites = 1000 }})
		cur := &atomic.Pointer[Params]{}
		cur.Store(defaultParams())
		r.p.params = func() *Params { return cur.Load() }
		clk := newFakeClock()
		r.p.health.now = clk.now
		r.p.health.OnFailure(0, fail(oradb.ErrSession), keyA)
		p := *defaultParams()
		p.FailFastWrites = 1000
		p.FailFastWindow = 120 * time.Second
		cur.Store(&p)
		if reload {
			r.p.ReloadHealth()
		}
		clk.add(60 * time.Second)
		afterMinute = r.p.health.OnFailure(0, fail(oradb.ErrSession), keyA)
		clk.add(60 * time.Second)
		afterTwo = r.p.health.OnFailure(0, fail(oradb.ErrSession), keyA)
		return
	}
	if a, _ := run(t, false); a != StopPool {
		t.Fatalf("non-vacuity: without ReloadHealth the 30 s budget should trip at 60 s, got %v", a)
	}
	a, b := run(t, true)
	if a != Continue {
		t.Errorf("at 60 s after reload to 120 s: %v, want Continue", a)
	}
	if b != StopPool {
		t.Errorf("at 120 s: %v, want StopPool", b)
	}
}

// TestReloadHealthIsRaceFreeAgainstFailures: ReloadHealth, OnFailure and State
// share one mutex; under -race a future unlocked ledger access fails here.
func TestReloadHealthIsRaceFreeAgainstFailures(t *testing.T) {
	r := newPoolRig(t, rigOpts{par: func(p *Params) { p.FailFastWrites = 1000000 }})
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			r.p.ReloadHealth()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			r.p.health.OnFailure(i%4, fail(oradb.ErrStatement), keyA)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = r.p.health.State()
		}
	}()
	wg.Wait()
}

// TestReloadHealthUpdatesTheStatementLedger: raising statementFailWriters to 3
// means two distinct writers failing the same statement no longer stops the pool.
//
// SETUP: health need=2. STIMULUS: reload need=3 via ReloadHealth; writers 0, 1
// report the same ErrStatement. ASSERTION: both RetireWriter, state not FailFast;
// a third writer then stops it. WOULD HAVE CAUGHT: a ledger that keeps the old
// need after the Runtime changed, so the s5.3 lever is cosmetic.
func TestReloadHealthUpdatesTheStatementLedger(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	p := *defaultParams()
	p.StatementFailWriters = 3
	r.p.params = func() *Params { return &p }
	r.p.ReloadHealth()
	if got := r.p.health.OnFailure(0, fail(oradb.ErrStatement), keyA); got != RetireWriter {
		t.Fatalf("writer 0: %v", got)
	}
	if got := r.p.health.OnFailure(1, fail(oradb.ErrStatement), keyA); got != RetireWriter {
		t.Fatalf("writer 1 with need=3: %v, want RetireWriter", got)
	}
	if got := r.p.health.OnFailure(2, fail(oradb.ErrStatement), keyA); got != StopPool {
		t.Fatalf("writer 2: %v, want StopPool", got)
	}
}

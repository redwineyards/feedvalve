package writer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
)

// Retry loop, health and stop wiring, driven through a real Pool over the
// oratest fake.

func (r *poolRig) consec(id int) int {
	r.p.health.mu.Lock()
	defer r.p.health.mu.Unlock()
	return r.p.health.consec[id]
}

func (r *poolRig) retries() (n uint64) {
	for _, c := range r.inst.WriteRetry {
		if c != nil {
			n += c.Load()
		}
	}
	return n
}

// TestRetriedSuccessDoesNotRecoverADegradedPool: D6c, with the pool starting
// Degraded and a non-zero consecutive count, so a stray OnSuccess is visible.
//
// SETUP: health marked Degraded (last class session) with writer 0's count at 1;
// the first Begin fails with ORA-03113, the retry's succeeds.
// STIMULUS: one batch.
// ASSERTION: it commits; write_retry{session} is 1; the pool is still Degraded
// and the count is still 1.
// WOULD HAVE CAUGHT: calling OnSuccess for a batch that needed a retry.
func TestRetriedSuccessDoesNotRecoverADegradedPool(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	r.p.health.mu.Lock()
	r.p.health.markDegraded(oradb.ErrSession)
	r.p.health.consec[0] = 1
	r.p.health.mu.Unlock()
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpBegin && c.N == 1 {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	r.start()
	r.batchQ <- r.batch(0)
	allCommitted(t, r.recv(0))
	if got := r.inst.WriteRetry[obs.RetrySession].Load(); got != 1 {
		t.Errorf("write_retry{session} = %d, want 1", got)
	}
	if r.p.health.State() != HealthDegraded || !r.p.health.degraded.Load() {
		t.Error("a retried success recovered the Degraded pool")
	}
	if got := r.consec(0); got != 1 {
		t.Errorf("consecutive failures = %d, want 1 unchanged", got)
	}
}

// TestFailureAfterTheRetryIsGivenBackWithExactlyOneRetry: a retryable class that
// fails twice.
//
// SETUP: every Begin fails with one code per class.
// STIMULUS: one batch.
// ASSERTION: Err carries the class; exactly two Begins ran; write_retry of that
// class is 1 and no other; writes_failed is 1 and the writer's consecutive count
// is 1 (one OnFailure per batch, not per attempt).
// WOULD HAVE CAUGHT: a second retry; failure accounting per attempt; a class
// that is not retried.
func TestFailureAfterTheRetryIsGivenBackWithExactlyOneRetry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		code  int
		class oradb.ErrClass
		retry obs.RetryClass
	}{
		{"session", 3113, oradb.ErrSession, obs.RetrySession},
		{"deadlock", 60, oradb.ErrDeadlock, obs.RetryDeadlock},
		{"space", 1653, oradb.ErrSpace, obs.RetrySpace},
		{"unknown", 24381, oradb.ErrUnknown, obs.RetryUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// failFastWrites 3 with a failing Ping: the batch's failure is the
			// writer's count 1 and the probe after it is 2, so the pool stays
			// Degraded. A failure counted once per attempt would reach 3 and stop it.
			r := newPoolRig(t, rigOpts{par: func(p *Params) { p.FailFastWrites = 3 }})
			var begins atomic.Int32
			r.f.SetScript(func(c oratest.Call) oratest.Result {
				switch c.SQL {
				case oratest.OpBegin:
					begins.Add(1)
					return oratest.Result{Err: oratest.ORA(tc.code)}
				case oratest.OpPing:
					return oratest.Result{Err: oratest.ORA(12541)}
				}
				return oratest.Result{}
			})
			r.p.sleep = func(time.Duration) {}
			r.start()
			r.batchQ <- r.batch(0)
			b := r.recv(0)
			var oe *oradb.Error
			if !errors.As(b.Err, &oe) || oe.Class != tc.class {
				t.Fatalf("Err = %v, want class %v", b.Err, tc.class)
			}
			eventually(t, "the probe", func() bool { return r.inst.Probe[obs.ProbeFail].Load() == 1 })
			if got := begins.Load(); got != 2 {
				t.Errorf("Begins = %d, want 2 (one attempt, one retry)", got)
			}
			if got := r.retries(); got != 1 {
				t.Errorf("write_retry total = %d, want 1", got)
			}
			if got := r.inst.WriteRetry[tc.retry].Load(); got != 1 {
				t.Errorf("write_retry{%v} = %d, want 1", tc.retry, got)
			}
			if got := r.inst.WritesFailed.Load(); got != 1 {
				t.Errorf("writes_failed = %d, want 1", got)
			}
			if stopped(r.p) || r.p.health.State() != HealthDegraded {
				t.Errorf("pool state %v, stopped %v: want Degraded and running (one failure per batch)",
					r.p.health.State(), stopped(r.p))
			}
		})
	}
}

// TestStatementFailureIsNotRetried: ErrStatement is given back after one attempt.
func TestStatementFailureIsNotRetried(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 2})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if isMerge(c) {
			return oratest.Result{Err: oratest.ORA(942)}
		}
		return oratest.Result{}
	})
	r.start()
	r.batchQ <- r.batch(0)
	assertStatementFailure(t, r.recv(0))
	if got := r.f.Counts().Begins; got != 1 {
		t.Errorf("Begins = %d, want 1: an ErrStatement was retried", got)
	}
	if got := r.retries(); got != 0 {
		t.Errorf("write_retry total = %d, want 0", got)
	}
}

// TestNoRetryAfterPhaseTwo: D6d.
//
// SETUP: the first MERGE fails with ORA-01722 (a row-data trigger for phase 2);
// every later MERGE, the phase 2 scalar ones, fails with ORA-03113.
// STIMULUS: one batch.
// ASSERTION: the batch is given back failed, nothing counts as a retry, and
// writes_failed is 1.
// WOULD HAVE CAUGHT: retrying a session failure that happened after phase 2.
func TestNoRetryAfterPhaseTwo(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		switch {
		case isMerge(c) && c.N == 1:
			return oratest.Result{Err: oratest.ORA(1722)}
		case isMerge(c):
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	r.p.sleep = func(time.Duration) {}
	r.start()
	r.batchQ <- r.batch(0)
	if b := r.recv(0); b.Err == nil {
		t.Fatal("batch came back with Err nil")
	}
	if got := r.retries(); got != 0 {
		t.Errorf("write_retry total = %d, want 0 after phase 2", got)
	}
	if got := r.inst.WritesFailed.Load(); got != 1 {
		t.Errorf("writes_failed = %d, want 1", got)
	}
}

// TestFailedProbeStopsThePool: V3 9.3 on a quiet market.
//
// SETUP: failFastWrites 2; every Begin and every Ping fails.
// STIMULUS: one batch. Its failure is the writer's first consecutive failure;
// the backoff probe that follows fails and is the second.
// ASSERTION: Stopped closes with kind fail_fast and write_probe{fail} is 1.
// WOULD HAVE CAUGHT: a probe whose failure is counted but never reaches the
// fail-fast gate.
func TestFailedProbeStopsThePool(t *testing.T) {
	r := newPoolRig(t, rigOpts{par: func(p *Params) { p.FailFastWrites = 2 }})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpBegin || c.SQL == oratest.OpPing {
			return oratest.Result{Err: oratest.ORA(12541)}
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
	if got := r.inst.Probe[obs.ProbeFail].Load(); got != 1 {
		t.Errorf("write_probe{fail} = %d, want 1", got)
	}
}

// TestFailFastWriterWithFailuresDoesNotSleep: D2 against V3 9.2.
//
// SETUP: failFastWrites 1, every Begin fails, so the first batch trips FailFast
// and leaves the writer with failures > 0; the sleep seam counts calls.
// STIMULUS: that batch, then three more.
// ASSERTION: all four come back failed and the writer never slept.
// WOULD HAVE CAUGHT: a backoff before the next batch in FailFast, which holds
// the shards' in-flight slots for up to retryMax.
func TestFailFastWriterWithFailuresDoesNotSleep(t *testing.T) {
	r := newPoolRig(t, rigOpts{par: func(p *Params) { p.FailFastWrites = 1 }})
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpBegin {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	var sleeps atomic.Int32
	r.p.sleep = func(time.Duration) { sleeps.Add(1) }
	r.start()
	for range 4 {
		r.batchQ <- r.batch(0)
		if b := r.recv(0); b.Err == nil {
			t.Fatal("a batch came back with Err nil")
		}
	}
	if got := sleeps.Load(); got != 0 {
		t.Errorf("the writer slept %d times with the pool in FailFast", got)
	}
}

// TestNoPostSendAccessOnFailurePaths is TestNoPostSendAccess over the paths
// that read the most locals: a retried success, a failure after the retry
// (health, logging, backoff) and a phase 2 trigger. Only meaningful under
// -race, which reports a post-send read of the batch; without it the test just
// exercises the paths.
func TestNoPostSendAccessOnFailurePaths(t *testing.T) {
	r := newPoolRig(t, rigOpts{writers: 2, compCap: 4})
	var begins, merges atomic.Int64
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		switch {
		case c.SQL == oratest.OpBegin:
			// n%4==1: a retried success. n%11 in {0,1}: a double failure.
			if n := begins.Add(1); n%4 == 1 || n%11 == 0 || n%11 == 1 {
				return oratest.Result{Err: oratest.ORA(3113)}
			}
		case isMerge(c):
			if merges.Add(1)%13 == 0 {
				return oratest.Result{Err: oratest.ORA(1722)}
			}
		}
		return oratest.Result{}
	})
	r.p.sleep = func(time.Duration) {}
	r.start()
	r.batchQ <- r.batch(0)
	r.batchQ <- r.batch(0)
	for done := 0; done < 300; done++ {
		b := r.recv(0)
		scribble(b)
		r.batchQ <- b
	}
}

// scribble rewrites every field of a returned batch and restores a valid one in
// place, as a shard recycling it would.
func scribble(b *model.Batch) {
	b.Shard, b.Epoch = 9, ^uint64(0)
	b.DestStart, b.DestCount = [3]int32{9, 9, 9}, [3]int32{9, 9, 9}
	b.N = -1
	b.Err = errors.New("scribble")
	for i := range b.Rows {
		row := &b.Rows[i]
		row.Row, row.Mask, row.Digest, row.Slot = 1, 1, 1, 1
		row.Outcome, row.ErrCode = model.OutcomeFailed, 1
		row.Image[0] = 0xFF
	}
	b.Rows = b.Rows[:0]
	*b = *standard()
}

// TestParamsAreLoadedOncePerBatch: a Params change mid-batch is not seen by that
// batch or its retry, and is seen by the next batch.
//
// SETUP: ConnHold is 30 min; the first Begin swaps in ConnHold = 11 min and
// fails with ORA-03113, so the retry acquires a session.
// ASSERTION: the retry's session was armed with 30 min, never 11; after the hold
// timer fires and a second batch needs a new session, it is armed with 11 min.
func TestParamsAreLoadedOncePerBatch(t *testing.T) {
	const newHold = 11 * time.Minute
	r := newPoolRig(t, rigOpts{})
	cur := &atomic.Pointer[Params]{}
	cur.Store(defaultParams())
	r.p.params = func() *Params { return cur.Load() }
	r.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpBegin && c.N == 1 {
			p := *defaultParams()
			p.ConnHold = newHold
			cur.Store(&p)
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	r.start()
	r.batchQ <- r.batch(0)
	allCommitted(t, r.recv(0))
	if got := r.timers.count(newHold); got != 0 {
		t.Fatalf("the retry saw the new ConnHold (%d timers)", got)
	}
	if got := r.timers.count(holdD); got != 2 {
		t.Fatalf("hold timers at the old value = %d, want 2 (attempt and retry)", got)
	}
	r.timers.latest(holdD).c <- time.Now()
	eventually(t, "the hold release", func() bool { return r.inst.ConnRecycled[obs.RecycleHold].Load() == 1 })
	r.batchQ <- r.batch(0)
	allCommitted(t, r.recv(0))
	if got := r.timers.count(newHold); got != 1 {
		t.Errorf("the next batch armed %d timers at the new ConnHold, want 1", got)
	}
}

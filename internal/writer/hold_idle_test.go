package writer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/obs"
	"feedvalve/internal/oradb/oratest"
)

// inUse is the number of connections database/sql has handed out: a writer's
// pinned session counts, a released one does not.
func (r *poolRig) inUse() int { return r.db.Stats().InUse }

// reGate is a gate that can be armed and released repeatedly: while armed,
// every commit blocks until release.
type reGate struct {
	mu      sync.Mutex
	armed   bool
	open    chan struct{}
	entered chan struct{}
}

func newReGate() *reGate { return &reGate{entered: make(chan struct{}, 8)} }

func (g *reGate) arm() {
	g.mu.Lock()
	g.armed, g.open = true, make(chan struct{})
	g.mu.Unlock()
}

func (g *reGate) release() {
	g.mu.Lock()
	g.armed = false
	close(g.open)
	g.mu.Unlock()
}

func (g *reGate) script(c oratest.Call) oratest.Result {
	g.mu.Lock()
	armed, open := g.armed, g.open
	g.mu.Unlock()
	if armed && c.SQL == oratest.OpCommit {
		g.entered <- struct{}{}
		<-open
	}
	return oratest.Result{}
}

// TestHoldAndIdleReleaseHappenOnlyBetweenBatches is V3 2.3's two MUSTs
// (finding W1) with the timers and the clock injected.
//
// SETUP: one writer; Pool.after is replaced so the 60 s idle timer and the
// 30 min hold timer are channels the test fires by hand, Pool.now is a clock
// the test can push forward, and commits can be gated to hold a batch in
// flight.
//
// STIMULUS / ASSERTION, in four steps:
//  1. a batch commits and the session stays pinned (InUse 1); the idle timer
//     fires: the session is released and writer_conn_recycled{idle} is 1.
//  2. a batch is gated at commit and the HOLD timer fires mid-batch: the
//     session is still pinned and nothing is counted; once the batch returns
//     the writer releases it, writer_conn_recycled{hold} is 1.
//  3. with the session held and the writer waiting, a batch is gated and the
//     idle timer that was armed for that wait fires mid-batch: it is ignored;
//     the session stays pinned through the batch and after it (the next wait
//     arms a fresh timer), and only the fresh timer releases it (idle 2).
//  4. the injected clock moved past the session's lifetime releases it after
//     the next batch through Session.Expired (hold 2).
//
// WOULD HAVE CAUGHT: a release with a transaction open; a stale timer from a
// previous wait releasing a busy session; a hold deadline nobody checks.
func TestHoldAndIdleReleaseHappenOnlyBetweenBatches(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	g := newReGate()
	r.f.SetScript(g.script)
	var skew atomic.Int64
	r.p.now = func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }
	r.start()

	// 1. idle.
	idles := r.timers.count(idleD)
	r.batchQ <- r.batch(0)
	allCommitted(t, r.recv(0))
	eventually(t, "the wait to arm an idle timer", func() bool { return r.timers.count(idleD) > idles })
	if got := r.inUse(); got != 1 {
		t.Fatalf("session not pinned between batches: InUse = %d", got)
	}
	r.timers.latest(idleD).c <- time.Now()
	eventually(t, "the idle release", func() bool { return r.inUse() == 0 })
	if got := r.inst.ConnRecycled[obs.RecycleIdle].Load(); got != 1 {
		t.Errorf("writer_conn_recycled{idle} = %d, want 1", got)
	}

	// 2. hold, fired mid-batch.
	g.arm()
	r.batchQ <- r.batch(0)
	<-g.entered
	r.timers.latest(holdD).c <- time.Now()
	time.Sleep(50 * time.Millisecond)
	if got := r.inUse(); got != 1 {
		t.Fatalf("the hold timer released the session mid-batch: InUse = %d", got)
	}
	if got := r.inst.ConnRecycled[obs.RecycleHold].Load(); got != 0 {
		t.Fatalf("hold release counted mid-batch: %d", got)
	}
	g.release()
	allCommitted(t, r.recv(0))
	eventually(t, "the hold release after the batch", func() bool { return r.inUse() == 0 })
	if got := r.inst.ConnRecycled[obs.RecycleHold].Load(); got != 1 {
		t.Errorf("writer_conn_recycled{hold} = %d, want 1", got)
	}

	// 3. a stale idle timer fired mid-batch.
	idles = r.timers.count(idleD)
	r.batchQ <- r.batch(0)
	allCommitted(t, r.recv(0))
	eventually(t, "a wait timer", func() bool { return r.timers.count(idleD) > idles })
	stale := r.timers.latest(idleD)
	g.arm()
	r.batchQ <- r.batch(0)
	<-g.entered
	idles = r.timers.count(idleD)
	stale.c <- time.Now()
	time.Sleep(50 * time.Millisecond)
	if got := r.inUse(); got != 1 {
		t.Fatalf("a timer fired mid-batch released the session: InUse = %d", got)
	}
	g.release()
	allCommitted(t, r.recv(0))
	eventually(t, "a fresh wait timer", func() bool { return r.timers.count(idleD) > idles })
	time.Sleep(50 * time.Millisecond)
	if got := r.inUse(); got != 1 {
		t.Fatalf("the stale timer released the session after the batch: InUse = %d", got)
	}
	if got := r.inst.ConnRecycled[obs.RecycleIdle].Load(); got != 1 {
		t.Fatalf("idle releases = %d before the fresh timer fired, want 1", got)
	}
	r.timers.latest(idleD).c <- time.Now()
	eventually(t, "the fresh timer's release", func() bool { return r.inUse() == 0 })
	if got := r.inst.ConnRecycled[obs.RecycleIdle].Load(); got != 2 {
		t.Errorf("writer_conn_recycled{idle} = %d, want 2", got)
	}

	// 4. the clock passes the session's lifetime.
	skew.Store(int64(time.Hour))
	r.batchQ <- r.batch(0)
	allCommitted(t, r.recv(0))
	eventually(t, "the Expired release", func() bool { return r.inUse() == 0 })
	if got := r.inst.ConnRecycled[obs.RecycleHold].Load(); got != 2 {
		t.Errorf("writer_conn_recycled{hold} = %d, want 2", got)
	}
}

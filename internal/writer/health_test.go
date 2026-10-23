package writer

import (
	"sync"
	"testing"
	"time"

	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock           { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }
func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }

func testHealth() (*poolHealth, *fakeClock) {
	c := newFakeClock()
	return newPoolHealth(healthConfig{
		failFastWindow:      30 * time.Second,
		failFastWrites:      20,
		statementFailNeed:   2,
		statementFailWindow: 60 * time.Second,
	}, c.now), c
}

func fail(class oradb.ErrClass) *oradb.Error { return &oradb.Error{Class: class} }

var keyA = stmtKey{Dest: 0, Mask: 0xFFFF}
var keyB = stmtKey{Dest: 1, Mask: 0x1FF}

func TestHealthTransitions(t *testing.T) {
	// Hand-written table of the V3 9.3 state table.
	for _, class := range []oradb.ErrClass{oradb.ErrSession, oradb.ErrSpace, oradb.ErrDeadlock, oradb.ErrUnknown} {
		h, _ := testHealth()
		if got := h.OnFailure(0, fail(class), keyA); got != Continue {
			t.Fatalf("%v: action = %v, want Continue", class, got)
		}
		if h.State() != HealthDegraded {
			t.Fatalf("%v: state = %v, want Degraded", class, h.State())
		}
		h.OnSuccess(1) // any writer's commit
		if h.State() != HealthHealthy {
			t.Fatalf("%v: after commit state = %v, want Healthy", class, h.State())
		}
	}
}

func TestHealthTimeGate(t *testing.T) {
	h, c := testHealth()
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	c.add(29 * time.Second)
	if got := h.OnFailure(1, fail(oradb.ErrSession), keyA); got != Continue {
		t.Fatalf("at 29s: action = %v, want Continue", got)
	}
	c.add(1 * time.Second)
	if got := h.OnFailure(2, fail(oradb.ErrSession), keyA); got != StopPool {
		t.Fatalf("at 30s: action = %v, want StopPool", got)
	}
	if h.State() != HealthFailFast {
		t.Fatalf("state = %v, want FailFast", h.State())
	}
	h.OnSuccess(0)
	if h.State() != HealthFailFast {
		t.Fatalf("FailFast must be terminal, state = %v", h.State())
	}
}

func TestHealthPerWriterCountGate(t *testing.T) {
	h, _ := testHealth() // clock never advances: only the count can fire
	for i := 1; i < 20; i++ {
		if got := h.OnFailure(3, fail(oradb.ErrDeadlock), keyA); got != Continue {
			t.Fatalf("failure %d: action = %v, want Continue", i, got)
		}
	}
	if got := h.OnFailure(3, fail(oradb.ErrDeadlock), keyA); got != StopPool {
		t.Fatalf("failure 20: action = %v, want StopPool", got)
	}
}

// Three writers failing seven times each is 21 pool-wide, which a summed
// count would trip; per-writer counts must not.
func TestHealthCountIsPerWriterNotSummed(t *testing.T) {
	h, c := testHealth()
	for round := 0; round < 7; round++ {
		for w := 0; w < 3; w++ {
			c.add(time.Second)
			if got := h.OnFailure(w, fail(oradb.ErrSession), keyA); got != Continue {
				t.Fatalf("round %d writer %d: action = %v, want Continue", round, w, got)
			}
		}
	}
	if h.State() != HealthDegraded {
		t.Fatalf("state = %v, want Degraded", h.State())
	}
}

func TestHealthRowRejectionLeavesState(t *testing.T) {
	h, _ := testHealth()
	if got := h.OnFailure(0, fail(oradb.ErrRowData), keyA); got != Continue {
		t.Fatalf("action = %v, want Continue", got)
	}
	if h.State() != HealthHealthy || h.degraded.Load() {
		t.Fatalf("row data changed health: state=%v degraded=%v", h.State(), h.degraded.Load())
	}
	// And in Degraded it neither advances the clock nor clears anything.
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	h.OnFailure(0, fail(oradb.ErrRowData), keyA)
	if h.State() != HealthDegraded || h.consec[0] != 1 {
		t.Fatalf("state=%v consec=%d, want Degraded and 1", h.State(), h.consec[0])
	}
}

func TestHealthSpaceSurvivesPing(t *testing.T) {
	h, _ := testHealth()
	h.OnFailure(0, fail(oradb.ErrSpace), keyA)
	h.OnProbeSuccess(0)
	if h.State() != HealthDegraded {
		t.Fatalf("a Ping cleared Space: state = %v", h.State())
	}
	// Space then Session: the open Space failure still holds.
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	h.OnProbeSuccess(0)
	if h.State() != HealthDegraded {
		t.Fatalf("a Ping cleared an open Space failure: state = %v", h.State())
	}
	h.OnSuccess(0)
	if h.State() != HealthHealthy {
		t.Fatalf("a commit must clear Space: state = %v", h.State())
	}
}

func TestHealthPingRestoresAfterSession(t *testing.T) {
	h, _ := testHealth()
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	h.OnProbeSuccess(0)
	if h.State() != HealthHealthy || h.degraded.Load() {
		t.Fatalf("state = %v, want Healthy", h.State())
	}
	// A Deadlock as the last class is not restored by a Ping.
	h.OnFailure(0, fail(oradb.ErrDeadlock), keyA)
	h.OnProbeSuccess(0)
	if h.State() != HealthDegraded {
		t.Fatalf("state = %v, want Degraded", h.State())
	}
}

func TestHealthProbeFailureFiresClock(t *testing.T) {
	h, c := testHealth()
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	c.add(30 * time.Second)
	if got := h.OnProbeFailure(0); got != StopPool {
		t.Fatalf("action = %v, want StopPool", got)
	}
}

func TestHealthStatementRule(t *testing.T) {
	h, _ := testHealth()
	if got := h.OnFailure(0, fail(oradb.ErrStatement), keyA); got != RetireWriter {
		t.Fatalf("first writer: %v, want RetireWriter", got)
	}
	if h.State() != HealthHealthy {
		t.Fatalf("a statement failure must not degrade: %v", h.State())
	}
	if got := h.OnFailure(1, fail(oradb.ErrStatement), keyA); got != StopPool {
		t.Fatalf("second writer: %v, want StopPool", got)
	}
	if h.State() != HealthFailFast {
		t.Fatalf("state = %v, want FailFast", h.State())
	}
}

// Non-vacuity for the lock-free path: OnSuccess on a Healthy pool must not
// take the mutex, so holding it from here must not block it.
func TestHealthOnSuccessHealthyIsLockFree(t *testing.T) {
	h, _ := testHealth()
	h.mu.Lock()
	defer h.mu.Unlock()
	done := make(chan struct{})
	go func() { h.OnSuccess(0); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnSuccess blocked on the mutex while Healthy")
	}
}

func TestHealthConcurrent(t *testing.T) {
	h, _ := testHealth()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				h.OnSuccess(w)
				h.OnFailure(w, fail(oradb.ErrSession), keyA)
				h.OnProbeSuccess(w)
				_ = h.State()
			}
		}()
	}
	wg.Wait()
	if h.degraded.Load() != (h.State() != HealthHealthy) {
		t.Fatalf("degraded flag %v disagrees with state %v", h.degraded.Load(), h.State())
	}
	if h.State() == HealthFailFast {
		t.Fatal("frozen clock and <20 consecutive failures must not reach FailFast")
	}

	// FailFast is terminal under concurrent commits.
	h, _ = testHealth()
	for i := 0; i < 20; i++ {
		h.OnFailure(9, fail(oradb.ErrDeadlock), keyA)
	}
	if h.State() != HealthFailFast {
		t.Fatalf("setup: state = %v, want FailFast", h.State())
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				h.OnSuccess(w)
				h.OnProbeSuccess(w)
			}
		}()
	}
	wg.Wait()
	if h.State() != HealthFailFast || !h.degraded.Load() {
		t.Fatalf("FailFast un-tripped: state=%v degraded=%v", h.State(), h.degraded.Load())
	}
}

func TestHealthRecoveryResetsCounters(t *testing.T) {
	// A commit clears per-writer counts: 19 + commit + 19 must not trip.
	h, _ := testHealth()
	for i := 0; i < 19; i++ {
		h.OnFailure(0, fail(oradb.ErrDeadlock), keyA)
	}
	h.OnSuccess(0)
	for i := 0; i < 19; i++ {
		if got := h.OnFailure(0, fail(oradb.ErrDeadlock), keyA); got != Continue {
			t.Fatalf("failure %d after recovery: %v, want Continue", i, got)
		}
	}
	// A new degradation restarts the 30 s clock.
	h, c := testHealth()
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	c.add(25 * time.Second)
	h.OnSuccess(0)
	c.add(25 * time.Second)
	h.OnFailure(0, fail(oradb.ErrSession), keyA) // new episode
	c.add(10 * time.Second)
	if got := h.OnFailure(0, fail(oradb.ErrSession), keyA); got != Continue {
		t.Fatalf("stale degradedSince: %v, want Continue", got)
	}
}

// The 30 s clock runs from the FIRST failure, not the latest.
func TestHealthClockFromFirstFailure(t *testing.T) {
	h, c := testHealth()
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	c.add(20 * time.Second)
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	c.add(10 * time.Second)
	if got := h.OnFailure(0, fail(oradb.ErrSession), keyA); got != StopPool {
		t.Fatalf("t=30s from first failure: %v, want StopPool", got)
	}
}

func TestHealthProbeFailureWhenHealthy(t *testing.T) {
	h, c := testHealth()
	c.add(time.Hour)
	if got := h.OnProbeFailure(0); got != Continue {
		t.Fatalf("action = %v, want Continue", got)
	}
}

// A RowData failure never fires the gate, even of a Degraded pool whose
// clock has run out.
func TestHealthRowDataDoesNotFireGate(t *testing.T) {
	h, c := testHealth()
	h.OnFailure(0, fail(oradb.ErrSession), keyA)
	c.add(time.Minute)
	if got := h.OnFailure(0, fail(oradb.ErrRowData), keyA); got != Continue {
		t.Fatalf("RowData: %v, want Continue", got)
	}
	if h.State() != HealthDegraded {
		t.Fatalf("state = %v, want Degraded", h.State())
	}
}

func TestHealthPingClassMatrix(t *testing.T) {
	cases := []struct {
		name  string
		seq   []oradb.ErrClass
		heals bool
	}{
		{"session", []oradb.ErrClass{oradb.ErrSession}, true},
		{"unknown", []oradb.ErrClass{oradb.ErrUnknown}, false},
		{"deadlock", []oradb.ErrClass{oradb.ErrDeadlock}, false},
		{"session then deadlock", []oradb.ErrClass{oradb.ErrSession, oradb.ErrDeadlock}, false},
		{"deadlock then session", []oradb.ErrClass{oradb.ErrDeadlock, oradb.ErrSession}, true},
	}
	for _, tc := range cases {
		h, _ := testHealth()
		for _, c := range tc.seq {
			h.OnFailure(0, fail(c), keyA)
		}
		if !h.degraded.Load() {
			t.Fatalf("%s: degraded flag not set", tc.name)
		}
		h.OnProbeSuccess(0)
		if healed := h.State() == HealthHealthy; healed != tc.heals {
			t.Errorf("%s: healed = %v, want %v", tc.name, healed, tc.heals)
		}
	}
}

// TestG_STMT_1_SameIdentityOnTwoWritersStopsPool pins the pure rule of gate
// G-STMT-1 (V3 9.4): one statement identity failing on two distinct writers
// inside 60 s stops the pool.
//
// SETUP: poolHealth with need=2, window=60s, injected clock.
// STIMULUS: writer 0 then writer 1 report ErrStatement for keyA 10 s apart.
// ASSERTION: first is RetireWriter (state Healthy), second is StopPool/FailFast.
// WOULD HAVE CAUGHT: a pool that retires writers one by one until none is
// left, turning one bad statement into a silent total outage.
func TestG_STMT_1_SameIdentityOnTwoWritersStopsPool(t *testing.T) {
	h, c := testHealth()
	if got := h.OnFailure(0, fail(oradb.ErrStatement), keyA); got != RetireWriter {
		t.Fatalf("first writer: %v, want RetireWriter", got)
	}
	c.add(10 * time.Second)
	if got := h.OnFailure(1, fail(oradb.ErrStatement), keyA); got != StopPool {
		t.Fatalf("second writer: %v, want StopPool", got)
	}
	if h.State() != HealthFailFast {
		t.Fatalf("state = %v, want FailFast", h.State())
	}
}

// TestG_STMT_1_NonVacuity runs the same stimulus with each ingredient of the
// rule removed in turn; none may stop the pool, so the test above cannot
// pass for the wrong reason.
//
// SETUP: as above.
// STIMULUS: (a) second writer id equals the first, (b) second failure 61 s
// after the first, (c) different identities, (d) id 32 aliasing id 0, and as
// the positive control (e) exactly 60 s apart.
// ASSERTION: (a)-(d) never StopPool; (e) does (the boundary is inclusive).
// WOULD HAVE CAUGHT: a rule that counts failures rather than distinct
// writers, ignores the window or the identity, or is off by one at 60 s.
func TestG_STMT_1_NonVacuity(t *testing.T) {
	cases := []struct {
		name string
		w2   int
		gap  time.Duration
		key2 stmtKey
		stop bool
	}{
		{"same writer twice", 0, 10 * time.Second, keyA, false},
		{"61s apart", 1, 61 * time.Second, keyA, false},
		{"different identities", 1, 10 * time.Second, keyB, false},
		{"id 32 aliases id 0", 32, 10 * time.Second, keyA, false},
		{"exactly 60s is inside", 1, 60 * time.Second, keyA, true},
	}
	for _, tc := range cases {
		h, c := testHealth()
		h.OnFailure(0, fail(oradb.ErrStatement), keyA)
		c.add(tc.gap)
		got := h.OnFailure(tc.w2, fail(oradb.ErrStatement), tc.key2)
		if (got == StopPool) != tc.stop {
			t.Errorf("%s: action = %v, want stop=%v", tc.name, got, tc.stop)
		}
	}
}

// TestG_B_V3b_RetryPolicy pins the batch retry policy of V3 9.2.
//
// SETUP: a fresh batchRetry per class.
// STIMULUS: decide() twice for each class.
// ASSERTION: Session, Deadlock, Space, Unknown retry once then give back;
// Statement never retries; RowData always goes to phase 2.
// WOULD HAVE CAUGHT: retrying a statement error, a second retry (unbounded
// hammering), or RowData given back whole (one bad row failing its good
// neighbours). The Statement row is the non-vacuity check: a policy that
// retried everything would fail it.
func TestG_B_V3b_RetryPolicy(t *testing.T) {
	TestRetryDecisions(t)
}

func TestLedger(t *testing.T) {
	c := newFakeClock()
	l := newStmtLedger(2, 60*time.Second)

	if l.record(keyA, 0, c.now()) {
		t.Fatal("one writer once must not stop")
	}
	c.add(time.Second)
	if l.record(keyA, 0, c.now()) {
		t.Fatal("one writer twice must not stop")
	}
	if l.record(keyB, 1, c.now()) {
		t.Fatal("different identities never combine")
	}
	c.add(10 * time.Second)
	if !l.record(keyA, 1, c.now()) {
		t.Fatal("two writers inside 60s must stop")
	}

	l = newStmtLedger(2, 60*time.Second)
	l.record(keyA, 0, c.now())
	c.add(61 * time.Second)
	if l.record(keyA, 1, c.now()) {
		t.Fatal("two writers 61s apart must not stop")
	}
	if len(l.m) != 1 {
		t.Fatalf("expired entry not dropped lazily: %d entries", len(l.m))
	}
}

// The 60 s boundary is inclusive: exactly at the edge still counts, one
// nanosecond later it does not.
func TestLedgerWindowBoundary(t *testing.T) {
	for _, tc := range []struct {
		gap  time.Duration
		stop bool
	}{{60 * time.Second, true}, {60*time.Second + 1, false}} {
		c := newFakeClock()
		l := newStmtLedger(2, 60*time.Second)
		l.record(keyA, 0, c.now())
		c.add(tc.gap)
		if got := l.record(keyA, 1, c.now()); got != tc.stop {
			t.Errorf("gap %v: stop = %v, want %v", tc.gap, got, tc.stop)
		}
	}
}

// Writer ids share a 32-bit mask: 0 and 32 alias and count as one.
func TestLedgerMaskAliasing(t *testing.T) {
	c := newFakeClock()
	l := newStmtLedger(2, 60*time.Second)
	l.record(keyA, 0, c.now())
	if l.record(keyA, 32, c.now()) {
		t.Fatal("ids 0 and 32 alias; documented as delaying, not stopping")
	}
	if !l.record(keyA, 1, c.now()) {
		t.Fatal("a genuinely distinct id must stop")
	}
}

func TestBackoffBounds(t *testing.T) {
	// Hand-written, from V3 9.1's table (pre-jitter).
	want := []time.Duration{0, 100, 200, 400, 800, 1600, 3200, 5000, 5000, 5000}
	for failures, ms := range want {
		base := ms * time.Millisecond
		for _, r := range []float64{0, 0.5, 0.999999} {
			b := backoff{initial: 100 * time.Millisecond, max: 5 * time.Second, rnd: func() float64 { return r }}
			got := b.delay(failures)
			lo, hi := time.Duration(float64(base)*0.8), time.Duration(float64(base)*1.2)
			if got < lo || got > hi {
				t.Fatalf("failures=%d r=%v: delay %v outside [%v,%v]", failures, r, got, lo, hi)
			}
		}
		mid := backoff{initial: 100 * time.Millisecond, max: 5 * time.Second, rnd: func() float64 { return 0.5 }}
		if got := mid.delay(failures); got != base {
			t.Fatalf("failures=%d: unjittered delay %v, want %v", failures, got, base)
		}
	}
	// Large counts neither overflow nor exceed the cap plus jitter.
	b := backoff{initial: 100 * time.Millisecond, max: 5 * time.Second, rnd: func() float64 { return 0.999999 }}
	if got := b.delay(1 << 20); got > 6*time.Second {
		t.Fatalf("delay(1<<20) = %v, exceeds cap+jitter", got)
	}
}

func TestRetryDecisions(t *testing.T) {
	cases := []struct {
		class oradb.ErrClass
		first retryDecision
		again retryDecision
	}{
		{oradb.ErrSession, retryOnce, giveBack},
		{oradb.ErrDeadlock, retryOnce, giveBack},
		{oradb.ErrSpace, retryOnce, giveBack},
		{oradb.ErrUnknown, retryOnce, giveBack},
		{oradb.ErrStatement, giveBack, giveBack},
		{oradb.ErrRowData, phase2, phase2},
	}
	for _, tc := range cases {
		var r batchRetry
		if got := r.decide(tc.class); got != tc.first {
			t.Errorf("%v first = %v, want %v", tc.class, got, tc.first)
		}
		if got := r.decide(tc.class); got != tc.again {
			t.Errorf("%v again = %v, want %v", tc.class, got, tc.again)
		}
	}
}

func TestRetryClassLabels(t *testing.T) {
	want := map[oradb.ErrClass]obs.RetryClass{
		oradb.ErrSession:  obs.RetrySession,
		oradb.ErrDeadlock: obs.RetryDeadlock,
		oradb.ErrSpace:    obs.RetrySpace,
		oradb.ErrUnknown:  obs.RetryUnknown,
	}
	for c, w := range want {
		if got, ok := retryClassOf(c); !ok || got != w {
			t.Errorf("%v -> %v,%v want %v", c, got, ok, w)
		}
	}
	for _, c := range []oradb.ErrClass{oradb.ErrRowData, oradb.ErrStatement} {
		if _, ok := retryClassOf(c); ok {
			t.Errorf("%v must not map to a retry class", c)
		}
	}
}

package app

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/health"
	"feedvalve/internal/model"
)

// steppedClock is an injected stats clock whose wall reading can be stepped
// while its monotonic reading keeps running, which is what an NTP step or a
// manual date change does to a real process.
type steppedClock struct {
	offsetNs atomic.Int64
	base     time.Time
}

func newSteppedClock() *steppedClock {
	return &steppedClock{base: time.Date(2026, 3, 2, 10, 0, 0, 0, time.UTC)}
}

func (c *steppedClock) now() (time.Time, time.Duration) {
	mono := time.Since(processStart)
	return c.base.Add(mono + time.Duration(c.offsetNs.Load())), mono
}

func (c *steppedClock) step(d time.Duration) { c.offsetNs.Add(int64(d)) }

// TestClockStepCheckIsTheWallMinusMonotonicDifference pins the detector's
// arithmetic with no goroutine: what it flags, what it does not, and the three
// shapes that must not false-positive.
func TestClockStepCheckIsTheWallMinusMonotonicDifference(t *testing.T) {
	t0 := time.Date(2026, 3, 2, 10, 0, 0, 0, time.UTC)
	const iv = 60 * time.Second
	for _, c := range []struct {
		name        string
		wallDelta   time.Duration
		monoDelta   time.Duration
		wantStepped bool
		wantSkew    time.Duration
	}{
		{"exact interval", iv, iv, false, 0},
		{"jittery tick, 40 ms late", iv + 40*time.Millisecond, iv + 40*time.Millisecond, false, 0},
		{"wall slewed 300 ms by ntp", iv + 300*time.Millisecond, iv, false, 300 * time.Millisecond},
		{"ticker backlog: a stalled goroutine, 10 minutes late, both clocks agree", 10 * time.Minute, 10 * time.Minute, false, 0},
		{"wall +1 h", iv + time.Hour, iv, true, time.Hour},
		{"wall -1 h", iv - time.Hour, iv, true, -time.Hour},
		{"wall +1.5 s", iv + 1500*time.Millisecond, iv, true, 1500 * time.Millisecond},
	} {
		s := clockStep{wall: t0, mono: 5 * time.Second}
		skew, stepped := s.check(t0.Add(c.wallDelta), 5*time.Second+c.monoDelta)
		if stepped != c.wantStepped || skew != c.wantSkew {
			t.Errorf("%s: skew=%v stepped=%v, want skew=%v stepped=%v",
				c.name, skew, stepped, c.wantSkew, c.wantStepped)
		}
	}

	// The first reading after a (re)start is the baseline, not a comparison:
	// a fresh clockStep initialised from the same reading reports nothing.
	s := clockStep{wall: t0, mono: time.Second}
	if _, stepped := s.check(t0, time.Second); stepped {
		t.Error("the first emission after a start reported a step")
	}
}

// TestG_CLOCK_1_AWallClockStepIsCountedAndNothingElseMoves is gate G-CLOCK-1.
//
// SETUP: a running graph whose stats goroutine reads an injected clock -- a
// real monotonic reading plus a wall reading that the test can step -- with a
// 1 s stats interval and a datagram already observed, so the circuit is Live.
// STIMULUS: step the wall clock +3600 s, wait for an emission, then step it
// -3600 s and wait for another.
// ASSERTION: clock_step_detected is 1 after the first and 2 after the second,
// each with a WARN naming the direction and the 1h0m0s magnitude; an interval
// with no step in it adds nothing; and the circuit is still Live afterwards,
// because it is fed monotonic time and an hour of wall jump would otherwise
// read as an hour of silence.
// WOULD HAVE CAUGHT: the previous detector, which compared two ticker values
// and so measured the ticker's lateness and not the wall clock; and any use of
// t1.UnixNano() - t0.UnixNano() for an elapsed measurement (Vol 0 section
// 10.2), here in the circuit's input.
// NON-VACUITY: the counter starts at 0 and the first emission with no step
// leaves it there, so the increments are the steps' doing.
func TestG_CLOCK_1_AWallClockStepIsCountedAndNothingElseMoves(t *testing.T) {
	clk := newSteppedClock()
	g, buf := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		c.Stats.IntervalSec = 1
		c.Liveness.HeartbeatWarnSec = 3000 // the one datagram must not age out mid-test
		c.Liveness.HeartbeatDownSec = 3600
	})
	g.statsNow = clk.now
	in := g.inst()
	in.lastDatagramMonoNs.Store(int64(time.Since(processStart)))

	cancel, done := runGraph(t, g)
	defer func() { cancel(); <-done }()

	emissions := func() int { return strings.Count(buf.String(), "INFO stats gen=") }
	awaitEmissions := func(n int) {
		t.Helper()
		waitUntil(t, "an emission", func() bool { return emissions() >= n })
	}

	// One quiet interval first: no step, no count.
	awaitEmissions(1)
	if n := in.clockStepDetected.Load(); n != 0 {
		t.Fatalf("clock_step_detected = %d after an interval with no step", n)
	}

	base := emissions()
	clk.step(time.Hour)
	awaitEmissions(base + 1)
	if n := in.clockStepDetected.Load(); n != 1 {
		t.Fatalf("clock_step_detected = %d after a +3600 s step, want 1", n)
	}

	base = emissions()
	clk.step(-time.Hour)
	awaitEmissions(base + 1)
	if n := in.clockStepDetected.Load(); n != 2 {
		t.Fatalf("clock_step_detected = %d after a -3600 s step, want 2", n)
	}

	// An interval with no step after the steps adds nothing.
	base = emissions()
	awaitEmissions(base + 1)
	if n := in.clockStepDetected.Load(); n != 2 {
		t.Errorf("clock_step_detected = %d after a quiet interval, want it to stay 2", n)
	}

	out := buf.String()
	for _, want := range []string{"wall clock stepped forward by 1h0m0s", "wall clock stepped backward by 1h0m0s"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in the log:\n%s", want, out)
		}
	}
	if st := health.CircuitState(in.circuitState.Load()); st != health.CircuitLive {
		t.Errorf("circuit_state = %v after wall steps, want live: the circuit must read monotonic time", st)
	}
	if in.circuitQuietEvents.Load() != 0 || in.circuitDownEvents.Load() != 0 {
		t.Errorf("a wall step manufactured a circuit event: quiet=%d down=%d",
			in.circuitQuietEvents.Load(), in.circuitDownEvents.Load())
	}
}

// TestAPanicInTheStatsGoroutineRestartsItAndEmissionContinues is F28.
//
// SETUP: a running graph with a 1 s stats interval whose injected clock panics
// on one call. STIMULUS: let the first emission fire into the panic.
// ASSERTION: panics_recovered{stage=stats} is 1, the log carries the stack
// through the AppLogger, and emissions CONTINUE afterwards -- from a restarted
// goroutine with all three timers, which is what the circuit tick proves by
// reaching Down on its own.
// WOULD HAVE CAUGHT: no recover (the process dies with exit 2 on the first
// bad tick), and a recover that returns without restarting (observability
// silently ends, which is the failure R12 exists to prevent).
func TestAPanicInTheStatsGoroutineRestartsItAndEmissionContinues(t *testing.T) {
	var calls atomic.Int32
	clk := newSteppedClock()
	g, buf := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		c.Stats.IntervalSec = 1
		c.Liveness.StartupGraceSec = 2
		c.Liveness.HeartbeatWarnSec = 1
		c.Liveness.HeartbeatDownSec = 2
	})
	g.statsNow = func() (time.Time, time.Duration) {
		// Call 1 is the baseline read at goroutine start; call 2 is the first
		// circuit or emission tick. Panic on the second.
		if calls.Add(1) == 2 {
			panic("injected stats failure")
		}
		return clk.now()
	}

	cancel, done := runGraph(t, g)
	defer func() { cancel(); <-done }()

	waitUntil(t, "the recovered panic", func() bool {
		return g.inst().panicsRecoveredStats.Load() == 1
	})
	if out := buf.String(); !strings.Contains(out, "injected stats failure") ||
		!strings.Contains(out, "goroutine ") {
		t.Errorf("the recovery did not log the value and a stack:\n%s", out)
	}

	before := strings.Count(buf.String(), "INFO stats gen=")
	waitUntil(t, "an emission after the restart", func() bool {
		return strings.Count(buf.String(), "INFO stats gen=") > before
	})
	waitUntil(t, "the restarted circuit timer reaching Down", func() bool {
		return g.inst().circuitState.Load() == int64(health.CircuitDown)
	})
	if n := g.inst().panicsRecoveredStats.Load(); n != 1 {
		t.Errorf("panics_recovered{stats} = %d, want exactly the one injected", n)
	}
}

// TestSessionEventsQueuedAcrossAStatsRestartAreDrainedNotDropped is the P5-12
// restart-mid-fill case: while the stats goroutine is down, the decoder keeps
// publishing; capacity 64 absorbs it and the restarted goroutine drains every
// event with session_events_dropped still 0.
func TestSessionEventsQueuedAcrossAStatsRestartAreDrainedNotDropped(t *testing.T) {
	var calls atomic.Int32
	clk := newSteppedClock()
	g, _ := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		c.Stats.IntervalSec = 1
		c.Liveness.HeartbeatWarnSec = 1
	})
	g.statsNow = func() (time.Time, time.Duration) {
		if calls.Add(1) == 2 {
			panic("injected stats failure")
		}
		return clk.now()
	}
	// Filled before Run, which is the same state as filling during the restart
	// delay: events present, no drain running.
	for i := range 20 {
		g.sessionCh <- model.SessionEvent{MarketType: 1, Code: 6511, RecvMono: int64(i + 1)}
	}

	cancel, done := runGraph(t, g)
	defer func() { cancel(); <-done }()

	waitUntil(t, "the recovered panic", func() bool {
		return g.inst().panicsRecoveredStats.Load() == 1
	})
	waitUntil(t, "the restarted goroutine to drain channel H", func() bool {
		return len(g.sessionCh) == 0
	})
	if n := g.inst().sessionEventsDropped.Load(); n != 0 {
		t.Errorf("session_events_dropped = %d, want 0", n)
	}
	if n := g.inst().sessionTransitions[0].Load(); n != 20 {
		t.Errorf("session_transitions{1} = %d, want all 20 applied", n)
	}
}

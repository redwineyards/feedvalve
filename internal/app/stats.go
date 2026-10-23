package app

import (
	"runtime/debug"
	"time"

	"feedvalve/internal/obs"
)

// The stats goroutine (Vol 4 sections 5.5 and 6.1). startStats in lifecycle.go
// launches statsLoop; the body lives here so its hardening -- the F28 recover
// and the F23 clock-step detector -- sits beside the loop it hardens.

// statsRestartDelay is how long the supervisor waits before restarting a
// stats goroutine that panicked. Not for politeness: a panic at the top of the
// body would otherwise restart in a hot loop. Not a configuration key.
const statsRestartDelay = 100 * time.Millisecond

// statsRestartMax caps the doubling of that delay across consecutive panics.
const statsRestartMax = 5 * time.Second

// clockStepThreshold is how far wall time may drift from monotonic time over
// one emission interval before it is called a step. NTP slews are far below
// it; a manual or stepped adjustment is far above.
const clockStepThreshold = time.Second

// clockNow is the stats goroutine's one clock read: the wall reading, with its
// monotonic part STRIPPED (Round(0)), and the monotonic time since
// processStart. Wall is for the log's timestamps and the trading window; mono
// is for every elapsed-time decision, and is the same origin as the receiver's
// RecvMono and last_datagram_mono_ns.
//
// Both come from one time.Now, so the pair is coherent. Graph.statsNow
// replaces it in tests; the production value is nil.
func (g *Graph) clockNow() (wall time.Time, mono time.Duration) {
	if g.statsNow != nil {
		return g.statsNow()
	}
	t := time.Now()
	return t.Round(0), t.Sub(processStart)
}

// clockStep is the F23 detector's memory: the previous emission's wall and
// monotonic readings.
type clockStep struct {
	wall time.Time
	mono time.Duration
}

// check compares how far the wall clock moved since the last call with how far
// the monotonic clock moved. They agree to within scheduling jitter unless the
// wall clock was stepped, so the difference IS the step -- independent of the
// ticker's lateness, which moves both equally and used to be (wrongly) what the
// previous check measured. Positive skew means the wall clock jumped forward.
func (c *clockStep) check(wall time.Time, mono time.Duration) (skew time.Duration, stepped bool) {
	skew = wall.Sub(c.wall) - (mono - c.mono)
	c.wall, c.mono = wall, mono
	return skew, skew > clockStepThreshold || skew < -clockStepThreshold
}

// statsLoop supervises runStats: the goroutine the lifecycle joins.
//
// F28: observability is the whole safety story, so a panic in the stats body is
// logged and the goroutine restarted, with all three of its timers, for as long
// as statsStop is open (D18). Anything that makes the body return normally is
// the lifecycle asking it to stop.
func (g *Graph) statsLoop() {
	defer close(g.statsDone)
	delay := statsRestartDelay
	for {
		if g.runStats() {
			return
		}
		// A persistent panic logs a full stack per restart under a class the
		// rate limiter exempts, so the delay doubles to a cap: the first
		// panics are prompt, a stuck one costs a stack every few seconds and
		// not ten a second. The counter counts every one regardless.
		select {
		case <-g.statsStop:
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, statsRestartMax)
	}
}

// statsPanic is the F28 recover handler. It logs through the AppLogger and
// nothing else before counting, because it must not depend on the registry it
// may have just corrupted. It is the obsClassStartup class, which the Allow
// scan exempts, for stallFatal's reason: a stack that a drained bucket could
// suppress is a stack nobody gets.
func (g *Graph) statsPanic(rec any) {
	g.deps.Log.Errorf(obsClassStartup, "recovered from a panic in the stats goroutine, restarting it: %v\n%s",
		rec, debug.Stack())
	g.inst().panicsRecoveredStats.Add(1)
}

// runStats is one life of the stats goroutine: three timers, each re-armed
// from the LIVE configuration at every fire, so a reload takes effect within
// one old period. Returns true when told to stop and false after a recovered
// panic.
func (g *Graph) runStats() (stopped bool) {
	defer func() {
		if rec := recover(); rec != nil {
			g.statsPanic(rec)
			stopped = false
		}
	}()

	// The emission timer. A Timer and not a Ticker, for the re-arm.
	emit := time.NewTimer(g.loadRuntime().Cfg.StatsInterval())
	defer emit.Stop()
	// The ordering guard's evaluator, every ordering.guardEvalIntervalSec --
	// deliberately not the 60 s stats interval, because a frozen transcode's
	// rows stop updating entirely until it runs (Vol 2 section 5.5). A fixed
	// timer, re-armed from the live configuration like the other two;
	// guardEvalOverride is the seam a test shortens below the 1 s floor.
	guard := time.NewTimer(g.guardInterval())
	defer guard.Stop()
	// The circuit's evaluation tick (section 3.3).
	circ := time.NewTimer(circuitInterval(&g.loadRuntime().Cfg.Liveness))
	defer circ.Stop()

	var step clockStep
	step.wall, step.mono = g.clockNow()

	for {
		select {
		case <-g.statsStop:
			return true
		case <-guard.C:
			g.evaluateGuards()
			guard.Reset(g.guardInterval())
		case <-circ.C:
			wall, mono := g.clockNow()
			g.healthTick(int64(mono), wall)
			circ.Reset(circuitInterval(&g.loadRuntime().Cfg.Liveness))
		case <-emit.C:
			wall, mono := g.clockNow()
			if skew, stepped := step.check(wall, mono); stepped {
				g.clockStepped(skew)
			}
			if err := g.emitStats(); err != nil {
				g.deps.Log.Errorf(obsClassStartup, "emitting the stats line: %v", err)
			}
			emit.Reset(g.loadRuntime().Cfg.StatsInterval())
		}
	}
}

// guardInterval is the guard evaluator's period: the live
// ordering.guardEvalIntervalSec, unless a test has overridden it.
func (g *Graph) guardInterval() time.Duration {
	if g.guardEvalOverride != 0 {
		return g.guardEvalOverride
	}
	return g.loadRuntime().Mirror.GuardEvalInterval
}

// clockStepped counts and reports one wall-clock step (F23). The step makes
// every timestamp in the log discontinuous; saying so within one interval is
// the whole value of the counter. Nothing else changes: ordering, the circuit's
// thresholds, the flush tick and ConnMaxLifetime all measure monotonic time.
func (g *Graph) clockStepped(skew time.Duration) {
	g.inst().clockStepDetected.Add(1)
	dir, mag := "forward", skew
	if skew < 0 {
		dir, mag = "backward", -skew
	}
	if g.deps.Log.Allow(obs.ClassClockStep) {
		g.deps.Log.Warnf(obs.ClassClockStep,
			"wall clock stepped %s by %s since the last stats line; log timestamps are "+
				"discontinuous here, elapsed-time decisions use the monotonic clock", dir, mag)
	}
}

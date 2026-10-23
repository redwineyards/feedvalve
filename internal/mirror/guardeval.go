package mirror

import (
	"strconv"

	"feedvalve/internal/obs"
)

// GuardSnapshot is the guard evaluator's state (Vol 2 section 5.5):
// guard_superseded and guard_offered per configured transcode as of that
// transcode's last ACTUAL evaluation, and the ordering_guard_disabled gauges
// the evaluator writes, all indexed like the GuardTable.
//
// Owned by the one goroutine that calls EvaluateGuards (the stats goroutine,
// wired by P3-13); no shard reads or writes it, so it needs no synchronisation.
type GuardSnapshot struct {
	superseded []uint64
	offered    []uint64
	disabled   []*obs.Gauge
}

// NewGuardSnapshot is t's snapshot at zero: the guard counters are process
// lifetime and start at zero, so the first evaluation's deltas are the totals.
//
// disabled is TranscodeInstruments.GuardDisabled, indexed like t. It is checked
// here, at startup, as NewGuardTable checks its counters: a gauge is touched
// only when its guard disables, so a short slice or a nil gauge would otherwise
// pass every evaluation and panic the stats goroutine hours into a session.
func NewGuardSnapshot(t *GuardTable, disabled []*obs.Gauge) *GuardSnapshot {
	if len(disabled) != len(t.entries) {
		panic("mirror: NewGuardSnapshot given " + strconv.Itoa(len(disabled)) +
			" ordering_guard_disabled gauges for " + strconv.Itoa(len(t.entries)) +
			" configured transcodes; they must be zipped by configured position")
	}
	for i, g := range disabled {
		if g == nil {
			panic("mirror: NewGuardSnapshot given a nil ordering_guard_disabled gauge for transcode " +
				strconv.Itoa(int(t.entries[i].code)) + "; the gauges must be resolved before the evaluator is built")
		}
	}
	return &GuardSnapshot{
		superseded: make([]uint64, len(t.entries)),
		offered:    make([]uint64, len(t.entries)),
		disabled:   disabled,
	}
}

// EvaluateGuards is one pass of Vol 2 section 5.5's per-transcode auto-disable.
// It has no ticker of its own: the caller runs it every
// ordering.guardEvalIntervalSec (P3-13).
//
// For each configured transcode still guarded, it takes the deltas of
// guard_offered and guard_superseded since that transcode's last evaluation.
// Below p.GuardMinSamples it skips WITHOUT advancing the snapshot, so a
// low-rate transcode accumulates across intervals instead of never reaching the
// floor in any one of them (section 5.5 point 1). Otherwise it evaluates: if
// ordering.guard is auto and Δsuperseded/Δoffered is STRICTLY greater than
// p.GuardSuppressionRatio (so a configured ratio of exactly 1.0 can never
// fire), it sets the transcode's off bit, stores 1 into its
// ordering_guard_disabled gauge and logs one ERROR; either way the snapshot
// advances. Under on or off the snapshot still advances, so the window stays
// current; only the action is gated on auto.
//
// Disabling is one-way (point 4): an entry already off is skipped, so it is
// never re-logged and nothing here ever stores false. This function is the
// ONLY writer of the off bits (point 3); the shards only load them.
//
// The ratio is per transcode, never global (HLD section 6.5): one transcode's
// dead sequence field must neither hide inside the feed's healthy average nor
// switch the guard off for the well-behaved codes beside it. It is also per
// window, never lifetime: hours of healthy history must not dilute a sequence
// field that dies later.
//
// The ERROR goes through ClassGuardDisabled's rate limit like every other
// ERROR, so at a low stats.eventRatePerMin a disable close behind another can
// lose its line (counted in log_events_suppressed{guard_disabled}). The gauge,
// set unconditionally, is the durable record.
//
// snap must be NewGuardSnapshot(t, ...).
func EvaluateGuards(t *GuardTable, p *Params, snap *GuardSnapshot, log *obs.AppLogger) {
	for i := range t.entries {
		g := &t.entries[i]
		if g.off.Load() {
			continue
		}
		// superseded BEFORE offered: a shard adds offered before superseded
		// for the same comparison, so every superseded increment read here
		// has its offered increment in the later read, and the totals keep
		// superseded <= offered. The two loads are not one snapshot, though:
		// every comparison that finishes between them lands its offered in
		// this snapshot and its superseded in the next window. Nothing bounds
		// how many (a preempted evaluator lets a shard finish any number), so
		// a window's Δsuperseded can exceed its Δoffered by any amount, while
		// the totals across windows stay exact. It is clamped, because the
		// true ratio never exceeds 1 and a 1.0 threshold must stay
		// unreachable.
		supTotal := g.superseded.Load()
		offTotal := g.offered.Load()
		dSup, dOff := supTotal-snap.superseded[i], offTotal-snap.offered[i]
		if dOff < uint64(p.GuardMinSamples) {
			continue // do NOT advance: let it accumulate (point 1)
		}
		if dSup > dOff {
			dSup = dOff
		}
		ratio := float64(dSup) / float64(dOff)
		if p.Guard == GuardAuto && ratio > p.GuardSuppressionRatio {
			g.off.Store(true)
			snap.disabled[i].Store(1)
			log.Errorf(obs.ClassGuardDisabled,
				"ordering guard disabled for the remainder of this session: transcode=%d ratio=%.3f superseded=%d offered=%d threshold=%g",
				g.code, ratio, dSup, dOff, p.GuardSuppressionRatio)
		}
		snap.superseded[i], snap.offered[i] = supTotal, offTotal
	}
}

package obs

import (
	"io"
	"strings"
	"testing"
	"time"
)

// AppLogger.Allow is Vol 1 section 11.5's gate: the ONLY thing on the packet
// path that may be called before a log line's arguments are built. These are
// the properties a call site depends on.

// TestClassSeverityBoundary pins firstErrorClass against the enum.
//
// Allow derives a class's severity from its position relative to that
// boundary, which is only sound while the enum keeps WARN and ERROR in two
// contiguous runs. Inserting a WARN class after firstErrorClass would mis-gate
// it -- a WARN that survives at ERROR level, or an ERROR whose arguments are
// built and thrown away -- and neither shows up in anything but an allocation
// count, which is why this is pinned rather than trusted.
func TestClassSeverityBoundary(t *testing.T) {
	// The last WARN and the first ERROR of Vol 4 section 6.2's table, named
	// rather than computed, so a reordering has to disagree with a name.
	if firstErrorClass != ClassDecompressFailed {
		t.Errorf("firstErrorClass = %v, want ClassDecompressFailed -- the first ERROR class "+
			"of Vol 4 section 6.2", firstErrorClass)
	}
	if ClassEpochStalled != firstErrorClass-1 {
		t.Errorf("ClassEpochStalled = %d and firstErrorClass = %d: the WARN run no longer "+
			"ends immediately before the ERROR run, so Allow's severity test is wrong",
			ClassEpochStalled, firstErrorClass)
	}
	// The numeric position, pinned. P3-04 inserted the WARN epoch_stalled at the
	// end of the WARN run, which moved firstErrorClass from 10 to 11 and
	// relabelled every ERROR class's log_events_suppressed{class} slot. That was
	// deliberate; the next move should be too, so it has to edit this number.
	if firstErrorClass != 11 || numClasses != 22 {
		t.Errorf("firstErrorClass = %d, numClasses = %d; want 11 and 22. A class was "+
			"added or moved: check its severity run and update this pin deliberately",
			firstErrorClass, numClasses)
	}
	// And the four P3-04 classes are on the side Vol 2 logs them at.
	for _, c := range []Class{ClassBlobLenMismatch, ClassGuardDisabled, ClassDrainDeadline} {
		if c < firstErrorClass {
			t.Errorf("%v is in the WARN run; Vol 2 logs it at ERROR", c)
		}
	}
	if ClassEpochStalled >= firstErrorClass {
		t.Errorf("epoch_stalled is in the ERROR run; Vol 2 section 2.4 logs it at WARN")
	}
	// Every class is on one side or the other, and both sides are non-empty.
	if firstErrorClass == 0 || firstErrorClass >= numClasses {
		t.Errorf("firstErrorClass = %d with %d classes: one of the two runs is empty",
			firstErrorClass, numClasses)
	}
}

// TestAllowDoesNotConsumeATokenItReports is the advisory property, and the one
// a call site would notice: Allow must not spend what the Warnf after it needs.
//
// If it consumed, a guarded site would emit half as many lines as its
// configured rate -- the bucket would be charged twice per line -- and the
// first thing to go would be the line an operator is waiting for.
func TestAllowDoesNotConsumeATokenItReports(t *testing.T) {
	const rate = 4
	var buf strings.Builder
	l := NewAppLogger(&buf, LevelInfo, rate, nil)

	// Peek repeatedly: none of these may cost anything.
	for range 100 {
		if !l.Allow(ClassSeqGap) {
			t.Fatalf("Allow went false with a full bucket; it is consuming tokens")
		}
	}

	// The full rate is still available to the emitter.
	for i := range rate {
		l.Warnf(ClassSeqGap, "line %d", i)
	}
	if got := strings.Count(buf.String(), "\n"); got != rate {
		t.Errorf("emitted %d lines at a rate of %d after 100 Allow calls, want %d", got, rate, rate)
	}

	// And now the bucket really is empty, which Allow must report.
	if l.Allow(ClassSeqGap) {
		t.Errorf("Allow is true with a drained bucket")
	}
}

// TestAllowTracksItsOwnClass confirms the gate is per class, like the bucket it
// reads. A single shared gate would let one noisy class silence every other --
// the failure mode obs/logger.go rejects a global bucket for.
func TestAllowTracksItsOwnClass(t *testing.T) {
	l := NewAppLogger(io.Discard, LevelInfo, 1, nil)

	l.Warnf(ClassSeqGap, "spend seq_gap's only token")

	if l.Allow(ClassSeqGap) {
		t.Errorf("Allow(ClassSeqGap) is true after its token was spent")
	}
	if !l.Allow(ClassNetidMismatch) {
		t.Errorf("Allow(ClassNetidMismatch) is false -- a drained class is starving an " +
			"untouched one")
	}
}

// TestAllowAppliesTheLevelOfItsClassSeverity covers the reason Allow checks the
// level at all: without it, a deployment running at ERROR still pays to build
// every WARN's arguments, which is exactly the cost section 11.5 removes.
func TestAllowAppliesTheLevelOfItsClassSeverity(t *testing.T) {
	l := NewAppLogger(io.Discard, LevelError, 100, nil)

	if l.Allow(ClassSeqGap) {
		t.Errorf("Allow(ClassSeqGap) is true at ERROR level; Warnf would not emit it")
	}
	// ERROR classes are not level-gated, matching Errorf, which has no level
	// check of its own.
	if !l.Allow(ClassDecompressFailed) {
		t.Errorf("Allow(ClassDecompressFailed) is false at ERROR level, but Errorf would emit")
	}
}

// TestAllowRefillsSoAGuardedClassIsNotSilencedForever is the bug a
// non-refilling peek would have: the refill is the only thing that ever ADDS a
// token, so a peek that skipped it would report empty forever once drained --
// and a call site gating its Warnf behind it would silence the class for the
// life of the process, with the counters still incrementing and no line ever
// appearing again.
func TestAllowRefillsSoAGuardedClassIsNotSilencedForever(t *testing.T) {
	// 60/min is one token per second, so the refill is reachable by moving the
	// logger's origin rather than by sleeping.
	l := NewAppLogger(io.Discard, LevelInfo, 60, nil)

	// Drain it.
	for range 60 {
		l.Warnf(ClassSeqGap, "drain")
	}
	if l.Allow(ClassSeqGap) {
		t.Fatalf("the bucket did not drain")
	}

	// Backdate the origin by two seconds: two tokens are now due.
	l.start = l.start.Add(-2 * time.Second)

	if !l.Allow(ClassSeqGap) {
		t.Errorf("Allow is still false after two seconds' worth of refill at 60/min -- a " +
			"guarded call site would be silenced permanently")
	}
}

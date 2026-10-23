package obs

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestG_LOG_1_RateLimitBoundsOneClassAndReportsTheRemainder is gate G-LOG-1, at
// the figures Vol 4 gives it: eventRatePerMin = 10 and 1,000 events of one
// class inside one minute. It extends the mechanism covered at 10,000 events by
// TestTokenBucketBoundsOneClassAndCountsTheRest with the spec's remaining
// clauses (suppressed=N, the counter equal to the remainder, other classes
// unaffected).
//
// SETUP: a registry-backed logger at 10/min, with log_events_suppressed bound
// for the flooded class and for a bystander class.
// STIMULUS: 1,000 WARNs of one class, then one WARN of the bystander class,
// then (after a hand refill, so the test does not sleep) one more of the
// flooded class.
// ASSERTION: at most 10 lines of the flooded class; the bystander is not
// suppressed; log_events_suppressed{class} equals exactly the remainder; the
// next emitted line of the class carries suppressed=<that remainder>.
// WOULD HAVE CAUGHT: a misframed feed making the logger the loudest and
// slowest thing in the process (HLD 10.2) -- a bucket that leaked, a single
// global bucket starving other classes, or a counter that undercounts.
// NON-VACUITY: the remainder is required to be large, so a run that suppressed
// nothing cannot pass, and the suffix is required to be absent beforehand.
func TestG_LOG_1_RateLimitBoundsOneClassAndReportsTheRemainder(t *testing.T) {
	const rate, burst = 10, 1000
	var buf bytes.Buffer
	r := New(testOptions())
	flooded := r.CounterAt(IDLogEventsSuppressed, int(ClassNetidMismatch))
	bystander := r.CounterAt(IDLogEventsSuppressed, int(ClassSeqGap))
	r.Freeze()
	lg := NewAppLogger(&buf, LevelInfo, rate, r)

	for range burst {
		lg.Warnf(ClassNetidMismatch, "flood")
	}
	lg.Warnf(ClassSeqGap, "bystander")

	out := buf.String()
	lines := strings.Count(out, "flood")
	if lines > rate || lines == 0 {
		t.Fatalf("a burst of %d emitted %d lines, want 1..%d", burst, lines, rate)
	}
	if strings.Contains(out, "suppressed=") {
		t.Errorf("a suppression suffix appeared before the bucket was refilled:\n%s", out)
	}
	if !strings.Contains(out, "bystander") || bystander.Load() != 0 {
		t.Errorf("another class was affected by the flood (emitted=%v, suppressed=%d)",
			strings.Contains(out, "bystander"), bystander.Load())
	}
	remainder := uint64(burst - lines)
	if remainder < 900 {
		t.Fatalf("remainder %d is too small to prove suppression happened", remainder)
	}
	if got := flooded.Load(); got != remainder {
		t.Errorf("log_events_suppressed{netid_mismatch} = %d, want the remainder %d", got, remainder)
	}

	// Hand-refill one token instead of sleeping a minute.
	lg.buckets[ClassNetidMismatch].tokens.Store(1)
	buf.Reset()
	lg.Warnf(ClassNetidMismatch, "flood")
	if want := "suppressed=" + strconv.FormatUint(remainder, 10); !strings.Contains(buf.String(), want) {
		t.Errorf("the next emitted line lacks %q:\n%s", want, buf.String())
	}
}

// TestLevelAndDebugAreReloadSafe is the P5-01 race test: SetLevel and
// DebugLogger.Set run while other goroutines log and read the trace selection,
// which is what a SIGHUP reload does under load. It proves the race half only
// under -race, where the old plain-field writes are reported; without it the
// publication assertions at the end still run.
func TestLevelAndDebugAreReloadSafe(t *testing.T) {
	lg := NewAppLogger(discard{}, LevelInfo, 1000000, nil)
	dbg := NewDebugLogger(discard{}, nil, false)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lg.Infof("i")
				lg.Warnf(ClassSeqGap, "w")
				_ = lg.Allow(ClassSeqGap)
				_ = dbg.Enabled(7208)
				_ = dbg.Any()
				_ = dbg.HexDumpEnabled()
				dbg.HexDump([]byte{1})
			}
		}()
	}
	for i := range 2000 {
		lg.SetLevel(Level(i % 3))
		if i%2 == 0 {
			dbg.Set([]int{7208, 7305}, true)
		} else {
			dbg.Set(nil, false)
		}
	}
	close(stop)
	wg.Wait()

	dbg.Set([]int{7305}, true)
	if !dbg.Enabled(7305) || dbg.Enabled(7208) || !dbg.Any() || !dbg.HexDumpEnabled() {
		t.Error("Set did not publish the new selection")
	}
	lg.SetLevel(LevelError)
	if lg.Allow(ClassSeqGap) {
		t.Error("SetLevel(LevelError) did not take effect for a WARN class")
	}
	allocs := testing.AllocsPerRun(100, func() {
		dbg.Enabled(7208)
		dbg.Any()
		dbg.HexDumpEnabled()
	})
	if allocs > 0 {
		t.Errorf("the trace readers allocate %.1f times after Set", allocs)
	}
}

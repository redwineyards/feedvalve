package obs

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestLevelThresholdFilters(t *testing.T) {
	for _, tc := range []struct {
		level            Level
		info, warn, errs bool
	}{
		{LevelInfo, true, true, true},
		{LevelWarn, false, true, true},
		{LevelError, false, false, true},
	} {
		t.Run(tc.level.name(), func(t *testing.T) {
			var buf bytes.Buffer
			lg := NewAppLogger(&buf, tc.level, 1000, nil)
			lg.Infof("info line")
			lg.Warnf(ClassSeqGap, "warn line")
			lg.Errorf(ClassWriteFailed, "error line")

			out := buf.String()
			for _, c := range []struct {
				want bool
				text string
			}{{tc.info, "info line"}, {tc.warn, "warn line"}, {tc.errs, "error line"}} {
				if got := strings.Contains(out, c.text); got != c.want {
					t.Errorf("%q present = %v, want %v", c.text, got, c.want)
				}
			}
		})
	}
}

func (l Level) name() string {
	switch l {
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	}
	return "info"
}

// TestTerminalfAlwaysEmits pins that there is no configuration in which a
// terminal condition goes unlogged. Vol 4 section 6.2 lists FATAL as emitted
// "once, with the full reason and the counter snapshot that led to it", and
// section 1.2's five exit-code-4 conditions are the only things that reach it.
func TestTerminalfAlwaysEmits(t *testing.T) {
	var buf bytes.Buffer
	lg := NewAppLogger(&buf, LevelError, 1, nil)
	lg.Terminalf("Q1 overflow on shard %d", 2)
	if !strings.Contains(buf.String(), "FATAL Q1 overflow on shard 2") {
		t.Errorf("Terminalf did not emit: %q", buf.String())
	}
}

// TestSeverityIsTheFirstFieldAfterTheTimestamp matters because stderr is the
// whole observability surface and grep is the query language. A severity that
// moved would break every operator filter at once.
func TestSeverityIsTheFirstFieldAfterTheTimestamp(t *testing.T) {
	var buf bytes.Buffer
	lg := NewAppLogger(&buf, LevelInfo, 1000, nil)
	lg.Infof("socket bound listen=:9000")

	line := strings.TrimRight(buf.String(), "\n")
	fields := strings.Fields(line)
	if len(fields) < 3 {
		t.Fatalf("line has too few fields: %q", line)
	}
	if fields[2] != "INFO" {
		t.Errorf("field 3 is %q, want INFO (fields 1 and 2 are the date and time)", fields[2])
	}
}

// TestLogTimestampParsesWithTheEmissionLayout is the identity check between the
// two output paths.
//
// The stats emission cannot go through *log.Logger, because Printf allocates
// and gate G-OBS-2 forbids it, so obs hand-builds the timestamp while the
// logger uses log.LstdFlags|log.Lmicroseconds. If the two ever drift by a
// character, interleaving a WARN with a trend by grep stops working -- and that
// correlation is the only tool Vol 4 section 6.7 leaves an operator, because
// there is no metrics system to join them in.
func TestLogTimestampParsesWithTheEmissionLayout(t *testing.T) {
	var buf bytes.Buffer
	lg := NewAppLogger(&buf, LevelInfo, 1000, nil)
	lg.Infof("x")

	line := strings.TrimRight(buf.String(), "\n")
	if len(line) < len(timeLayout) {
		t.Fatalf("line is shorter than the timestamp layout: %q", line)
	}
	if _, err := time.Parse(timeLayout, line[:len(timeLayout)]); err != nil {
		t.Errorf("the logger's timestamp does not parse with the emission layout %q: "+
			"%v (line %q). The two formats must stay byte-identical",
			timeLayout, err, line)
	}
}

// TestTokenBucketBoundsOneClassAndCountsTheRest is Vol 4 section 6.2's rate
// control, which the document is explicit is not decoration: "a misframed feed
// produces one WARN per sub-packet. At ~10,000 messages/s an uncapped logger is
// both the loudest and the slowest thing in the process -- it converts a data
// problem into an availability problem, and it does so on the receive
// goroutine, which is the one NFR4 protects."
func TestTokenBucketBoundsOneClassAndCountsTheRest(t *testing.T) {
	const rate = 10
	var buf bytes.Buffer
	r := New(testOptions())
	suppressed := r.CounterAt(IDLogEventsSuppressed, int(ClassNetidMismatch))
	r.Freeze()
	lg := NewAppLogger(&buf, LevelInfo, rate, r)

	const burst = 10000
	for range burst {
		lg.Warnf(ClassNetidMismatch, "netId mismatch")
	}

	lines := strings.Count(buf.String(), "netId mismatch")
	if lines > rate {
		t.Errorf("a burst of %d emitted %d lines, want at most %d (stats.eventRatePerMin)",
			burst, lines, rate)
	}
	if lines == 0 {
		t.Error("the bucket suppressed everything; the first events of a class must " +
			"always get through or the condition is invisible")
	}
	if got := suppressed.Load(); got == 0 {
		t.Error("log_events_suppressed{netid_mismatch} is zero after a suppressed " +
			"burst. Vol 0 section 8.2a: a large value is itself a signal, and without " +
			"it the rate limit hides the thing it is protecting against")
	}
	if got, want := suppressed.Load()+uint64(lines), uint64(burst); got != want {
		t.Errorf("emitted %d plus suppressed %d = %d, want %d: every event must be "+
			"either printed or counted", lines, suppressed.Load(), got, want)
	}
}

// TestSuppressionSuffixAppearsOnTheNextLineOfTheClass is what makes the rate
// limit honest. A reader of the surviving line learns both what happened and
// that it happened more often than the line implies.
func TestSuppressionSuffixAppearsOnTheNextLineOfTheClass(t *testing.T) {
	var buf bytes.Buffer
	// One token per minute, so the first line consumes it and the rest are
	// suppressed with no chance of a refill inside the test.
	lg := NewAppLogger(&buf, LevelInfo, 1, nil)

	lg.Warnf(ClassOversizeDatagram, "oversize datagram")
	for range 5 {
		lg.Warnf(ClassOversizeDatagram, "oversize datagram")
	}

	// Refill the bucket by hand rather than sleeping a minute.
	lg.buckets[ClassOversizeDatagram].tokens.Store(1)
	lg.Warnf(ClassOversizeDatagram, "oversize datagram")

	out := buf.String()
	if !strings.Contains(out, "suppressed=5") {
		t.Errorf("the next emitted line of the class does not carry suppressed=5:\n%s", out)
	}

	// And the count resets, so the following line does not re-report it.
	lg.buckets[ClassOversizeDatagram].tokens.Store(1)
	buf.Reset()
	lg.Warnf(ClassOversizeDatagram, "oversize datagram")
	if strings.Contains(buf.String(), "suppressed=") {
		t.Errorf("the suppression count was reported twice:\n%s", buf.String())
	}
}

// TestBucketsAreIndependentPerClass is why there is a bucket per class rather
// than one global one. A single bucket would be worse than none: one noisy
// class would starve every other class of tokens, so the first symptom would
// hide the second -- and at 03:00 the second is usually the cause.
func TestBucketsAreIndependentPerClass(t *testing.T) {
	var buf bytes.Buffer
	lg := NewAppLogger(&buf, LevelInfo, 1, nil)

	for range 100 {
		lg.Warnf(ClassNetidMismatch, "noisy")
	}
	buf.Reset()
	lg.Errorf(ClassWriteFailed, "the quiet one that matters")

	if !strings.Contains(buf.String(), "the quiet one that matters") {
		t.Error("a flood of one class suppressed a different class; the buckets are " +
			"not independent")
	}
}

// TestSuppressedPathDoesNotAllocate keeps the cheap half cheap. The bucket is
// taken before the line is formatted, so a suppressed event costs one atomic
// add and no formatting, no write and no syscall.
func TestSuppressedPathDoesNotAllocate(t *testing.T) {
	lg := NewAppLogger(discard{}, LevelInfo, 1, nil)
	lg.Warnf(ClassSeqGap, "spend the single token")

	allocs := testing.AllocsPerRun(100, func() {
		lg.Warnf(ClassSeqGap, "suppressed")
	})
	if allocs > 0 {
		t.Errorf("a suppressed WARN allocates %.1f times. At 10,000 messages/s that "+
			"is the garbage the rate limit exists to prevent", allocs)
	}
}

func TestParseLevelFallsBackToTheMostVerbose(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Level
	}{
		{"info", LevelInfo},
		{"warn", LevelWarn},
		{"error", LevelError},
		{"", LevelInfo},
		{"nonsense", LevelInfo},
	} {
		if got := ParseLevel(tc.in); got != tc.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

package health

import (
	"testing"
	"time"
)

const sec = nsPerSec

// run drives c once per second of synthetic monotonic time from `from` to `to`
// seconds inclusive, calling datagram(t) first so tests can say when traffic
// arrives. It returns every transition with the second it occurred at.
type stamped struct {
	at int64
	Transition
}

func run(c *Circuit, cfg LivenessConfig, from, to int64, datagram func(t int64) bool) []stamped {
	var out []stamped
	for t := from; t <= to; t++ {
		if datagram != nil && datagram(t) {
			c.Observe(t * sec)
		}
		if tr := c.Tick(t*sec, cfg); tr.Kind != TransitionNone {
			out = append(out, stamped{t, tr})
		}
	}
	return out
}

// TestG_LIVE_1_TransitionsHappenAtTheConfiguredThresholds is gate G-LIVE-1.
//
// SETUP: a Circuit that saw one datagram at t=1s, then total silence, driven
// once a second by synthetic monotonic time with no real clock.
// STIMULUS: run past the warn and down thresholds with 12/27, then build a
// fresh run with 5/11 (the reload) and repeat.
// ASSERTION: Quiet at last+warn and Down at last+down exactly, both times.
// WOULD HAVE CAUGHT: a hard-coded 9 / 12 / 27. The pulse interval is
// exchange-changeable (bible 4.12), so a constant is a future false alarm or
// a future missed one (ADR-013).
func TestG_LIVE_1_TransitionsHappenAtTheConfiguredThresholds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		warn, down int
	}{
		{"defaults", 12, 27},
		{"reloaded", 5, 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := LivenessConfig{WarnSec: tc.warn, DownSec: tc.down, GraceSec: 60}
			var c Circuit
			got := run(&c, cfg, 1, 100, func(t int64) bool { return t == 1 })

			want := []struct {
				at   int64
				kind TransitionKind
			}{
				{1, StartingToLive},
				{1 + int64(tc.warn), LiveToQuiet},
				{1 + int64(tc.down), QuietToDown},
			}
			if len(got) != len(want) {
				t.Fatalf("transitions = %+v, want %d of them", got, len(want))
			}
			for i, w := range want {
				if got[i].at != w.at || got[i].Kind != w.kind {
					t.Errorf("transition %d = %v at %ds, want %v at %ds",
						i, got[i].Kind, got[i].at, w.kind, w.at)
				}
			}
			if got[1].SilentSec != int64(tc.warn) || got[1].ThresholdSec != int64(tc.warn) {
				t.Errorf("quiet carries silent=%d threshold=%d, want %d", got[1].SilentSec, got[1].ThresholdSec, tc.warn)
			}
			if got[2].SilentSec != int64(tc.down) {
				t.Errorf("down carries silent=%d, want %d", got[2].SilentSec, tc.down)
			}
			if c.State() != CircuitDown {
				t.Errorf("final state = %v, want down", c.State())
			}
		})
	}
}

// TestG_LIVE_1_ThresholdsAreReadAtEveryTick proves a reload mid-silence takes
// effect on the next Tick, not only on a fresh machine.
// WOULD HAVE CAUGHT: thresholds copied into the Circuit at construction.
func TestG_LIVE_1_ThresholdsAreReadAtEveryTick(t *testing.T) {
	var c Circuit
	c.Observe(1 * sec)
	c.Tick(2*sec, LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60}) // Live
	if tr := c.Tick(8*sec, LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60}); tr.Kind != TransitionNone {
		t.Fatalf("7s of silence with warn=12 moved the machine: %+v", tr)
	}
	// Reload: warn drops to 5. 7s of silence is now Quiet, on this very tick.
	if tr := c.Tick(8*sec, LivenessConfig{WarnSec: 5, DownSec: 9, GraceSec: 60}); tr.Kind != LiveToQuiet {
		t.Fatalf("after the reload got %+v, want LiveToQuiet", tr)
	}
}

// TestCircuitFullCycle walks every edge of Figure 4.6 once and checks the
// fields each log line needs.
func TestCircuitFullCycle(t *testing.T) {
	cfg := LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60}
	var c Circuit
	c.SetLastTranscode(7208)
	arrive := map[int64]bool{5: true, 100: true, 110: true}
	got := run(&c, cfg, 1, 120, func(t int64) bool { return arrive[t] })

	kinds := []TransitionKind{
		StartingToLive, LiveToQuiet, QuietToDown, DownToLive, // 5, 17, 32, 101
		// a datagram at 110 keeps it Live; silence from 110 -> Quiet at 122 is past the run
	}
	if len(got) != len(kinds) {
		t.Fatalf("got %+v", got)
	}
	for i, k := range kinds {
		if got[i].Kind != k {
			t.Errorf("transition %d = %v, want %v", i, got[i].Kind, k)
		}
	}
	if got[0].FirstDatagramMs != 5000 {
		t.Errorf("first_datagram_after_ms = %d, want 5000", got[0].FirstDatagramMs)
	}
	if got[1].LastTranscode != 7208 {
		t.Errorf("quiet last_transcode = %d, want 7208", got[1].LastTranscode)
	}
	rec := got[3]
	if rec.OutageSec != 95 || rec.OutageNs != 95*sec || rec.FromStartup {
		t.Errorf("recovery = %+v, want outage 95s, not from startup", rec)
	}
}

// TestCircuitQuietRecoversWithoutGoingDown covers Quiet to Live and its quiet_s.
func TestCircuitQuietRecoversWithoutGoingDown(t *testing.T) {
	cfg := LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60}
	var c Circuit
	got := run(&c, cfg, 1, 40, func(t int64) bool { return t == 1 || t == 20 })
	if len(got) < 3 || got[2].Kind != QuietToLive || got[2].OutageSec != 19 {
		t.Fatalf("got %+v, want QuietToLive with quiet_s=19 third", got)
	}
}

// TestCircuitStartupGrace covers Starting to Down and the D17 recovery, which
// must not claim a BCSeqNo gap because no datagram was ever seen before it.
func TestCircuitStartupGrace(t *testing.T) {
	cfg := LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60}
	var c Circuit
	got := run(&c, cfg, 1, 100, func(t int64) bool { return t == 90 })
	if len(got) != 2 || got[0].Kind != StartingToDown || got[0].at != 60 ||
		got[0].ThresholdSec != 60 {
		t.Fatalf("got %+v, want StartingToDown at 60s", got)
	}
	if got[1].Kind != DownToLive || !got[1].FromStartup || got[1].OutageSec != 90 {
		t.Errorf("recovery = %+v, want DownToLive FromStartup outage 90s", got[1])
	}

	// A datagram inside the grace period never reaches Down.
	var d Circuit
	got = run(&d, cfg, 1, 59, func(t int64) bool { return t == 59 })
	if len(got) != 1 || got[0].Kind != StartingToLive {
		t.Errorf("got %+v, want only StartingToLive", got)
	}
}

// TestCircuitIgnoresNonPositiveAndBackwardsTimes: the gauge starts at 0, and
// an older value must never make the machine believe the feed went quiet.
func TestCircuitIgnoresNonPositiveAndBackwardsTimes(t *testing.T) {
	var c Circuit
	c.Observe(0)
	c.Observe(-5)
	if tr := c.Tick(1*sec, LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60}); tr.Kind != TransitionNone {
		t.Fatalf("a zero gauge moved the machine: %+v", tr)
	}
	c.Observe(10 * sec)
	c.Observe(3 * sec)
	c.Tick(11*sec, LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60})
	if tr := c.Tick(21*sec, LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60}); tr.Kind != TransitionNone {
		t.Fatalf("a backwards Observe shortened the silence: %+v", tr)
	}
}

// TestSeverityGateNeverChangesTheState is the health-level half of the
// severity gate's contract (Vol 4 3.5 rule 1).
//
// SETUP: one scripted outage. STIMULUS: derive severities for it in-window and
// out-of-window. ASSERTION: the state sequence and transition list are the same
// either way (SeverityOf takes no part in Tick), and the table is exactly
// Quiet and Down lowered by one level, everything else Info.
// WOULD HAVE CAUGHT: downgrading the state instead of the log line, which
// would hide an overnight outage in the morning's counters.
func TestSeverityGateNeverChangesTheState(t *testing.T) {
	cfg := LivenessConfig{WarnSec: 12, DownSec: 27, GraceSec: 60}
	var c Circuit
	got := run(&c, cfg, 1, 60, func(t int64) bool { return t == 1 })
	if len(got) != 3 {
		t.Fatalf("setup produced %+v", got)
	}
	for _, in := range []bool{true, false} {
		for _, s := range got {
			SeverityOf(s.Kind, in)
		}
	}
	if c.State() != CircuitDown {
		t.Errorf("state = %v after severity evaluation, want down", c.State())
	}

	table := []struct {
		kind    TransitionKind
		in, out Severity
	}{
		{StartingToLive, SeverityInfo, SeverityInfo},
		{StartingToDown, SeverityError, SeverityWarn},
		{LiveToQuiet, SeverityWarn, SeverityInfo},
		{QuietToLive, SeverityInfo, SeverityInfo},
		{QuietToDown, SeverityError, SeverityWarn},
		{DownToLive, SeverityInfo, SeverityInfo},
	}
	for _, r := range table {
		if g := SeverityOf(r.kind, true); g != r.in {
			t.Errorf("kind %d in-window = %d, want %d", r.kind, g, r.in)
		}
		if g := SeverityOf(r.kind, false); g != r.out {
			t.Errorf("kind %d outside = %d, want %d", r.kind, g, r.out)
		}
	}
}

// TestG_LIVE_3_UnresolvableZoneFailsOpen is gate G-LIVE-3 at health level.
//
// SETUP: a TradingWindow whose zone does not exist.
// STIMULUS: build it; ask about midnight and noon, and about a window that
// resolves, as a control.
// ASSERTION: exactly one error (the caller's one WARN) naming the zone; every
// hour is in-window; the severities used are therefore the louder ones. The
// control proves Contains can say "outside", so the fail-open answer is not
// vacuous.
// The spec's "Start" leg cannot occur at app level because V-TZ1 rejects the
// config first (D3); config's V-TZ1 test covers that.
// WOULD HAVE CAUGHT: failing closed, which silences a circuit alarm because
// of a timezone-database problem.
func TestG_LIVE_3_UnresolvableZoneFailsOpen(t *testing.T) {
	w, err := NewWindow(TradingWindow{Start: "09:00", End: "23:55", TZ: "Not/AZone"})
	if err == nil {
		t.Fatal("no warning for an unresolvable zone")
	}
	if w == nil {
		t.Fatal("NewWindow returned a nil Window with its error; callers would panic")
	}
	for h := 0; h < 24; h++ {
		if !w.Contains(time.Date(2026, 1, 5, h, 30, 0, 0, time.UTC)) {
			t.Errorf("hour %d is outside a fail-open window", h)
		}
	}
	if SeverityOf(QuietToDown, w.Contains(time.Now())) != SeverityError {
		t.Error("fail-open window did not leave Down at ERROR")
	}

	// Control: a real zone does exclude hours.
	real, err := NewWindow(TradingWindow{Start: "09:00", End: "23:55", TZ: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	if real.Contains(time.Date(2026, 1, 5, 3, 0, 0, 0, time.UTC)) {
		t.Error("03:00 UTC is inside a 09:00-23:55 UTC window")
	}
	if !real.Contains(time.Date(2026, 1, 5, 11, 0, 0, 0, time.UTC)) {
		t.Error("11:00 UTC is outside a 09:00-23:55 UTC window")
	}
}

func TestNewWindowBlankOrLocalZoneFailsOpen(t *testing.T) {
	for _, tz := range []string{"", "Local"} {
		w, err := NewWindow(TradingWindow{Start: "09:00", End: "10:00", TZ: tz})
		if err == nil {
			t.Errorf("zone %q: no warning", tz)
		}
		if !w.Contains(time.Date(2026, 1, 5, 3, 0, 0, 0, time.UTC)) {
			t.Errorf("zone %q: window excluded 03:00, so it did not fail open", tz)
		}
	}
}

func TestWindowBoundariesAndMidnightSpan(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 1, 5, h, m, 0, 0, time.UTC) }
	w, err := NewWindow(TradingWindow{Start: "09:00", End: "23:55", TZ: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		h, m int
		in   bool
	}{{8, 59, false}, {9, 0, true}, {23, 54, true}, {23, 55, false}, {0, 0, false}} {
		if g := w.Contains(at(c.h, c.m)); g != c.in {
			t.Errorf("%02d:%02d in=%v, want %v", c.h, c.m, g, c.in)
		}
	}
	span, _ := NewWindow(TradingWindow{Start: "22:00", End: "02:00", TZ: "UTC"})
	for _, c := range []struct {
		h  int
		in bool
	}{{21, false}, {22, true}, {1, true}, {2, false}, {12, false}} {
		if g := span.Contains(at(c.h, 0)); g != c.in {
			t.Errorf("span %02d:00 in=%v, want %v", c.h, g, c.in)
		}
	}
	// The window is evaluated in its own zone, not the instant's.
	if _, err := time.LoadLocation("Asia/Kolkata"); err == nil {
		wk, _ := NewWindow(TradingWindow{Start: "09:00", End: "23:55", TZ: "Asia/Kolkata"})
		// 04:00 UTC is 09:30 IST.
		if !wk.Contains(at(4, 0)) {
			t.Error("04:00 UTC should be inside an IST window starting 09:00")
		}
	}
	for _, bad := range []TradingWindow{
		{Start: "9", End: "23:55", TZ: "UTC"},
		{Start: "09:00", End: "", TZ: "UTC"},
	} {
		if _, err := NewWindow(bad); err == nil {
			t.Errorf("%+v: no error for an unparsable time", bad)
		}
	}
	var nilw *Window
	if !nilw.Contains(time.Now()) {
		t.Error("a nil Window must fail open")
	}
}

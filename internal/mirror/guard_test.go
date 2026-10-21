package mirror

import (
	"math"
	"math/rand"
	"strings"
	"testing"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// newTestGuardTable is a GuardTable over codes with fresh, unregistered
// counters.
func newTestGuardTable(codes ...model.TransCode) *GuardTable {
	offered := make([]*obs.Counter, len(codes))
	superseded := make([]*obs.Counter, len(codes))
	for i := range codes {
		offered[i], superseded[i] = &obs.Counter{}, &obs.Counter{}
	}
	return NewGuardTable(codes, offered, superseded)
}

// guardUnit is guard as the merge calls it: steps 1-2 (guardOff) evaluated
// for the unit, then guard for one contribution.
func guardUnit(mode GuardMode, g *guardEntry, e *model.Entry, c *model.Contribution, u *model.UpdateUnit) verdict {
	return guard(guardOff(mode, g), g, e, c, u)
}

// ord is one slot's ordering value: (BCSeqNo, LogTime, RecvMono).
type ord struct {
	seq, time int32
	recv      int64
}

// seenEntry is an entry whose contributor slot already holds o -- the state
// section 4 step 3 leaves after an Apply.
func seenEntry(slot uint8, o ord) *model.Entry {
	e := &model.Entry{}
	e.LastSeq[slot], e.LastTime[slot], e.LastRecv[slot] = o.seq, o.time, o.recv
	e.MarkSlotSeen(slot)
	return e
}

// testUnit is a unit of code carrying o, token-scoped unless general.
func testUnit(code model.TransCode, o ord, general bool) *model.UpdateUnit {
	u := &model.UpdateUnit{Code: code, Seq: o.seq, LogTime: o.time, RecvMono: o.recv}
	if general {
		u.Flags |= model.FlagGeneral
	}
	return u
}

// TestG_ORD_3_SlotInitialisingContributionsAreNeverOffered is gate G-ORD-3,
// held at the guard: the merge calls guard exactly once per contribution, and
// guard is the only place guard_offered is counted, so a guard that counts
// nothing for a contribution with no history holds the gate's assertion.
// V4:1625's "Merge all N" stimulus -- where step 1 creates each entry, the
// "entry was created by THIS contribution" path of section 5.3 step 4 -- is
// re-run through merge by P3-07 (phase3-tasks.md, P3-07's tests).
//
// SETUP: N distinct fresh entries (INV-ZERO: SlotSeen == 0), every third of
// them with OTHER contributor slots already seen, and N contributions from one
// transcode, one per row, slot 3.
//
// STIMULUS: guard each contribution against its row.
//
// ASSERTION: every call Bypasses, and guard_offered == guard_superseded == 0,
// so `auto` cannot trip on a workload that never repeats a key. Non-vacuity:
// the same contribution offered again once its slot has a history IS counted.
//
// WOULD HAVE CAUGHT: counting slot-initialising contributions in the
// denominator, which makes the auto-disable ratio drift with the inventory mix
// rather than with field health (HLD section 6.5), and a row-level "seen" test
// that lets another contributor's history stand in for this slot's (Vol 2
// section 5.6, Trace B).
func TestG_ORD_3_SlotInitialisingContributionsAreNeverOffered(t *testing.T) {
	const n = 10_000
	const slot = 3
	tab := newTestGuardTable(6511, 6521, 6522, 6531, 6571)
	g := tab.lookup(6531)

	entries := make([]model.Entry, n)
	for i := range entries {
		entries[i].Row = model.RowKey(i)
		if i%3 == 0 {
			entries[i].MarkSlotSeen(0)
			entries[i].MarkSlotSeen(4)
		}
		c := &model.Contribution{Row: model.RowKey(i), Mask: 1, Slot: slot}
		u := testUnit(6531, ord{seq: int32(i), time: 100, recv: int64(i)}, true)
		if v := guardUnit(GuardAuto, g, &entries[i], c, u); v != verdictBypass {
			t.Fatalf("row %d: verdict %d, want Bypass: slot %d has never been applied", i, v, slot)
		}
	}
	if got := g.offered.Load(); got != 0 {
		t.Fatalf("guard_offered = %d after %d slot-initialising contributions, want 0", got, n)
	}
	if got := g.superseded.Load(); got != 0 {
		t.Fatalf("guard_superseded = %d, want 0", got)
	}

	// Non-vacuity: once step 3 has recorded the slot, the same row's next
	// contribution reaches the comparison and is counted.
	e := seenEntry(slot, ord{seq: 1, time: 100, recv: 1})
	c := &model.Contribution{Mask: 1, Slot: slot}
	if v := guardUnit(GuardAuto, g, e, c, testUnit(6531, ord{time: 101, recv: 2}, true)); v != verdictApply {
		t.Fatalf("a newer contribution to a seen slot: verdict %d, want Apply", v)
	}
	if got := g.offered.Load(); got != 1 {
		t.Fatalf("guard_offered = %d after one comparable contribution, want 1: the gate counted nothing "+
			"because nothing was ever counted", got)
	}
}

// TestG_ORD_4_GuardSeqIsSerialLogTimeIsPlain is gate G-ORD-4's guard half, not
// counted (phase3-tasks.md section 2; the merge half, that the Apply writes the
// slot, is P3-07's).
//
// SETUP: a token-scoped slot at LastSeq = 0x7FFFFFFF; a general slot at
// LastTime = 0x7FFFFFFF. In both, the later tiebreaks point the OTHER way, so
// only the primary comparison can produce the asserted verdict.
//
// STIMULUS: BCSeqNo = 0x80000000; LogTime = 0x80000000.
//
// ASSERTION: the BCSeqNo is newer (serial, int32(a-b) > 0) and Applies; the
// LogTime is older (plain signed) and Supersedes. And the reverses.
// Non-vacuity: a modelled mutant with the two rules swapped (BCSeqNo plain,
// LogTime serial) disagrees with at least one BCSeqNo row and one LogTime row.
//
// WOULD HAVE CAUGHT: a plain a > b on BCSeqNo, which freezes every affected
// row after a wrap (ASM-11, ~59 h at 10,000 msg/s); and serial arithmetic on
// LogTime, harmless until 2048 and wrong in a way nobody would find
// (Vol 0 section 10.3).
func TestG_ORD_4_GuardSeqIsSerialLogTimeIsPlain(t *testing.T) {
	g := newTestGuardTable(7208).lookup(7208)
	c := &model.Contribution{Mask: 1, Slot: 0}
	cases := []guardCase{
		{"BCSeqNo wraps 0x7FFFFFFF -> 0x80000000: newer", false,
			ord{math.MaxInt32, 50, 900}, ord{math.MinInt32, 40, 100}, verdictApply},
		{"BCSeqNo 0x80000000 -> 0x7FFFFFFF: older", false,
			ord{math.MinInt32, 40, 100}, ord{math.MaxInt32, 50, 900}, verdictSupersede},
		{"LogTime 0x7FFFFFFF -> 0x80000000: plainly older", true,
			ord{0, math.MaxInt32, 100}, ord{0, math.MinInt32, 900}, verdictSupersede},
		{"LogTime 0x80000000 -> 0x7FFFFFFF: plainly newer", true,
			ord{0, math.MinInt32, 900}, ord{0, math.MaxInt32, 100}, verdictApply},
		{"token-scoped secondary LogTime is plain too", false,
			ord{7, math.MaxInt32, 100}, ord{7, math.MinInt32, 900}, verdictSupersede},
	}
	for _, tc := range cases {
		if v := guardUnit(GuardOn, g, seenEntry(0, tc.last), c, testUnit(7208, tc.u, tc.general)); v != tc.want {
			t.Errorf("%s: verdict %d, want %d", tc.name, v, tc.want)
		}
	}
	assertKillsSerialPlainSwap(t, cases)
}

// TestGuardSerialCompareIsAntisymmetric: for any two BCSeqNo values whose
// serial distance is below 2^31, exactly one of them is newer than the other,
// and the newer one is the one a forward serial step reaches.
func TestGuardSerialCompareIsAntisymmetric(t *testing.T) {
	g := newTestGuardTable(7208).lookup(7208)
	c := &model.Contribution{Mask: 1, Slot: 0}
	// Equal LogTime and RecvMono, so step 6 alone decides.
	at := func(last, seq int32) verdict {
		return guardUnit(GuardOn, g, seenEntry(0, ord{last, 5, 5}), c, testUnit(7208, ord{seq, 5, 5}, false))
	}

	rng := rand.New(rand.NewSource(6))
	edges := []int32{0, 1, -1, math.MaxInt32, math.MinInt32, math.MaxInt32 - 1, math.MinInt32 + 1}
	dists := []int32{1, -1, math.MaxInt32, -math.MaxInt32, 1 << 30, -(1 << 30)}
	check := func(a, d int32) {
		b := a + d // b is d serial steps after a; wraps, deliberately
		ab, ba := at(a, b), at(b, a)
		wantAB, wantBA := verdictApply, verdictSupersede
		if d < 0 {
			wantAB, wantBA = verdictSupersede, verdictApply
		}
		if ab != wantAB || ba != wantBA {
			t.Fatalf("a=%#x b=%#x (serial distance %d): b over a = %d, a over b = %d; want %d and %d",
				uint32(a), uint32(b), d, ab, ba, wantAB, wantBA)
		}
	}
	for _, a := range edges {
		for _, d := range dists {
			check(a, d)
		}
	}
	for range 100_000 {
		d := int32(rng.Uint32())
		if d == 0 || d == math.MinInt32 { // equal, or exactly 2^31 apart: no order exists
			continue
		}
		check(int32(rng.Uint32()), d)
	}
}

// guardCase is one row of the comparison table: a slot's history, an arriving
// unit, and the verdict.
type guardCase struct {
	name    string
	general bool
	last, u ord
	want    verdict
}

var guardCases = []guardCase{
	// token-scoped: BCSeqNo primary
	{"seq newer", false, ord{10, 50, 100}, ord{11, 40, 50}, verdictApply},
	{"seq older", false, ord{10, 50, 100}, ord{9, 60, 200}, verdictSupersede},
	{"seq wraps forward", false, ord{math.MaxInt32, 50, 100}, ord{math.MinInt32, 40, 50}, verdictApply},
	{"seq wraps backward", false, ord{math.MinInt32, 40, 50}, ord{math.MaxInt32, 50, 100}, verdictSupersede},
	{"seq equal, time newer", false, ord{10, 50, 100}, ord{10, 51, 50}, verdictApply},
	{"seq equal, time older", false, ord{10, 50, 100}, ord{10, 49, 200}, verdictSupersede},
	{"seq equal, time straddles sign bit", false, ord{10, math.MaxInt32, 100}, ord{10, math.MinInt32, 200}, verdictSupersede},
	{"seq, time equal, recv greater", false, ord{10, 50, 100}, ord{10, 50, 101}, verdictApply},
	{"seq, time equal, recv equal: a re-delivery", false, ord{10, 50, 100}, ord{10, 50, 100}, verdictSupersede},
	{"seq, time equal, recv less", false, ord{10, 50, 100}, ord{10, 50, 99}, verdictSupersede},
	// general: LogTime primary, BCSeqNo ignored
	{"general time newer, seq older", true, ord{10, 50, 100}, ord{9, 51, 50}, verdictApply},
	{"general time older, seq newer", true, ord{10, 50, 100}, ord{11, 49, 200}, verdictSupersede},
	{"general time straddles sign bit", true, ord{0, math.MaxInt32, 100}, ord{0, math.MinInt32, 200}, verdictSupersede},
	{"general same second, recv greater", true, ord{10, 50, 100}, ord{9, 50, 101}, verdictApply},
	{"general same second, recv equal", true, ord{10, 50, 100}, ord{11, 50, 100}, verdictSupersede},
}

// TestGuardComparisonTable pins steps 5-8 row by row, with the counters, and
// checks the table is strong enough to fail a serial/plain swap.
func TestGuardComparisonTable(t *testing.T) {
	c := &model.Contribution{Mask: 1, Slot: 2}
	for _, tc := range guardCases {
		g := newTestGuardTable(7305, 7340).lookup(7340)
		e := seenEntry(c.Slot, tc.last)
		before := *e
		v := guardUnit(GuardAuto, g, e, c, testUnit(7340, tc.u, tc.general))
		if v != tc.want {
			t.Errorf("%s: verdict %d, want %d", tc.name, v, tc.want)
		}
		if *e != before {
			t.Errorf("%s: guard wrote the entry; recording the slot is the merge's step 3", tc.name)
		}
		wantSup := uint64(0)
		if tc.want == verdictSupersede {
			wantSup = 1
		}
		if g.offered.Load() != 1 || g.superseded.Load() != wantSup {
			t.Errorf("%s: guard_offered %d, guard_superseded %d; want 1 and %d",
				tc.name, g.offered.Load(), g.superseded.Load(), wantSup)
		}
	}

	assertKillsSerialPlainSwap(t, guardCases)
}

// assertKillsSerialPlainSwap is a non-vacuity check on a comparison table: a
// modelled mutant with the two rules swapped (BCSeqNo plain, LogTime serial)
// must disagree with the table on at least one BCSeqNo row and one LogTime row,
// or the table could not tell the correct guard from the swapped one.
func assertKillsSerialPlainSwap(t *testing.T, cases []guardCase) {
	t.Helper()
	mutant := func(tc guardCase) verdict {
		if !tc.general {
			if tc.u.seq > tc.last.seq {
				return verdictApply
			} else if tc.u.seq < tc.last.seq {
				return verdictSupersede
			}
		}
		if d := tc.u.time - tc.last.time; d > 0 {
			return verdictApply
		} else if d < 0 {
			return verdictSupersede
		}
		if tc.u.recv > tc.last.recv {
			return verdictApply
		}
		return verdictSupersede
	}
	var seqKilled, timeKilled bool
	for _, tc := range cases {
		if mutant(tc) != tc.want {
			if !tc.general && tc.u.seq != tc.last.seq {
				seqKilled = true
			} else {
				timeKilled = true
			}
		}
	}
	if !seqKilled || !timeKilled {
		t.Fatalf("the table does not fail a serial/plain swap (BCSeqNo row killed %v, LogTime row killed %v)",
			seqKilled, timeKilled)
	}
}

// TestGuardBypassesComeFirst pins section 5.3's precedence: each of the four
// bypass conditions, alone and in every pair, returns Bypass BEFORE anything is
// counted or compared -- on an input that, with no bypass condition, Supersedes
// and counts both counters.
func TestGuardBypassesComeFirst(t *testing.T) {
	type cond int
	const (
		modeOff cond = iota
		codeOff
		unusable
		unseen
		nConds
	)
	names := [nConds]string{"ordering.guard off", "GuardOff[code]", "FlagOrderingUnusable", "!SlotSeen"}
	c := &model.Contribution{Mask: 1, Slot: 1}

	run := func(on [nConds]bool) (verdict, *guardEntry) {
		g := newTestGuardTable(6511, 6531).lookup(6531)
		mode := GuardAuto
		if on[modeOff] {
			mode = GuardOff
		}
		if on[codeOff] {
			g.off.Store(true)
		}
		e := seenEntry(c.Slot, ord{10, 50, 100})
		if on[unseen] {
			e = seenEntry(0, ord{10, 50, 100}) // another slot's history only
		}
		u := testUnit(6531, ord{9, 49, 99}, false) // older on every component
		if on[unusable] {
			u.Flags |= model.FlagOrderingUnusable
		}
		return guardUnit(mode, g, e, c, u), g
	}

	// The baseline must be a counted Supersede, or every Bypass below is vacuous.
	if v, g := run([nConds]bool{}); v != verdictSupersede || g.offered.Load() != 1 || g.superseded.Load() != 1 {
		t.Fatalf("baseline: verdict %d, offered %d, superseded %d; want Supersede, 1, 1",
			v, g.offered.Load(), g.superseded.Load())
	}
	// GuardOn behaves as GuardAuto: only the evaluator distinguishes them.
	g := newTestGuardTable(6531).lookup(6531)
	if v := guardUnit(GuardOn, g, seenEntry(c.Slot, ord{10, 50, 100}), c, testUnit(6531, ord{9, 49, 99}, false)); v != verdictSupersede {
		t.Fatalf("GuardOn: verdict %d, want Supersede", v)
	}

	for i := range nConds {
		for j := i; j < nConds; j++ {
			var on [nConds]bool
			on[i], on[j] = true, true
			name := names[i]
			if j != i {
				name += " + " + names[j]
			}
			v, g := run(on)
			if v != verdictBypass {
				t.Errorf("%s: verdict %d, want Bypass", name, v)
			}
			if g.offered.Load() != 0 || g.superseded.Load() != 0 {
				t.Errorf("%s: guard_offered %d, guard_superseded %d; a bypass counts in neither",
					name, g.offered.Load(), g.superseded.Load())
			}
		}
	}

	// Steps 1-2 are the caller's, hoisted once per unit (section 5.3, "Cost"):
	// guard judges by the off it is handed, so an evaluator flipping the bit
	// between two of a unit's contributions cannot judge the unit two ways.
	g = newTestGuardTable(6531).lookup(6531)
	off := guardOff(GuardAuto, g)
	g.off.Store(true) // the evaluator, mid-unit
	if v := guard(off, g, seenEntry(c.Slot, ord{10, 50, 100}), c, testUnit(6531, ord{9, 49, 99}, false)); v != verdictSupersede {
		t.Fatalf("bit flipped after the unit's hoisted load: verdict %d, want Supersede -- guard re-read the bit", v)
	}
	if !guardOff(GuardAuto, g) {
		t.Fatal("guardOff ignored GuardOff[code]")
	}

	// A code with no table entry -- a wiring defect -- bypasses rather than
	// dereferencing nil or discarding.
	if v := guardUnit(GuardAuto, nil, seenEntry(c.Slot, ord{10, 50, 100}), c, testUnit(1, ord{9, 49, 99}, false)); v != verdictBypass {
		t.Fatalf("unconfigured code: verdict %d, want Bypass", v)
	}
}

// TestGuardTableIsPerTranscode: each code resolves to the counters at its own
// configured position, and one code's auto-disable leaves every other code
// guarded -- HLD section 6.5's global ratio avoided by construction.
func TestGuardTableIsPerTranscode(t *testing.T) {
	codes := []model.TransCode{7208, 7305, 7340}
	offered := []*obs.Counter{{}, {}, {}}
	superseded := []*obs.Counter{{}, {}, {}}
	tab := NewGuardTable(codes, offered, superseded)

	for i, code := range codes {
		g := tab.lookup(code)
		if g == nil || g.offered != offered[i] || g.superseded != superseded[i] {
			t.Fatalf("lookup(%d) did not resolve to position %d's counters", code, i)
		}
	}
	if tab.lookup(6511) != nil {
		t.Fatal("lookup of an unconfigured code returned an entry")
	}

	tab.lookup(7305).off.Store(true)
	c := &model.Contribution{Mask: 1, Slot: 1}
	older := ord{9, 49, 99}
	if v := guardUnit(GuardAuto, tab.lookup(7305), seenEntry(1, ord{10, 50, 100}), c, testUnit(7305, older, false)); v != verdictBypass {
		t.Fatalf("7305 after its auto-disable: verdict %d, want Bypass", v)
	}
	if v := guardUnit(GuardAuto, tab.lookup(7340), seenEntry(1, ord{10, 50, 100}), c, testUnit(7340, older, false)); v != verdictSupersede {
		t.Fatalf("7340 with only 7305 disabled: verdict %d, want Supersede -- the disable leaked across codes", v)
	}
}

// TestNewGuardTableRejectsMiswiring: counters that do not line up with the
// codes, a nil counter, or a repeated code are startup wiring defects and must
// panic at construction with NewGuardTable's own message -- not reach a shard,
// and not be confused with an incidental runtime panic such as an index out of
// range.
func TestNewGuardTableRejectsMiswiring(t *testing.T) {
	two := func() []*obs.Counter { return []*obs.Counter{{}, {}} }
	cases := []struct {
		name                string
		codes               []model.TransCode
		offered, superseded []*obs.Counter
		want                string
	}{
		{"fewer superseded than codes", []model.TransCode{7305, 7340}, two(), []*obs.Counter{{}},
			"zipped by configured position"},
		{"fewer offered than codes", []model.TransCode{7305, 7340}, []*obs.Counter{{}}, two(),
			"zipped by configured position"},
		{"more counters than codes", []model.TransCode{7305}, two(), two(),
			"zipped by configured position"},
		{"nil offered", []model.TransCode{7305}, []*obs.Counter{nil}, []*obs.Counter{{}},
			"nil guard counter for transcode 7305"},
		{"nil superseded", []model.TransCode{7305, 7340}, two(), []*obs.Counter{{}, nil},
			"nil guard counter for transcode 7340"},
		{"repeated code", []model.TransCode{7305, 7305}, two(), two(),
			"transcode 7305 twice"},
	}
	for _, tc := range cases {
		func() {
			defer func() {
				r := recover()
				if msg, ok := r.(string); !ok || !strings.Contains(msg, tc.want) {
					t.Errorf("%s: recovered %v, want NewGuardTable's panic containing %q", tc.name, r, tc.want)
				}
			}()
			NewGuardTable(tc.codes, tc.offered, tc.superseded)
		}()
	}

	// Non-vacuity: well-wired input constructs, so the panics above are about
	// the miswiring and not about the constructor.
	if tab := NewGuardTable([]model.TransCode{7305, 7340}, two(), two()); tab.lookup(7340) == nil {
		t.Fatal("a well-wired table did not resolve its second code")
	}
}

package obs

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// touchAll resolves every slot of every instrument before Freeze, then gives
// each one a non-zero value afterwards.
//
// This is gate G-OBS-3's stimulus: "one emission with every instrument
// touched". It has to resolve first and write second, because resolution is
// startup-only (G-OBS-1) while the writes model what the running process does.
func touchAll(t *testing.T, r *Reg) {
	t.Helper()

	var cs []*Counter
	var ms []*Max
	var gs []*Gauge

	for id := range numInstruments {
		row := &ledger[id]
		d := &r.dims[row.dim]
		if d.kind == dimDynamic {
			continue // claimed after Freeze, through their own accessors
		}
		rank := 0
		switch {
		case row.dim == dimNone:
			rank = 0
		case d.inner > 1:
			rank = 2
		default:
			rank = 1
		}
		outer := d.size / d.inner

		for i := range outer {
			for j := range d.inner {
				switch row.shape {
				case shapeCounter:
					switch rank {
					case 0:
						cs = append(cs, r.Counter(id))
					case 1:
						cs = append(cs, r.CounterAt(id, int(i)))
					case 2:
						cs = append(cs, r.CounterAt2(id, int(i), int(j)))
					}
				case shapeMax:
					switch rank {
					case 0:
						ms = append(ms, r.Max(id))
					case 1:
						ms = append(ms, r.MaxAt(id, int(i)))
					case 2:
						t.Fatalf("%s is shape M on a cartesian dimension; add MaxAt2",
							row.name)
					}
				case shapeGauge:
					switch rank {
					case 0:
						gs = append(gs, r.Gauge(id))
					case 1:
						gs = append(gs, r.GaugeAt(id, int(i)))
					case 2:
						gs = append(gs, r.GaugeAt2(id, int(i), int(j)))
					}
				}
			}
		}
	}

	r.Freeze()

	for _, c := range cs {
		c.Add(7)
	}
	for _, m := range ms {
		m.Observe(11)
	}
	for _, g := range gs {
		g.Store(3)
	}

	// The two dynamic tables, through the accessors that claim a slot.
	if c, ok := r.UnknownTranscode(9999); ok {
		c.Add(1)
	}
	r.OracleError(1438).Add(1)
}

// emittedNames extracts every instrument name from an emission, with its label
// stripped. Group lines carry the values after the group= token.
func emittedNames(t *testing.T, out string) map[string]int {
	t.Helper()
	names := map[string]int{}
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		i := strings.Index(line, " group=")
		if i < 0 {
			t.Fatalf("line has no group= token: %q", line)
		}
		rest := line[i+len(" group="):]
		// Skip the group name itself.
		if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			rest = rest[sp+1:]
		} else {
			continue // a group line with no values
		}
		for _, tok := range strings.Fields(rest) {
			eq := strings.IndexByte(tok, '=')
			if eq < 0 {
				t.Errorf("value token has no '=': %q", tok)
				continue
			}
			name := tok[:eq]
			if b := strings.IndexByte(name, '{'); b >= 0 {
				name = name[:b]
			}
			names[name]++
		}
	}
	return names
}

// TestG_OBS_3_EveryInstrumentIsEmittedAndNothingElse is gate G-OBS-3.
//
// Setup: the Vol 0 section 8.2 ledger plus section 10's additions, as data.
// Stimulus: one emission with every instrument touched. Assertion: EVERY named
// instrument appears in the output, and every instrument in the output is in
// the ledger.
//
// Both directions, because the gate's rationale names the defect precisely: "a
// counter defined and incremented but never emitted -- which is EXACTLY how Vol
// 0 section 8.3's gaps G1 to G9 arose: nine instruments named in HLD prose and
// absent from its own section 10.1 table". The reverse direction catches the
// other half, an emitted name nobody declared, which no operator could look up.
func TestG_OBS_3_EveryInstrumentIsEmittedAndNothingElse(t *testing.T) {
	r := New(testOptions())
	touchAll(t, r)

	var buf bytes.Buffer
	if err := r.Emit(&buf); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	got := emittedNames(t, buf.String())

	declared := map[string]bool{}
	for id := range numInstruments {
		declared[ledger[id].name] = true
	}

	for name := range declared {
		if got[name] == 0 {
			t.Errorf("%q is declared in the ledger but never reached the output. A "+
				"counter that is defined and incremented but never emitted is gap G1 "+
				"to G9 all over again: the value exists and nobody can read it", name)
		}
	}
	for name := range got {
		if !declared[name] {
			t.Errorf("%q was emitted but is not in the ledger. An operator reading it "+
				"has nothing to look it up in", name)
		}
	}
}

// TestHealthMembersAreEmittedTwiceByDesign records the consequence of health
// being a projection, so it reads as intended rather than as a duplication bug.
func TestHealthMembersAreEmittedTwiceByDesign(t *testing.T) {
	r := New(testOptions())
	touchAll(t, r)

	var buf bytes.Buffer
	if err := r.Emit(&buf); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	got := emittedNames(t, buf.String())

	// datagrams_received is on the health line and in ingress. One slot, two
	// renderings.
	if got["datagrams_received"] != 2 {
		t.Errorf("datagrams_received appears %d times, want 2: once on the health "+
			"line and once in its home group", got["datagrams_received"])
	}
	// An instrument that is not on the health line appears once.
	if got["subpackets_total"] != 1 {
		t.Errorf("subpackets_total appears %d times, want 1", got["subpackets_total"])
	}
}

// TestHealthProjectionDoesNotConsumeTheHomeGroupsValues is the structural test
// for the two-pass design.
//
// An instrument on the health line is rendered twice. If each render site read
// the instrument itself rather than the snapshot, the second counter delta
// would come out zero -- and worse, the second LoadAndReset of a shape M would
// come out zero. No health member is shape M today, so that half is latent: the
// first max added to the health line would silently zero its home group's
// value. The snapshot pass is what makes it impossible, and this is the
// assertion that holds it in place.
func TestHealthProjectionDoesNotConsumeTheHomeGroupsValues(t *testing.T) {
	r := New(testOptions())
	c := r.Counter(IDDatagramsReceived)
	r.Freeze()
	c.Add(100)

	var buf bytes.Buffer
	if err := r.Emit(&buf); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	// The same total and the same delta on both lines.
	want := "datagrams_received=100(+100)"
	if n := strings.Count(buf.String(), want); n != 2 {
		t.Errorf("%q appears %d times, want 2. The health line and the ingress line "+
			"must publish identical values; a differing pair means one of them read "+
			"the instrument instead of the snapshot.\n%s", want, n, buf.String())
	}
}

// TestEmissionShapeMatchesTheSpecifiedLine checks the line against Vol 4
// section 6.1's example, field by field.
func TestEmissionShapeMatchesTheSpecifiedLine(t *testing.T) {
	r := New(testOptions())
	c := r.Counter(IDHeartbeatSeen)
	g := r.Gauge(IDCircuitState)
	m := r.Max(IDStallBudgetUsedMaxNs)
	r.Freeze()
	r.SetIntervalSec(60)
	c.Add(214)
	g.Store(1)
	m.Observe(418203)

	var buf bytes.Buffer
	if err := r.Emit(&buf); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	out := buf.String()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")

	// Rule 1: health first.
	if !strings.Contains(lines[0], "group=health") {
		t.Errorf("the first line is not the health line: %q", lines[0])
	}

	for _, want := range []string{
		"INFO stats gen=",      // severity, marker, generation
		"interval_s=60",        // the reloadable interval
		"heartbeat_seen=214(+", // a counter: total AND delta
		"circuit_state=1",      // a gauge: sampled, no delta
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the emission does not contain %q.\n%s", want, out)
		}
	}

	// A max publishes the interval maximum with no delta.
	if !strings.Contains(out, "stall_budget_used_max_ns=418203") {
		t.Errorf("the stall max is missing or carries a delta.\n%s", out)
	}
	if strings.Contains(out, "stall_budget_used_max_ns=418203(+") {
		t.Error("a shape M instrument must not publish a delta: the value IS the interval")
	}

	// One timestamp and one gen for the whole emission, so a grep reassembles
	// one consistent snapshot (Vol 4 section 6.1, delta D4-6).
	stamp := lines[0][:len(timeLayout)]
	for _, l := range lines {
		if !strings.HasPrefix(l, stamp) {
			t.Errorf("line does not share the emission timestamp %q: %q", stamp, l)
		}
	}
}

// TestMaxIsResetBetweenEmissions is the interval semantics of shape M, observed
// end to end rather than on the type alone.
func TestMaxIsResetBetweenEmissions(t *testing.T) {
	r := New(testOptions())
	m := r.Max(IDStallBudgetUsedMaxNs)
	r.Freeze()
	m.Observe(500)

	var first, second bytes.Buffer
	if err := r.Emit(&first); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := r.Emit(&second); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	if !strings.Contains(first.String(), "stall_budget_used_max_ns=500") {
		t.Errorf("first emission missing the mark:\n%s", first.String())
	}
	if strings.Contains(second.String(), "stall_budget_used_max_ns=500") {
		t.Error("the second emission republished the first interval's maximum; " +
			"LoadAndReset must consume it")
	}
}

// TestG_OBS_4_HealthEveryIntervalOthersAtLeastEveryTen is gate G-OBS-4.
//
// Setup: a system with zero activity for three intervals. Stimulus: Emit.
// Assertion: the health group line is emitted EVERY interval; every other group
// is emitted at least once every ten.
//
// The defect it catches is "a suppression rule that hides a silent system
// entirely, which is indistinguishable from a dead stats goroutine" -- the
// worst possible failure for an observability surface that is the only one
// there is.
func TestG_OBS_4_HealthEveryIntervalOthersAtLeastEveryTen(t *testing.T) {
	r := New(testOptions())
	r.Freeze()

	const intervals = 12
	seen := make([]map[string]bool, intervals)
	for i := range intervals {
		var buf bytes.Buffer
		if err := r.Emit(&buf); err != nil {
			t.Fatalf("Emit %d: %v", i, err)
		}
		seen[i] = map[string]bool{}
		for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
			j := strings.Index(line, " group=")
			rest := line[j+len(" group="):]
			if sp := strings.IndexByte(rest, ' '); sp >= 0 {
				rest = rest[:sp]
			}
			seen[i][rest] = true
		}
	}

	// Rule 1: health, always.
	for i := range intervals {
		if !seen[i]["health"] {
			t.Errorf("interval %d emitted no health line. An operator who reads one "+
				"line reads that one, and a silent system must still produce it", i+1)
		}
	}

	// The first emission is a full baseline: nothing has been published yet, so
	// every group is due.
	for g := range numGroups {
		if !seen[0][groupNames[g]] {
			t.Errorf("group %s is missing from the first emission, which must be a "+
				"complete startup snapshot", g)
		}
	}

	// Rule 2: a quiet group stays quiet in between.
	if seen[1]["ingress"] {
		t.Error("an idle ingress group was emitted on the second interval; rule 2 " +
			"emits a group only when a counter moved or a gauge changed")
	}

	// Rule 3: and comes back by the tenth interval after it last appeared, so
	// a trend never has an unbounded gap.
	for g := range numGroups {
		name := groupNames[g]
		found := false
		for i := 1; i < intervals; i++ {
			if seen[i][name] {
				found = true
				if i != forcedEmitEvery {
					t.Errorf("group %s reappeared at interval %d, want %d",
						name, i+1, forcedEmitEvery+1)
				}
				break
			}
		}
		if !found {
			t.Errorf("group %s never reappeared in %d idle intervals; rule 3 forces "+
				"every group at least once every %d so a value that has been zero all "+
				"night is still visible as zero", name, intervals, forcedEmitEvery)
		}
	}
}

// TestMovementBringsAGroupBackImmediately is rule 2 in the positive direction.
func TestMovementBringsAGroupBackImmediately(t *testing.T) {
	r := New(testOptions())
	c := r.Counter(IDSubpacketsTotal)
	r.Freeze()

	var buf bytes.Buffer
	_ = r.Emit(&buf) // the baseline emission
	buf.Reset()
	_ = r.Emit(&buf) // idle: ingress should be absent
	if strings.Contains(buf.String(), "group=ingress") {
		t.Fatalf("idle ingress was emitted:\n%s", buf.String())
	}

	c.Add(1)
	buf.Reset()
	_ = r.Emit(&buf)
	if !strings.Contains(buf.String(), "group=ingress") {
		t.Errorf("a non-zero counter delta did not bring its group back:\n%s", buf.String())
	}
}

// TestSparseSlotsStayOffTheLineUntilTheyCarryData is the fix for the problem
// Vol 4 section 6.1's arithmetic predates.
//
// Section 6.1 sized the group split against "about 200 values, about 5 KB on
// one line", counting the dimensions it knew. Section 8.2a then added three
// {destination,column} instruments, which at three destinations by sixty-four
// columns is 576 slots -- nearly all of them in schema_fit. Dense, that one
// line would run to roughly 18 KB: worse than the single line the grouping
// exists to avoid, and truncated by the same transports.
func TestSparseSlotsStayOffTheLineUntilTheyCarryData(t *testing.T) {
	r := New(testOptions())
	trunc := r.CounterAt2(IDMessageTruncated, 0, 1)
	r.Freeze()

	var buf bytes.Buffer
	_ = r.Emit(&buf)
	if strings.Contains(buf.String(), "message_truncated") {
		t.Errorf("an untouched {destination,column} slot reached the line:\n%s", buf.String())
	}

	trunc.Add(1)
	buf.Reset()
	_ = r.Emit(&buf)
	out := buf.String()
	if !strings.Contains(out, "message_truncated{md_token_quote,open_price}=1(+1)") {
		t.Errorf("a touched slot did not appear with its label:\n%s", out)
	}
	// And only that one, not all 192.
	if n := strings.Count(out, "message_truncated"); n != 1 {
		t.Errorf("message_truncated appears %d times, want 1: the other slots carry "+
			"no data and must stay off the line", n)
	}
}

// TestDenseSlotsAppearEvenAtZero is the other side of sparsity, and it is what
// keeps rule 3 meaningful: the point of a forced emission is that "a value that
// has been zero all night is still visible as zero".
func TestDenseSlotsAppearEvenAtZero(t *testing.T) {
	r := New(testOptions())
	r.Freeze()

	var buf bytes.Buffer
	_ = r.Emit(&buf)
	out := buf.String()
	for _, want := range []string{
		"datagrams_received=0(+0)",
		"flush_on_tick{3}=0(+0)",
		"dirty_depth{0,md_token_quote}=0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("%q is missing; a dense slot must publish its zero.\n%s", want, out)
		}
	}
}

// TestLabelsRenderDomainValuesNotSlotIndices catches a misattribution that
// would be entirely silent.
//
// Market types are 1 to 4 (bible Appendix A), and Vol 4 section 6.1's example
// shows session_state{1} for the first of them -- while shard_depth_max{0}
// shows that {k} is zero-based. Rendering market types zero-based would shift
// every session reading by one market, and the only way to notice would be the
// free cross-check against MD_MARKET_STATUS in Oracle that section 6.6 Q9
// describes.
func TestLabelsRenderDomainValuesNotSlotIndices(t *testing.T) {
	r := New(testOptions())
	s := r.GaugeAt(IDSessionState, 0)
	k := r.MaxAt(IDShardDepthMax, 0)
	code := r.CounterAt(IDPacketsByTranscode, 0)
	r.Freeze()
	s.Store(3)
	k.Observe(5)
	code.Add(1)

	var buf bytes.Buffer
	_ = r.Emit(&buf)
	out := buf.String()

	for _, tc := range []struct{ want, why string }{
		{"session_state{1}=3", "market types are 1-based (bible Appendix A)"},
		{"shard_depth_max{0}=5", "shard numbers are 0-based"},
		{"packets_by_transcode{7208}=1(+1)", "the {code} label is the transaction code"},
		{"dirty_depth{0,md_token_quote}=0", "{destination} renders the NAME, not the slot id"},
	} {
		if !strings.Contains(out, tc.want) {
			t.Errorf("%q is missing -- %s.\n%s", tc.want, tc.why, out)
		}
	}
}

// TestDynamicLabelsRenderTheirCode covers the one dynamic label and the Oracle
// overflow bucket.
func TestDynamicLabelsRenderTheirCode(t *testing.T) {
	r := New(testOptions())
	r.Freeze()

	c, ok := r.UnknownTranscode(7777)
	if !ok {
		t.Fatal("a fresh 64-slot table had no room for one code")
	}
	c.Add(2)
	r.OracleError(1438).Add(1)

	var buf bytes.Buffer
	_ = r.Emit(&buf)
	out := buf.String()

	if !strings.Contains(out, "unknown_transcode{7777}=2(+2)") {
		t.Errorf("the dynamic label did not render its code.\n%s", out)
	}
	if !strings.Contains(out, "oracle_error{1438}=1(+1)") {
		t.Errorf("the Oracle code did not render.\n%s", out)
	}
	// The reserved overflow slot HAS a label, unlike an unclaimed code slot,
	// but it is still sparse: an empty overflow bucket says nothing and stays
	// off the line.
	if strings.Contains(out, "oracle_error{other}") {
		t.Errorf("an empty Oracle overflow bucket reached the line.\n%s", out)
	}
	// And a slot nobody claimed renders nothing at all -- there is no label to
	// render, which is why a dynamic dimension must be sparse.
	if strings.Contains(out, "unknown_transcode{0}") {
		t.Errorf("an unclaimed dynamic slot was rendered with a zero code.\n%s", out)
	}
}

// TestDynamicTableOverflowIsNamedNotNumbered exercises both tables past
// capacity, which is the case the overflow buckets exist for.
//
// The two tables resolve overflow differently and the asymmetry is in the
// ledger, not a choice made here: unknown_transcode has a separate named
// instrument (unknown_transcode_other, Vol 0 section 8.2), while oracle_error
// has none, so its overflow lands in a reserved slot rendered {other}. Either
// way no code is invented for a bucket that aggregates several.
func TestDynamicTableOverflowIsNamedNotNumbered(t *testing.T) {
	r := New(testOptions())
	other := r.Counter(IDUnknownTranscodeOther)
	r.Freeze()

	// Fill the 64-slot transcode table, then one more.
	for code := 1; code <= unknownTranscodeSlots; code++ {
		c, ok := r.UnknownTranscode(code)
		if !ok {
			t.Fatalf("slot %d of %d was refused while the table should still have room",
				code, unknownTranscodeSlots)
		}
		c.Add(1)
	}
	c, ok := r.UnknownTranscode(9001)
	if ok {
		t.Error("a 65th code was accepted into a 64-slot table")
	}
	if c != other {
		t.Error("an overflowing code did not fall back to unknown_transcode_other. " +
			"Returning some other counter would aggregate it invisibly, and the " +
			"overflow bucket exists precisely so the two cases stay distinguishable")
	}
	c.Add(1)

	// Fill the Oracle table past its 24 slots.
	for code := 1; code <= oracleErrorSlots+5; code++ {
		r.OracleError(code * 100).Add(1)
	}

	var buf bytes.Buffer
	_ = r.Emit(&buf)
	out := buf.String()

	if !strings.Contains(out, "unknown_transcode_other=1(+1)") {
		t.Errorf("the transcode overflow counter did not publish.\n%s", out)
	}
	if !strings.Contains(out, "oracle_error{other}=") {
		t.Errorf("the Oracle overflow bucket did not publish once it had data.\n%s", out)
	}
}

// TestG_OBS_2_EmitDoesNotAllocate is gate G-OBS-2.
//
// Setup: a fully populated registry. Stimulus: AllocsPerRun over one Emit.
// Assertion: a small constant, independent of the interval count, and ZERO
// growth across repeated emissions.
//
// The defect it catches is "an emission that builds its output by string
// concatenation or rebuilds its label set each time, turning observability into
// a per-minute garbage source".
func TestG_OBS_2_EmitDoesNotAllocate(t *testing.T) {
	r := New(testOptions())
	touchAll(t, r)

	// Warm up, so the first emission's all-groups-dirty path is behind us and
	// the measurement is of the steady state.
	for range 3 {
		if err := r.Emit(io.Discard); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	capBefore := cap(r.buf)

	run := func() {
		if err := r.Emit(io.Discard); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	first := testing.AllocsPerRun(50, run)
	second := testing.AllocsPerRun(50, run)

	if first != 0 {
		t.Errorf("Emit allocates %.1f times per call, want 0. Every buffer is sized "+
			"in Freeze and every label prefix is pre-rendered, so the emission path "+
			"must only append bytes (gate G-OBS-2)", first)
	}
	if second > first {
		t.Errorf("allocations GREW across emissions: %.1f then %.1f. Something is "+
			"accumulating per interval, which is what G-OBS-2's zero-growth clause "+
			"exists to catch", first, second)
	}
	if got := cap(r.buf); got != capBefore {
		t.Errorf("the emission buffer was reallocated (cap %d -> %d); maxLineLen "+
			"under-estimates the longest line", capBefore, got)
	}
}

// BenchmarkEmit measures the once-a-minute cost against a fully populated
// registry. Vol 4 section 6.1 budgets it at "about 200 atomic loads and one
// strconv pass per value, once a minute, on a goroutine that is otherwise
// asleep -- unmeasurable against a 4-way LPAR". This is where that claim stops
// being a claim.
func BenchmarkEmit(b *testing.B) {
	r := New(testOptions())
	cs := make([]*Counter, 0, numInstruments)
	for id := range numInstruments {
		if ledger[id].dim == dimNone && ledger[id].shape == shapeCounter {
			cs = append(cs, r.Counter(id))
		}
	}
	r.Freeze()
	for _, c := range cs {
		c.Add(1)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = r.Emit(io.Discard)
	}
}

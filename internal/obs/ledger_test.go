package obs

import (
	"regexp"
	"strings"
	"testing"
)

// TestInstrumentCountReconcilesWithTheLedgerRows is the test that would have
// prevented a day of confusion, so it states the arithmetic rather than just
// asserting a number.
//
// The task card and section 2 of docs/development/phase1-tasks.md both say
// "129 instruments". That is a count of TABLE ROWS in Vol 0 section 8.2 (69)
// plus section 8.2a (60). The instrument count is 131, because two rows name
// several instruments each and one name appears in both tables.
//
// A reviewer re-counting section 8.2a with a naive grep for lines beginning
// with a backticked name gets 62, not 60, because the collision sub-table at
// the end of that section has two rows that also begin that way -- shard_depth
// versus shard_depth_max, and blob_overrun versus blob_len_mismatch. Those
// rows are prose about a decision, not ledger entries.
func TestInstrumentCountReconcilesWithTheLedgerRows(t *testing.T) {
	if numInstruments != 130 {
		t.Errorf("numInstruments = %d, want 130. Vol 0 section 8.2 and section 8.2a "+
			"name 131 distinct instruments across 129 table rows, and one "+
			"(qd_wake_parked) is withdrawn", numInstruments)
	}
	if got := ledgerRows + multiNameRows - duplicatedNames - withdrawnRows; got != int(numInstruments) {
		t.Errorf("the reconciliation does not hold: %d rows + %d extra names - %d "+
			"duplicate - %d withdrawn = %d, but numInstruments is %d",
			ledgerRows, multiNameRows, duplicatedNames, withdrawnRows, got, numInstruments)
	}
}

// TestEveryIDHasARow catches the one failure mode the keyed array literal
// cannot: an ID constant declared in the iota block with no row written for it.
// The compiler rejects duplicate and out-of-range keys, so this is the whole
// remaining gap.
func TestEveryIDHasARow(t *testing.T) {
	for id := range numInstruments {
		if ledger[id].name == "" {
			t.Errorf("instrument id %d has no ledger row. An ID was added to the iota "+
				"block in ids.go without a matching entry in ledger.go, so it resolves "+
				"to a nameless slot that no emission can publish -- which is exactly "+
				"how Vol 0 section 8.3's gaps G1 to G9 arose", id)
		}
	}
}

func TestInstrumentNamesAreUnique(t *testing.T) {
	seen := make(map[string]ID, numInstruments)
	for id := range numInstruments {
		name := ledger[id].name
		if prev, dup := seen[name]; dup {
			t.Errorf("%q is declared twice, as id %d and id %d. Two instruments under "+
				"one name are indistinguishable in the output and their counts are "+
				"split, not summed", name, prev, id)
			continue
		}
		seen[name] = id
	}
}

var nameRE = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// TestInstrumentNamesAreGreppable matters because stderr is the entire
// observability surface (Vol 4 section 6.7) and grep is the only query
// language. A name with a space or a capital in it is a name an operator
// cannot match reliably.
func TestInstrumentNamesAreGreppable(t *testing.T) {
	for id := range numInstruments {
		if name := ledger[id].name; !nameRE.MatchString(name) {
			t.Errorf("%q is not lower_snake_case", name)
		}
	}
}

// TestMaxShapeAndMaxSuffixAgree turns Vol 0 section 8.3's gap G5 into an
// assertion, in BOTH directions.
//
// Section 8.1 makes the suffix a MUST: shape M names "MUST end _max or
// _max_ns". G5 is the record of what happens when that is not checked -- HLD
// section 10.1 listed epoch_duration and active_tokens_1s with no suffix,
// breaking its own naming rule, and a reader could not tell a cumulative total
// from an interval maximum. The reverse direction matters just as much: a
// counter called something_max would read as a high-water mark and be
// interpreted as an interval value when it is a running total.
func TestMaxShapeAndMaxSuffixAgree(t *testing.T) {
	for id := range numInstruments {
		row := &ledger[id]
		hasSuffix := strings.HasSuffix(row.name, "_max") || strings.HasSuffix(row.name, "_max_ns")
		isMax := row.shape == shapeMax

		switch {
		case isMax && !hasSuffix:
			t.Errorf("%q is shape M but does not end _max or _max_ns. Vol 0 section 8.1 "+
				"makes the suffix a MUST, because the value is an INTERVAL maximum that "+
				"is reset at emission and nothing else in the line behaves that way",
				row.name)
		case !isMax && strings.Contains(row.name, "_max"):
			t.Errorf("%q contains _max but is shape %s. A name that reads as a "+
				"high-water mark and publishes a cumulative total is gap G5 in the "+
				"other direction", row.name, row.shape)
		}
	}
}

// TestDurationNamesCarryTheUnit is the other half of Vol 4 section 10.1's
// table: "Instrument name MUST carry the unit for durations".
//
// CARRY, not end with. epoch_ns_total is the case that makes the difference:
// it is a cumulative total of nanoseconds rather than a measurement of one
// duration, so the ledger spells the unit in the middle and the suffix records
// that it is a total. Requiring a trailing _ns would reject a name the contract
// fixes -- the shape suffix rule in TestMaxShapeAndMaxSuffixAgree is the one
// that is positional, and only for shape M.
func TestDurationNamesCarryTheUnit(t *testing.T) {
	for id := range numInstruments {
		row := &ledger[id]
		if row.unit == "ns" && !strings.Contains(row.name, "_ns") {
			t.Errorf("%q is measured in ns but its name does not say so. Vol 4 "+
				"section 10.1: an instrument name MUST carry the unit for durations, "+
				"so a reader never has to guess whether a number is nanoseconds or "+
				"milliseconds", row.name)
		}
	}
}

// TestEveryGroupHasAnInstrument proves no group line can be structurally empty,
// which is what makes Vol 4 section 6.1 rule 3 meaningful: a forced emission of
// a group with nothing in it would be a line with a group= and no values.
func TestEveryGroupHasAnInstrument(t *testing.T) {
	var count [numGroups]int
	for id := range numInstruments {
		count[ledger[id].group]++
	}
	for g := range numGroups {
		if count[g] == 0 {
			t.Errorf("group %s has no instruments", g)
		}
	}
}

// TestEveryGroupHasADenseInstrument is the stronger form, and it is what
// closes the hole sparsity opens.
//
// Slot-level sparsity means a quiet instrument prints nothing. If a group held
// ONLY sparse instruments, its forced ten-interval line would carry a group=
// and nothing else -- defeating rule 3's stated purpose, that "a value that has
// been zero all night is still visible as zero".
func TestEveryGroupHasADenseInstrument(t *testing.T) {
	var dense [numGroups]int
	for id := range numInstruments {
		if dimSpecs[ledger[id].dim].kind == dimDense {
			dense[ledger[id].group]++
		}
	}
	for g := range numGroups {
		if dense[g] == 0 {
			t.Errorf("group %s has no DENSE instrument, so its forced ten-interval "+
				"line would be empty on a quiet process", g)
		}
	}
}

// TestEveryDimensionIsUsed catches a dimension declared and then orphaned by a
// row that was later re-labelled -- dead sizing nobody notices, which is how
// the slab grows for no reason.
func TestEveryDimensionIsUsed(t *testing.T) {
	var used [numDims]int
	for id := range numInstruments {
		used[ledger[id].dim]++
	}
	for d := range numDims {
		if used[d] == 0 {
			t.Errorf("dimension %d (%s) is declared but no instrument uses it",
				d, dimSpecs[d].brace)
		}
	}
}

// TestHealthMembersAreRealInstrumentsWithHomeGroups pins the decision that
// health is a PROJECTION.
//
// Every member must be a real instrument -- so the line cannot reference
// something that does not exist -- and every member must still have a home
// group, because health is not an ownership group. An instrument that appeared
// only on the health line would be missing from the group lines a trend is
// built from.
func TestHealthMembersAreRealInstrumentsWithHomeGroups(t *testing.T) {
	if len(healthLine) == 0 {
		t.Fatal("the health line is empty; Vol 4 section 6.1 rule 1 requires it always")
	}
	seen := map[ID]bool{}
	for _, id := range healthLine {
		if id >= numInstruments {
			t.Errorf("health member id %d is out of range", id)
			continue
		}
		if seen[id] {
			t.Errorf("%q appears twice on the health line", ledger[id].name)
		}
		seen[id] = true
		if ledger[id].group >= numGroups {
			t.Errorf("%q has no home group; health is a projection, not an owner",
				ledger[id].name)
		}
	}
}

// TestHealthLineCoversTheFirstThreeRunbookQuestions keeps the membership
// honest against its source.
//
// Vol 4 section 6.1's example line and section 6.6's questions Q1 to Q3
// disagreed about four instruments; the union is what this ledger emits, and
// this test is where that decision is recorded so a future edit has to argue
// with it rather than silently drop one.
func TestHealthLineCoversTheFirstThreeRunbookQuestions(t *testing.T) {
	want := []string{
		"uptime_seconds",         // Q1, and section 6.1's example as uptime_s
		"config_generation",      // Q1 and the example
		"config_reload_rejected", // the example only
		"circuit_state",          // Q2 and the example
		"datagrams_received",     // Q2 and the example
		"heartbeat_seen",         // Q2 and the example
		"session_state",          // the example only
		"seq_gap_events",         // Q3 and the example
		"seq_gap_messages",       // Q3 ONLY -- the example omits it
		"writer_health_state",    // the example only
	}

	got := make(map[string]bool, len(healthLine))
	for _, id := range healthLine {
		got[ledger[id].name] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("%q is missing from the health line. It is named by Vol 4 section "+
				"6.1's example, by section 6.6 Q1-Q3, or by both", name)
		}
	}
	if len(healthLine) != len(want) {
		t.Errorf("the health line has %d members, want %d", len(healthLine), len(want))
	}
}

// TestUptimeUsesTheLedgerNameNotTheExamples records the fourth discrepancy
// found while settling the health line.
//
// Vol 4 section 6.1's example line writes uptime_s=19847. The ledger name is
// uptime_seconds, and the ledger wins: gate G-OBS-3 asserts the emitted name
// set equals the ledger's, so a shortened name in the output would fail it.
// The document's example is the thing that is wrong.
func TestUptimeUsesTheLedgerNameNotTheExamples(t *testing.T) {
	if got := ledger[IDUptimeSeconds].name; got != "uptime_seconds" {
		t.Errorf("uptime instrument is named %q, want uptime_seconds", got)
	}
}

// TestDynamicDimensionsAreSparse pins an invariant the emission relies on: an
// unoccupied dynamic slot has no label text at all, so rendering it densely
// would produce a name with empty braces.
func TestDynamicDimensionsAreSparse(t *testing.T) {
	for _, d := range []Dim{dimUnknownCode, dimOracleCode} {
		if dimSpecs[d].kind != dimDynamic {
			t.Errorf("dimension %s must be dimDynamic: its label is a code nobody "+
				"configured, so an unoccupied slot has no text to render and a dense "+
				"pass would emit a name with empty braces", dimSpecs[d].brace)
		}
	}
}

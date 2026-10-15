package app

import (
	"strings"
	"testing"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// The two instrument defects P2-09 closed, each pinned by the condition that
// would have caught it.

// TestMessageTruncatedSurvivesAFrozenRegistry is the regression test for a
// latent production panic.
//
// message_truncated{destination,column} used to be resolved LAZILY, by a
// closure calling reg.CounterAt2 at the moment of use. Resolution panics once
// the registry is frozen (gate G-OBS-1), and the registry is frozen from
// startup step 9 onward -- so the FIRST truncated value in production would
// have panicked inside decodeSubPacket's recover boundary: swallowed as
// panics_recovered{decode}, costing that message its units, and never
// incrementing the counter an operator reads to find out which column is being
// cut. Three failures from one lazy lookup, none of them visible as itself.
//
// Frozen first, then counted, which is the whole assertion.
func TestMessageTruncatedSurvivesAFrozenRegistry(t *testing.T) {
	cfg := config.Defaults()
	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}

	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg, dests)
	reg.Freeze()

	cnt := newDecodeCounters(&inst)

	// Every destination's every column, because the arity is ragged (16, 9 and
	// 6) and a rectangular resolution would be in range for the widest one and
	// out of range for the others -- which is the shape of mistake that would
	// otherwise be found only by whichever column truncated first in
	// production.
	all := dests.All()
	for d := range all {
		for c := range all[d].Columns {
			cnt.MessageTruncated(model.DestID(d), uint8(c))
		}
	}

	// And the counts landed where they were asked to land.
	for d := range all {
		for c := range all[d].Columns {
			if got := inst.messageTruncated[d][c].Load(); got != 1 {
				t.Errorf("message_truncated{%d,%d} = %d, want 1", d, c, got)
			}
		}
	}
}

// TestMessageTruncatedIgnoresAnOutOfRangeIndex covers the bounds check.
//
// Both indices come from a DECODER, so a ColSpec whose bit has drifted past
// its destination's column list is a code defect -- and one that would
// otherwise index a slot the registry never allocated, from inside the recover
// boundary, where the panic says nothing useful about why.
func TestMessageTruncatedIgnoresAnOutOfRangeIndex(t *testing.T) {
	cfg := config.Defaults()
	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg, dests)
	reg.Freeze()
	cnt := newDecodeCounters(&inst)

	// Must not panic, and must not write anywhere.
	cnt.MessageTruncated(model.DestID(len(dests.All())), 0)
	cnt.MessageTruncated(0, 250)
}

// TestActiveTokens1sMaxReachesTheStatsLine is the other half of req section
// 12.4: the figure has to be VISIBLE, not merely computed.
//
// active_tokens_1s_max was declared in the ledger from P1-07 and resolved by
// nothing until P2-09 -- an allocated slab slot that could never carry a
// number. A test that only checked Observe had been called would not have
// noticed, because the gap was between the instrument and the emission, so this
// drives datagrams across a second boundary and then reads the emitted line.
func TestActiveTokens1sMaxReachesTheStatsLine(t *testing.T) {
	f := newAllocGateFixture(t, nil)

	// Two datagrams in one second, carrying two records each: four units, but
	// only two DISTINCT tokens, since mbp7208Payload plants the same two.
	d := datagramOf(t, uncompressedSub(mbp7208Payload(2)))
	d.RecvMono = 1
	if err := f.r.decodeDatagram(d); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}
	d2 := datagramOf(t, uncompressedSub(mbp7208Payload(2)))
	d2.RecvMono = 2
	if err := f.r.decodeDatagram(d2); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}

	if got := f.r.activeTokens.Count(); got != 2 {
		t.Fatalf("the set holds %d distinct tokens after four units carrying two distinct "+
			"tokens, want 2", got)
	}

	// Crossing into the next second is what publishes the figure.
	d3 := datagramOf(t, uncompressedSub(mbp7208Payload(2)))
	d3.RecvMono = 1_500_000_000
	if err := f.r.decodeDatagram(d3); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}

	var out strings.Builder
	if err := f.g.deps.Reg.Emit(&out); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	line := out.String()
	if !strings.Contains(line, "active_tokens_1s_max=2") {
		t.Errorf("active_tokens_1s_max=2 is not in the emitted stats line -- the instrument "+
			"is resolved but the figure is not reaching an operator:\n%s", line)
	}
}

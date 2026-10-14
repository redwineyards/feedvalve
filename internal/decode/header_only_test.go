package decode

import (
	"testing"

	"feedvalve/internal/model"
)

// 6541 and 6503: recognised, NOT decoded (Vol 1 section 9.6).
//
// There is no decoder file for either, and this test is the ABSENCE made
// explicit. An absence nobody asserts is indistinguishable from an omission,
// and these two are easy to add by mistake: both carry a MESSAGE_HEADER, both
// are catalogued by the bible, and both would decode fine. The reason they must
// not is that neither has a destination column to write -- MaxUnits is 0, there
// is nothing to store, and a decoder for either would be a decoder whose output
// has nowhere to go.
//
//	6541 BC_CIRCUIT_CHECK -- bible section 4.12. The broadcast circuit's
//	pulse: "if no data has flowed on the broadcast circuit for a stipulated
//	period, the host sends a pulse. The period is currently 9 seconds." It is
//	a MESSAGE_HEADER and nothing else, 40 bytes, so the payload is 48.
//	Counted as heartbeat_seen and message_header_seen{6541}; it feeds the
//	liveness FSM and nothing else.
//
//	6503 MS_ASSET_UPDT_INT_RATE_INFO -- bible section 4.15. Five fields past
//	the header, 52 bytes declared, payload 60. Counted as
//	message_header_seen{6503}. Nothing in the Phase 1 destination set consumes
//	an asset interest rate.
//
// Both are resolved by P2-02's (code, length) rule -- 6541 at payload 48, 6503
// at payload 60 -- and P2-05's sub-packet path counts them WITHOUT a registry
// lookup and without running the gap detector. The gap detector is the
// important omission: a MESSAGE_HEADER has no BCSeqNo, and offset 14 holds the
// top half of a nanosecond Timestamp, so feeding it to the detector would
// fabricate a sequence gap on every nine-second pulse (gate G-HDR-1).

// TestHeaderOnlyCodesHaveNoDecoder is the assertion this file exists for.
func TestHeaderOnlyCodesHaveNoDecoder(t *testing.T) {
	for _, code := range []model.TransCode{6541, 6503} {
		if CanDecode(code) {
			t.Errorf("%d has a decoder in the catalogue. It is recognised but NOT decoded "+
				"(Vol 1 section 9.6): MaxUnits is 0, no destination column consumes it, and a "+
				"decoder for it would produce units with nowhere to go", code)
		}
	}
}

// TestHeaderOnlyCodesCannotBeConfigured follows from the absence: a
// configuration naming one is rejected at load by V-T1 rather than discovered
// in production, which is the whole point of the catalogue being level 1.
func TestHeaderOnlyCodesCannotBeConfigured(t *testing.T) {
	for _, code := range []int{6541, 6503} {
		_, err := BuildRegistry(specsFor(code), testDests(t), NopCounters{})
		if err == nil {
			t.Errorf("transcode %d was accepted into the registry", code)
			continue
		}
		if !hasKind(err, ProblemNoDecoder) {
			t.Errorf("transcode %d was rejected as %v, want a ProblemNoDecoder (V-T1)", code, err)
		}
	}
}

// TestTheCatalogueIsExactlyTheNineDecodingCodes pins the whole inventory.
//
// Vol 1 section 9.1 names nine decoding codes and two recognised-but-not-decoded
// ones, and everything else the bible catalogues -- 7201, 7202, 7206, 7210,
// 7211, 7213, 7214, 7220, 7306, 7320, 7324, 9010, 9011 -- is deliberately OUT.
// A decoder added speculatively is a decoder for a schema OQ-13 has not chosen,
// and unknown_transcode{code} is the mechanism for closing the inventory later
// (ADR-011): a non-zero count in the 60-second line is the evidence that adds
// one, and adding it is then one file plus one config entry.
//
// This test is what makes "the inventory stops here" a fact rather than an
// intention.
func TestTheCatalogueIsExactlyTheNineDecodingCodes(t *testing.T) {
	want := []model.TransCode{6511, 6521, 6522, 6531, 6571, 7200, 7208, 7305, 7340}

	got := Catalogue()
	if len(got) != len(want) {
		t.Fatalf("the catalogue holds %v, want exactly the nine of section 9.1: %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("the catalogue is %v, want %v", got, want)
		}
	}

	// The out-of-scope codes, named individually so a future addition has to
	// edit this list and say why.
	for _, code := range []model.TransCode{
		7201, 7202, 7206, 7210, 7211, 7213, 7214, 7220, 7306, 7320, 7324, 9010, 9011,
		6541, 6503,
	} {
		if CanDecode(code) {
			t.Errorf("%d is in the catalogue but is out of scope for Phase 1 (section 9.1). "+
				"If it belongs now, say so here and in the inventory", code)
		}
	}
}

// TestEveryCataloguedCodeDeclaresItsBodyLength keeps the registry's runtime
// length check meaningful: BodyLen of zero means variable-length, and nothing
// in this inventory is.
func TestEveryCataloguedCodeDeclaresItsBodyLength(t *testing.T) {
	want := map[model.TransCode]int{
		7208: 470, 7200: 410, 7305: 294, 7340: 294,
		6511: 320, 6521: 320, 6522: 320, 6531: 320, 6571: 320,
	}

	codes := make([]int, 0, len(want))
	for code := range want {
		codes = append(codes, int(code))
	}
	reg := buildAllForTest(t, codes...)

	for code, bodyLen := range want {
		e := reg.Lookup(code)
		if e == nil {
			t.Errorf("%d did not resolve", code)
			continue
		}
		if got := e.Dec.BodyLen(); got != bodyLen {
			t.Errorf("%d BodyLen = %d, want the bible's declared %d", code, got, bodyLen)
		}
		if e.Dec.Code() != code {
			t.Errorf("%d resolved to a decoder reporting %d", code, e.Dec.Code())
		}
		if e.Dec.MaxUnits() < 1 {
			t.Errorf("%d declares MaxUnits %d; a decoding code emits at least one unit",
				code, e.Dec.MaxUnits())
		}
	}

	// 7208 is the only one in this inventory that fans out, and it fans out to
	// exactly two (bible section 4.8).
	if got := reg.Lookup(7208).Dec.MaxUnits(); got != 2 {
		t.Errorf("7208 MaxUnits = %d, want 2", got)
	}
	for _, code := range []model.TransCode{7200, 7305, 7340, 6511} {
		if got := reg.Lookup(code).Dec.MaxUnits(); got != 1 {
			t.Errorf("%d MaxUnits = %d, want 1", code, got)
		}
	}
}

// buildAllForTest builds a registry over several codes, giving each the full
// column set of whichever destination declares it as a contributor.
func buildAllForTest(t *testing.T, codes ...int) Registry {
	t.Helper()

	dests := testDests(t)
	specs := make([]TranscodeSpec, 0, len(codes))
	for _, code := range codes {
		spec := TranscodeSpec{Code: code, Class: model.ClassRateLimited}
		for _, d := range dests.All() {
			if !d.Contributes(model.TransCode(code)) {
				continue
			}
			cols := make([]string, 0, len(d.Columns))
			for i := range d.Columns {
				cols = append(cols, d.Columns[i].Name())
			}
			spec.Contributions = append(spec.Contributions,
				ContribSpec{Destination: d.Name, Columns: cols})
		}
		specs = append(specs, spec)
	}
	reg, err := BuildRegistry(specs, dests, NopCounters{})
	if err != nil {
		t.Fatalf("BuildRegistry(%v): %v", codes, err)
	}
	return reg
}

// TestTheWholeInventoryBuildsTogether is the configuration a deployment
// actually runs: every code at once, each resolving to its decoder with its own
// slots and ColSpecs.
//
// It also covers the case gate G-SC-2 names -- 7200 and 7208 declared together
// is ACCEPTED -- against the real catalogue rather than fakes.
func TestTheWholeInventoryBuildsTogether(t *testing.T) {
	all := []int{6511, 6521, 6522, 6531, 6571, 7200, 7208, 7305, 7340}
	reg := buildAllForTest(t, all...)

	if reg.Len() != len(all) {
		t.Fatalf("registry holds %d codes, want %d", reg.Len(), len(all))
	}
	for i, code := range all {
		if got := reg.Slot(model.TransCode(code)); got != i {
			t.Errorf("Slot(%d) = %d, want %d -- ascending order is what obs's {code} label "+
				"index relies on", code, got, i)
		}
	}

	// Each code contributes to exactly one destination in this inventory, and
	// the widths are the three ImageLens.
	for _, tc := range []struct {
		code  model.TransCode
		dest  model.DestID
		width uint16
	}{
		{7208, destQuote, 181},
		{7200, destQuote, 181},
		{7305, destSecurityMaster, 37},
		{7340, destSecurityMaster, 37},
		{6511, destMarketStatus, 142},
		{6571, destMarketStatus, 142},
	} {
		e := reg.Lookup(tc.code)
		if got := e.Contrib[tc.dest].Width; got != tc.width {
			t.Errorf("%d width for destination %d = %d, want %d",
				tc.code, tc.dest, got, tc.width)
		}
		for d := range model.DestID(model.MaxDestinations) {
			if d == tc.dest {
				continue
			}
			if e.Contributes(d) {
				t.Errorf("%d contributes to destination %d as well as %d",
					tc.code, d, tc.dest)
			}
		}
	}
}

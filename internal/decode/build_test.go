package decode

import (
	"errors"
	"testing"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
)

// BuildRegistry: Vol 1 section 8.2, and rules V-T1 and V-T6 -- the two the
// configuration validator has reported as un-evaluable since P1-06, because
// neither can be answered without the catalogue and the resolved ColSpecs.

func testDests(t *testing.T) *destinations.Set {
	t.Helper()
	d, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	return d
}

// specsFor builds a minimal inventory: one contribution per code, to whichever
// destination declares it as a contributor, naming that destination's first
// column.
//
// Minimal on purpose. These tests are about RESOLUTION -- does the code have a
// decoder, does the slot exist, does the width match the mask -- and a fuller
// inventory would make a failure harder to attribute without testing anything
// resolution does not already cover.
func specsFor(codes ...int) []TranscodeSpec {
	out := make([]TranscodeSpec, 0, len(codes))
	for _, c := range codes {
		out = append(out, TranscodeSpec{Code: c, Class: model.ClassRateLimited})
	}
	return out
}

// specsWithContribution is specsFor plus a real contribution, for the tests
// that exercise ColSpec resolution.
func specsWithContribution(t *testing.T, code int, dest string, columns ...string) []TranscodeSpec {
	t.Helper()
	return []TranscodeSpec{{
		Code:  code,
		Class: model.ClassRateLimited,
		Contributions: []ContribSpec{{
			Destination: dest,
			Columns:     columns,
		}},
	}}
}

func buildForTest(t *testing.T, codes ...int) Registry {
	t.Helper()
	reg, err := BuildRegistry(specsFor(codes...), testDests(t), NopCounters{})
	if err != nil {
		t.Fatalf("BuildRegistry(%v): %v", codes, err)
	}
	return reg
}

func hasKind(err error, kind ProblemKind) bool {
	var be *BuildError
	if !errors.As(err, &be) {
		return false
	}
	for _, p := range be.Problems {
		if p.Kind == kind {
			return true
		}
	}
	return false
}

// TestG_SC_2_EveryConfiguredTranscodeResolves is gate G-SC-2's first half and
// rule V-T1: a configured transcode with no decoder in this build is rejected
// at load, not discovered in production.
//
// The defect it prevents is the quiet one: the process starts, looks healthy,
// counts the datagrams, and stores nothing for that code -- because nothing
// can decode it and the only signal is a counter nobody is watching yet.
func TestG_SC_2_EveryConfiguredTranscodeResolves(t *testing.T) {
	withCatalogue(t, fake(7208))

	if _, err := BuildRegistry(specsFor(7208), testDests(t), NopCounters{}); err != nil {
		t.Fatalf("a configured code WITH a decoder was rejected: %v", err)
	}

	_, err := BuildRegistry(specsFor(7208, 7305), testDests(t), NopCounters{})
	if err == nil {
		t.Fatal("7305 was configured with no decoder registered, and the registry accepted it")
	}
	if !hasKind(err, ProblemNoDecoder) {
		t.Errorf("error = %v, want a ProblemNoDecoder (V-T1)", err)
	}
	// The message must name the code AND what this build can decode, because
	// the operator's next question is "then what CAN it decode?"
	if msg := err.Error(); !contains(msg, "7305") || !contains(msg, "7208") {
		t.Errorf("the rejection names neither the bad code nor the catalogue: %q", msg)
	}
}

// TestG_SC_2_BothMBPCodesAreAccepted is gate G-SC-2's named case: a
// configuration declaring BOTH 7200 and 7208 is ACCEPTED, not rejected.
//
// They are mutually exclusive by NSEIL control (bible section 4.8a), so which
// one arrives is the exchange's choice and not ours. A build that can only
// decode one is a build that goes blind the day they switch -- and a validator
// that rejected the pair would force exactly that, by making the safe
// configuration the invalid one.
func TestG_SC_2_BothMBPCodesAreAccepted(t *testing.T) {
	withCatalogue(t, fake(7200), fake(7208))

	reg, err := BuildRegistry(specsFor(7200, 7208), testDests(t), NopCounters{})
	if err != nil {
		t.Fatalf("a configuration declaring both 7200 and 7208 was rejected: %v", err)
	}
	if reg.Len() != 2 {
		t.Fatalf("registry holds %d codes, want both", reg.Len())
	}
	for _, code := range []model.TransCode{7200, 7208} {
		if reg.Lookup(code) == nil {
			t.Errorf("Lookup(%d) = nil after both were configured", code)
		}
	}

	// Both are contributors to MD_TOKEN_QUOTE, and they must hold DIFFERENT
	// slots: the slot indexes Entry.LastSeq, so two codes sharing one would
	// overwrite each other's ordering state.
	q := testDests(t).ByName("MD_TOKEN_QUOTE")
	s7200, ok1 := q.Slot(7200)
	s7208, ok2 := q.Slot(7208)
	if !ok1 || !ok2 {
		t.Fatal("MD_TOKEN_QUOTE does not declare both MBP codes as contributors")
	}
	if s7200 == s7208 {
		t.Errorf("7200 and 7208 share contributor slot %d", s7200)
	}
}

// TestV_T6_ColSpecWidthMatchesItsMask is rule V-T6.
//
// Width is precomputed so a decoder bound-checks its blob write in ONE
// comparison rather than walking the mask per message. That only holds while
// the two agree, and they are computed in the same loop here precisely so they
// cannot drift apart silently.
func TestV_T6_ColSpecWidthMatchesItsMask(t *testing.T) {
	withCatalogue(t, fake(7208))
	dests := testDests(t)
	q := dests.ByName("MD_TOKEN_QUOTE")

	first := q.Columns[0].Name()
	second := q.Columns[1].Name()
	reg, err := BuildRegistry(specsWithContribution(t, 7208, "MD_TOKEN_QUOTE", first, second), dests, NopCounters{})
	if err != nil {
		t.Fatalf("BuildRegistry: %v", err)
	}

	e := reg.Lookup(7208)
	spec := e.Contrib[q.ID]
	wantWidth := q.Columns[0].Width + q.Columns[1].Width
	if spec.Width != wantWidth {
		t.Errorf("ColSpec.Width = %d, want %d (%s + %s)", spec.Width, wantWidth, first, second)
	}
	if !spec.Mask.Has(q.Columns[0].Bit) || !spec.Mask.Has(q.Columns[1].Bit) {
		t.Errorf("ColSpec.Mask = %b, missing one of bits %d and %d",
			spec.Mask, q.Columns[0].Bit, q.Columns[1].Bit)
	}
	if spec.Mask.Count() != 2 {
		t.Errorf("ColSpec.Mask has %d bits set, want 2", spec.Mask.Count())
	}
	if !e.Contributes(q.ID) {
		t.Error("Contributes = false for a destination this code supplies")
	}

	// And a destination it does not contribute to has width zero, which is the
	// test the decode path uses rather than consulting the mask.
	other := dests.ByName("MD_MARKET_STATUS")
	if e.Contributes(other.ID) {
		t.Error("Contributes = true for a destination this code does not supply")
	}
}

// TestBuildRegistryReportsEveryProblem is section 8.2's requirement: "an error
// naming every problem, not the first one -- a config author fixing one name at
// a time is how a five-minute change becomes an afternoon".
func TestBuildRegistryReportsEveryProblem(t *testing.T) {
	withCatalogue(t, fake(7208))

	specs := []TranscodeSpec{
		// No decoder: V-T1.
		{Code: 7305, Class: model.ClassRateLimited},
		// A decoder, but three bad contributions.
		{Code: 7208, Class: model.ClassRateLimited, Contributions: []ContribSpec{
			{Destination: "MD_NOT_A_DESTINATION", Columns: []string{"x"}},
			{Destination: "MD_MARKET_STATUS", Columns: []string{"y"}},
			{Destination: "MD_TOKEN_QUOTE", Columns: []string{"not_a_column"}},
		}},
	}

	_, err := BuildRegistry(specs, testDests(t), NopCounters{})
	if err == nil {
		t.Fatal("an inventory with four distinct problems was accepted")
	}

	var be *BuildError
	if !errors.As(err, &be) {
		t.Fatalf("error is %T, want *BuildError", err)
	}
	for _, want := range []ProblemKind{
		ProblemNoDecoder,          // 7305 has none
		ProblemUnknownDestination, // MD_NOT_A_DESTINATION
		ProblemNotAContributor,    // 7208 is not a contributor to MD_MARKET_STATUS
		ProblemUnknownColumn,      // not_a_column
	} {
		if !hasKind(err, want) {
			t.Errorf("problem kind %d was not reported; got %d problems: %v",
				want, len(be.Problems), err)
		}
	}
	if len(be.Problems) < 4 {
		t.Errorf("reported %d problems, want at least 4 -- every problem, not the first",
			len(be.Problems))
	}
}

func TestBuildRegistryRejectsADuplicateColumn(t *testing.T) {
	withCatalogue(t, fake(7208))
	dests := testDests(t)
	name := dests.ByName("MD_TOKEN_QUOTE").Columns[0].Name()

	_, err := BuildRegistry(specsWithContribution(t, 7208, "MD_TOKEN_QUOTE", name, name), dests, NopCounters{})
	if err == nil {
		t.Fatal("the same column contributed twice was accepted")
	}
	if !hasKind(err, ProblemDuplicateColumn) {
		t.Errorf("error = %v, want a ProblemDuplicateColumn", err)
	}
}

func TestBuildRegistryAcceptsAnEmptyInventory(t *testing.T) {
	// The honest state of this build until P2-06: nothing registered, nothing
	// configured, and that is not an error. A relay with no decoders still
	// frames, resolves and counts -- which is exactly what P2-05 delivers.
	withCatalogue(t)

	reg, err := BuildRegistry(nil, testDests(t), NopCounters{})
	if err != nil {
		t.Fatalf("an empty inventory was rejected: %v", err)
	}
	if reg.Len() != 0 {
		t.Errorf("registry holds %d codes, want 0", reg.Len())
	}
	if e := reg.Lookup(7208); e != nil {
		t.Error("an empty registry resolved a code")
	}
}

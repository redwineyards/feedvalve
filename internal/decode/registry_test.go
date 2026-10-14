package decode

import (
	"testing"

	"feedvalve/internal/model"
)

// The catalogue and the registry: Vol 1 section 8.1's two levels, and section
// 8.3's dispatch.
//
// Every test here registers its OWN decoders. The catalogue is a package-level
// slice populated from init functions, so a test that appended to it would
// leak into the next one -- and the decoders those inits will add in P2-06
// would make these assertions depend on how many codes the build happens to
// contain. withCatalogue swaps it out and restores it, which keeps each test
// independent of both.

// fakeDecoder is a Decoder that decodes nothing. The registry's job is
// resolution, not decoding, so the units it would produce are irrelevant here;
// what matters is that each one reports a distinct code.
type fakeDecoder struct {
	code     model.TransCode
	bodyLen  int
	maxUnits int
	decode   func(model.MsgHeader, []byte, []model.UpdateUnit) (int, error)

	// What the factory was handed, so a test can assert the construction
	// seam passes them through.
	cnt   Counters
	entry *RegistryEntry
}

func (f fakeDecoder) Code() model.TransCode { return f.code }
func (f fakeDecoder) BodyLen() int          { return f.bodyLen }
func (f fakeDecoder) MaxUnits() int         { return f.maxUnits }

func (f fakeDecoder) Decode(h model.MsgHeader, p []byte, out []model.UpdateUnit) (int, error) {
	if f.decode != nil {
		return f.decode(h, p, out)
	}
	return 0, nil
}

func fake(code model.TransCode) fakeDecoder {
	return fakeDecoder{code: code, bodyLen: 0, maxUnits: 1}
}

// factory returns the constructor the catalogue holds. The closure captures a
// copy, so each registry gets its own instance -- which is the lifetime the
// real decoders have.
func (f fakeDecoder) factory() func(Counters, *RegistryEntry) Decoder {
	return func(c Counters, e *RegistryEntry) Decoder {
		inst := f
		inst.cnt = c
		inst.entry = e
		return inst
	}
}

// withCatalogue replaces the compile-time catalogue for one test.
//
// It registers FACTORIES, like the real per-code files do, so a test exercises
// the same construction path production uses rather than a shortcut that
// bypasses it.
func withCatalogue(t *testing.T, decs ...fakeDecoder) {
	t.Helper()
	saved := catalogue
	catalogue = nil
	for _, d := range decs {
		register(d.code, d.factory())
	}
	t.Cleanup(func() { catalogue = saved })
}

func TestCatalogueIsSortedAndComplete(t *testing.T) {
	// Registered out of order on purpose: the catalogue must report ascending
	// regardless, because -selfcheck prints it and two runs of the same binary
	// have to produce identical output for a diff of them to mean anything.
	withCatalogue(t, fake(7305), fake(6541), fake(7208))

	got := Catalogue()
	want := []model.TransCode{6541, 7208, 7305}
	if len(got) != len(want) {
		t.Fatalf("Catalogue() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Catalogue() = %v, want ascending %v", got, want)
		}
	}

	for _, code := range want {
		if !CanDecode(code) {
			t.Errorf("CanDecode(%d) = false for a registered code", code)
		}
	}
	// The level-1 test that makes unconfigured_transcode distinguishable from
	// unknown_transcode (ADR-011).
	if CanDecode(9999) {
		t.Error("CanDecode(9999) = true for a code nothing registered")
	}
}

// TestRegisterPanicsOnADuplicate pins the one thing register must not do
// quietly.
//
// Every register call runs from an init before main, so a duplicate is a
// programming error discovered at process start. Keeping one of the two
// silently would mean the binary decodes a transcode with whichever decoder
// happened to LINK FIRST -- a difference invisible in the source and dependent
// on file order, which is the worst kind of difference to debug.
func TestRegisterPanicsOnADuplicate(t *testing.T) {
	withCatalogue(t, fake(7208))

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("registering a duplicate code did not panic")
		}
		if msg, ok := r.(string); ok {
			if !contains(msg, "7208") {
				t.Errorf("the panic does not name the duplicate code: %q", msg)
			}
		}
	}()
	dup := fake(7208)
	register(dup.code, dup.factory())
}

func TestLookupResolvesAndMisses(t *testing.T) {
	withCatalogue(t, fake(7208), fake(7305))
	reg := buildForTest(t, 7208, 7305)

	for _, code := range []model.TransCode{7208, 7305} {
		e := reg.Lookup(code)
		if e == nil {
			t.Fatalf("Lookup(%d) = nil for a configured code", code)
		}
		if e.Code != code {
			t.Errorf("Lookup(%d) returned the entry for %d", code, e.Code)
		}
		if e.Dec.Code() != code {
			t.Errorf("Lookup(%d) carries a decoder for %d", code, e.Dec.Code())
		}
	}

	// A miss is nil rather than an error: the dispatcher counts and logs it at
	// a bounded rate (ADR-011), because an unknown code is a protocol
	// observation and not a failure of ours.
	for _, code := range []model.TransCode{0, 7200, 6541, 65535} {
		if e := reg.Lookup(code); e != nil {
			t.Errorf("Lookup(%d) = %v for an unconfigured code", code, e.Code)
		}
	}
}

// TestLookupSlotMatchesTheCounterIndex is the invariant Vol 1 section 7.7
// relies on: "reusing the counter layer's slot index is what keeps
// per-transcode from needing a map on the receive path".
//
// The registry's codes and obs's {code} dimension are both the configured
// inventory in ascending order, so index i means the same code in both. If they
// diverged, a correct number would be rendered against the wrong transaction
// code -- which reads as real and is therefore worse than a missing counter.
func TestLookupSlotMatchesTheCounterIndex(t *testing.T) {
	withCatalogue(t, fake(6541), fake(7208), fake(7305))
	reg := buildForTest(t, 7305, 6541, 7208) // configured out of order

	codes := reg.Codes()
	want := []model.TransCode{6541, 7208, 7305}
	for i := range want {
		if codes[i] != want[i] {
			t.Fatalf("Codes() = %v, want ascending %v", codes, want)
		}
		if got := reg.Slot(want[i]); got != i {
			t.Errorf("Slot(%d) = %d, want %d -- the slot IS the counter's label index",
				want[i], got, i)
		}
	}
	if got := reg.Slot(9999); got != -1 {
		t.Errorf("Slot(9999) = %d, want -1", got)
	}
}

func TestRegistryHoldsItsBound(t *testing.T) {
	// maxRegistered codes, which is the compile-time bound, and one more than
	// that must be refused rather than overrun the array.
	decs := make([]fakeDecoder, 0, maxRegistered+1)
	codes := make([]int, 0, maxRegistered+1)
	for i := range maxRegistered + 1 {
		code := model.TransCode(7000 + i)
		decs = append(decs, fake(code))
		codes = append(codes, int(code))
	}
	withCatalogue(t, decs...)

	if _, err := BuildRegistry(specsFor(codes[:maxRegistered]...), testDests(t), NopCounters{}); err != nil {
		t.Fatalf("exactly maxRegistered (%d) transcodes was rejected: %v", maxRegistered, err)
	}

	_, err := BuildRegistry(specsFor(codes...), testDests(t), NopCounters{})
	if err == nil {
		t.Fatalf("%d transcodes was accepted; the registry holds %d", len(codes), maxRegistered)
	}
	if !hasKind(err, ProblemTooManyTranscodes) {
		t.Errorf("error = %v, want a ProblemTooManyTranscodes", err)
	}
}

func TestLookupAllocatesNothing(t *testing.T) {
	withCatalogue(t, fake(7208), fake(7305))
	reg := buildForTest(t, 7208, 7305)

	if n := testing.AllocsPerRun(1000, func() {
		_ = reg.Lookup(7208) // a hit, late in the scan
		_ = reg.Lookup(9999) // a miss, the whole scan
		_ = reg.Slot(7305)
	}); n != 0 {
		t.Fatalf("dispatch allocates %v times, want 0", n)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

package decode

import (
	"slices"
	"strconv"

	"feedvalve/internal/model"
)

// Level 1 of Vol 1 section 8.1: the catalogue. Compile-time, immutable, every
// Decoder the binary contains.
//
// The two levels have to be distinguishable (ADR-011), because "we can decode
// this but nobody told us what to do with it" is a configuration mistake an
// operator can fix in a minute, while "we have never seen this code" is a
// protocol change that needs the exchange. One counter for both would make the
// first invisible inside the second:
//
//	code in neither -> unknown_transcode{code}++
//	code in L1 only -> unconfigured_transcode{code}++
//	code in both    -> decode
//
// Registration is from an init() in each per-code file, so adding a transcode
// is ONE NEW FILE plus one init -- never an edit to a central switch, and never
// an edit to a decoder that already works. That is the open/closed property
// NFR11 and ADR-012 ask for, and it is why this is a package-level slice
// rather than a constructor argument threaded through app.
//
// # Constructors, not values
//
// The catalogue holds a FACTORY per code rather than a ready decoder, which is
// what section 8.2's heading ("Registration and construction") separates.
// P2-05 registered values; P2-06 needed decoders to carry their Counters, and
// those are not knowable at init -- they are resolved from the instrument
// registry at startup step 9, after every init has run. The alternatives were a
// package-level counter variable (global mutable state on the one path that
// must stay testable in isolation) or a wider Decode signature, which Vol 0
// section 3.9 fixes.
//
// One consequence worth stating: a decoder instance belongs to ONE registry. A
// reload builds a new registry and therefore new decoder instances, which is
// the same lifetime the ColSpecs already have.
var catalogue []entry

// entry is one catalogued code and the constructor that makes its decoder.
type entry struct {
	code model.TransCode
	make func(Counters, *RegistryEntry) Decoder
}

// register adds a code and its constructor to the compile-time catalogue.
//
// Panics on a duplicate code, and that is deliberate: every register call runs
// from an init before main, so a duplicate is a programming error discovered at
// process start rather than a runtime condition. Silently keeping one of the
// two would mean the binary decodes a transcode with whichever decoder happened
// to link first -- a difference invisible in the source and dependent on file
// order.
//
// The code is passed explicitly rather than read from a constructed decoder, so
// registration needs no Counters and the catalogue can be listed without
// building anything.
func register(code model.TransCode, make func(Counters, *RegistryEntry) Decoder) {
	for _, existing := range catalogue {
		if existing.code == code {
			panic("decode: transaction code " + strconv.Itoa(int(code)) +
				" is registered twice; each code has exactly one decoder")
		}
	}
	catalogue = append(catalogue, entry{code: code, make: make})
}

// Catalogue returns every code this binary can decode, sorted ascending.
//
// Sorted, and the order matters beyond tidiness: -selfcheck and the startup
// INFO line both print it, so "which codes does this build know?" is
// answerable from a log rather than from the source tree -- and a stable order
// means two runs of -selfcheck on the same binary produce identical output,
// which is what makes a diff of them meaningful.
func Catalogue() []model.TransCode {
	out := make([]model.TransCode, 0, len(catalogue))
	for _, e := range catalogue {
		out = append(out, e.code)
	}
	slices.Sort(out)
	return out
}

// catalogueLookup returns the registered decoder for a code, or nil.
//
// A linear scan over at most a few dozen entries, called at config load and by
// -selfcheck, never per message -- the per-message lookup is Registry.Lookup.
func catalogueLookup(code model.TransCode) func(Counters, *RegistryEntry) Decoder {
	for _, e := range catalogue {
		if e.code == code {
			return e.make
		}
	}
	return nil
}

// CanDecode reports whether the binary contains a decoder for code, regardless
// of whether the configuration asked for it. This is the level-1 test that
// makes unconfigured_transcode distinguishable from unknown_transcode.
func CanDecode(code model.TransCode) bool { return catalogueLookup(code) != nil }

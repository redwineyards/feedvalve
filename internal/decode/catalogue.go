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
var catalogue []Decoder

// register adds d to the compile-time catalogue.
//
// Panics on a duplicate code, and that is deliberate: every register call runs
// from an init before main, so a duplicate is a programming error discovered at
// process start rather than a runtime condition. Silently keeping one of the
// two would mean the binary decodes a transcode with whichever decoder happened
// to link first -- a difference invisible in the source and dependent on file
// order.
func register(d Decoder) {
	code := d.Code()
	for _, existing := range catalogue {
		if existing.Code() == code {
			panic("decode: transaction code " + strconv.Itoa(int(code)) +
				" is registered twice; each code has exactly one decoder")
		}
	}
	catalogue = append(catalogue, d)
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
	for _, d := range catalogue {
		out = append(out, d.Code())
	}
	slices.Sort(out)
	return out
}

// catalogueLookup returns the registered decoder for a code, or nil.
//
// A linear scan over at most a few dozen entries, called at config load and by
// -selfcheck, never per message -- the per-message lookup is Registry.Lookup.
func catalogueLookup(code model.TransCode) Decoder {
	for _, d := range catalogue {
		if d.Code() == code {
			return d
		}
	}
	return nil
}

// CanDecode reports whether the binary contains a decoder for code, regardless
// of whether the configuration asked for it. This is the level-1 test that
// makes unconfigured_transcode distinguishable from unknown_transcode.
func CanDecode(code model.TransCode) bool { return catalogueLookup(code) != nil }

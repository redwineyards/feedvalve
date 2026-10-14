package decode

import (
	"feedvalve/internal/model"
)

// maxRegistered bounds the registry.
//
// 32, and the number is derived rather than round: the bible catalogues 21
// broadcast codes (section 4.0) and the 6511 family accounts for five of those
// as one structure, so 32 holds the whole catalogue plus growth. It is a
// compile-time bound because the arrays below are values inside the Registry:
// nothing here is a pointer the collector has to trace.
const maxRegistered = 32

// The two-sided layout gate for it (G-LAY-1; the idiom and the rule for
// choosing between the two forms are in layout.go). EXACT rather than a bound,
// because 32 is not "at least as big as the catalogue" -- it is a chosen size
// whose reasoning is written above, and changing it should require changing
// this line too.
//
// In this file rather than layout.go so the compile error names the file whose
// constant moved, which is what the mutation harness requires.
const registeredHeadroom = maxRegistered - 32

const _ = uint(registeredHeadroom) + uint(-registeredHeadroom)

// Registry is level 2 of section 8.1: the configured inventory, resolved
// against the catalogue and against the destination table, with contributor
// slots and ColSpecs precomputed so the decode path does no lookups.
//
// AN INTERFACE, as Vol 0 section 3.9 declares it, though section 8.2's
// BuildRegistry signature returns a *Registry pointer and the two therefore
// contradict each other. The interface wins, and the deciding argument is a
// test double rather than taste: the recover boundary that gate G-FRM-8
// exercises lives in app, and the only way for app's test to feed it a decoder
// that panics is to supply its own registry. Without the interface that test
// would need register() exported, which would let any package add to a
// catalogue whose whole point is that it is populated by one file per code.
//
// CLAUDE.md forbids an interface with one implementation AND no test double;
// this is the case it carves out.
//
// Lookup returns a pointer INTO an immutable array that outlives every caller,
// which the interface does not change. Nothing mutates a published registry: a
// reload builds a new one and swaps the snapshot pointer (ADR-L11), which is
// what lets the decoder hold one for a whole datagram without a lock.
type Registry interface {
	// Lookup resolves a code to its entry, or nil for one that is not
	// configured.
	Lookup(code model.TransCode) *RegistryEntry

	// Slot is the index of a code within the configured inventory, or -1. It
	// is the same index obs assigns that code's {code} label.
	Slot(code model.TransCode) int

	// Codes is the configured inventory, ascending.
	Codes() []model.TransCode

	// Len is how many codes are configured.
	Len() int
}

// table is the one production implementation: section 8.3's parallel arrays.
type table struct {
	// Parallel arrays, scanned linearly. codes is uint16 rather than
	// model.TransCode so the scan reads one cache line: 32 uint16s is 64
	// bytes, which is the whole comparison set in a single fetch.
	codes   [maxRegistered]uint16
	entries [maxRegistered]RegistryEntry
	n       int
}

// Lookup resolves a transaction code to its entry, or nil.
//
// Runs once per message on the receive goroutine at 7,500 to 10,000 a second.
// Three implementations were considered (section 8.3):
//
//	map[TransCode]*RegistryEntry   a hash, a bucket probe and a pointer chase,
//	                               plus non-deterministic iteration order that
//	                               complicates -selfcheck output. A map to hold
//	                               nine entries is a reflex, not a decision.
//	[65536]*RegistryEntry          O(1), and 512 KB of pointers the garbage
//	                               collector traces forever to dispatch nine
//	                               codes. ADR-006's whole point is not to hand
//	                               the collector work on this path.
//	linear scan over uint16        at most 32 comparisons in one cache line,
//	                               values not pointers, nothing to trace,
//	                               deterministic order. CHOSEN.
//
// The returned pointer is into an immutable array that outlives every caller,
// so it is safe to hold for the duration of a message.
//
// Ordering the scan by observed frequency so 7208 comes first was considered
// and rejected: it would save perhaps three comparisons on a path that has
// just spent ~15 microseconds decompressing, and it would make the scan order
// depend on traffic. codes stays sorted ascending so -selfcheck output and log
// lines are stable.
func (r *table) Lookup(code model.TransCode) *RegistryEntry {
	c := uint16(code)
	for i := range r.n {
		if r.codes[i] == c {
			return &r.entries[i]
		}
	}
	return nil
}

// Codes returns the configured codes, ascending. Allocates, and is called by
// -selfcheck and the startup line, never per message.
func (r *table) Codes() []model.TransCode {
	out := make([]model.TransCode, 0, r.n)
	for i := range r.n {
		out = append(out, model.TransCode(r.codes[i]))
	}
	return out
}

// Len is how many transcodes are configured.
func (r *table) Len() int { return r.n }

// Slot is the index of a code within the configured inventory, or -1.
//
// It is the SAME index obs assigns that code's {code} label, because both are
// the configured inventory in ascending order (Vol 1 section 7.7: "reusing the
// counter layer's slot index is what keeps per-transcode from needing a map on
// the receive path"). The agreement is load-bearing and asserted by a test
// rather than trusted: a mismatch would render a correct number against the
// wrong transaction code, which reads as real.
func (r *table) Slot(code model.TransCode) int {
	c := uint16(code)
	for i := range r.n {
		if r.codes[i] == c {
			return i
		}
	}
	return -1
}

// add appends one resolved entry. Called only by BuildRegistry, in ascending
// code order, so the scan order is the sorted order.
func (r *table) add(e RegistryEntry) {
	r.codes[r.n] = uint16(e.Code)
	r.entries[r.n] = e
	r.n++
}

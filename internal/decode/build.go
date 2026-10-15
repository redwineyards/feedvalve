package decode

import (
	"fmt"
	"slices"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
)

// Building the registry: level 2 of Vol 1 section 8.1.
//
// # Why this does not take a *config.Config
//
// Vol 1 section 8.2 writes the signature as
// BuildRegistry(cfg *config.Config, dests *destinations.Table), and that
// signature cannot be implemented: config carries oradb.DBSettings, so
// importing config here would make decode reach database/sql and go-ora
// transitively -- which Vol 0 section 1.3 forbids for every package on the
// receive path, and which gate G-DEP-1 machine-checks. The contradiction is
// between two parts of the contract, and the import rule is the one that is
// enforced and the one with the stronger argument: "importing oradb into decode
// just for the error type re-couples the receive path to the database".
//
// So the inventory arrives as a projection, exactly as obs takes obs.Options
// rather than a *config.Config for the same reason (internal/app/instruments.go
// explains that translation). app is the package allowed to see both halves, so
// app does the projecting.
//
// fmt is imported for CONFIG-LOAD-TIME messages only; nothing here runs per
// message. The decode path's rule is on the Decoder interface and is absolute.

// TranscodeSpec is one configured transaction code, projected from
// config.TranscodeConfig by app.
type TranscodeSpec struct {
	Code          int
	Class         model.Class
	Contributions []ContribSpec
}

// ContribSpec names the columns one transcode supplies to one destination.
type ContribSpec struct {
	Destination string
	Columns     []string
}

// ProblemKind classifies what BuildRegistry rejected.
//
// A KIND rather than a rule identifier string, because the V- identifiers are
// config's taxonomy (config/problem.go) and decode cannot import config. app
// maps these onto config.RuleDecoderExists and friends, which keeps each
// identifier declared exactly once -- the alternative is the same string
// spelled in two packages, which drift makes silently inconsistent.
type ProblemKind uint8

const (
	// ProblemNoDecoder is V-T1: a configured transcode this build cannot
	// decode. A deployment defect, not a runtime condition -- the process
	// would start, look healthy, and store nothing for that code.
	ProblemNoDecoder ProblemKind = iota
	// ProblemUnknownDestination is a contribution naming a destination the
	// table does not declare.
	ProblemUnknownDestination
	// ProblemNotAContributor is a transcode contributing to a destination
	// whose contributor list does not name it -- which means no slot, and the
	// slot is what indexes every stored ordering value.
	ProblemNotAContributor
	// ProblemUnknownColumn is a contributed column the destination does not
	// declare.
	ProblemUnknownColumn
	// ProblemDuplicateColumn is the same column contributed twice.
	ProblemDuplicateColumn
	// ProblemColSpecWidth is V-T6: a ColSpec whose width disagrees with the
	// sum of its mask's declared widths.
	ProblemColSpecWidth
	// ProblemTooManyTranscodes is more configured codes than the registry's
	// compile-time bound.
	ProblemTooManyTranscodes
)

// Problem is one rejection, with a message in the operator's terms.
type Problem struct {
	Kind ProblemKind
	Msg  string
}

// BuildError is every problem the inventory had, never just the first: a
// config author fixing one name at a time is how a five-minute change becomes
// an afternoon.
type BuildError struct {
	Problems []Problem
}

func (e *BuildError) Error() string {
	if len(e.Problems) == 1 {
		return "decoder registry: " + e.Problems[0].Msg
	}
	msg := fmt.Sprintf("decoder registry rejected the inventory (%d problems):", len(e.Problems))
	for _, p := range e.Problems {
		msg += "\n  - " + p.Msg
	}
	return msg
}

// BuildRegistry resolves the configured inventory against the catalogue and
// the destination table, assigning each transcode its contributor slot per
// destination and precomputing its ColSpec.
func BuildRegistry(specs []TranscodeSpec, dests *destinations.Set, cnt Counters) (Registry, error) {
	var p problems
	reg := &table{}

	if dests == nil {
		p.add(ProblemUnknownDestination, "a registry needs a destination table")
		return nil, p.err()
	}
	if cnt == nil {
		// A nil sink would panic on the first clamp, which is the one moment
		// the counter matters. Tests that do not care pass NopCounters.
		cnt = NopCounters{}
	}
	if len(specs) > maxRegistered {
		p.add(ProblemTooManyTranscodes, "%d transcodes are configured, more than the %d the "+
			"registry holds. maxRegistered is a compile-time bound sized from the bible's "+
			"catalogue of 21 broadcast codes; raising it is a deliberate edit, not a "+
			"configuration change", len(specs), maxRegistered)
		return nil, p.err()
	}

	// Ascending code, so the scan order, -selfcheck's output, every log line
	// that prints the inventory, AND obs's {code} label index all agree. The
	// last of those is load-bearing: Registry.Slot is the index the
	// per-transcode counters use.
	ordered := slices.Clone(specs)
	slices.SortFunc(ordered, func(a, b TranscodeSpec) int { return a.Code - b.Code })

	for _, spec := range ordered {
		tc := model.TransCode(spec.Code)

		make := catalogueLookup(tc)
		if make == nil {
			p.add(ProblemNoDecoder, "transcode %d is configured but this build has no decoder "+
				"for it. Registered codes: %v", spec.Code, Catalogue())
			continue
		}

		// The entry goes into its final slot FIRST, then its contributions are
		// resolved, and only then is the decoder constructed -- with a pointer
		// to that slot. The order is what makes the pointer stable: a decoder
		// handed a pointer to a local would read ColSpecs that were copied away
		// from under it, and the registry is immutable afterwards, so the slot
		// outlives every caller (Vol 1 section 10.2: the decoder reads the
		// constants the registry resolved).
		reg.add(RegistryEntry{Code: tc, Class: spec.Class})
		e := &reg.entries[reg.n-1]
		resolveContributions(&p, e, spec, dests)
		e.Dec = make(cnt, e)

		// The trace capability, resolved once here so the dispatcher never
		// asserts on the receive path (see RegistryEntry.Trace).
		if tr, ok := e.Dec.(Traceable); ok {
			e.Trace = tr
		}
	}

	if err := p.err(); err != nil {
		return nil, err
	}
	return reg, nil
}

// resolveContributions fills one entry's Slots and Contrib from the projected
// inventory and the destination table.
//
// Everything here REUSES what P1-04 built rather than recomputing it:
// Set.ByName for the destination, Destination.Slot for the contributor slot --
// assigned by the destinations loader, ascending code within a destination --
// and Destination.Columns for the bit index and the declared width.
// Recomputing a slot here would create a second answer to a question that
// already has one, and the slot indexes stored ordering state.
func resolveContributions(p *problems, entry *RegistryEntry, spec TranscodeSpec,
	dests *destinations.Set) {

	for j, con := range spec.Contributions {
		dest := dests.ByName(con.Destination)
		if dest == nil {
			p.add(ProblemUnknownDestination, "transcode %d contributions[%d] names destination "+
				"%q, which is not declared", entry.Code, j, con.Destination)
			continue
		}

		slot, ok := dest.Slot(entry.Code)
		if !ok {
			p.add(ProblemNotAContributor, "transcode %d contributes to %s but is not one of its "+
				"declared contributors. The contributor list fixes the slot that indexes every "+
				"stored ordering value, which is why it is startup-only and must name every "+
				"writer", entry.Code, dest.Name)
			continue
		}

		var spec ColSpec
		for _, name := range con.Columns {
			col := columnByName(dest, name)
			if col == nil {
				p.add(ProblemUnknownColumn, "transcode %d contributes column %q to %s, which "+
					"does not declare it", entry.Code, name, dest.Name)
				continue
			}
			if spec.Mask.Has(col.Bit) {
				p.add(ProblemDuplicateColumn, "transcode %d declares column %q of %s twice",
					entry.Code, name, dest.Name)
				continue
			}
			spec.Mask = spec.Mask.Set(col.Bit)
			spec.Len[col.Bit] = col.Width
		}

		// The packed offsets, in ASCENDING BIT ORDER over the mask that was
		// actually declared (Vol 0 section 3.4). Computed in a second pass
		// because the first accepts columns in whatever order the
		// configuration lists them, while the packing order is the bit order
		// -- and Width falls out of the same walk, so the two cannot disagree.
		spec.Width = packOffsets(&spec)

		// V-T6. The mask and the width are accumulated in the same loop from
		// the same columns, and then checked against an independent sum: a
		// Width maintained separately from its Mask is a number that drifts,
		// and the decode path bound-checks its blob write against it in one
		// comparison.
		if got := maskWidth(dest, spec.Mask); got != spec.Width {
			p.add(ProblemColSpecWidth, "transcode %d ColSpec for %s has width %d but its mask's "+
				"declared widths sum to %d", entry.Code, dest.Name, spec.Width, got)
		}
		if spec.Mask&^dest.FullMask() != 0 {
			p.add(ProblemUnknownColumn, "transcode %d ColSpec for %s names a column bit the "+
				"destination has not declared", entry.Code, dest.Name)
		}

		entry.Slots[dest.ID] = slot
		entry.Contrib[dest.ID] = spec
	}
}

// packOffsets fills spec.Off from spec.Mask and spec.Len, and returns the total
// packed width.
//
// Ascending bit order, no padding: bit b's offset is the sum of the widths of
// every lower set bit (Vol 0 section 3.4). This is the one place that order is
// implemented, and the merge on the shard side walks the identical order, which
// is what makes the blob a shared format rather than a convention.
func packOffsets(spec *ColSpec) uint16 {
	var off uint16
	for bit := range uint8(model.MaxColumns) {
		if !spec.Mask.Has(bit) {
			continue
		}
		spec.Off[bit] = off
		off += spec.Len[bit]
	}
	return off
}

// columnByName finds a declared column by its field name.
func columnByName(dest *destinations.Destination, name string) *destinations.Column {
	for i := range dest.Columns {
		if dest.Columns[i].Name() == name {
			return &dest.Columns[i]
		}
	}
	return nil
}

// maskWidth sums the declared widths of a mask's set bits -- V-T6's right-hand
// side, computed independently of ColSpec.Width so the check is a check rather
// than a tautology.
func maskWidth(dest *destinations.Destination, mask model.ColMask) uint16 {
	var w uint16
	for i := range dest.Columns {
		if mask.Has(dest.Columns[i].Bit) {
			w += dest.Columns[i].Width
		}
	}
	return w
}

// problems accumulates rejections.
type problems []Problem

func (p *problems) add(kind ProblemKind, format string, a ...any) {
	*p = append(*p, Problem{Kind: kind, Msg: fmt.Sprintf(format, a...)})
}

func (p *problems) err() error {
	if len(*p) == 0 {
		return nil
	}
	return &BuildError{Problems: *p}
}

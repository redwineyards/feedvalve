package mirror

import "feedvalve/internal/model"

// verdict is the ordering guard's decision on one contribution (Vol 2 section
// 5.3). Apply and Bypass both proceed to the merge; they differ only in whether
// the comparison was counted. Zero is not a verdict, so an unset one is never
// mistaken for Apply.
type verdict uint8

const (
	verdictApply     verdict = iota + 1 // newer: merge it
	verdictSupersede                    // older or a re-delivery: touch nothing
	verdictBypass                       // nothing to compare, or the guard is off: merge it, count nothing
)

// guardOff is steps 1-2 of Vol 2 section 5.3, evaluated by the caller ONCE per
// unit and passed to guard for each of the unit's contributions (section 5.3,
// "Cost": "one atomic load, hoisted once per unit and reused across its <= 3
// contributions"). Hoisting is also what makes the unit's contributions agree:
// an evaluator flipping g.off between two of them cannot judge one unit two
// ways.
//
// mode is the loaded ordering.guard; g is u.Code's GuardTable entry, looked up
// once per unit as well. A nil g is a code with no table entry, which only a
// wiring defect can produce; it reads as off, since the guard cannot judge it.
func guardOff(mode GuardMode, g *guardEntry) bool {
	return mode == GuardOff || // 1. ordering.guard == off
		g == nil || g.off.Load() // 2. GuardOff[u.Code], set one-way by the evaluator
}

// guard decides whether contribution c of unit u may be applied to entry e
// (Vol 2 section 5.3), evaluated strictly top to bottom, first match wins.
//
// The guard is a defence, not the ordering mechanism (section 5.1): ordering is
// structural (ADR-007), and every ambiguous case resolves to Apply, because a
// wrong Supersede freezes a row silently while a wrong Apply costs one duplicate
// write.
//
// off is guardOff(mode, g) -- steps 1-2, hoisted by the caller once per unit --
// and g is u.Code's GuardTable entry, also looked up once per unit. Section
// 5.3's signature also takes the destination; nothing here depends on it ("no
// branch that depends on the destination"), so it is not a parameter.
//
// Counts guard_offered and guard_superseded for u.Code. The unlabelled
// units_superseded_out_of_order is the merge's, counted at section 4 step 2 on
// this function's Supersede, so it is not counted twice (section 6 erratum to
// V2:1267).
//
// Reads nothing outside its arguments; writes nothing but the two counters.
// Does not touch e.
func guard(off bool, g *guardEntry, e *model.Entry, c *model.Contribution, u *model.UpdateUnit) verdict {
	// 1-4: the bypasses, none of them counted.
	switch {
	case off, // 1-2. ordering.guard == off, or GuardOff[u.Code] (guardOff)
		u.Flags.Has(model.FlagOrderingUnusable), // 3. the decoder found no usable ordering value
		!e.SeenSlot(c.Slot):                     // 4. a fresh entry or slot: nothing to compare (section 5.4)
		return verdictBypass
	}

	// 5. The denominator starts here and only here (section 5.4).
	g.offered.Add(1)

	v := compareOrdering(e, c.Slot, u)
	if v == verdictSupersede {
		g.superseded.Add(1)
	}
	return v
}

// compareOrdering is steps 6-8: u's ordering value against slot's history.
//
// The two comparison rules are different and not interchangeable (Vol 0 section
// 10.3): BCSeqNo is a wrapping counter, compared SERIALLY as int32(a-b), so a
// wrap from 0x7FFFFFFF to 0x80000000 reads as newer (ASM-11); LogTime is a
// timestamp, compared PLAINLY as signed seconds. RecvMono is the final
// tiebreak for both scopes, and it is strictly greater-than: an equal value is
// the same unit re-delivered.
func compareOrdering(e *model.Entry, slot uint8, u *model.UpdateUnit) verdict {
	// 6. Primary: BCSeqNo for a token-scoped unit. A general unit (FR8) has
	// only LogTime, which is step 7's comparison, so it starts there.
	if !u.General() {
		// int32 subtraction wraps in Go, so this is serial-number arithmetic.
		if d := u.Seq - e.LastSeq[slot]; d > 0 {
			return verdictApply
		} else if d < 0 {
			return verdictSupersede
		}
	}

	// 7. LogTime: the general primary, and the token-scoped secondary.
	if u.LogTime > e.LastTime[slot] {
		return verdictApply
	}
	if u.LogTime < e.LastTime[slot] {
		return verdictSupersede
	}

	// 8. The receipt monotonic clock, both scopes.
	if u.RecvMono > e.LastRecv[slot] {
		return verdictApply
	}
	return verdictSupersede
}

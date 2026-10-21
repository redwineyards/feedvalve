package mirror

import (
	"math/bits"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// merge applies one UpdateUnit to this shard's mirrors (Vol 2 section 4). It is
// called only from the main loop's unit arm and runs to completion before the
// loop returns to select, which is what makes ADR-007 mechanism 1 -- exactly
// one goroutine applies a destination row's contributions, in wire arrival
// order -- a property of the code rather than an aspiration.
//
// merge never blocks, never allocates in steady state (a new row's arena chunk
// and an Index insert aside), and touches no state outside this shard except
// FlushBus.Trigger at step 9 and the shared atomic counters.
//
// u.Contribs is indexed by destination, and a zero Mask means "this unit does
// not touch d" (Vol 0 section 3.4), so steps 1-8 run once per present
// contribution and steps 9-10 once per unit. INV-MASK (ColMask is only ever
// OR-ed here; pump clears it) and INV-STATE (Dirty && InFlight never both
// true; ReDirty implies InFlight) hold after every step.
func (s *Shard) merge(u model.UpdateUnit) {
	// Steps 1-2 of the guard, and u.Code's configured position, resolved once
	// per unit and shared by its contributions (section 5.3, "Cost").
	pos := s.guards.index(u.Code)
	var g *guardEntry
	if pos >= 0 {
		g = &s.guards.entries[pos]
	}
	off := guardOff(s.guard, g)
	nonRateLimited := u.Flags.Has(model.FlagNonRateLimited)

	dirtied := false   // step 9's extra condition
	var applied uint64 // contributions whose fields reached the image (step 10)

	for i := range u.Contribs {
		c := &u.Contribs[i]
		if !c.Present() {
			continue
		}
		d := model.DestID(i)
		dst := s.dst.ByID(d)

		// Step 1 -- resolve or create the entry. A new one is all-zero
		// (INV-ZERO), so its SlotSeen is clear and step 2 bypasses: the "entry
		// was created by THIS contribution" case of section 5.3 needs no flag.
		slot, ok := s.Index[d][c.Row]
		if !ok {
			slot = s.allocEntry(d)
			s.entry(d, slot).Row = c.Row
			s.Index[d][c.Row] = slot
			s.inst.RowsCreated[d].Add(1)
		}
		e := s.entry(d, slot) // stable for the rest of the merge (INV-SLOT)

		// Step 2 -- the ordering guard. A Supersede touches NOTHING; this is
		// the only place units_superseded_out_of_order is counted (the guard
		// counts only guard_superseded -- section 6 erratum to V2:1267).
		if guard(off, g, e, c, &u) == verdictSupersede {
			s.inst.UnitsSupersededOutOfOrder.Add(1)
			continue
		}

		// Step 4's validation, run ahead of step 3: a contribution whose packed
		// width is wrong is rejected whole, so it advances no ordering slot
		// either -- step 2's "no partial application", and a corrupt unit must
		// not make later valid ones look out of order. Erratum to V2 section 4
		// (section 6).
		if !fieldsFit(dst, c, &u) {
			if pos >= 0 {
				s.inst.Transcode.BlobLenMismatch[pos].Add(1)
			}
			if s.log.Allow(obs.ClassBlobLenMismatch) {
				s.log.Errorf(obs.ClassBlobLenMismatch,
					"blob_len_mismatch dest=%s code=%d mask=%#x want=%d blob_len=%d blob_off=%d",
					dst.Name, u.Code, uint64(c.Mask), maskedWidth(dst, c.Mask), c.BlobLen, c.BlobOff)
			}
			continue // merge NOTHING; the image and the slots stay consistent
		}

		// Step 3 -- the three ordering components, written together so a
		// later comparison never pairs a fresh LastSeq with a stale LastTime.
		e.LastSeq[c.Slot] = u.Seq
		e.LastTime[c.Slot] = u.LogTime
		e.LastRecv[c.Slot] = u.RecvMono
		e.MarkSlotSeen(c.Slot)

		// Step 4 -- copy the masked fields.
		copyFields(dst, e, c, &u)
		applied++

		// Step 5 -- dirty the columns (INV-MASK: only ever OR-ed here).
		e.ColMask |= c.Mask

		// Step 6 -- a quarantined row keeps a current image and mask, but is
		// not queued: re-offering it would drag its whole batch serial (9.4).
		if e.Quarantined {
			s.inst.MergeIntoQuarantined[d].Add(1)
			continue
		}

		// Step 7 -- NFR13 suppression, rate-limited units only, cheap clauses
		// first. ColMask, not c.Mask: the question is whether the next
		// statement would name a column no statement has named (ADR-L04).
		// A non-rate-limited unit skips the predicate entirely.
		//
		// Never on an in-flight row (Figure 2.4 has no suppression arc out of
		// InFlight): Digest describes the last COMMIT, not the image in flight,
		// so a value that returns to the committed one mid-flight would match
		// it, be dropped, and leave Oracle holding the batch's image while the
		// mirror holds this one -- with nothing to requeue the row. Erratum to
		// V0 section 5.3 / V2 section 4 step 7 (section 6).
		if !nonRateLimited && !e.InFlight && e.Digest != 0 && e.ColMask&^e.Written == 0 &&
			digest(s.seed, e.Image[:dst.ImageLen]) == e.Digest {
			s.inst.ChangeSuppressed.Add(1)
			continue // clean; ColMask stays set (Vol 0 section 6.2 step 3)
		}

		// Step 8 -- dirty the row and place it.
		switch {
		case e.InFlight:
			// Batched and unconfirmed: the completion requeues it (8.3). It
			// MUST NOT enter a ring, or the row would be in two batches.
			e.ReDirty = true
			s.inst.RedirtyTotal.Add(1)
		case e.Dirty:
			// Already on the accumulating ring: coalescing.
		default:
			e.Dirty = true
			if s.Accum[d].PushBack(slot) { // the tail; the head is for retries
				s.inst.RingGrown[d].Add(1)
			}
			s.inst.RowsDirtied.Add(1)
		}
		dirtied = true
	}

	// Step 9 -- the class action, once per unit, after the merge, and only if
	// this unit actually left something dirty.
	if nonRateLimited && dirtied {
		s.bus.Trigger(&s.busInst)
		s.inst.FlushTriggered.Add(1)
	}

	// Step 10 -- count the unit.
	s.inst.UnitsApplied.Add(1)
	s.inst.UnitsMerged.Add(applied)
}

// fieldsFit is step 4's validation: c's packed run must be exactly the sum of
// the declared widths of c.Mask's set bits and lie inside the bytes of u.Blob
// in use (u.BlobLen, itself bounded by the array). It is separate from the copy
// because c.Mask may be a strict subset of the transcode's column set (Vol 0
// section 6.1), so BlobLen varies at runtime, and a copy that discovered the
// mismatch part-way would leave a partly merged image. A mask bit the
// destination never declared is a mismatch too, not an index panic.
func fieldsFit(dst *destinations.Destination, c *model.Contribution, u *model.UpdateUnit) bool {
	want := maskedWidth(dst, c.Mask)
	return want >= 0 && want == int(c.BlobLen) && int(c.BlobOff)+want <= min(int(u.BlobLen), len(u.Blob))
}

// copyFields is step 4's copy, for a contribution fieldsFit accepted: each
// field, low bit to high, at its full declared width to its image offset.
// Copying the full width -- never just the valid length -- is what keeps a
// shorter text value from inheriting a longer one's tail (gate G-DIG-3, Vol 0
// section 4.1 R5): the decoder packs every field zero-filled to its width.
func copyFields(dst *destinations.Destination, e *model.Entry, c *model.Contribution, u *model.UpdateUnit) {
	cur := int(c.BlobOff)
	for m := c.Mask; m != 0; m &= m - 1 {
		col := &dst.Columns[bits.TrailingZeros64(uint64(m))]
		w := int(col.Width)
		copy(e.Image[col.Offset:int(col.Offset)+w], u.Blob[cur:cur+w])
		cur += w
	}
}

// maskedWidth is the sum of the declared widths of mask's set bits, or -1 if
// mask names a bit dst does not declare.
func maskedWidth(dst *destinations.Destination, mask model.ColMask) int {
	want := 0
	for m := mask; m != 0; m &= m - 1 {
		col := dst.Column(uint8(bits.TrailingZeros64(uint64(m))))
		if col == nil {
			return -1
		}
		want += int(col.Width)
	}
	return want
}

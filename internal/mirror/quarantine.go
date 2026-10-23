package mirror

import (
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// quarantine is the OutcomeRejected arm (Vol 2 sections 8.4 and 9.2): Oracle
// refused the values this batch carried, permanently, and retrying cannot
// help (F14). The row leaves every ring and is marked Quarantined -- zero
// retries -- until a configuration reload releases it (section 9.5, P3-10).
//
// Digest and Written are unchanged: nothing was written. ColMask holds what a
// mid-flight merge dirtied since the pump; a row that stays quarantined also
// gets the refused columns back (ColMask |= r.Mask, as the Failed arm does), so
// the reload's release rewrites them with the latest state (section 9.4) --
// without it, a row not merged into while quarantined would be released with
// an empty mask, which Vol 3 W-4 refuses as a Vol 2 defect. Erratum to V2
// section 8.4 and G-ROW-1 (phase 3 section 6, P3-10 review).
//
// Delta V0-Delta-1: a row re-dirtied in flight carries columns Oracle has never
// seen. If they are DISJOINT from the refused ones (r.Mask), the retry's SET
// list names no refused column, so the row is released at once to the tail
// and counted in quarantine_skipped. If they overlap it stays quarantined: a
// re-offered refused column would drag its whole batch through Vol 3's serial
// re-run every time it is queued (section 9.4).
//
// row_quarantined counts every rejection, released at once or not, which is
// section 8.4's order (quarantine, then release); rows_quarantined_current is
// the standing set, so a row released at once never enters it. row_rejected is
// NOT counted here: it is the writer's (phase 3 ruling D11).
func (s *Shard) quarantine(d model.DestID, r *model.BatchRow, e *model.Entry) {
	s.inst.RowQuarantined[d].Add(1)
	if predictable(r.ErrCode) {
		// Pre-validation passed a value of a kind it exists to refuse: a gap
		// between the mapping's DDL facts and the real table (Vol 0 5A.5).
		s.inst.RowsRejectedByOracleUnpredicted[d].Add(1)
	}

	release := e.ReDirty && !e.ColMask.Intersects(r.Mask)
	if s.log.Allow(obs.ClassRowRejected) {
		what := "row quarantined -- not retried until a configuration reload"
		if release {
			what = "row rejected -- requeued at once: its mid-flight columns avoid the refused ones"
		}
		s.log.Errorf(obs.ClassRowRejected, "%s dest=%s row=%d mask=%#x ora=%d",
			what, s.dst.ByID(d).Name, uint64(r.Row), uint64(r.Mask), r.ErrCode)
	}

	e.ReDirty = false
	if release {
		e.Quarantined, e.Dirty = false, true
		if s.Accum[d].PushBack(r.Slot) { // TAIL: new work, not a retry
			s.inst.RingGrown[d].Add(1)
		}
		s.inst.QuarantineSkipped[d].Add(1)
		return
	}
	e.Quarantined, e.Dirty = true, false
	e.ColMask |= r.Mask // AFTER the V0-Delta-1 test above, which needs it without
	s.Quarantine[d] = append(s.Quarantine[d], r.Slot)
	s.inst.RowsQuarantinedCurrent[d].Add(1)
}

// releaseQuarantine releases every quarantined row (Vol 2 section 9.5). Called
// from maybeOpenEpoch's step 7 when Params.Gen has moved since the shard last
// looked: a reload, the operator saying "I changed something" (ADR-L23). The
// only release besides V0-Delta-1's at-once one; there is no timer (OQ-21).
//
// Each row is re-dirtied and pushed to the TAIL of its accumulating ring, in
// the order the rows were quarantined: no urgency. Step 7 runs after step 5's
// swap, so a released row rides the NEXT epoch, carrying the refused columns
// (folded back by the reject arm) and whatever merge step 6 kept current while
// it sat quarantined.
//
// rows_quarantined_current is one gauge per destination SHARED by every shard
// (it has no shard label), so the release subtracts this shard's count rather
// than storing V2 section 9.5's "= 0", which would hide the rows other shards
// still hold. Erratum to section 9.5 (phase 3 section 6).
//
// Quarantine[d] keeps its capacity: expected tiny, and a row rejected again
// re-enters it through quarantine. Only quarantine_released is this
// function's to count; row_quarantined is the reject arm's (P3-09).
//
// Reached in production when a SIGHUP reload moves Gen.
func (s *Shard) releaseQuarantine() {
	var released [model.MaxDestinations]int
	for d := range model.MaxDestinations {
		for _, slot := range s.Quarantine[d] {
			e := s.entry(model.DestID(d), slot)
			e.Quarantined, e.Dirty = false, true
			if s.Accum[d].PushBack(slot) { // TAIL
				s.inst.RingGrown[d].Add(1)
			}
		}
		released[d] = len(s.Quarantine[d])
		s.inst.QuarantineReleased[d].Add(uint64(released[d]))
		s.inst.RowsQuarantinedCurrent[d].Add(-int64(released[d]))
		s.Quarantine[d] = s.Quarantine[d][:0]
	}
	if released[0]+released[1]+released[2] > 0 {
		s.log.Infof("quarantine released on configuration reload: shard=%d %s=%d %s=%d %s=%d",
			s.ID, s.dst.ByID(0).Name, released[0], s.dst.ByID(1).Name, released[1],
			s.dst.ByID(2).Name, released[2])
	}
}

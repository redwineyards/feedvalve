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
// Digest, Written and ColMask are all unchanged: nothing was written, and
// ColMask holds exactly what a mid-flight merge dirtied since the pump.
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
	s.Quarantine[d] = append(s.Quarantine[d], r.Slot)
	s.inst.RowsQuarantinedCurrent[d].Add(1)
}

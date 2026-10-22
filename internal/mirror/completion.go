package mirror

import (
	"errors"
	"fmt"

	"feedvalve/internal/model"
)

// errBatchRowUnreported is defect S3 (Vol 2 section 2.7): a batch came back
// with Err == nil and a row the writer never adjudicated. It is distinguishable
// from every other applyCompletion error because it is the one the shard
// survives: the rows were requeued before it was returned, so the shard's state
// is consistent and Run reports it (ruling D9, F27) and keeps running into the
// drain rather than returning. Match it with errors.Is.
var errBatchRowUnreported = errors.New("batch returned with a row left Pending (writer defect S3)")

// adjudicated reports whether o is one of the writer's three verdicts. Step 0
// treats anything else -- Pending, or a value outside the enumeration -- as a
// row the writer never adjudicated, so the per-row switch has exactly three
// arms and no unreachable fourth.
func adjudicated(o model.RowOutcome) bool {
	return o == model.OutcomeCommitted || o == model.OutcomeRejected || o == model.OutcomeFailed
}

// applyCompletion applies one returned batch's outcomes to this shard's
// entries, returns the batch to the free list and advances the epoch (Vol 2
// section 8). Called only from the main loop's completion arm.
//
// Errors, all fatal except the last (ruling D9):
//   - step 0a: a batch from another shard, one this shard does not own, one
//     not out (returned twice), or one stamped with a future epoch -- a
//     routing or construction defect;
//   - S2: a row whose entry is not InFlight, i.e. a row reached two batches;
//   - the pump's or the re-opened epoch's own errors (S1, INV-STATE,
//     cohort_not_empty);
//   - S3, errBatchRowUnreported, returned only after the batch has been fully
//     applied and its unreported rows requeued at the head.
//
// It allocates nothing on the Committed and Failed arms: failedRows is
// pre-sized at the batch ceiling and Free at m. The Rejected arm's quarantine
// append may grow a slice that is expected to stay empty.
func (s *Shard) applyCompletion(b *model.Batch) error {
	i := s.batchIndex(b)

	// Step 0a -- before any entry is touched. A batch this shard does not own
	// would index sentAt out of range and enter a Free list it was never in; one
	// that is not out -- returned twice -- would enter Free twice, and the next
	// pump would hand one *Batch to two writers.
	if b.Shard != s.ID || i < 0 || !s.out[i] || b.Epoch > s.Epoch {
		return fmt.Errorf("shard %d: completion for a batch of shard %d, epoch %d (current %d, owned %v) -- "+
			"a routing defect, or a batch returned twice", s.ID, b.Shard, b.Epoch, s.Epoch, i >= 0)
	}
	s.out[i] = false

	// Steps 0b/0c -- normalise, so the walk below has exactly one shape.
	// Err != nil: the batch never reached a per-row verdict, and every
	// unadjudicated row is Failed (Vol 0 section 3.8 note 2). Err == nil with an
	// unadjudicated row is S3: count it, treat the rows as Failed so they are
	// requeued at the head, and report it once the batch is applied.
	unreported := uint64(0)
	for j := range b.Rows[:b.N] {
		if r := &b.Rows[j]; !adjudicated(r.Outcome) {
			r.Outcome = model.OutcomeFailed
			unreported++
		}
	}
	if b.Err != nil {
		unreported = 0 // 0b: an expected failure, not a writer defect
	} else if unreported > 0 {
		s.inst.BatchRowUnreported.Add(unreported)
	}

	// Steps 1-8 -- the per-row walk, by destination segment: BatchRow carries no
	// DestID, so the destination is known only from the segment.
	for d := range model.MaxDestinations {
		start := b.DestStart[d]
		for j := start; j < start+b.DestCount[d]; j++ {
			r := &b.Rows[j]
			e := s.entry(model.DestID(d), r.Slot) // INV-SLOT: no map lookup
			if !e.InFlight {
				// S2: the only thing that clears InFlight is this walk, so the
				// row reached two batches. Continuing would write a stale
				// image over a newer one and never say so.
				s.inst.InflightDesync.Add(1)
				return fmt.Errorf("shard %d: completion of epoch %d names row %d of destination %d, "+
					"which is not in flight (defect S2: a row reached two batches)", s.ID, b.Epoch, r.Row, d)
			}
			e.InFlight = false
			switch r.Outcome {
			case model.OutcomeCommitted:
				s.committed(model.DestID(d), r, e)
			case model.OutcomeFailed:
				s.failed(model.DestID(d), r, e)
			case model.OutcomeRejected:
				s.quarantine(model.DestID(d), r, e)
			}
		}
	}
	s.inst.RowsInflight.Add(-int64(b.N))

	// Step 9 -- reset Rows[:N] BEFORE N = 0, while N still names the range.
	for j := range b.Rows[:b.N] {
		b.Rows[j].Outcome, b.Rows[j].ErrCode = model.OutcomePending, 0
	}
	b.N, b.Err = 0, nil
	b.DestStart, b.DestCount = [model.MaxDestinations]int32{}, [model.MaxDestinations]int32{}

	// Steps 10-11 -- the baton returns. Free never exceeds m, its capacity.
	s.Free = append(s.Free, b)
	s.CohortOut--
	s.inst.CompletionLagMaxNs.Observe(uint64(s.now() - s.sentAt[i]))

	// Step 14 BEFORE step 13 (ruling D8): the failed rows head the accumulating
	// ring as one block, in batch order, before closeEpoch can re-open from it
	// -- otherwise they would miss the very next epoch's first batch.
	for d := range s.failedRows {
		if len(s.failedRows[d]) > 0 {
			if s.Accum[d].PushFrontBlock(s.failedRows[d]) {
				s.inst.RingGrown[d].Add(1)
			}
			s.failedRows[d] = s.failedRows[d][:0]
		}
	}

	// Step 12 -- publish the depths, after the head push so dirty_depth counts it.
	s.publishDepths()

	// Step 13 -- close the epoch or keep pumping.
	var err error
	if s.Cohort[0].Len()+s.Cohort[1].Len()+s.Cohort[2].Len() == 0 && s.CohortOut == 0 {
		err = s.closeEpoch()
	} else {
		err = s.pump()
	}
	if err != nil {
		// Fatal, and outranks S3, whose counter has already recorded it.
		return err
	}
	if unreported > 0 {
		return fmt.Errorf("shard %d: epoch %d: %d rows: %w", s.ID, b.Epoch, unreported, errBatchRowUnreported)
	}
	return nil
}

// committed is the OutcomeCommitted arm (Vol 2 section 8.4): the columns are
// in Oracle.
//
//   - Digest is the batch's, never recomputed: the entry may have been
//     re-merged in flight, and recording the newer image as written would let
//     NFR13 suppress it forever (P2, HLD section 7.12 risk 8).
//   - Written grows here and only here, on a confirmed commit (ADR-L04).
//   - ColMask is NOT touched: pump cleared it, and it now holds exactly what was
//     dirtied since (P1, INV-MASK).
//   - A re-dirtied row goes to the TAIL: its values arrived after the rows
//     already queued.
func (s *Shard) committed(d model.DestID, r *model.BatchRow, e *model.Entry) {
	e.Digest = r.Digest
	e.Written |= r.Mask
	e.Quarantined = false
	if e.ReDirty {
		e.ReDirty, e.Dirty = false, true
		if s.Accum[d].PushBack(r.Slot) {
			s.inst.RingGrown[d].Add(1)
		}
		return
	}
	e.Dirty = false
}

// failed is the OutcomeFailed arm (Vol 2 section 8.4): transient, retry.
// Digest and Written are unchanged -- nothing was confirmed -- and ColMask gets
// back exactly the columns the pump cleared, OR-ed with any a mid-flight merge
// added. The slot is collected for one block head push after the walk (section
// 8.6). Never quarantines: an outage fails every row of every batch.
func (s *Shard) failed(d model.DestID, r *model.BatchRow, e *model.Entry) {
	e.ColMask |= r.Mask
	e.ReDirty = false // folded in: its bits are already in ColMask
	e.Dirty = true
	s.failedRows[d] = append(s.failedRows[d], r.Slot)
	s.inst.RetryRequeued.Add(1)
}

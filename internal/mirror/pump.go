package mirror

import (
	"fmt"

	"feedvalve/internal/model"
)

// pump emits batches from the open epoch's cohort while the free list allows
// (Vol 2 section 7). Called on epoch open (section 6.3 step 9) and again on
// every completion (section 8.5). It is NOT a loop over commits: it emits what
// the free list permits and returns to the main loop, which is what keeps the
// shard goroutine from ever waiting on a writer (HLD section 7.3, ADR-018).
//
// Section 7.3's four rules, each held by the shape of the loop below:
//  1. maxBatchSize is ONE budget per batch across all three destinations (n is
//     shared by the three inner loops), never a per-destination allowance.
//  2. Destinations are drawn 0, then 1, then 2 -- the global acquisition order.
//  3. Oldest-first within a destination (PopFront).
//  4. A row leaves the cohort the moment it is copied, so no row reaches two
//     batches of one epoch.
//
// Rows are NOT sorted by key here: the writer sorts each destination segment
// (ADR-L05), which keeps O(n log n) off the shard goroutine.
//
// Returns an error only for the impossible batchQ send of section 7.4, after
// counting flush_deferred{k}: fatal, because with CohortOut == 0 no completion
// would ever resume a deferred pump and the epoch would never close -- and for
// fillRow's INV-STATE assertion. Both are fatal, so the half-filled batch is
// not returned to Free.
//
// PRECONDITION:  s.EpochOpen
// POSTCONDITION: len(s.Free) == 0, or every s.Cohort[d] is empty
func (s *Shard) pump() error {
	for len(s.Free) > 0 && s.Cohort[0].Len()+s.Cohort[1].Len()+s.Cohort[2].Len() > 0 {
		b := s.Free[len(s.Free)-1]
		s.Free = s.Free[:len(s.Free)-1]
		b.Shard, b.Epoch, b.N, b.Err = s.ID, s.Epoch, 0, nil

		n := int32(0)
		for d := range model.MaxDestinations {
			b.DestStart[d] = n
			for s.Cohort[d].Len() > 0 && int(n) < s.maxBatchSize {
				slot, _ := s.Cohort[d].PopFront() // rule 4: out of the cohort before the copy
				if err := s.fillRow(&b.Rows[n], model.DestID(d), slot); err != nil {
					return err
				}
				n++
			}
			b.DestCount[d] = n - b.DestStart[d]
		}
		b.N = n

		i := s.batchIndex(b)
		select {
		case s.batchQ <- b:
			// b is the writer's now; only the shard's own bookkeeping follows.
			s.CohortOut++ // ONLY after a successful send
			s.sentAt[i] = s.now()
			s.out[i] = true
			s.inst.EpochBatches.Add(1)
			s.inst.EpochRows.Add(uint64(n))
			s.inst.RowsInflight.Add(int64(n))
			for d := range s.Cohort {
				s.inst.CohortDepth[d].Store(int64(s.Cohort[d].Len()))
			}
		default:
			s.inst.FlushDeferred.Add(1)
			return fmt.Errorf("shard %d: batchQ send failed with %d batches outstanding "+
				"(epoch %d, %d rows) -- see LLD Vol 2 section 7.4", s.ID, s.CohortOut, s.Epoch, n)
		}
	}
	return nil
}

// fillRow copies slot's entry of destination d into r (Vol 2 section 7.2), in
// the order that matters:
//
//   - Mask is copied BEFORE ColMask is cleared, and ColMask is cleared HERE, at
//     fill, never at completion (INV-MASK). A completion that cleared the
//     written bits would erase the ones a mid-flight merge had just set, and
//     the next statement would name no column (HLD R14, Vol 0 Figure 0.3).
//   - The digest is computed here and carried on the batch, never recomputed at
//     completion, which would record a newer mid-flight image as written
//     (HLD section 7.12 risk 8).
//   - The whole 256-byte image is copied, not ImageLen, so a reused BatchRow
//     never carries its previous occupant's tail.
//
// Outcome and ErrCode are reset because the BatchRow is reused: the writer
// fills both, and a stale ErrCode would otherwise ride along on a row the
// writer reports Committed.
//
// The row came off a cohort, so it is Dirty and neither InFlight nor ReDirty
// (INV-STATE): merge never rings an in-flight row (section 4 step 8). That is
// asserted here (V2:1780), before anything is copied: a row reaching a cohort
// in flight is the G-INFL-1 failure class, and re-batching it silently would
// put it in two outstanding batches at once.
func (s *Shard) fillRow(r *model.BatchRow, d model.DestID, slot int32) error {
	e := s.entry(d, slot)
	if e.InFlight || e.ReDirty {
		return fmt.Errorf("shard %d: INV-STATE: row %d of destination %d on a cohort with InFlight %v ReDirty %v",
			s.ID, e.Row, d, e.InFlight, e.ReDirty)
	}
	r.Row = e.Row
	r.Mask = e.ColMask // before the clear below
	r.Image = e.Image
	r.Digest = digest(s.seed, e.Image[:s.dst.ByID(d).ImageLen])
	r.Slot = slot
	r.Outcome = model.OutcomePending
	r.ErrCode = 0
	e.ColMask = 0 // INV-MASK: cleared at fill
	e.Dirty = false
	e.InFlight = true
	return nil
}

package mirror

import (
	"fmt"
	"time"
)

// monoStart is the origin of nowMono. Go exposes no raw monotonic clock, but a
// time.Time from time.Now carries a monotonic reading and time.Since uses it,
// so a duration from a fixed origin is a monotonic nanosecond count that a
// wall-clock step cannot move.
var monoStart = time.Now()

// nowMono is monotonic nanoseconds since monoStart: epochOpenedAt and sentAt.
// Only ever differenced against another nowMono reading, never compared with
// app's RecvMono, whose origin is a different instant.
func nowMono() int64 { return int64(time.Since(monoStart)) }

// maybeOpenEpoch opens an epoch if and only if there is something to flush
// (Vol 2 section 6.3). It is the single call site for the ring swap, called
// only from stage B of the main loop and from closeEpoch's immediate re-open.
//
// PRECONDITION: !s.EpochOpen -- stage B's guard, re-asserted as step 0
// (V2:1558). Opening over an open epoch would zero CohortOut under batches
// still out, and they would come home stamped with a past epoch, which step 0a
// of the completion path does not reject.
//
// The step order is load-bearing. Step 2 MUST precede step 3: clearing the
// flush request and only then finding nothing to flush would consume a
// non-rate-limited request that no epoch ever serves (HLD section 7.3's
// ordering defect). The tick is consumed whatever happens (step 1); the
// request only when an epoch definitely opens (step 3).
//
// It returns an error only for a broken precondition, the cohort-not-empty
// construction defect, and a failed batchQ send or INV-STATE violation in the
// pump it tails into. All are fatal (section 2.7); the caller maps them (phase
// 3 ruling D9).
func (s *Shard) maybeOpenEpoch() error {
	// Step 0 -- the precondition, re-asserted.
	if s.EpochOpen {
		return fmt.Errorf("shard %d: maybeOpenEpoch with epoch %d open", s.ID, s.Epoch)
	}

	// Step 1 -- a tick is a periodic sample, consumed whatever we decide.
	s.tickPending = false

	// Step 2 -- nothing to flush: count it and leave req[k] EXACTLY as it was.
	if s.Accum[0].Len()+s.Accum[1].Len()+s.Accum[2].Len() == 0 {
		s.inst.EpochEmpty.Add(1)
		return nil
	}

	// Step 3 -- an epoch is definitely opening: only now clear the request,
	// and before the snapshot, so a request raised during the drain is kept.
	s.bus.Taken(s.ID)

	// Step 4 -- HLD section 7.12 risk 10's detector: rows left in a cohort are
	// unreachable, and the swap would hide them one epoch late.
	for d := range s.Cohort {
		if n := s.Cohort[d].Len(); n != 0 {
			s.inst.CohortNotEmpty.Add(1)
			return fmt.Errorf("shard %d: cohort[%d] non-empty at epoch open: %d rows", s.ID, d, n)
		}
	}

	// Step 5 -- the whole snapshot: three word moves per destination.
	for d := range s.Accum {
		s.Accum[d].Swap(&s.Cohort[d])
	}

	// Step 6 -- the epoch fields.
	s.EpochOpen = true
	s.Epoch++
	s.CohortOut = 0
	s.TickMissed = false
	s.epochOpenedAt = nowMono()

	// Step 7 -- ONE Params load, held for the whole epoch (ruling D3), so two
	// batches of one epoch never see two maxBatchSize values. ordering.guard
	// is refreshed from the same load: merge reads it from s.guard.
	p := s.params()
	s.maxBatchSize = p.MaxBatchSize
	s.guard = p.Guard
	if p.Gen != s.cfgGen {
		s.releaseQuarantine()
		s.cfgGen = p.Gen
	}

	// Step 8 -- count it and publish the depth gauges (shape G).
	s.inst.EpochOpened.Add(1)
	for d := range s.Cohort {
		s.inst.DirtyDepth[d].Store(0)
		s.inst.CohortDepth[d].Store(int64(s.Cohort[d].Len()))
	}

	// Step 9.
	return s.pump()
}

// closeEpoch closes the open epoch (Vol 2 section 6.4). Called only from
// applyCompletion's step 13, once every cohort is empty and CohortOut is 0 --
// and after the failed rows' head push (ruling D8), so a re-open here draws
// them first.
//
// Step 4 re-opens at once when a non-rate-limited request is waiting, or a tick
// was missed while this epoch was open and there is work for it. It cannot
// recurse (section 2.5): maybeOpenEpoch proceeds only on a non-empty ring, so
// the epoch it opens has a non-empty cohort and cannot close inside its own
// pump. Depth is bounded at 2 by construction.
func (s *Shard) closeEpoch() error {
	s.EpochOpen = false
	dur := uint64(nowMono() - s.epochOpenedAt)
	s.inst.EpochDurationMaxNs.Observe(dur)
	s.inst.EpochNsTotal.Add(dur)
	for d := range s.Cohort {
		s.inst.CohortDepth[d].Store(0)
		s.inst.DirtyDepth[d].Store(int64(s.Accum[d].Len()))
	}
	if s.bus.Pending(s.ID) || (s.TickMissed && s.Accum[0].Len()+s.Accum[1].Len()+s.Accum[2].Len() > 0) {
		return s.maybeOpenEpoch()
	}
	return nil
}

// releaseQuarantine releases every quarantined row on a configuration
// generation change (Vol 2 section 9.5). Card P3-10 fills it; until then a Gen
// advance at epoch open drops the release and the rows stay quarantined.
// Unreachable in production: Gen does not move until Phase 5's SIGHUP reload.
func (s *Shard) releaseQuarantine() {}

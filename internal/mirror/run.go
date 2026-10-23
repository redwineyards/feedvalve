package mirror

import (
	"errors"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// Run is the shard goroutine (Vol 2 section 2, ADR-L24; closes L2). ADR-007's
// correctness claim -- exactly one goroutine applies contributions to a row,
// in the order the receiver produced them -- is a claim about this loop: no
// arm blocks on anything but the select, and merge, applyCompletion,
// maybeOpenEpoch and pump never select, receive or re-enter one another
// (section 2.5).
//
// It returns nil when shardCh is closed and the drain completes, or when the
// shared drain deadline expires (section 2.6); and an error for a defect that
// must stop the process: S1, S2, cohort_not_empty, a misrouted or twice
// returned batch (step 0a), or a broken precondition. There is no context arm
// (ruling D5): a clean stop is the shardCh close, an abnormal one the
// deadline. Defect S3 is NOT returned: the rows were requeued, the state is
// consistent, so Run hands it to report and keeps going into the normal drain
// (ruling D9).
//
// Run returns errors rather than ending the process itself, so the fatal path
// unwinds through app and every defer fires (Vol 0 section 10.4).
func (s *Shard) Run() error {
	l := loop{in: s.in}
	s.owner.enter(s.ID)
	s.publishGauges()
	s.owner.exit()
	for {
		s.owner.enter(s.ID)
		done, err := s.pass(&l)
		s.owner.exit()
		if done {
			return err
		}
	}
}

// loop is Run's own state: the two arms it disables or arms at the drain, and
// whether it is draining.
type loop struct {
	in       <-chan model.UpdateUnit // shardCh[k]; nil once seen closed
	drain    <-chan struct{}         // nil until in is seen closed (ruling D4)
	draining bool
	rows0    uint64 // epoch_rows when in was seen closed: the drain's baseline
}

// pass is one loop pass: stage A, the select, then stage B, the level check
// that runs after every case (section 2.3). done reports that Run returns err.
func (s *Shard) pass(l *loop) (done bool, err error) {
	var start int64
	select {
	case u, ok := <-l.in:
		start = s.now()
		if !ok {
			// Only now is the deadline reachable: with drainTimeoutMs = 0 it
			// is already closed, and armed any earlier it could pre-empt units
			// still buffered in Q1 (ruling D4).
			l.in, l.drain, l.draining = nil, s.drainExpired, true
			l.rows0 = s.inst.EpochRows.Load()
			s.log.Infof("shard draining: shard=%d dirty=%d/%d/%d cohort_out=%d",
				s.ID, s.Accum[0].Len(), s.Accum[1].Len(), s.Accum[2].Len(), s.CohortOut)
			break
		}
		s.merge(u)

	case b := <-s.completion:
		start = s.now()
		if err := s.applyCompletion(b); err != nil {
			if !errors.Is(err, errBatchRowUnreported) {
				return true, err
			}
			s.report(err)
		}

	case <-s.tick:
		start = s.now()
		s.onTick()

	case <-s.wake:
		// The edge only: stage B reads the level. No other work (section 3.3).
		start = s.now()
		s.inst.FlushWakeReceived.Add(1)

	case <-l.drain:
		s.abandon()
		return true, nil
	}

	// Stage B. tickPending is in the guard (V2:481; V2:409 omits it).
	if !s.EpochOpen && (l.draining || s.tickPending || s.bus.Pending(s.ID)) {
		if err := s.maybeOpenEpoch(); err != nil {
			return true, err
		}
	}
	if l.draining && s.quiescent() {
		// Rows handed to writers since the close; at quiescence every one of
		// them is home, so this is the drain's written count (V2:545).
		s.log.Infof("shard drained: shard=%d rows=%d epoch=%d", s.ID, s.inst.EpochRows.Load()-l.rows0, s.Epoch)
		return true, nil
	}

	depth := len(s.in)
	s.inst.ShardDepth.Store(int64(depth))
	s.inst.ShardDepthMax.Observe(uint64(depth))
	s.inst.ShardLoopMaxNs.Observe(uint64(s.now() - start))
	return false, nil
}

// publishGauges stores the depth gauges from the shard's current state: Run's
// init (V2:378, Figure 2.2), so a scrape before the first pass ends reads the
// state Run starts from, and the depth points of shape G.
func (s *Shard) publishGauges() {
	s.inst.ShardDepth.Store(int64(len(s.in)))
	s.publishDepths()
}

// publishDepths stores dirty_depth and cohort_depth per destination.
func (s *Shard) publishDepths() {
	for d := range s.Accum {
		s.inst.DirtyDepth[d].Store(int64(s.Accum[d].Len()))
		s.inst.CohortDepth[d].Store(int64(s.Cohort[d].Len()))
	}
}

// onTick is the tick arm (Vol 2 section 2.4). It loads Params once (ruling D3)
// -- refreshing ordering.guard for the merge -- and either records a tick for
// stage B to act on or, with an epoch open, records the miss and checks for a
// stall. It never opens an epoch itself, and it MUST NOT clear req[k]:
// clearing a flag it is not acting on discards a non-rate-limited request.
func (s *Shard) onTick() {
	p := s.params()
	s.guard = p.Guard
	if !s.EpochOpen {
		s.tickPending = true
		return
	}
	s.TickMissed = true
	s.inst.TickSkippedEpochOpen.Add(1)
	// HLD section 7.12 risk 9's runtime detector. No timer: ticks arrive
	// every W, so a level check on each one is exact. Once per epoch.
	if !s.stallWarned && s.now()-s.epochOpenedAt > int64(p.EpochStallWarn) {
		s.stallWarned = true
		s.inst.EpochStalled.Add(1)
		if s.log.Allow(obs.ClassEpochStalled) {
			s.log.Warnf(obs.ClassEpochStalled,
				"epoch stalled: shard=%d epoch=%d open_ms=%d cohort=%d/%d/%d cohort_out=%d free=%d",
				s.ID, s.Epoch, (s.now()-s.epochOpenedAt)/1e6,
				s.Cohort[0].Len(), s.Cohort[1].Len(), s.Cohort[2].Len(), s.CohortOut, len(s.Free))
		}
	}
}

// quiescent is the drain's exit condition (section 2.6): no epoch, nothing
// out, nothing dirty, every batch home. Free's length is part of it, or the
// shard could leave with written-but-unconfirmed rows uncounted.
func (s *Shard) quiescent() bool {
	return !s.EpochOpen && s.CohortOut == 0 &&
		s.Accum[0].Len()+s.Accum[1].Len()+s.Accum[2].Len() == 0 &&
		len(s.Free) == len(s.all)
}

// abandon is the drain deadline (section 2.6, teardown step 8): count the rows
// that will never be written, per destination, and say so if there are any.
// That is the accumulating and cohort rings plus rowsOut[d], the rows of
// batches still with a writer: an outstanding batch is never read (it is the
// writer's), so its rows are counted from the shard's own bookkeeping.
// Returning with batches still out is safe: completionCh[k] holds m, so their
// writers never block.
func (s *Shard) abandon() {
	var n [model.MaxDestinations]int
	for d := range n {
		n[d] = s.Accum[d].Len() + s.Cohort[d].Len() + s.rowsOut[d]
		s.inst.DrainAbandonedRows[d].Add(uint64(n[d]))
	}
	if n[0]+n[1]+n[2] > 0 && s.log.Allow(obs.ClassDrainDeadline) {
		s.log.Errorf(obs.ClassDrainDeadline,
			"drain deadline reached with rows unwritten: shard=%d %s=%d %s=%d %s=%d cohort_out=%d free=%d",
			s.ID, s.dst.ByID(0).Name, n[0], s.dst.ByID(1).Name, n[1], s.dst.ByID(2).Name, n[2],
			s.CohortOut, len(s.Free))
	}
}

package mirror

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// quarantineRow merges ltp into row through x, opens an epoch that batches it
// alone, and completes that batch Rejected with ORA-01438: the real reject
// arm, so the row is quarantined exactly as P3-09 leaves it.
func quarantineRow(t testing.TB, s *Shard, q chan *model.Batch, x *sender7208, row model.RowKey, ltpv uint32) {
	t.Helper()
	x.send(row, fv{0, be32(ltpv)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	if !slices.Equal(rowsOf(b), []model.RowKey{row}) {
		t.Fatalf("setup: batch carries %q, want only %q", rowsOf(b), row)
	}
	if err := writeBack(s, b, perRow([]model.RowOutcome{model.OutcomeRejected}, []uint16{1438})); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if e := rowEntry(s, tokenQuote, row); !e.Quarantined || e.Dirty || s.EpochOpen {
		t.Fatalf("setup: row %q Quarantined %v Dirty %v, epoch open %v; want quarantined, clean, closed",
			row, e.Quarantined, e.Dirty, s.EpochOpen)
	}
}

// commitNext completes the next batch on q all-Committed and returns its rows.
func commitNext(t testing.TB, s *Shard, q chan *model.Batch) []model.RowKey {
	t.Helper()
	b := next(t, q)
	rows := rowsOf(b)
	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return rows
}

// TestReleaseQuarantineOnAGenerationChange is Vol 2 section 9.5's release.
//
// SETUP: row R rejected (ORA-01438) and quarantined; a newer ltp merged into it
// while quarantined (merge step 6: image and ColMask kept current, not queued);
// row X dirtied so the next epoch has something to open on.
//
// STIMULUS: the Params loader returns Gen + 1 -- a reload -- and an epoch
// opens.
//
// ASSERTION: R is released at that open -- Quarantined false, Dirty, at the
// TAIL of the accumulating ring, so it rides the NEXT epoch (step 7 runs after
// step 5's swap); quarantine_released 1, rows_quarantined_current back to 0,
// dirty_depth 1, Quarantine[d] empty with its capacity kept, cfgGen advanced,
// one INFO line. The next epoch's batch carries R with the value merged while
// it was quarantined, and the commit records it.
//
// Non-vacuity: before the reload R is quarantined and in no ring, so the ring
// and counter checks can only pass through the release.
//
// WOULD HAVE CAUGHT: defect L4's missing release half -- a row quarantined
// forever, the operator's fix-then-SIGHUP runbook step having no effect.
func TestReleaseQuarantineOnAGenerationChange(t *testing.T) {
	s, q, set := newEpochShard(t, 8, 1000)
	var logged bytes.Buffer
	s.log = obs.NewAppLogger(&logged, obs.LevelInfo, 1000, nil)
	x := newSender7208(t, s)
	quarantineRow(t, s, q, x, 'R', 1000)
	in := s.inst
	if in.RowsQuarantinedCurrent[tokenQuote].Load() != 1 {
		t.Fatalf("setup: rows_quarantined_current %d, want 1", in.RowsQuarantinedCurrent[tokenQuote].Load())
	}

	x.send('R', fv{0, be32(2000)})
	x.send('X', fv{0, be32(7)})
	if got := queued(s, tokenQuote); !slices.Equal(got, slotsOf(s, tokenQuote, 'X')) {
		t.Fatalf("setup: ring %v, want only X -- a quarantined row is not queued by a merge", got)
	}
	logged.Reset()

	set(&Params{Gen: 2, MaxBatchSize: 1000, Guard: GuardAuto})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open after reload: %v", err)
	}
	r := rowEntry(s, tokenQuote, 'R')
	if r.Quarantined || !r.Dirty || r.InFlight {
		t.Fatalf("released row: Quarantined %v Dirty %v InFlight %v; want false true false", r.Quarantined, r.Dirty, r.InFlight)
	}
	if got := queued(s, tokenQuote); !slices.Equal(got, slotsOf(s, tokenQuote, 'R')) {
		t.Fatalf("accumulating ring %v, want R %v", got, slotsOf(s, tokenQuote, 'R'))
	}
	if in.QuarantineReleased[tokenQuote].Load() != 1 || in.RowsQuarantinedCurrent[tokenQuote].Load() != 0 ||
		in.DirtyDepth[tokenQuote].Load() != 1 {
		t.Fatalf("quarantine_released %d, rows_quarantined_current %d, dirty_depth %d; want 1, 0, 1",
			in.QuarantineReleased[tokenQuote].Load(), in.RowsQuarantinedCurrent[tokenQuote].Load(),
			in.DirtyDepth[tokenQuote].Load())
	}
	if len(s.Quarantine[tokenQuote]) != 0 || cap(s.Quarantine[tokenQuote]) == 0 || s.cfgGen != 2 {
		t.Fatalf("Quarantine len %d cap %d, cfgGen %d; want 0, kept, 2",
			len(s.Quarantine[tokenQuote]), cap(s.Quarantine[tokenQuote]), s.cfgGen)
	}
	if line := logged.String(); strings.Count(line, "INFO") != 1 || !strings.Contains(line, "quarantine released") ||
		!strings.Contains(line, s.dst.ByID(tokenQuote).Name+"=1") {
		t.Fatalf("release log = %q, want one INFO line counting 1 for %s", line, s.dst.ByID(tokenQuote).Name)
	}

	if rows := commitNext(t, s, q); !slices.Equal(rows, []model.RowKey{'X'}) {
		t.Fatalf("the reload epoch carried %q, want X only", rows)
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("next open: %v", err)
	}
	b := next(t, q)
	if !slices.Equal(rowsOf(b), []model.RowKey{'R'}) || ltp(&b.Rows[0].Image) != 2000 || b.Rows[0].Mask != model.Bit(0) {
		t.Fatalf("next epoch's batch carries %q ltp %d mask %#x; want R, 2000 (merged while quarantined), ltp",
			rowsOf(b), ltp(&b.Rows[0].Image), b.Rows[0].Mask)
	}
	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if r.Digest == 0 || r.Written != model.Bit(0) || r.Quarantined || r.Dirty {
		t.Fatalf("after commit: Digest %#x Written %#x Quarantined %v Dirty %v", r.Digest, r.Written, r.Quarantined, r.Dirty)
	}
}

// TestReleaseQuarantineNeedsAGenerationChange: an epoch open without a Gen
// change releases nothing, and neither does a fresh shard's first epoch.
//
// SETUP and STIMULUS, first half: R quarantined, X dirtied, an epoch opened
// with the Params unchanged (Gen 1). Second half: a fresh shard (Params Gen 1)
// with a row planted in its quarantine behind the API, and X dirtied; its first
// epoch opens.
//
// ASSERTION: in both, R stays quarantined and out of every ring, and
// quarantine_released and rows_quarantined_current are untouched.
//
// Non-vacuity: the fresh shard's Gen is 1, not 0, so a cfgGen left at its zero
// value instead of seeded from the first Params would see a change and release.
//
// WOULD HAVE CAUGHT: releasing on every epoch open (the quarantine then costs
// a serial re-run per epoch, section 9.4), or an unseeded cfgGen.
func TestReleaseQuarantineNeedsAGenerationChange(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	quarantineRow(t, s, q, x, 'R', 1000)
	x.send('X', fv{0, be32(7)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	assertStillQuarantined(t, s, 'R')

	f, _, _ := newEpochShard(t, 8, 1000)
	if f.cfgGen != 1 || f.Epoch != 0 {
		t.Fatalf("setup: fresh shard cfgGen %d Epoch %d; want 1, 0", f.cfgGen, f.Epoch)
	}
	slot := f.allocEntry(tokenQuote)
	e := f.entry(tokenQuote, slot)
	e.Row, e.Quarantined, e.ColMask = 'R', true, model.Bit(0)
	f.Index[tokenQuote]['R'] = slot
	f.Quarantine[tokenQuote] = append(f.Quarantine[tokenQuote], slot)
	f.inst.RowsQuarantinedCurrent[tokenQuote].Add(1)
	dirty(f, tokenQuote, 'X', model.Bit(0))
	if err := f.maybeOpenEpoch(); err != nil {
		t.Fatalf("fresh shard's first open: %v", err)
	}
	if f.Epoch != 1 {
		t.Fatalf("setup: the fresh shard's first epoch did not open (Epoch %d)", f.Epoch)
	}
	assertStillQuarantined(t, f, 'R')
}

// assertStillQuarantined checks row of destination 0 was not released.
func assertStillQuarantined(t testing.TB, s *Shard, row model.RowKey) {
	t.Helper()
	e := rowEntry(s, tokenQuote, row)
	if !e.Quarantined || e.Dirty || slices.Contains(queued(s, tokenQuote), s.Index[tokenQuote][row]) {
		t.Fatalf("row %q released: Quarantined %v Dirty %v ring %v", row, e.Quarantined, e.Dirty, queued(s, tokenQuote))
	}
	if !slices.Equal(s.Quarantine[tokenQuote], slotsOf(s, tokenQuote, row)) ||
		s.inst.QuarantineReleased[tokenQuote].Load() != 0 || s.inst.RowsQuarantinedCurrent[tokenQuote].Load() != 1 {
		t.Fatalf("Quarantine %v, quarantine_released %d, rows_quarantined_current %d; want [%q], 0, 1",
			s.Quarantine[tokenQuote], s.inst.QuarantineReleased[tokenQuote].Load(),
			s.inst.RowsQuarantinedCurrent[tokenQuote].Load(), row)
	}
}

// TestReleaseQuarantineOncePerChangeAndReQuarantine: two reloads between
// epochs are one release, and a released row rejected again goes back.
//
// SETUP: R quarantined at Gen 1, a newer value merged into it, X dirtied.
//
// STIMULUS: Gen 2 then Gen 3 published before the next open; that epoch and the
// next run, R's batch completing Rejected again; Y dirtied and an epoch opened
// at Gen 3 (unchanged); finally Gen 4 and an open.
//
// ASSERTION: the open at Gen 3 releases R once (quarantine_released 1, R once
// on the ring, cfgGen 3). The second rejection re-quarantines it: Quarantine
// holds it once, row_quarantined 2, rows_quarantined_current 1. The unchanged
// Gen 3 open releases nothing; Gen 4 releases it again (quarantine_released 2).
//
// Non-vacuity: R's second batch exists only because the first release worked,
// and the Gen 4 release shows the Gen 3 non-release was the compare, not a
// quarantine that can no longer be released.
//
// WOULD HAVE CAUGHT: a release per generation step rather than per observed
// change (R twice on the ring: two outstanding batches, G-INFL-1's class), or a
// Quarantine slice left stale after a release, double-counting the gauge.
func TestReleaseQuarantineOncePerChangeAndReQuarantine(t *testing.T) {
	s, q, set := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	in := s.inst
	quarantineRow(t, s, q, x, 'R', 1000)
	x.send('R', fv{0, be32(2000)})
	x.send('X', fv{0, be32(7)})

	set(&Params{Gen: 2, MaxBatchSize: 1000, Guard: GuardAuto})
	set(&Params{Gen: 3, MaxBatchSize: 1000, Guard: GuardAuto})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := queued(s, tokenQuote); !slices.Equal(got, slotsOf(s, tokenQuote, 'R')) ||
		in.QuarantineReleased[tokenQuote].Load() != 1 || s.cfgGen != 3 {
		t.Fatalf("ring %v quarantine_released %d cfgGen %d; want R once, 1, 3",
			got, in.QuarantineReleased[tokenQuote].Load(), s.cfgGen)
	}
	commitNext(t, s, q) // X

	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	if !slices.Equal(rowsOf(b), []model.RowKey{'R'}) {
		t.Fatalf("batch carries %q, want R", rowsOf(b))
	}
	if err := writeBack(s, b, perRow([]model.RowOutcome{model.OutcomeRejected}, []uint16{1438})); err != nil {
		t.Fatalf("second reject: %v", err)
	}
	if in.RowQuarantined[tokenQuote].Load() != 2 {
		t.Fatalf("row_quarantined %d, want 2", in.RowQuarantined[tokenQuote].Load())
	}

	x.send('Y', fv{0, be32(9)})
	if err := s.maybeOpenEpoch(); err != nil { // Gen 3, unchanged
		t.Fatalf("open: %v", err)
	}
	r := rowEntry(s, tokenQuote, 'R')
	if !r.Quarantined || r.Dirty || !slices.Equal(s.Quarantine[tokenQuote], slotsOf(s, tokenQuote, 'R')) ||
		in.RowsQuarantinedCurrent[tokenQuote].Load() != 1 || in.QuarantineReleased[tokenQuote].Load() != 1 {
		t.Fatalf("re-quarantine: Quarantined %v Dirty %v Quarantine %v current %d released %d; want true false [R] 1 1",
			r.Quarantined, r.Dirty, s.Quarantine[tokenQuote],
			in.RowsQuarantinedCurrent[tokenQuote].Load(), in.QuarantineReleased[tokenQuote].Load())
	}
	commitNext(t, s, q) // Y

	set(&Params{Gen: 4, MaxBatchSize: 1000, Guard: GuardAuto})
	x.send('Z', fv{0, be32(11)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if r.Quarantined || in.QuarantineReleased[tokenQuote].Load() != 2 || in.RowsQuarantinedCurrent[tokenQuote].Load() != 0 {
		t.Fatalf("Gen 4: Quarantined %v released %d current %d; want false 2 0",
			r.Quarantined, in.QuarantineReleased[tokenQuote].Load(), in.RowsQuarantinedCurrent[tokenQuote].Load())
	}
}

// allRejected refuses every row of a batch with ORA-01438.
func allRejected() fakeMode {
	return fakeMode{adjudicate: func(b *model.Batch) {
		for i := range b.Active() {
			b.Rows[i].Outcome, b.Rows[i].ErrCode = model.OutcomeRejected, 1438
		}
	}}
}

// TestReleaseQuarantineRewritesTheRefusedColumns: a row nobody merged into
// while it sat quarantined -- the runbook's common case: rejected, token quiet,
// column widened, SIGHUP -- is released with the columns Oracle refused.
//
// SETUP: R rejected (ORA-01438) on ltp and quarantined; NOTHING merged since;
// X dirtied so the reload epoch has something to open on.
//
// STIMULUS: Gen + 1, an epoch opens and X commits; the next epoch opens and its
// batch commits.
//
// ASSERTION: the next batch carries R with Mask = ltp (the refused column) and
// the ltp it was refused with; the commit records it (Written ltp), and
// row_quarantined stays at 1 -- runbook step 4's "the fix took".
//
// Non-vacuity: R has no merge after the reject, so its ColMask can hold ltp
// only through the reject arm's fold.
//
// WOULD HAVE CAUGHT: the reject arm leaving ColMask as the pump cleared it --
// a release batching R with Mask 0, which Vol 3 W-4 refuses as a Vol 2 defect
// (batch_row_empty_mask, OutcomeRejected), re-quarantining a row whose fix
// worked.
func TestReleaseQuarantineRewritesTheRefusedColumns(t *testing.T) {
	s, q, set := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	quarantineRow(t, s, q, x, 'R', 1000)
	x.send('X', fv{0, be32(7)})

	set(&Params{Gen: 2, MaxBatchSize: 1000, Guard: GuardAuto})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open after reload: %v", err)
	}
	if rows := commitNext(t, s, q); !slices.Equal(rows, []model.RowKey{'X'}) {
		t.Fatalf("the reload epoch carried %q, want X only", rows)
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("next open: %v", err)
	}
	b := next(t, q)
	if !slices.Equal(rowsOf(b), []model.RowKey{'R'}) || b.Rows[0].Mask != model.Bit(0) || ltp(&b.Rows[0].Image) != 1000 {
		t.Fatalf("released batch carries %q mask %#x ltp %d; want R, the refused ltp column, 1000",
			rowsOf(b), b.Rows[0].Mask, ltp(&b.Rows[0].Image))
	}
	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	r := rowEntry(s, tokenQuote, 'R')
	if r.Written != model.Bit(0) || r.Quarantined || s.inst.RowQuarantined[tokenQuote].Load() != 1 {
		t.Fatalf("after commit: Written %#x Quarantined %v row_quarantined %d; want ltp, false, 1",
			r.Written, r.Quarantined, s.inst.RowQuarantined[tokenQuote].Load())
	}
}

// TestReleaseQuarantineEveryDestinationInQuarantineOrder: the release walks all
// three destinations, each counted on its own, and re-queues in quarantine
// order.
//
// SETUP: 17 rows of destination 1 and one of destination 2 rejected in one
// batch and quarantined, destination 0 empty. One plain epoch runs in between
// so that the reload's step-5 swap hands step 7 the ring the rejected batch did
// NOT grow: 16 slots (ExpectedRows 64 / 4 shards).
//
// STIMULUS: Gen + 1, an epoch opens on a row X of destination 0.
//
// ASSERTION: per destination, Quarantine[d] empty, quarantine_released, the
// accumulating ring and dirty_depth match the quarantined rows -- destination
// 1's ring holding them oldest first, in the order they were quarantined, its
// growth counted in ring_grown -- rows_quarantined_current 0, and one INFO line
// counting 0, 17 and 1 by destination name.
//
// Non-vacuity: 17 rows, more than the 16-slot ring, so the release must grow
// it; and destination 0 has nothing quarantined, so a loop that stops at 1
// leaves destinations 1 and 2 untouched.
//
// WOULD HAVE CAUGHT: a release loop over destination 0 alone, a reversed walk
// or a head push (releases out of quarantine order), an uncounted growth.
func TestReleaseQuarantineEveryDestinationInQuarantineOrder(t *testing.T) {
	s, q, set := newEpochShard(t, 8, 1000)
	var logged bytes.Buffer
	s.log = obs.NewAppLogger(&logged, obs.LevelInfo, 1000, nil)
	in := s.inst
	var master []model.RowKey
	for i := range 17 {
		row := model.RowKey(117 - i) // keys descending: quarantine order is not key order
		master = append(master, row)
		dirty(s, securityMaster, row, model.Bit(0))
	}
	dirty(s, marketStatus, 'M', model.Bit(0))
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := writeBack(s, next(t, q), allRejected()); err != nil {
		t.Fatalf("reject: %v", err)
	}
	want := [model.MaxDestinations][]int32{nil, slotsOf(s, securityMaster, master...), slotsOf(s, marketStatus, 'M')}
	for d := range model.MaxDestinations {
		if !slices.Equal(s.Quarantine[d], want[d]) {
			t.Fatalf("setup: Quarantine[%d] %v, want %v", d, s.Quarantine[d], want[d])
		}
	}

	dirty(s, tokenQuote, 'Y', model.Bit(0)) // the in-between epoch
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	commitNext(t, s, q)

	dirty(s, tokenQuote, 'X', model.Bit(0))
	grown := in.RingGrown[securityMaster].Load()
	logged.Reset()
	set(&Params{Gen: 2, MaxBatchSize: 1000, Guard: GuardAuto})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open after reload: %v", err)
	}
	for d := range model.MaxDestinations {
		if got := queued(s, model.DestID(d)); !slices.Equal(got, want[d]) {
			t.Fatalf("destination %d: accumulating ring %v, want the quarantine order %v", d, got, want[d])
		}
		if len(s.Quarantine[d]) != 0 || in.QuarantineReleased[d].Load() != uint64(len(want[d])) ||
			in.RowsQuarantinedCurrent[d].Load() != 0 || in.DirtyDepth[d].Load() != int64(len(want[d])) {
			t.Fatalf("destination %d: Quarantine %v released %d current %d dirty_depth %d; want empty, %d, 0, %d",
				d, s.Quarantine[d], in.QuarantineReleased[d].Load(), in.RowsQuarantinedCurrent[d].Load(),
				in.DirtyDepth[d].Load(), len(want[d]), len(want[d]))
		}
	}
	if in.RingGrown[securityMaster].Load() != grown+1 {
		t.Fatalf("ring_grown %d, want %d: releasing 17 rows onto a 16-slot ring grows it once",
			in.RingGrown[securityMaster].Load(), grown+1)
	}
	name := func(d model.DestID) string { return s.dst.ByID(d).Name }
	line := logged.String()
	for _, c := range []string{name(tokenQuote) + "=0", name(securityMaster) + "=17", name(marketStatus) + "=1"} {
		if strings.Count(line, "INFO") != 1 || !strings.Contains(line, c) {
			t.Fatalf("release log = %q, want one INFO line containing %q", line, c)
		}
	}
}

// TestReleaseQuarantineLeavesOtherShardsInTheGauge: rows_quarantined_current
// has no shard label -- app hands every shard the same gauge per destination
// (instruments.go) -- so one shard's release must take away its own rows only.
//
// SETUP: shards A and B sharing destination 0's gauge, each with one row
// quarantined (gauge 2).
//
// STIMULUS: A sees the reload first and releases; its row is rejected again;
// then B sees the reload and releases.
//
// ASSERTION: after A's release the gauge is 1 (B's row); after A's re-reject 2;
// after B's release 1 (A's row).
//
// Non-vacuity: each release really moved a row (quarantine_released 1 on the
// releasing shard), so the gauge readings are not two idle shards.
//
// WOULD HAVE CAUGHT: V2 section 9.5's "publish rows_quarantined_current = 0",
// correct only with one shard: the alerting gauge reading 0 while B holds a row.
func TestReleaseQuarantineLeavesOtherShardsInTheGauge(t *testing.T) {
	a, qa, setA := newEpochShard(t, 8, 1000)
	b, qb, setB := newEpochShard(t, 8, 1000)
	b.inst.RowsQuarantinedCurrent = a.inst.RowsQuarantinedCurrent // one gauge per destination, as app wires it
	gauge := a.inst.RowsQuarantinedCurrent[tokenQuote]
	xa, xb := newSender7208(t, a), newSender7208(t, b)
	quarantineRow(t, a, qa, xa, 'A', 1000)
	quarantineRow(t, b, qb, xb, 'B', 1000)
	if gauge.Load() != 2 {
		t.Fatalf("setup: gauge %d, want 2", gauge.Load())
	}

	setA(&Params{Gen: 2, MaxBatchSize: 1000, Guard: GuardAuto})
	xa.send('X', fv{0, be32(7)})
	if err := a.maybeOpenEpoch(); err != nil {
		t.Fatalf("A open: %v", err)
	}
	if a.inst.QuarantineReleased[tokenQuote].Load() != 1 || gauge.Load() != 1 {
		t.Fatalf("after A's release: released %d gauge %d; want 1, 1 (B still holds a row)",
			a.inst.QuarantineReleased[tokenQuote].Load(), gauge.Load())
	}
	commitNext(t, a, qa) // X
	if err := a.maybeOpenEpoch(); err != nil {
		t.Fatalf("A open: %v", err)
	}
	if err := writeBack(a, next(t, qa), allRejected()); err != nil {
		t.Fatalf("A re-reject: %v", err)
	}
	if gauge.Load() != 2 {
		t.Fatalf("after A's re-reject: gauge %d, want 2", gauge.Load())
	}

	setB(&Params{Gen: 2, MaxBatchSize: 1000, Guard: GuardAuto})
	xb.send('X', fv{0, be32(7)})
	if err := b.maybeOpenEpoch(); err != nil {
		t.Fatalf("B open: %v", err)
	}
	if b.inst.QuarantineReleased[tokenQuote].Load() != 1 || gauge.Load() != 1 {
		t.Fatalf("after B's release: released %d gauge %d; want 1, 1 (A still holds a row)",
			b.inst.QuarantineReleased[tokenQuote].Load(), gauge.Load())
	}
}

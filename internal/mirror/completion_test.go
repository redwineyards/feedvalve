package mirror

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// next takes the next batch off q without blocking: every test below drives
// the shard synchronously, so a batch that is not already there never comes.
func next(t testing.TB, q chan *model.Batch) *model.Batch {
	t.Helper()
	select {
	case b := <-q:
		return b
	default:
		t.Fatal("no batch on batchQ")
		return nil
	}
}

// writeBack is one writer round trip done in-line: adjudicate b exactly as the
// fake writer's mode would (it writes only Outcome, ErrCode and Err), then hand
// it to the completion path.
func writeBack(s *Shard, b *model.Batch, mode fakeMode) error {
	mode.adjudicate(b)
	return s.applyCompletion(b)
}

// sender7208 merges 7208 contributions into destination 0 of s, one unit per
// call, with ever-increasing sequence numbers so the guard always applies.
type sender7208 struct {
	t    testing.TB
	s    *Shard
	slot uint8
	seq  int32
}

func newSender7208(t testing.TB, s *Shard) *sender7208 {
	return &sender7208{t: t, s: s, slot: slotOf(t, s, tokenQuote, 7208)}
}

func (x *sender7208) send(row model.RowKey, fs ...fv) {
	x.seq++
	u := model.UpdateUnit{Code: 7208, Seq: x.seq, LogTime: 100, RecvMono: int64(x.seq)}
	put(x.t, x.s, &u, tokenQuote, row, x.slot, fs...)
	x.s.merge(u)
}

// imageDigest is digest(entry.Image) for destination d -- what a completion
// that recomputed the digest would store.
func imageDigest(s *Shard, d model.DestID, e *model.Entry) uint64 {
	return digest(s.seed, e.Image[:s.dst.ByID(d).ImageLen])
}

// rowsOf is the row keys of b's active rows, in batch order.
func rowsOf(b *model.Batch) []model.RowKey {
	var out []model.RowKey
	for i := range b.Active() {
		out = append(out, b.Rows[i].Row)
	}
	return out
}

// slotsOf is the arena slots of rows in destination d.
func slotsOf(s *Shard, d model.DestID, rows ...model.RowKey) []int32 {
	var out []int32
	for _, r := range rows {
		out = append(out, s.Index[d][r])
	}
	return out
}

// TestG_MASK_1_MidFlightColumnSurvivesCommit is gate G-MASK-1 (V4:1618).
//
// SETUP: one destination-0 row, ltp 1000, batched and in flight.
//
// STIMULUS: ltp 2000 merged -- a column the batch also wrote -- then the batch
// completes Committed.
//
// ASSERTION: after the commit ColMask still names the column; the next epoch's
// batch carries it, with the newer value.
//
// Non-vacuity: the batch did write the column (its Mask has the bit), so a
// completion that cleared the written bits would clear exactly this one.
//
// WOULD HAVE CAUGHT: HLD section 6.7 / R14 -- clearing ColMask at completion
// instead of at fill, which drops every column re-dirtied in flight.
func TestG_MASK_1_MidFlightColumnSurvivesCommit(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	x.send(7, fv{0, be32(1000)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	if !b.Rows[0].Mask.Has(0) {
		t.Fatal("non-vacuity: the batch did not write ltp")
	}
	x.send(7, fv{0, be32(2000)})

	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("completion: %v", err)
	}
	e := rowEntry(s, tokenQuote, 7)
	if !e.ColMask.Has(0) {
		t.Fatalf("ColMask = %#x after the commit: the mid-flight column was cleared at completion", e.ColMask)
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	b = next(t, q)
	if b.N != 1 || !b.Rows[0].Mask.Has(0) || ltp(&b.Rows[0].Image) != 2000 {
		t.Fatalf("next batch: %d rows, Mask %#x, ltp %d; want row 7 naming ltp at 2000",
			b.N, b.Rows[0].Mask, ltp(&b.Rows[0].Image))
	}
}

// TestG_DIG_1_CommitStoresTheBatchDigest is gate G-DIG-1 (V4:1620).
//
// SETUP: a row batched at ltp 1000, then re-merged at 2000 while in flight.
//
// STIMULUS: the batch completes Committed.
//
// ASSERTION: Entry.Digest == BatchRow.Digest, the digest of the image the
// batch carried, and != digest(entry.Image).
//
// Non-vacuity: the two digests differ, so the assertion can tell them apart.
//
// WOULD HAVE CAUGHT: recomputing the digest at completion (HLD section 7.12
// risk 8), which records the newer image as written and lets NFR13 suppress
// it permanently.
func TestG_DIG_1_CommitStoresTheBatchDigest(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	x.send(7, fv{0, be32(1000)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	batchDigest := b.Rows[0].Digest
	x.send(7, fv{0, be32(2000)})
	e := rowEntry(s, tokenQuote, 7)
	if imageDigest(s, tokenQuote, e) == batchDigest {
		t.Fatal("non-vacuity: the mid-flight image digests like the batch's")
	}

	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("completion: %v", err)
	}
	if e.Digest != batchDigest {
		t.Fatalf("Entry.Digest = %#x, want the batch's %#x", e.Digest, batchDigest)
	}
	if e.Digest == imageDigest(s, tokenQuote, e) {
		t.Fatal("Entry.Digest is the current image's: the digest was recomputed at completion")
	}
}

// TestG_DIG_2_FirstZeroValueIsNotSuppressed is gate G-DIG-2 (V4:1621).
//
// SETUP: a row that has ALREADY committed once (Digest != 0, Written = ltp),
// so the predicate's Digest == 0 short-circuit (V2:996) cannot decide.
//
// STIMULUS: a rate-limited merge of volume_traded_today's first-ever value, 0
// -- equal to the zero fill, its bit not in Written; then flush and commit.
//
// ASSERTION: not suppressed; the batch names the column; on commit Written
// gains the bit.
//
// Non-vacuity: at the merge the image digest still equals Entry.Digest (the
// zero value changed no byte), so only the Written clause can have decided;
// and once Written has the bit the same merge IS suppressed.
//
// WOULD HAVE CAUGHT: ADR-L04's permanent NULL -- a first-ever value equal to
// the zero fill suppressed because the digest matches, so no statement ever
// names the column.
func TestG_DIG_2_FirstZeroValueIsNotSuppressed(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	x.send(1, fv{0, be32(500)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := writeBack(s, next(t, q), allCommitted()); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	e := rowEntry(s, tokenQuote, 1)
	if e.Digest == 0 || e.Written != model.Bit(0) || e.Dirty {
		t.Fatalf("setup: Digest %#x Written %#x Dirty %v; want an earlier commit of ltp alone", e.Digest, e.Written, e.Dirty)
	}

	x.send(1, fv{1, be32(0)})
	if imageDigest(s, tokenQuote, e) != e.Digest {
		t.Fatal("non-vacuity: the zero value changed the image, so the digest clause alone would decide")
	}
	if s.inst.ChangeSuppressed.Load() != 0 || !e.Dirty {
		t.Fatalf("first-ever zero value: change_suppressed %d Dirty %v; want not suppressed",
			s.inst.ChangeSuppressed.Load(), e.Dirty)
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	b := next(t, q)
	if !b.Rows[0].Mask.Has(1) {
		t.Fatalf("batch Mask %#x does not name volume_traded_today", b.Rows[0].Mask)
	}
	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("second completion: %v", err)
	}
	if !e.Written.Has(1) {
		t.Fatalf("Written = %#x after the commit, want the bit gained", e.Written)
	}

	x.send(1, fv{1, be32(0)})
	if s.inst.ChangeSuppressed.Load() != 1 {
		t.Fatal("non-vacuity: with the bit in Written the same value must now be suppressed")
	}
}

// TestG_INFL_2_InFlightMergeIsDirtyAgainAfterCompletion is gate G-INFL-2
// (V4:1640).
//
// SETUP: a row batched with ltp 1000.
//
// STIMULUS: volume_traded_today merged while in flight, then the batch
// completes Committed.
//
// ASSERTION: the row is Dirty again, on the accumulating ring at the TAIL --
// behind Z, dirtied while the batch was out, because its values arrived later
// -- and the next flush carries the newer image (both columns).
//
// Non-vacuity: before the completion the row is ReDirty and on no ring, so the
// requeue is the completion's doing; and Z is already queued, so a head push
// would put the row in front of it.
//
// WOULD HAVE CAUGHT: HLD R15 -- a row losing the update that arrived in
// flight, sitting clean with an image never written.
func TestG_INFL_2_InFlightMergeIsDirtyAgainAfterCompletion(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	x.send(7, fv{0, be32(1000)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	x.send(7, fv{1, be32(77)})
	x.send('Z', fv{0, be32(5)})
	e := rowEntry(s, tokenQuote, 7)
	if !e.ReDirty || e.Dirty || !slices.Equal(queued(s, tokenQuote), slotsOf(s, tokenQuote, 'Z')) {
		t.Fatal("non-vacuity: the in-flight merge must leave the row ReDirty and off every ring, Z alone queued")
	}

	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("completion: %v", err)
	}
	if !e.Dirty || e.ReDirty || e.InFlight || !slices.Equal(queued(s, tokenQuote), slotsOf(s, tokenQuote, 'Z', 7)) {
		t.Fatalf("after commit: Dirty %v ReDirty %v InFlight %v queued %v; want dirty again, at the tail behind Z",
			e.Dirty, e.ReDirty, e.InFlight, queued(s, tokenQuote))
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	b = next(t, q)
	if !slices.Equal(rowsOf(b), []model.RowKey{'Z', 7}) {
		t.Fatalf("next flush carries %q, want Z then row 7", rowsOf(b))
	}
	r := &b.Rows[1]
	col := s.dst.ByID(tokenQuote).Column(1)
	vol := r.Image[col.Offset:col.End()]
	if r.Mask != model.Bit(1) || ltp(&r.Image) != 1000 || !bytes.Equal(vol, be32(77)) {
		t.Fatalf("next flush: Mask %#x ltp %d volume % x; want volume 77 named over the committed ltp 1000",
			r.Mask, ltp(&r.Image), vol)
	}
}

// TestG_EPOCH_1_OneTickFlushesEveryDirtyRow is gate G-EPOCH-1 (V4:1635).
//
// SETUP: N = 11 dirty rows, flush.maxBatchSize 3, m = 2, two fake writers in
// allCommitted.
//
// STIMULUS: one epoch open, then every completion applied as it comes back.
//
// ASSERTION: all N rows are committed before the epoch closes, by exactly
// ceil(N/3) = 4 batches, every one stamped with the one epoch; exactly one
// epoch opened.
//
// Non-vacuity: N exceeds m x maxBatchSize, so the epoch can only finish
// through completion-driven pumps, not the opening pump alone.
//
// WOULD HAVE CAUGHT: ADR-018's defect -- a tick flushing only part of the
// dirty set, or an epoch closing with batches still out.
func TestG_EPOCH_1_OneTickFlushesEveryDirtyRow(t *testing.T) {
	const n, maxBatch = 11, 3
	s, q, _ := newEpochShard(t, 8, maxBatch)
	completion := []chan *model.Batch{make(chan *model.Batch, 2)}
	w := startFakeWriters(2, allCommitted(), q, completion)
	stop := w.stopper(t, make(chan struct{}), q)
	for row := range model.RowKey(n) {
		dirty(s, tokenQuote, row, 1)
	}
	if n <= 2*maxBatch {
		t.Fatal("non-vacuity: N must exceed what the opening pump can send")
	}

	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	committed, batches := 0, 0
	for s.EpochOpen {
		b := <-completion[0]
		if b.Epoch != 1 {
			t.Fatalf("a batch of epoch %d, want 1", b.Epoch)
		}
		batches++
		committed += int(b.N)
		if err := s.applyCompletion(b); err != nil {
			t.Fatalf("completion: %v", err)
		}
	}
	stop()

	if committed != n || batches != (n+maxBatch-1)/maxBatch {
		t.Fatalf("epoch closed after %d rows in %d batches, want %d in %d", committed, batches, n, (n+maxBatch-1)/maxBatch)
	}
	if s.Epoch != 1 || s.inst.EpochOpened.Load() != 1 || s.CohortOut != 0 || len(s.Free) != 2 {
		t.Fatalf("epoch %d opened %d CohortOut %d Free %d; want 1, 1, 0, 2",
			s.Epoch, s.inst.EpochOpened.Load(), s.CohortOut, len(s.Free))
	}
	for row := range model.RowKey(n) {
		if e := rowEntry(s, tokenQuote, row); e.Dirty || e.InFlight || e.Digest == 0 {
			t.Fatalf("row %d: Dirty %v InFlight %v Digest %#x; want written", row, e.Dirty, e.InFlight, e.Digest)
		}
	}
	if got := s.inst.RowsInflight.Load(); got != 0 {
		t.Fatalf("rows_inflight = %d after the epoch closed, want 0", got)
	}
}

// TestG_EPOCH_2_MidDrainMergeWaitsForTheNextEpoch is gate G-EPOCH-2
// (V4:1636).
//
// SETUP: rows A, B, C dirty; maxBatchSize 1, m = 2: an epoch opens with A and
// B out and C still in the cohort.
//
// STIMULUS: D is merged mid-drain; the epoch's batches complete Committed.
//
// ASSERTION: D lands on the accumulating ring, not the cohort; the epoch
// closes having carried A, B, C only; the next epoch carries D.
//
// WOULD HAVE CAUGHT: HLD section 7.12 risk 9 -- arriving work extending the
// drain, so under load an epoch never terminates.
func TestG_EPOCH_2_MidDrainMergeWaitsForTheNextEpoch(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1)
	x := newSender7208(t, s)
	for _, r := range []model.RowKey{'A', 'B', 'C'} {
		x.send(r, fv{0, be32(1)})
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	x.send('D', fv{0, be32(1)})
	if s.Cohort[tokenQuote].Len() != 1 || !slices.Equal(queued(s, tokenQuote), slotsOf(s, tokenQuote, 'D')) {
		t.Fatalf("mid-drain merge: cohort %d, accum %v; want C alone in the cohort, D on Accum",
			s.Cohort[tokenQuote].Len(), queued(s, tokenQuote))
	}

	var carried []model.RowKey
	for s.EpochOpen {
		b := next(t, q)
		carried = append(carried, rowsOf(b)...)
		if err := writeBack(s, b, allCommitted()); err != nil {
			t.Fatalf("completion: %v", err)
		}
	}
	if !slices.Equal(carried, []model.RowKey{'A', 'B', 'C'}) || s.Epoch != 1 {
		t.Fatalf("epoch %d carried %q, want ABC", s.Epoch, carried)
	}
	if e := rowEntry(s, tokenQuote, 'D'); !e.Dirty || e.Digest != 0 {
		t.Fatal("D was written in the epoch it arrived during")
	}

	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	if b := next(t, q); !slices.Equal(rowsOf(b), []model.RowKey{'D'}) || b.Epoch != 2 {
		t.Fatalf("epoch 2's batch carries %q in epoch %d, want D in 2", rowsOf(b), b.Epoch)
	}
}

// TestG_ROW_1_ThreeOutcomesThreeObligations is gate G-ROW-1 (V4:1648, read
// per the D11 erratum: row_quarantined, not row_rejected, is the shard's).
//
// SETUP: a three-row batch, rows C, R, F, each with a prior commit (Digest,
// Written = ltp), plus a row X dirtied while it is in flight. Every row is
// merged through the real merge, so each value PASSED Vol 0 section 5A
// pre-validation -- the precondition P3-07b sets for the 1438 count below.
//
// STIMULUS: the batch returns Committed, Rejected (ORA-01438), Failed.
//
// ASSERTION, per Vol 0 section 5.4's table:
//   - C: Digest = BatchRow.Digest, Written |= Mask, clean, not requeued.
//   - R: Digest and Written unchanged, ColMask = Mask (the refused columns
//     folded back for the release, P3-10 erratum); on no ring; Quarantined and in
//     Quarantine[0]; row_quarantined and rows_quarantined_current 1; 1438 is
//     predictable and pre-validation passed it, so
//     rows_rejected_by_oracle_unpredicted 1; one ERROR line naming the row and
//     the ORA code.
//   - F: ColMask = Mask, Digest and Written unchanged, Dirty, at the HEAD of
//     the accumulating ring (ahead of X); retry_requeued 1.
//
// WOULD HAVE CAUGHT: collapsing three outcomes into two (defect L3): a
// rejected row requeued forever, or a failed row quarantined by an outage.
func TestG_ROW_1_ThreeOutcomesThreeObligations(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 3)
	var logged bytes.Buffer
	s.log = obs.NewAppLogger(&logged, obs.LevelInfo, 1000, nil)
	x := newSender7208(t, s)
	crf := []model.RowKey{'C', 'R', 'F'}
	for i, r := range crf {
		x.send(r, fv{0, be32(500 + uint32(i))})
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := writeBack(s, next(t, q), allCommitted()); err != nil {
		t.Fatalf("prior commit: %v", err)
	}
	prior := map[model.RowKey]uint64{}
	const priorWritten = model.ColMask(1) // ltp, from the prior commit
	for _, r := range crf {
		e := rowEntry(s, tokenQuote, r)
		if e.Digest == 0 || e.Written != priorWritten || e.Dirty {
			t.Fatalf("setup: row %c Digest %#x Written %#x Dirty %v; want the prior commit of ltp", r, e.Digest, e.Written, e.Dirty)
		}
		prior[r] = e.Digest
	}
	for i, r := range crf {
		x.send(r, fv{0, be32(1000 + uint32(i))}, fv{1, be32(10)})
	}
	if s.inst.ChangeSuppressed.Load() != 0 {
		t.Fatal("setup: a new value was suppressed")
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	b := next(t, q)
	if !slices.Equal(rowsOf(b), crf) {
		t.Fatalf("batch carries %q, want CRF", rowsOf(b))
	}
	cDigest, mask := b.Rows[0].Digest, b.Rows[0].Mask
	if mask != model.Bit(0)|model.Bit(1) {
		t.Fatalf("setup: batch Mask %#x, want ltp and volume", mask)
	}
	x.send('X', fv{0, be32(1)})
	priorDigest := prior['R']

	err := writeBack(s, b, perRow(
		[]model.RowOutcome{model.OutcomeCommitted, model.OutcomeRejected, model.OutcomeFailed},
		[]uint16{0, 1438, 3113}))
	if err != nil {
		t.Fatalf("completion: %v", err)
	}
	c, r, f := rowEntry(s, tokenQuote, 'C'), rowEntry(s, tokenQuote, 'R'), rowEntry(s, tokenQuote, 'F')

	if c.Digest != cDigest || c.Written != priorWritten|mask || c.ColMask != 0 || c.Dirty || c.InFlight {
		t.Fatalf("Committed: Digest %#x Written %#x ColMask %#x Dirty %v InFlight %v",
			c.Digest, c.Written, c.ColMask, c.Dirty, c.InFlight)
	}
	if r.Digest != priorDigest || r.Written != priorWritten || r.ColMask != mask {
		t.Fatalf("Rejected: Digest %#x Written %#x ColMask %#x; want unchanged, unchanged, the refused Mask",
			r.Digest, r.Written, r.ColMask)
	}
	if !r.Quarantined || r.Dirty || r.InFlight || !slices.Equal(s.Quarantine[tokenQuote], slotsOf(s, tokenQuote, 'R')) {
		t.Fatalf("Rejected: Quarantined %v Dirty %v InFlight %v quarantine %v",
			r.Quarantined, r.Dirty, r.InFlight, s.Quarantine[tokenQuote])
	}
	in := s.inst
	if in.RowQuarantined[tokenQuote].Load() != 1 || in.RowsQuarantinedCurrent[tokenQuote].Load() != 1 ||
		in.RowsRejectedByOracleUnpredicted[tokenQuote].Load() != 1 || in.QuarantineSkipped[tokenQuote].Load() != 0 {
		t.Fatalf("row_quarantined %d current %d unpredicted %d skipped %d; want 1 1 1 0",
			in.RowQuarantined[tokenQuote].Load(), in.RowsQuarantinedCurrent[tokenQuote].Load(),
			in.RowsRejectedByOracleUnpredicted[tokenQuote].Load(), in.QuarantineSkipped[tokenQuote].Load())
	}
	if line := logged.String(); strings.Count(line, "ERROR") != 1 ||
		!strings.Contains(line, "row quarantined") || !strings.Contains(line, "row=82") || !strings.Contains(line, "ora=1438") {
		t.Fatalf("quarantine log = %q, want one ERROR naming row 82 ('R') and ora=1438", line)
	}

	if f.ColMask != mask || f.Digest != prior['F'] || f.Written != priorWritten || !f.Dirty || f.InFlight || f.Quarantined {
		t.Fatalf("Failed: ColMask %#x Digest %#x Written %#x Dirty %v InFlight %v Quarantined %v",
			f.ColMask, f.Digest, f.Written, f.Dirty, f.InFlight, f.Quarantined)
	}
	if got := queued(s, tokenQuote); !slices.Equal(got, slotsOf(s, tokenQuote, 'F', 'X')) {
		t.Fatalf("accumulating ring %v, want F at the head then X %v", got, slotsOf(s, tokenQuote, 'F', 'X'))
	}
	if in.RetryRequeued.Load() != 1 || in.BatchRowUnreported.Load() != 0 {
		t.Fatalf("retry_requeued %d, batch_row_unreported %d; want 1, 0", in.RetryRequeued.Load(), in.BatchRowUnreported.Load())
	}
}

// TestG_ROW_2_BatchErrorRequeuesEveryRowInOrder is gate G-ROW-2 (V4:1649).
//
// SETUP: a four-row batch in flight, and a row Y dirtied meanwhile.
//
// STIMULUS: the batch returns with Err != nil and every row Pending, each
// carrying an ORA code.
//
// ASSERTION: every row is treated as Failed -- ColMask restored, Dirty -- and
// requeued at the head IN ORIGINAL ORDER, ahead of Y; no writer defect is
// reported (batch_row_unreported 0, nil error); dirty_depth counts all five
// and cohort_depth 0; the batch is back in Free, reset by step 9 (N, Err, the
// segment table, every row's Outcome and ErrCode).
//
// Non-vacuity: four distinct rows, so a one-at-a-time head push would reverse
// them and fail the order check.
//
// WOULD HAVE CAUGHT: defect L4's missing fallback branch, stranding every row
// of a batch that never reached the server; or a head push that inverts FIFO.
func TestG_ROW_2_BatchErrorRequeuesEveryRowInOrder(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 4)
	for _, r := range []model.RowKey{1, 2, 3, 4} {
		dirty(s, tokenQuote, r, model.Bit(0))
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	dirty(s, tokenQuote, 'Y', 1)

	// allFailedErr, with an ORA code left on every row so step 9's reset of
	// ErrCode is observable below.
	b.Err = errFakeBatch
	for j := range b.Active() {
		b.Rows[j].ErrCode = 3113
	}
	if err := s.applyCompletion(b); err != nil {
		t.Fatalf("completion: %v", err)
	}
	if got, want := queued(s, tokenQuote), slotsOf(s, tokenQuote, 1, 2, 3, 4, 'Y'); !slices.Equal(got, want) {
		t.Fatalf("accumulating ring %v, want %v: the batch's rows at the head, in order", got, want)
	}
	for _, r := range []model.RowKey{1, 2, 3, 4} {
		if e := rowEntry(s, tokenQuote, r); !e.Dirty || e.InFlight || e.ColMask != model.Bit(0) || e.Quarantined {
			t.Fatalf("row %d: Dirty %v InFlight %v ColMask %#x Quarantined %v; want Failed",
				r, e.Dirty, e.InFlight, e.ColMask, e.Quarantined)
		}
	}
	if s.inst.RetryRequeued.Load() != 4 || s.inst.BatchRowUnreported.Load() != 0 || len(s.Free) != 2 || s.EpochOpen {
		t.Fatalf("retry_requeued %d unreported %d Free %d open %v; want 4, 0, 2, closed",
			s.inst.RetryRequeued.Load(), s.inst.BatchRowUnreported.Load(), len(s.Free), s.EpochOpen)
	}
	if got, want := s.inst.DirtyDepth[tokenQuote].Load(), int64(5); got != want || s.inst.CohortDepth[tokenQuote].Load() != 0 {
		t.Fatalf("dirty_depth %d cohort_depth %d; want %d and 0", got, s.inst.CohortDepth[tokenQuote].Load(), want)
	}
	// Step 9: the batch is back in Free reset -- the writer's verdicts and the
	// segment table cleared over the range N named, before N went to 0.
	if !slices.Contains(s.Free, b) || b.N != 0 || b.Err != nil ||
		b.DestStart != [model.MaxDestinations]int32{} || b.DestCount != [model.MaxDestinations]int32{} {
		t.Fatalf("returned batch: in Free %v N %d Err %v DestStart %v DestCount %v; want reset",
			slices.Contains(s.Free, b), b.N, b.Err, b.DestStart, b.DestCount)
	}
	for j := range 4 {
		if r := b.Rows[j]; r.Outcome != model.OutcomePending || r.ErrCode != 0 {
			t.Fatalf("Rows[%d]: Outcome %v ErrCode %d; want Pending and 0", j, r.Outcome, r.ErrCode)
		}
	}
}

// TestG_ROW_3_UnreportedRowIsCountedRequeuedAndReported is gate G-ROW-3
// (V4:1650).
//
// SETUP: a three-row batch in flight, and a row Y dirtied meanwhile.
//
// STIMULUS: the batch returns with Err == nil and row 1 still Pending.
//
// ASSERTION: batch_row_unreported{k} 1; the Pending row is requeued at the
// head ahead of Y; the other two committed; the batch is back in Free; the
// error is errBatchRowUnreported -- distinguishable, so Run can report it and
// keep running (ruling D9).
//
// Non-vacuity: the same shape fully adjudicated returns nil and counts nothing.
//
// WOULD HAVE CAUGHT: a writer returning a batch it never adjudicated (F27),
// undetectable without the counter -- or detected but with the row lost.
func TestG_ROW_3_UnreportedRowIsCountedRequeuedAndReported(t *testing.T) {
	run := func(mode fakeMode) (*Shard, error) {
		s, q, _ := newEpochShard(t, 8, 3)
		for _, r := range []model.RowKey{1, 2, 3} {
			dirty(s, tokenQuote, r, model.Bit(0))
		}
		if err := s.maybeOpenEpoch(); err != nil {
			t.Fatalf("open: %v", err)
		}
		b := next(t, q)
		dirty(s, tokenQuote, 'Y', 1)
		return s, writeBack(s, b, mode)
	}

	s, err := run(unreportedRow(1))
	if !errors.Is(err, errBatchRowUnreported) {
		t.Fatalf("err = %v, want errBatchRowUnreported", err)
	}
	if got := s.inst.BatchRowUnreported.Load(); got != 1 {
		t.Fatalf("batch_row_unreported = %d, want 1", got)
	}
	if got, want := queued(s, tokenQuote), slotsOf(s, tokenQuote, 2, 'Y'); !slices.Equal(got, want) {
		t.Fatalf("accumulating ring %v, want %v: the unreported row first", got, want)
	}
	if e := rowEntry(s, tokenQuote, 2); !e.Dirty || e.InFlight || e.ColMask != model.Bit(0) {
		t.Fatal("the unreported row was not restored as Failed")
	}
	for _, r := range []model.RowKey{1, 3} {
		if e := rowEntry(s, tokenQuote, r); e.Dirty || e.InFlight || e.Digest == 0 {
			t.Fatalf("row %d was not committed", r)
		}
	}
	if len(s.Free) != 2 || s.CohortOut != 0 || s.EpochOpen {
		t.Fatalf("Free %d CohortOut %d open %v: the shard's state is not consistent after S3", len(s.Free), s.CohortOut, s.EpochOpen)
	}

	s, err = run(allCommitted())
	if err != nil || s.inst.BatchRowUnreported.Load() != 0 {
		t.Fatalf("non-vacuity: a fully adjudicated batch gave err %v, unreported %d", err, s.inst.BatchRowUnreported.Load())
	}
}

// TestCompletionHeadPushPrecedesTheReopen is ruling D8, adversarially: the
// completion that empties the cohort carries a Failed row, and a flush request
// makes closeEpoch re-open at once. The failed row must head the NEXT epoch's
// first batch, ahead of C, which was dirtied during the epoch.
//
// WOULD HAVE CAUGHT: step 13 before step 14 -- the re-open swaps in a ring
// without the failed row, which then waits a whole further epoch (G-EPOCH-3).
func TestCompletionHeadPushPrecedesTheReopen(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1)
	dirty(s, tokenQuote, 'A', 1)
	dirty(s, tokenQuote, 'B', 1)
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	a, b := next(t, q), next(t, q)
	dirty(s, tokenQuote, 'C', 1)
	s.bus.req[s.ID].Store(true) // a non-rate-limited request raised mid-epoch

	if err := writeBack(s, a, allCommitted()); err != nil || !s.EpochOpen {
		t.Fatalf("first completion: err %v open %v; the epoch must stay open with B out", err, s.EpochOpen)
	}
	if got, want := s.inst.DirtyDepth[tokenQuote].Load(), int64(s.Accum[tokenQuote].Len()); got != want || want != 1 {
		t.Fatalf("step 12: dirty_depth %d with the epoch still open, want C's %d", got, want)
	}
	if err := writeBack(s, b, allFailedErr()); err != nil {
		t.Fatalf("second completion: %v", err)
	}
	if s.Epoch != 2 || !s.EpochOpen || s.bus.Pending(s.ID) {
		t.Fatalf("epoch %d open %v pending %v: closeEpoch did not re-open on the request", s.Epoch, s.EpochOpen, s.bus.Pending(s.ID))
	}
	first, second := next(t, q), next(t, q)
	if !slices.Equal(rowsOf(first), []model.RowKey{'B'}) || !slices.Equal(rowsOf(second), []model.RowKey{'C'}) ||
		first.Epoch != 2 {
		t.Fatalf("epoch 2's batches carry %q then %q, want B (the retry) then C", rowsOf(first), rowsOf(second))
	}
}

// TestCompletionRejectsAMisroutedBatch is step 0a both ways -- a batch of
// another shard and one stamped with a future epoch -- plus one this shard
// does not own: each is an error and touches nothing. Non-vacuity: the same
// batch, its fields restored, applies cleanly.
func TestCompletionRejectsAMisroutedBatch(t *testing.T) {
	cases := []struct {
		name string
		bad  func(s *Shard, b *model.Batch) *model.Batch
	}{
		{"wrong shard", func(_ *Shard, b *model.Batch) *model.Batch { b.Shard = 1; return b }},
		{"future epoch", func(s *Shard, b *model.Batch) *model.Batch { b.Epoch = s.Epoch + 1; return b }},
		{"not owned", func(_ *Shard, b *model.Batch) *model.Batch { c := *b; return &c }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, q, _ := newEpochShard(t, 8, 1000)
			dirty(s, tokenQuote, 1, 1)
			if err := s.maybeOpenEpoch(); err != nil {
				t.Fatalf("open: %v", err)
			}
			b := next(t, q)
			commitAll(b)
			shard, epoch := b.Shard, b.Epoch

			if err := s.applyCompletion(tc.bad(s, b)); err == nil || !strings.Contains(err.Error(), "routing defect") {
				t.Fatalf("err = %v, want the routing defect", err)
			}
			if e := rowEntry(s, tokenQuote, 1); !e.InFlight || len(s.Free) != 1 || s.CohortOut != 1 || b.N != 1 {
				t.Fatal("a rejected completion touched the shard or the batch")
			}

			b.Shard, b.Epoch = shard, epoch
			if err := s.applyCompletion(b); err != nil || s.EpochOpen {
				t.Fatalf("non-vacuity: the restored batch: err %v open %v", err, s.EpochOpen)
			}
		})
	}
}

// TestCompletionInFlightDesyncIsFatal is S2: a completion naming a row whose
// InFlight was cleared behind the API is an error counted in inflight_desync.
// Non-vacuity: the rows before it in the walk were applied, so the check is
// per row and not a precondition on the whole batch.
func TestCompletionInFlightDesyncIsFatal(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	dirty(s, tokenQuote, 1, 1)
	dirty(s, tokenQuote, 2, 1)
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	rowEntry(s, tokenQuote, 2).InFlight = false // behind the API

	err := writeBack(s, b, allCommitted())
	if err == nil || !strings.Contains(err.Error(), "S2") {
		t.Fatalf("err = %v, want defect S2", err)
	}
	if got := s.inst.InflightDesync.Load(); got != 1 {
		t.Fatalf("inflight_desync = %d, want 1", got)
	}
	if rowEntry(s, tokenQuote, 1).Digest == 0 {
		t.Fatal("non-vacuity: row 1, ahead of the desync, was not applied")
	}
}

// TestCompletionV0Delta1DisjointMaskRelease is delta V0-Delta-1 both ways: a
// rejected row re-dirtied in flight is released at once when the mid-flight
// columns avoid the refused ones -- at the TAIL, behind Z, dirtied while the
// batch was out -- and stays quarantined when they overlap.
// ORA-00001 is not one pre-validation predicts, so the honesty counter stays 0
// (the non-vacuous half of G-ROW-1's 1438 count).
func TestCompletionV0Delta1DisjointMaskRelease(t *testing.T) {
	for _, tc := range []struct {
		name     string
		midBit   uint8
		released bool
	}{{"disjoint", 1, true}, {"overlapping", 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, q, _ := newEpochShard(t, 8, 1000)
			x := newSender7208(t, s)
			x.send(7, fv{0, be32(1000)})
			if err := s.maybeOpenEpoch(); err != nil {
				t.Fatalf("open: %v", err)
			}
			b := next(t, q)
			x.send(7, fv{tc.midBit, be32(2000)})
			x.send('Z', fv{0, be32(5)}) // queued first: a release goes behind it
			e := rowEntry(s, tokenQuote, 7)
			if !e.ReDirty {
				t.Fatal("setup: the mid-flight merge did not re-dirty the row")
			}

			err := writeBack(s, b, perRow([]model.RowOutcome{model.OutcomeRejected}, []uint16{1}))
			if err != nil {
				t.Fatalf("completion: %v", err)
			}
			in := s.inst
			if e.Quarantined == tc.released || e.Dirty != tc.released || e.ReDirty || e.InFlight {
				t.Fatalf("Quarantined %v Dirty %v ReDirty %v InFlight %v; want released %v",
					e.Quarantined, e.Dirty, e.ReDirty, e.InFlight, tc.released)
			}
			if e.ColMask != model.Bit(tc.midBit) || e.Digest != 0 || e.Written != 0 {
				t.Fatalf("ColMask %#x Digest %#x Written %#x; want the mid-flight column, nothing written",
					e.ColMask, e.Digest, e.Written)
			}
			wantQueued, wantCurrent, wantSkipped := slotsOf(s, tokenQuote, 'Z'), int64(1), uint64(0)
			if tc.released {
				wantQueued, wantCurrent, wantSkipped = slotsOf(s, tokenQuote, 'Z', 7), 0, 1 // TAIL
			}
			if got := queued(s, tokenQuote); !slices.Equal(got, wantQueued) {
				t.Fatalf("accumulating ring %v, want %v", got, wantQueued)
			}
			if len(s.Quarantine[tokenQuote]) != int(wantCurrent) || in.RowsQuarantinedCurrent[tokenQuote].Load() != wantCurrent ||
				in.QuarantineSkipped[tokenQuote].Load() != wantSkipped || in.RowQuarantined[tokenQuote].Load() != 1 {
				t.Fatalf("quarantine %v current %d skipped %d row_quarantined %d; want %d %d 1",
					s.Quarantine[tokenQuote], in.RowsQuarantinedCurrent[tokenQuote].Load(),
					in.QuarantineSkipped[tokenQuote].Load(), in.RowQuarantined[tokenQuote].Load(), wantCurrent, wantSkipped)
			}
			if in.RowsRejectedByOracleUnpredicted[tokenQuote].Load() != 0 {
				t.Fatal("ORA-00001 counted as a refusal pre-validation should have predicted")
			}
		})
	}
}

// TestCompletionFailedFoldsInTheMidFlightColumns is the Failed arm on a row
// re-merged in flight (Vol 2 section 8.4): ColMask gets back the columns the
// pump cleared OR-ed with the one the merge added, ReDirty is folded in and
// cleared, and the row heads the ring -- ahead of Z, dirtied meanwhile -- so
// the next epoch's batch carries both columns.
//
// WOULD HAVE CAUGHT: ColMask = Mask, dropping the mid-flight column; and a
// ReDirty left set beside Dirty, which the next fillRow refuses as INV-STATE --
// fatal, on every batch of a database outage.
func TestCompletionFailedFoldsInTheMidFlightColumns(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	x.send(7, fv{0, be32(1000)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	x.send(7, fv{1, be32(77)})
	x.send('Z', fv{0, be32(5)})
	e := rowEntry(s, tokenQuote, 7)
	if !e.ReDirty || e.ColMask != model.Bit(1) || b.Rows[0].Mask != model.Bit(0) {
		t.Fatalf("non-vacuity: ReDirty %v ColMask %#x batch Mask %#x; want the merge to add volume alone",
			e.ReDirty, e.ColMask, b.Rows[0].Mask)
	}

	if err := writeBack(s, b, allFailedErr()); err != nil {
		t.Fatalf("completion: %v", err)
	}
	if e.ColMask != model.Bit(0)|model.Bit(1) || e.ReDirty || !e.Dirty || e.InFlight {
		t.Fatalf("after Failed: ColMask %#x ReDirty %v Dirty %v InFlight %v; want both columns, Dirty alone",
			e.ColMask, e.ReDirty, e.Dirty, e.InFlight)
	}
	if got, want := queued(s, tokenQuote), slotsOf(s, tokenQuote, 7, 'Z'); !slices.Equal(got, want) {
		t.Fatalf("accumulating ring %v, want %v: the retry at the head", got, want)
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	b = next(t, q)
	if !slices.Equal(rowsOf(b), []model.RowKey{7, 'Z'}) || b.Rows[0].Mask != model.Bit(0)|model.Bit(1) {
		t.Fatalf("next batch carries %q, row 7 Mask %#x; want 7 first naming both columns", rowsOf(b), b.Rows[0].Mask)
	}
}

// TestCompletionReopensOnAMissedTickWithWork is closeEpoch's step 4 (Vol 2
// section 6.4) on its tick half: the last completion re-opens at once when a
// tick was missed while the epoch was open AND the accumulating ring has work.
// No flush request is raised in any case, so the request half cannot decide.
//
//   - missed, work: epoch 2 opens and carries the row dirtied mid-epoch.
//   - missed, nothing: no re-open, and epoch_empty is NOT counted -- an empty
//     ring is not an epoch attempt.
//   - not missed, work (non-vacuity): no re-open; the row waits for the tick.
//
// Every case closes with cohort_depth 0.
func TestCompletionReopensOnAMissedTickWithWork(t *testing.T) {
	for _, tc := range []struct {
		name         string
		missed, work bool
		reopen       bool
	}{
		{"missed, work", true, true, true},
		{"missed, nothing", true, false, false},
		{"not missed, work", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, q, _ := newEpochShard(t, 8, 1000)
			dirty(s, tokenQuote, 'A', 1)
			if err := s.maybeOpenEpoch(); err != nil {
				t.Fatalf("open: %v", err)
			}
			a := next(t, q)
			s.TickMissed = tc.missed
			if tc.work {
				dirty(s, tokenQuote, 'C', 1)
			}
			empty := s.inst.EpochEmpty.Load()

			if err := writeBack(s, a, allCommitted()); err != nil {
				t.Fatalf("completion: %v", err)
			}
			if s.bus.Pending(s.ID) {
				t.Fatal("a flush request appeared: the request half could have decided")
			}
			if tc.reopen {
				if s.Epoch != 2 || !s.EpochOpen {
					t.Fatalf("epoch %d open %v; want epoch 2 re-opened on the missed tick", s.Epoch, s.EpochOpen)
				}
				if b := next(t, q); !slices.Equal(rowsOf(b), []model.RowKey{'C'}) || b.Epoch != 2 {
					t.Fatalf("epoch 2's batch carries %q in epoch %d, want C in 2", rowsOf(b), b.Epoch)
				}
				return
			}
			if s.Epoch != 1 || s.EpochOpen || len(q) != 0 || s.inst.EpochEmpty.Load() != empty {
				t.Fatalf("epoch %d open %v queued batches %d epoch_empty %d (was %d); want closed, nothing sent, uncounted",
					s.Epoch, s.EpochOpen, len(q), s.inst.EpochEmpty.Load(), empty)
			}
			if got := s.inst.CohortDepth[tokenQuote].Load(); got != 0 {
				t.Fatalf("cohort_depth = %d after close, want 0", got)
			}
		})
	}
}

// TestCompletionObservesLagAndEpochDuration is completion_lag_max_ns (section
// 8.5) and closeEpoch's epoch_duration_max_ns and epoch_ns_total (section 6.4
// steps 2-3). The clock is moved rather than waited on -- Windows' resolution
// is too coarse for a > 0 check -- by setting the batch's send time and the
// epoch's open time back one second, so each metric must reach a second.
func TestCompletionObservesLagAndEpochDuration(t *testing.T) {
	const second = int64(1e9)
	s, q, _ := newEpochShard(t, 8, 1000)
	dirty(s, tokenQuote, 'A', 1)
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := next(t, q)
	s.sentAt[s.batchIndex(b)] -= second
	s.epochOpenedAt -= second

	if err := writeBack(s, b, allCommitted()); err != nil || s.EpochOpen {
		t.Fatalf("completion: err %v open %v; want the epoch closed", err, s.EpochOpen)
	}
	in := s.inst
	if lag := in.CompletionLagMaxNs.Load(); lag < uint64(second) {
		t.Fatalf("completion_lag_max_ns = %d, want >= %d", lag, second)
	}
	if dur, total := in.EpochDurationMaxNs.Load(), in.EpochNsTotal.Load(); dur < uint64(second) || total < dur {
		t.Fatalf("epoch_duration_max_ns %d epoch_ns_total %d; want >= %d, total >= max", dur, total, second)
	}
}

// TestCompletionRejectsABatchReturnedTwice is step 0a's ownership check
// widened to "is this batch out": a batch completed twice would enter Free
// twice, close the epoch with another batch still in flight, and let the next
// pump hand one *Batch to two writers. The second return is a routing defect
// and touches nothing. Non-vacuity: the other batch, genuinely out, applies.
func TestCompletionRejectsABatchReturnedTwice(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1)
	dirty(s, tokenQuote, 'A', 1)
	dirty(s, tokenQuote, 'B', 1)
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	a, b := next(t, q), next(t, q)
	if err := writeBack(s, a, allCommitted()); err != nil {
		t.Fatalf("first completion: %v", err)
	}

	if err := s.applyCompletion(a); err == nil || !strings.Contains(err.Error(), "returned twice") {
		t.Fatalf("second return of a: err %v, want the routing defect", err)
	}
	if len(s.Free) != 1 || s.CohortOut != 1 || !s.EpochOpen {
		t.Fatalf("Free %d CohortOut %d open %v after the rejected return; want 1, 1, open", len(s.Free), s.CohortOut, s.EpochOpen)
	}
	if err := writeBack(s, b, allCommitted()); err != nil || s.EpochOpen || len(s.Free) != 2 {
		t.Fatalf("non-vacuity: b's completion: err %v open %v Free %d", err, s.EpochOpen, len(s.Free))
	}
}

// TestApplyCompletionAllocatesNothing is AllocsPerRun = 0 for applyCompletion
// (V2:2718-2719) over the Committed and Failed arms, across all three
// destinations: each run opens an epoch, and every batch comes back half
// Committed, half Failed until the epoch closes. The Rejected arm is excluded:
// its quarantine append is the one allocation the completion path may make.
func TestApplyCompletionAllocatesNothing(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 4)
	for d := range model.DestID(model.MaxDestinations) {
		for row := range model.RowKey(3) {
			dirty(s, d, row, 1)
		}
	}
	mode := perRow([]model.RowOutcome{model.OutcomeCommitted, model.OutcomeFailed,
		model.OutcomeCommitted, model.OutcomeFailed}, nil)
	run := func() {
		for d := range model.DestID(model.MaxDestinations) {
			for row := range model.RowKey(3) {
				dirty(s, d, row, 1) // a no-op for the failed rows, already queued
			}
		}
		if err := s.maybeOpenEpoch(); err != nil {
			t.Fatalf("open: %v", err)
		}
		for s.EpochOpen {
			if err := writeBack(s, next(t, q), mode); err != nil {
				t.Fatalf("completion: %v", err)
			}
		}
	}
	run()
	before := s.inst.RetryRequeued.Load()
	if got := testing.AllocsPerRun(1000, run); got != 0 {
		t.Fatalf("applyCompletion allocated %.1f times per epoch, want 0", got)
	}
	if got := s.inst.RetryRequeued.Load() - before; got < 1000*4 {
		t.Fatalf("non-vacuity: %d failed rows over the measured runs, want >= %d", got, 1000*4)
	}
}

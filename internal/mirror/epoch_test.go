package mirror

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// newEpochShard is shard 0 of four with the default mapping, every instrument
// live, m = 2, a batchQ of capacity qcap, and a Params of generation 1 with
// flush.maxBatchSize = maxBatch. set publishes a new Params, as a reload would.
func newEpochShard(t testing.TB, qcap, maxBatch int) (s *Shard, q chan *model.Batch, set func(*Params)) {
	t.Helper()
	dst, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	sz := smallSizing(256)
	codes := configuredCodes(dst)
	inst := fullInstruments(sz.Shards, len(codes))
	guards := NewGuardTable(codes, inst.Transcode.GuardOffered, inst.Transcode.GuardSuperseded)
	log := obs.NewAppLogger(io.Discard, obs.LevelInfo, 1000, nil)
	cur := &Params{Gen: 1, MaxBatchSize: maxBatch, Guard: GuardAuto}
	q = make(chan *model.Batch, qcap)
	s = NewShard(0, sz, func() *Params { return cur }, dst, guards, NewFlushBus(sz.Shards), inst, log,
		Edges{BatchQ: q})
	return s, q, func(p *Params) { cur = p }
}

// dirty makes row of destination d dirty the way merge's step 8 does, behind
// merge's back: creating the entry if needed, OR-ing mask into ColMask, stamping
// a recognisable image byte, and queueing it at the accumulating tail. For
// tests about the epoch and the pump, where what merged the row is beside the
// point.
func dirty(s *Shard, d model.DestID, row model.RowKey, mask model.ColMask) *model.Entry {
	slot, ok := s.Index[d][row]
	if !ok {
		slot = s.allocEntry(d)
		s.entry(d, slot).Row = row
		s.Index[d][row] = slot
	}
	e := s.entry(d, slot)
	e.ColMask |= mask
	e.Image[0] = byte(row)
	if !e.Dirty {
		e.Dirty = true
		s.Accum[d].PushBack(slot)
	}
	return e
}

// ltp is destination 0's last_traded_price (bit 0, offset 0) in img.
func ltp(img *[model.ImageSize]byte) uint32 { return binary.BigEndian.Uint32(img[0:4]) }

// TestMaybeOpenEpochOnEmptyRingsLeavesTheRequestSet is section 6.3's step 2
// before step 3. Every ring empty with req[k] and tickPending set: the tick is
// consumed, epoch_empty counts, no epoch opens -- and req[k] is STILL set, so
// the first row dirtied afterwards flushes at once. Non-vacuity: with a row
// dirty, the same call does clear it (step 3 exists and runs).
//
// WOULD HAVE CAUGHT: HLD section 7.3's ordering defect -- Taken() before the
// ring test discards a non-rate-limited request no epoch ever serves.
func TestMaybeOpenEpochOnEmptyRingsLeavesTheRequestSet(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	s.bus.req[s.ID].Store(true)
	s.tickPending = true

	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("maybeOpenEpoch on empty rings: %v", err)
	}
	if got := s.inst.EpochEmpty.Load(); got != 1 {
		t.Fatalf("epoch_empty = %d, want 1", got)
	}
	if !s.bus.Pending(s.ID) {
		t.Fatal("req[k] was cleared by an epoch that had nothing to flush")
	}
	if s.tickPending {
		t.Fatal("tickPending survived: a tick is consumed whatever the epoch decides")
	}
	if s.EpochOpen || s.Epoch != 0 || s.inst.EpochOpened.Load() != 0 || len(q) != 0 {
		t.Fatalf("an epoch opened on empty rings: open %v epoch %d epoch_opened %d batches %d",
			s.EpochOpen, s.Epoch, s.inst.EpochOpened.Load(), len(q))
	}

	dirty(s, tokenQuote, 1, 1)
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("maybeOpenEpoch with a dirty row: %v", err)
	}
	if s.bus.Pending(s.ID) || !s.EpochOpen || s.inst.EpochOpened.Load() != 1 {
		t.Fatalf("non-vacuity: an opening epoch must take the request: pending %v open %v",
			s.bus.Pending(s.ID), s.EpochOpen)
	}
}

// TestMaybeOpenEpochRejectsANonEmptyCohort is step 4, HLD section 7.12 risk
// 10's detector: a row left in Cohort[d] at open is an error, counted in
// cohort_not_empty{k}, and nothing is swapped or opened.
func TestMaybeOpenEpochRejectsANonEmptyCohort(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	dirty(s, tokenQuote, 1, 1)
	s.Cohort[securityMaster].PushBack(0) // behind the API: a leaked row

	err := s.maybeOpenEpoch()
	if err == nil || !strings.Contains(err.Error(), "cohort[1] non-empty") {
		t.Fatalf("err = %v, want cohort[1] non-empty", err)
	}
	if got := s.inst.CohortNotEmpty.Load(); got != 1 {
		t.Fatalf("cohort_not_empty = %d, want 1", got)
	}
	if s.EpochOpen || s.Accum[tokenQuote].Len() != 1 || s.Cohort[securityMaster].Len() != 1 || len(q) != 0 {
		t.Fatal("a rejected open swapped, opened or emitted")
	}
}

// TestMaybeOpenEpochOverAnOpenEpochIsAnError is step 0 (V2:1558): the
// precondition re-asserted. An epoch is open with its cohort drained but a
// batch still out; a second open must refuse, leaving Epoch and CohortOut alone
// so the outstanding batch still comes home to the epoch it was stamped with.
// Non-vacuity: once the epoch is closed, the same call opens.
func TestMaybeOpenEpochOverAnOpenEpochIsAnError(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	dirty(s, tokenQuote, 1, 1)
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("first open: %v", err)
	}
	dirty(s, tokenQuote, 2, 1)

	err := s.maybeOpenEpoch()
	if err == nil || !strings.Contains(err.Error(), "with epoch 1 open") {
		t.Fatalf("err = %v, want the open-epoch precondition", err)
	}
	if s.Epoch != 1 || s.CohortOut != 1 || s.Accum[tokenQuote].Len() != 1 || len(q) != 1 {
		t.Fatalf("a refused open changed state: epoch %d CohortOut %d accum %d queued %d; want 1, 1, 1, 1",
			s.Epoch, s.CohortOut, s.Accum[tokenQuote].Len(), len(q))
	}

	s.EpochOpen = false // the close, modelled
	if err := s.maybeOpenEpoch(); err != nil || s.Epoch != 2 {
		t.Fatalf("non-vacuity: with the epoch closed the open must proceed: err %v epoch %d", err, s.Epoch)
	}
}

// TestFillRowRejectsAnInFlightRow is fillRow's INV-STATE assertion (V2:1780):
// a cohort row that is InFlight or ReDirty is the G-INFL-1 failure class, and
// is an error rather than a second outstanding batch. Non-vacuity: the same
// row, Dirty and nothing else, pumps cleanly.
func TestFillRowRejectsAnInFlightRow(t *testing.T) {
	for _, tc := range []struct {
		name              string
		inFlight, reDirty bool
	}{{"InFlight", true, false}, {"ReDirty", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			s, q, _ := newEpochShard(t, 8, 1000)
			e := dirty(s, tokenQuote, 1, 1)
			e.InFlight, e.ReDirty = tc.inFlight, tc.reDirty // behind merge's back

			err := s.maybeOpenEpoch()
			if err == nil || !strings.Contains(err.Error(), "INV-STATE") {
				t.Fatalf("err = %v, want INV-STATE", err)
			}
			if len(q) != 0 || s.CohortOut != 0 {
				t.Fatalf("a row violating INV-STATE was sent: queued %d CohortOut %d", len(q), s.CohortOut)
			}
		})
	}

	s, q, _ := newEpochShard(t, 8, 1000)
	dirty(s, tokenQuote, 1, 1)
	if err := s.maybeOpenEpoch(); err != nil || len(q) != 1 {
		t.Fatalf("non-vacuity: a Dirty row must pump: err %v queued %d", err, len(q))
	}
}

// TestPumpFullBatchQIsFatal is section 7.4 (S1): the send that cannot fail by
// construction, failing. It counts flush_deferred{k} and returns an error
// naming the shard's view of the world; CohortOut does not count a batch that
// was never sent.
func TestPumpFullBatchQIsFatal(t *testing.T) {
	s, q, _ := newEpochShard(t, 1, 1000)
	q <- &model.Batch{} // a construction defect: the queue is already full
	dirty(s, tokenQuote, 1, 1)

	err := s.maybeOpenEpoch()
	if err == nil || !strings.Contains(err.Error(), "batchQ send failed") {
		t.Fatalf("err = %v, want the batchQ send failure", err)
	}
	if got := s.inst.FlushDeferred.Load(); got != 1 {
		t.Fatalf("flush_deferred = %d, want 1", got)
	}
	if s.CohortOut != 0 || s.inst.EpochBatches.Load() != 0 {
		t.Fatalf("CohortOut = %d, epoch_batches = %d: a failed send was counted as sent",
			s.CohortOut, s.inst.EpochBatches.Load())
	}
}

// TestPumpObeysTheFourRules is section 7.3 on one epoch: seven rows (2, 2, 3
// over destinations 0, 1, 2), flush.maxBatchSize 3, m = 2.
//
//   - Rule 1: batch 1 is A B C -- destination 2 gets DestCount 0 although its
//     cohort is non-empty, because the budget is per batch, not per destination.
//   - Rule 2: destinations in order 0, 1, 2 inside each batch.
//   - Rule 3: oldest first.
//   - Rule 4: each row in exactly one batch; G stays in the cohort because Free
//     is empty (the postcondition), not because the pump lost it.
//
// And fillRow: Mask is the pre-pump ColMask, ColMask is 0 afterwards, the
// digest is the image's, the entry is InFlight and no longer Dirty.
func TestPumpObeysTheFourRules(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 3)
	rows := []struct {
		d   model.DestID
		row model.RowKey
	}{{0, 'A'}, {0, 'B'}, {1, 'C'}, {1, 'D'}, {2, 'E'}, {2, 'F'}, {2, 'G'}}
	masks := map[model.RowKey]model.ColMask{}
	for i, r := range rows {
		masks[r.row] = model.ColMask(1) << i
		dirty(s, r.d, r.row, masks[r.row])
	}

	// Stale state the open and the fill must overwrite: a reused BatchRow's
	// previous verdict, a dirty_depth gauge left by the last tick, a missed
	// tick, and a CohortOut that is not this epoch's.
	for _, b := range s.Free {
		for j := range b.Rows {
			b.Rows[j].Outcome, b.Rows[j].ErrCode = model.OutcomeRejected, 1438
		}
	}
	for d := range s.inst.DirtyDepth {
		s.inst.DirtyDepth[d].Store(99)
	}
	s.TickMissed, s.CohortOut = true, 5

	before := nowMono()
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("maybeOpenEpoch: %v", err)
	}
	if s.TickMissed || s.epochOpenedAt < before {
		t.Fatalf("step 6: TickMissed %v, epochOpenedAt %d before the call's %d", s.TickMissed, s.epochOpenedAt, before)
	}
	for d := range s.inst.DirtyDepth {
		if got := s.inst.DirtyDepth[d].Load(); got != 0 {
			t.Fatalf("step 8: dirty_depth{%d} = %d, want 0", d, got)
		}
	}
	if len(q) != 2 || s.CohortOut != 2 || len(s.Free) != 0 {
		t.Fatalf("batches sent %d, CohortOut %d, Free %d; want 2, 2, 0", len(q), s.CohortOut, len(s.Free))
	}
	want := []struct {
		rows  string
		start [3]int32
		count [3]int32
	}{
		{"ABC", [3]int32{0, 2, 3}, [3]int32{2, 1, 0}},
		{"DEF", [3]int32{0, 0, 1}, [3]int32{0, 1, 2}},
	}
	for k, w := range want {
		b := <-q
		var got []byte
		for i := range b.Active() {
			r := &b.Rows[i]
			got = append(got, byte(r.Row))
			d := rows[strings.IndexByte("ABCDEFG", byte(r.Row))].d
			e := s.entry(d, r.Slot)
			if e.Row != r.Row {
				t.Fatalf("row %c carries slot %d of row %c", r.Row, r.Slot, e.Row)
			}
			if r.Mask != masks[r.Row] || e.ColMask != 0 {
				t.Fatalf("row %c: Mask %#x (want %#x), entry ColMask %#x (want 0 -- cleared at fill)",
					r.Row, r.Mask, masks[r.Row], e.ColMask)
			}
			if r.Image != e.Image || r.Digest != digest(s.seed, e.Image[:s.dst.ByID(d).ImageLen]) {
				t.Fatalf("row %c: image or digest is not the entry's", r.Row)
			}
			if !e.InFlight || e.Dirty || e.ReDirty || r.Outcome != model.OutcomePending || r.ErrCode != 0 {
				t.Fatalf("row %c: InFlight %v Dirty %v ReDirty %v Outcome %v ErrCode %d (want a reused row's verdict reset)",
					r.Row, e.InFlight, e.Dirty, e.ReDirty, r.Outcome, r.ErrCode)
			}
		}
		if string(got) != w.rows || b.DestStart != w.start || b.DestCount != w.count {
			t.Fatalf("batch %d: rows %s start %v count %v; want %s %v %v",
				k+1, got, b.DestStart, b.DestCount, w.rows, w.start, w.count)
		}
		if b.Shard != s.ID || b.Epoch != 1 || b.Err != nil {
			t.Fatalf("batch %d: shard %d epoch %d err %v", k+1, b.Shard, b.Epoch, b.Err)
		}
		if s.sentAt[s.batchIndex(b)] == 0 {
			t.Fatalf("batch %d: sentAt was not recorded", k+1)
		}
	}
	if s.Cohort[0].Len() != 0 || s.Cohort[1].Len() != 0 || s.Cohort[2].Len() != 1 {
		t.Fatalf("cohort lengths %d %d %d, want 0 0 1 (G waits for a free batch)",
			s.Cohort[0].Len(), s.Cohort[1].Len(), s.Cohort[2].Len())
	}
	in := s.inst
	if in.EpochOpened.Load() != 1 || in.EpochBatches.Load() != 2 || in.EpochRows.Load() != 6 ||
		in.RowsInflight.Load() != 6 || in.CohortDepth[2].Load() != 1 || in.CohortDepth[0].Load() != 0 {
		t.Fatalf("instruments: opened %d batches %d rows %d inflight %d cohort_depth{2} %d",
			in.EpochOpened.Load(), in.EpochBatches.Load(), in.EpochRows.Load(), in.RowsInflight.Load(),
			in.CohortDepth[2].Load())
	}
}

// TestMaybeOpenEpochLoadsParamsOnce is step 7 and ruling D3: the Params loaded
// at open is held for the whole epoch -- a reload mid-epoch does not change the
// next batch's size -- and the same load refreshes ordering.guard and moves
// cfgGen.
func TestMaybeOpenEpochLoadsParamsOnce(t *testing.T) {
	s, q, set := newEpochShard(t, 8, 1000)
	set(&Params{Gen: 2, MaxBatchSize: 1, Guard: GuardOff})
	// Four rows, so the third batch has two left to take: under a reloaded
	// maxBatchSize of 1000 it would carry both (non-vacuity of the N check).
	for row := range model.RowKey(4) {
		dirty(s, tokenQuote, row, 1)
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("maybeOpenEpoch: %v", err)
	}
	if s.maxBatchSize != 1 || s.guard != GuardOff || s.cfgGen != 2 {
		t.Fatalf("after open: maxBatchSize %d guard %d cfgGen %d; want 1, off, 2", s.maxBatchSize, s.guard, s.cfgGen)
	}

	set(&Params{Gen: 3, MaxBatchSize: 1000, Guard: GuardOn}) // a reload mid-epoch
	b := <-q                                                 // a completion, modelled: the batch comes home
	s.Free = append(s.Free, b)
	s.CohortOut--
	<-q
	if err := s.pump(); err != nil {
		t.Fatalf("pump: %v", err)
	}
	if b = <-q; b.N != 1 {
		t.Fatalf("the epoch's third batch carries %d rows; the reload reached it mid-epoch", b.N)
	}
	if s.maxBatchSize != 1 || s.guard != GuardOff || s.cfgGen != 2 || s.Cohort[tokenQuote].Len() != 1 {
		t.Fatalf("after pump: maxBatchSize %d guard %d cfgGen %d cohort %d; want 1, off, 2, 1 -- "+
			"the pump loaded Params; only epoch open (and the tick) may",
			s.maxBatchSize, s.guard, s.cfgGen, s.Cohort[tokenQuote].Len())
	}
}

// TestG_INFL_1_ARowIsInExactlyOneOutstandingBatch is gate G-INFL-1 (V4:1639).
//
// SETUP: every writer stalled (two fakes in holdUntil); one row merged and
// batched by an epoch open.
//
// STIMULUS: 100 more updates to that row while its batch is held, plus one to
// a second row.
//
// ASSERTION: exactly one outstanding batch (CohortOut 1, the writers hold 1,
// batchQ empty, a further pump emits nothing); the row is InFlight and
// ReDirty, its updates recorded by ReDirty and not a ring entry (the
// accumulating ring holds only the second row). The returned batch carries
// the image as it was at pump time.
//
// Non-vacuity: the later updates did reach the image, and a row NOT in flight
// merged in the same window does go onto the ring.
//
// WOULD HAVE CAUGHT: a merge that rings an in-flight row, putting it in two
// batches at once once several writers serve one shard -- HLD section 8.4's
// deadlock layer 1 and FR9 broken together (HLD R15).
func TestG_INFL_1_ARowIsInExactlyOneOutstandingBatch(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	completion := []chan *model.Batch{make(chan *model.Batch, 2)}
	release := make(chan struct{})
	w := startFakeWriters(2, holdUntil(release), q, completion)
	stop := w.stopper(t, release, q)
	slot := slotOf(t, s, tokenQuote, 7208)
	seq := int32(0)
	send := func(row model.RowKey, price uint32) {
		seq++
		u := model.UpdateUnit{Code: 7208, Seq: seq, LogTime: 100, RecvMono: int64(seq)}
		put(t, s, &u, tokenQuote, row, slot, fv{0, be32(price)})
		s.merge(u)
	}

	send(7, 1000)
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("maybeOpenEpoch: %v", err)
	}
	w.waitHeld(t, 1)
	e := rowEntry(s, tokenQuote, 7)
	for i := range uint32(100) {
		send(7, 2000+i)
	}
	send(8, 5)

	if s.CohortOut != 1 || len(q) != 0 || w.held.Load() != 1 || len(s.Free) != 1 {
		t.Fatalf("outstanding: CohortOut %d queued %d held %d Free %d; want 1, 0, 1, 1",
			s.CohortOut, len(q), w.held.Load(), len(s.Free))
	}
	if !e.InFlight || !e.ReDirty || e.Dirty {
		t.Fatalf("row 7: InFlight %v ReDirty %v Dirty %v; want in flight and re-dirtied", e.InFlight, e.ReDirty, e.Dirty)
	}
	if got := queued(s, tokenQuote); len(got) != 1 || got[0] != s.Index[tokenQuote][8] {
		t.Fatalf("accumulating ring %v, want only row 8's slot: row 7 must not enter a ring while in flight", got)
	}
	if s.Cohort[tokenQuote].Len() != 0 || s.inst.RedirtyTotal.Load() != 100 {
		t.Fatalf("cohort %d, redirty_total %d; want 0, 100", s.Cohort[tokenQuote].Len(), s.inst.RedirtyTotal.Load())
	}
	if ltp(&e.Image) != 2099 {
		t.Fatalf("non-vacuity: the image holds %d, so the later updates never merged", ltp(&e.Image))
	}
	if err := s.pump(); err != nil || s.CohortOut != 1 || len(q) != 0 {
		t.Fatalf("a further pump: err %v CohortOut %d queued %d; want nothing emitted", err, s.CohortOut, len(q))
	}

	stop() // release the held batch, close batchQ, wait for both writers
	b := <-completion[0]
	if b.N != 1 || b.Rows[0].Row != 7 || ltp(&b.Rows[0].Image) != 1000 {
		t.Fatalf("the batch carries %d rows, row %d at %d; want row 7 alone at its pump-time 1000",
			b.N, b.Rows[0].Row, ltp(&b.Rows[0].Image))
	}
	if got := w.maxHeld.Load(); got != 1 {
		t.Fatalf("the writers held %d batches at once, want 1", got)
	}
}

// TestG_MASK_2_ShardHalfBatchMaskIsTheUnion is gate G-MASK-2's shard half
// (V4:1619): the statement's column list is built from BatchRow.Mask, so the
// shard must hand the writer the union.
//
// SETUP: one MD_MARKET_STATUS row. 6511 writes message_len and message_text;
// every other image byte, and the tail past ImageLen, is then set to a prior
// value standing for what Oracle already holds.
//
// STIMULUS: 6531 writes token -- a column set disjoint from 6511's -- and an
// epoch opens.
//
// ASSERTION: BatchRow.Mask is the union of the two masks; each transcode's
// columns hold its own values; every byte neither mentioned keeps its prior
// value, through the full 256-byte copy; ColMask is 0 after the fill (R14 at
// fill); the digest is the batch image's.
//
// WOULD HAVE CAUGHT: a pump that sends only the last contribution's mask, or
// a merge/fill that blanks columns the contributing transcode does not own
// (HLD R14).
func TestG_MASK_2_ShardHalfBatchMaskIsTheUnion(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	const d = marketStatus
	dst := s.dst.ByID(d)
	recv := int64(0)
	send := func(code model.TransCode, fs ...fv) model.ColMask {
		recv++
		u := model.UpdateUnit{Code: code, Flags: model.FlagGeneral | model.FlagNonRateLimited,
			LogTime: 100, RecvMono: recv}
		put(t, s, &u, d, 1, slotOf(t, s, d, code), fs...)
		s.merge(u)
		return u.Contribs[d].Mask
	}

	maskA := send(6511, fv{1, be16(5)}, fv{2, []byte("HELLO")})
	e := rowEntry(s, d, 1)
	owner := func(j int) model.ColMask { // the mask bit of the column holding image byte j
		for i := range dst.Columns {
			if c := &dst.Columns[i]; j >= int(c.Offset) && j < int(c.End()) {
				return model.Bit(c.Bit)
			}
		}
		return 0
	}
	for j := range e.Image {
		if owner(j)&maskA == 0 {
			e.Image[j] = 0xAB // the prior value: Oracle's, or a tail nothing reads
		}
	}
	prior := e.Image

	maskB := send(6531, fv{4, be32(42)})
	if maskA == 0 || maskB == 0 || maskA&maskB != 0 || e.ColMask != maskA|maskB {
		t.Fatalf("non-vacuity: masks %#x and %#x must be non-empty and disjoint, ColMask %#x their union",
			maskA, maskB, e.ColMask)
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("maybeOpenEpoch: %v", err)
	}
	b := <-q
	r := &b.Rows[0]
	if b.N != 1 || r.Mask != maskA|maskB {
		t.Fatalf("BatchRow.Mask = %#x over %d rows, want the union %#x", r.Mask, b.N, maskA|maskB)
	}
	tok := dst.Column(4)
	if !bytes.Equal(r.Image[tok.Offset:tok.End()], be32(42)) {
		t.Fatalf("token = % x, want 6531's 42", r.Image[tok.Offset:tok.End()])
	}
	if txt := dst.Column(2); !bytes.HasPrefix(r.Image[txt.Offset:txt.End()], []byte("HELLO")) {
		t.Fatal("message_text lost 6511's value")
	}
	for j := range r.Image {
		if owner(j)&(maskA|maskB) == 0 && r.Image[j] != prior[j] {
			t.Fatalf("image byte %d = %#x, want its prior %#x: a column no transcode mentioned changed",
				j, r.Image[j], prior[j])
		}
	}
	if e.ColMask != 0 {
		t.Fatalf("entry ColMask = %#x after the fill, want 0 (INV-MASK, R14 at fill)", e.ColMask)
	}
	if r.Digest != digest(s.seed, r.Image[:dst.ImageLen]) {
		t.Fatal("the batch digest is not the batch image's")
	}
}

// TestPumpAllocatesNothing is AllocsPerRun = 0 for pump (V2:2674-2675): one
// epoch's worth of rows across all three destinations, refilled each run,
// drawn into both batches.
func TestPumpAllocatesNothing(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 4)
	type rs struct {
		d    model.DestID
		slot int32
	}
	var all []rs
	for d := range model.DestID(model.MaxDestinations) {
		for row := range model.RowKey(3) {
			dirty(s, d, row, 1)
			all = append(all, rs{d, s.Index[d][row]})
		}
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("maybeOpenEpoch: %v", err)
	}
	run := func() {
		for len(q) > 0 { // the completions, modelled
			s.Free = append(s.Free, <-q)
		}
		s.CohortOut = 0
		for _, r := range all {
			if s.entry(r.d, r.slot).InFlight {
				e := s.entry(r.d, r.slot)
				e.InFlight, e.Dirty, e.ColMask = false, true, 1
				s.Cohort[r.d].PushBack(r.slot)
			}
		}
		if err := s.pump(); err != nil {
			t.Fatalf("pump: %v", err)
		}
	}
	run()
	before := s.inst.EpochRows.Load()
	if got := testing.AllocsPerRun(1000, run); got != 0 {
		t.Fatalf("pump allocated %.1f times per call, want 0", got)
	}
	if got := s.inst.EpochRows.Load() - before; got < 1000*8 {
		t.Fatalf("non-vacuity: the measured pumps carried %d rows, want >= %d (two batches of 4)", got, 1000*8)
	}
}

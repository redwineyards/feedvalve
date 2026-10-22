package mirror

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// Destinations of the default mapping, by id.
const (
	tokenQuote     = model.DestID(0) // MD_TOKEN_QUOTE, 7208/7200
	securityMaster = model.DestID(1) // MD_SECURITY_MASTER, 7305/7340
	marketStatus   = model.DestID(2) // MD_MARKET_STATUS, 6511..6571
)

// fullInstruments is a ShardInstruments with EVERY handle a fresh, unregistered
// instrument: FlushWakeDropped one per shard, the {transcode} slices one per
// configured code. Filled by reflection, so a handle a later card adds is
// covered without editing this helper.
func fullInstruments(shards, codes int) *ShardInstruments {
	in := &ShardInstruments{}
	var fill func(v reflect.Value, n int)
	fill = func(v reflect.Value, n int) {
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
		case reflect.Struct:
			for i := range v.NumField() {
				n := codes
				if v.Type().Field(i).Name == "FlushWakeDropped" {
					n = shards
				}
				fill(v.Field(i), n)
			}
		case reflect.Array:
			for i := range v.Len() {
				fill(v.Index(i), n)
			}
		case reflect.Slice:
			v.Set(reflect.MakeSlice(v.Type(), n, n))
			for i := range n {
				fill(v.Index(i), n)
			}
		}
	}
	fill(reflect.ValueOf(in).Elem(), 0)
	return in
}

// configuredCodes is every contributor of dst in ascending code order -- the
// configured position order the registry, obs and the GuardTable share.
func configuredCodes(dst *destinations.Set) []model.TransCode {
	var codes []model.TransCode
	for _, d := range dst.All() {
		for i := range d.NContributors {
			if !slices.Contains(codes, d.Contributors[i]) {
				codes = append(codes, d.Contributors[i])
			}
		}
	}
	slices.Sort(codes)
	return codes
}

// newMergeShard is shard 0 of four, with the default mapping, a GuardTable
// zipped with its instruments the way app builds one, ordering.guard = mode,
// and a logger writing to the returned buffer.
func newMergeShard(t testing.TB, mode GuardMode) (*Shard, *bytes.Buffer) {
	t.Helper()
	dst, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	sz := smallSizing(256)
	codes := configuredCodes(dst)
	inst := fullInstruments(sz.Shards, len(codes))
	guards := NewGuardTable(codes, inst.Transcode.GuardOffered, inst.Transcode.GuardSuperseded)
	var buf bytes.Buffer
	log := obs.NewAppLogger(&buf, obs.LevelInfo, 1000, nil)
	p := &Params{Gen: 1, MaxBatchSize: sz.MaxBatchSizeCeiling, Guard: mode}
	return NewShard(0, sz, func() *Params { return p }, dst, guards, NewFlushBus(sz.Shards), inst, log, Edges{}), &buf
}

// fv is one field of a contribution: a mask bit and its value, zero-filled by
// put to the column's declared width -- as the decoder packs it.
type fv struct {
	bit uint8
	val []byte
}

// put packs fs (ascending bits) as u's contribution to destination d for row,
// at contributor slot, appending to u.Blob.
func put(t testing.TB, s *Shard, u *model.UpdateUnit, d model.DestID, row model.RowKey, slot uint8, fs ...fv) {
	t.Helper()
	dst := s.dst.ByID(d)
	c := &u.Contribs[d]
	c.Row, c.Slot, c.BlobOff = row, slot, u.BlobLen
	for _, f := range fs {
		col := dst.Column(f.bit)
		if col == nil || len(f.val) > int(col.Width) || c.Mask>>f.bit != 0 {
			t.Fatalf("put: bit %d is undeclared, too wide, or out of ascending order", f.bit)
		}
		copy(u.Blob[u.BlobLen:], f.val)
		u.BlobLen += col.Width
		c.Mask = c.Mask.Set(f.bit)
	}
	c.BlobLen = u.BlobLen - c.BlobOff
}

// slotOf is code's contributor slot in destination d.
func slotOf(t testing.TB, s *Shard, d model.DestID, code model.TransCode) uint8 {
	t.Helper()
	slot, ok := s.dst.ByID(d).Slot(code)
	if !ok {
		t.Fatalf("transcode %d does not contribute to destination %d", code, d)
	}
	return slot
}

// rowEntry is row's entry in destination d, or nil if the row does not exist.
func rowEntry(s *Shard, d model.DestID, row model.RowKey) *model.Entry {
	slot, ok := s.Index[d][row]
	if !ok {
		return nil
	}
	return s.entry(d, slot)
}

// queued is the accumulating ring of d, oldest first, without draining it.
func queued(s *Shard, d model.DestID) []int32 {
	r := &s.Accum[d]
	out := make([]int32, r.n)
	for i := range out {
		out[i] = r.buf[(r.head+i)&(len(r.buf)-1)]
	}
	return out
}

func be16(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }
func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

// guardCounters is code's guard_offered and guard_superseded.
func guardCounters(s *Shard, code model.TransCode) (offered, superseded uint64) {
	g := s.guards.lookup(code)
	return g.offered.Load(), g.superseded.Load()
}

// ---- G-ORD-1 ----

// reopen is one run of the same-day re-open, 6531 -> 6571 -> 6511, against the
// market_type = 1 row of destination 2, all at one LogTime second.
type reopen struct {
	after      [3]uint16 // the row's transcode column after each merge
	offered    uint64    // guard_offered, summed over the three codes
	superseded uint64    // guard_superseded, summed
	outOfOrder uint64    // units_superseded_out_of_order
}

// ok is G-ORD-1's assertion: every contribution applied, in arrival order, the
// row ending at 6511, nothing superseded.
func (r reopen) ok() bool {
	return r.after == [3]uint16{6531, 6571, 6511} && r.superseded == 0 && r.outOfOrder == 0
}

const (
	reopenT  = int32(1_400_000_000) // the shared LogTime second
	reopenR0 = int64(5_000_000_000) // RecvMono of the earlier re-open, setup R
)

var reopenCodes = [3]model.TransCode{6531, 6571, 6511}

// runReopen merges the three codes with RecvMono recv[i]. preseed gives every
// contributor slot a history of (LogTime T, RecvMono r0) -- an earlier re-open
// in the same second. collapse stamps all three onto 6531's slot.
func runReopen(t *testing.T, preseed bool, recv [3]int64, collapse bool) reopen {
	t.Helper()
	s, _ := newMergeShard(t, GuardAuto)
	const row = model.RowKey(1)
	if preseed {
		slot := s.allocEntry(marketStatus)
		s.Index[marketStatus][row] = slot
		e := s.entry(marketStatus, slot)
		e.Row = row
		for i := range uint8(5) {
			e.LastTime[i], e.LastRecv[i] = reopenT, reopenR0
			e.MarkSlotSeen(i)
		}
	}
	var r reopen
	for i, code := range reopenCodes {
		u := model.UpdateUnit{Code: code, Flags: model.FlagGeneral | model.FlagNonRateLimited,
			LogTime: reopenT, RecvMono: recv[i]}
		slot := slotOf(t, s, marketStatus, code)
		if collapse {
			slot = slotOf(t, s, marketStatus, 6531)
		}
		put(t, s, &u, marketStatus, row, slot, fv{0, be16(uint16(code))})
		s.merge(u)
		r.after[i] = binary.BigEndian.Uint16(rowEntry(s, marketStatus, row).Image[0:2])
	}
	for _, code := range reopenCodes {
		o, sup := guardCounters(s, code)
		r.offered += o
		r.superseded += sup
	}
	r.outOfOrder = s.inst.UnitsSupersededOutOfOrder.Load()
	return r
}

// TestG_ORD_1_SameSecondReopenEndsOpen is gate G-ORD-1, over two setups --
// because each of the two mechanisms is load-bearing in a different one, and
// no single input makes both fail (phase3-tasks.md section 6, erratum to
// V4:1623).
//
// SETUP: one destination-2 row (market_type = 1), three contributors.
//   - R: every contributor slot pre-seeded with LastTime = T, LastRecv = r0 (an
//     earlier re-open in the same second, V2 section 5.6); arrivals at r0+1,
//     r0+2, r0+3.
//   - P: a fresh row; all three arrivals at the SAME RecvMono.
//
// STIMULUS: merge 6531, 6571, 6511, all with LogTime T.
//
// ASSERTION: in both, each contribution applies in arrival order, the row ends
// carrying 6511, and guard_superseded == units_superseded_out_of_order == 0. In
// R all three reach the comparison (guard_offered == 3), so the RecvMono
// tiebreak is what applied them; in P none does (Trace B, every slot fresh).
//
// Non-vacuity, by input: R with every RecvMono = r0 (the tiebreak can no
// longer discriminate) must fail; P with the three collapsed onto one slot
// (Trace A) must fail. And R with collapsed slots must NOT fail -- the reason
// one setup cannot carry both mechanisms.
//
// WOULD HAVE CAUGHT: the market never reaching Open in the database for the rest
// of the day -- a per-row LastTime (Trace A), or a >= / missing RecvMono
// tiebreak.
func TestG_ORD_1_SameSecondReopenEndsOpen(t *testing.T) {
	r0 := reopenR0
	t.Run("R the RecvMono tiebreak", func(t *testing.T) {
		got := runReopen(t, true, [3]int64{r0 + 1, r0 + 2, r0 + 3}, false)
		if !got.ok() {
			t.Fatalf("setup R: %+v; want transcode 6531, 6571, 6511 in turn and nothing superseded", got)
		}
		if got.offered != 3 {
			t.Fatalf("setup R: guard_offered = %d, want 3 -- the comparison was never reached, so "+
				"this setup is not exercising the tiebreak", got.offered)
		}
		if bad := runReopen(t, true, [3]int64{r0, r0, r0}, false); bad.ok() {
			t.Fatalf("non-vacuity: with every RecvMono = r0 the tiebreak cannot discriminate, yet the "+
				"setup still passed (%+v): it is not testing the tiebreak", bad)
		}
		if col := runReopen(t, true, [3]int64{r0 + 1, r0 + 2, r0 + 3}, true); !col.ok() {
			t.Fatalf("collapsing slots under a rising RecvMono should still pass (V2:1450) -- %+v. "+
				"If it now fails, the two-setup erratum's premise changed", col)
		}
	})
	t.Run("P the per-slot arrays", func(t *testing.T) {
		const r = int64(7_000)
		got := runReopen(t, false, [3]int64{r, r, r}, false)
		if !got.ok() {
			t.Fatalf("setup P: %+v; want transcode 6531, 6571, 6511 in turn and nothing superseded", got)
		}
		if got.offered != 0 {
			t.Fatalf("setup P: guard_offered = %d, want 0: every contribution initialises its own slot", got.offered)
		}
		bad := runReopen(t, false, [3]int64{r, r, r}, true)
		if bad.ok() {
			t.Fatalf("non-vacuity: three codes collapsed onto one slot (Trace A) still passed (%+v): "+
				"the setup is not testing the per-slot arrays", bad)
		}
		if bad.superseded != 2 || bad.outOfOrder != 2 || bad.after[2] != 6531 {
			t.Fatalf("Trace A: %+v; want the second and third superseded and the row stuck at 6531", bad)
		}
	})
}

// ---- G-DIG-3 ----

// TestG_DIG_3_TextRegionIsFullyRewritten is gate G-DIG-3.
//
// SETUP: a destination-2 row whose message_text holds a long value: the
// decoder's 200 wire bytes cut to the 128-byte declared width, every byte
// non-zero.
//
// STIMULUS: merge a 20-byte value (packed zero-filled to 128, as the decoder
// packs it); digest.
//
// ASSERTION: the 128-byte region is the 20 bytes followed by 108 zeros, and the
// row's image -- so its digest -- equals that of a second row that only ever
// saw the short value. Non-vacuity: the long tail really was there before, and
// a modelled prefix-only copy yields a different digest.
//
// WOULD HAVE CAUGHT: copying only the valid prefix and leaving the old tail --
// two identical logical values with different digests, which silently defeats
// suppression (Vol 0 section 5.2 normalisation rule 2).
func TestG_DIG_3_TextRegionIsFullyRewritten(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	text := s.dst.ByID(marketStatus).Column(2)
	if text.Width != 128 || text.Name() != "message_text" {
		t.Fatalf("bit 2 of destination 2 is %s/%d, want message_text/128", text.Name(), text.Width)
	}
	long := bytes.Repeat([]byte{'L'}, 128) // 200 on the wire, cut to the declared width
	short := bytes.Repeat([]byte{'S'}, 20)
	slot := slotOf(t, s, marketStatus, 6511)
	recv := int64(0)
	send := func(row model.RowKey, msgLen uint16, txt []byte) {
		recv++
		u := model.UpdateUnit{Code: 6511, Flags: model.FlagGeneral | model.FlagNonRateLimited,
			LogTime: 100, RecvMono: recv}
		put(t, s, &u, marketStatus, row, slot, fv{1, be16(msgLen)}, fv{2, txt})
		s.merge(u)
	}
	region := func(row model.RowKey) []byte {
		return rowEntry(s, marketStatus, row).Image[text.Offset:text.End()]
	}

	send(1, 200, long)
	if bytes.Count(region(1)[20:], []byte{'L'}) != 108 {
		t.Fatal("non-vacuity: the long value's tail is not in the image, so its removal proves nothing")
	}
	prefixOnly := rowEntry(s, marketStatus, 1).Image // a copy: the image a prefix-only merge would leave
	copy(prefixOnly[text.Offset:], short)
	binary.BigEndian.PutUint16(prefixOnly[2:], 20)

	send(1, 20, short)
	want := append(append([]byte{}, short...), make([]byte, 108)...)
	if got := region(1); !bytes.Equal(got, want) {
		t.Fatalf("message_text region after the short value:\n%q\nwant the 20 bytes and 108 zeros", got)
	}

	send(2, 20, short) // a row that only ever saw the short value
	imgLen := s.dst.ByID(marketStatus).ImageLen
	a, b := rowEntry(s, marketStatus, 1).Image[:imgLen], rowEntry(s, marketStatus, 2).Image[:imgLen]
	if !bytes.Equal(a, b) || digest(s.seed, a) != digest(s.seed, b) {
		t.Fatal("the short value's image or digest depends on the long value it replaced")
	}
	if digest(s.seed, prefixOnly[:imgLen]) == digest(s.seed, b) {
		t.Fatal("non-vacuity: a prefix-only copy digests the same, so the digest comparison cannot see the defect")
	}
}

// ---- G-ORD-3 through merge ----

// TestG_ORD_3_MergeAllNDistinctRows is V4:1625's stimulus for G-ORD-3, through
// merge: the gate is P3-06's, held at the guard; here every entry is created
// by its own contribution at step 1, the "entry was created by THIS
// contribution" path of section 5.3 step 4.
//
// SETUP: a fresh shard. STIMULUS: N 7208 units, one per distinct token.
// ASSERTION: guard_offered == guard_superseded == 0 and rows_created == N.
// Non-vacuity: a second unit to one of those rows IS offered.
//
// WOULD HAVE CAUGHT: a merge that offers a freshly created entry to the
// comparison, so `auto`'s ratio drifts with the inventory mix (HLD 6.5).
func TestG_ORD_3_MergeAllNDistinctRows(t *testing.T) {
	const n = 2_000
	s, _ := newMergeShard(t, GuardAuto)
	slot := slotOf(t, s, tokenQuote, 7208)
	for i := range n {
		u := model.UpdateUnit{Code: 7208, Seq: int32(i), LogTime: 100, RecvMono: int64(i + 1)}
		put(t, s, &u, tokenQuote, model.RowKey(i), slot, fv{0, be32(uint32(i))})
		s.merge(u)
	}
	if o, sup := guardCounters(s, 7208); o != 0 || sup != 0 {
		t.Fatalf("guard_offered = %d, guard_superseded = %d after %d entry-creating merges, want 0 and 0", o, sup, n)
	}
	if got := s.inst.RowsCreated[tokenQuote].Load(); got != n {
		t.Fatalf("rows_created{0} = %d, want %d", got, n)
	}

	u := model.UpdateUnit{Code: 7208, Seq: n, LogTime: 100, RecvMono: n + 1}
	put(t, s, &u, tokenQuote, 0, slot, fv{0, be32(1)})
	s.merge(u)
	if o, _ := guardCounters(s, 7208); o != 1 {
		t.Fatalf("non-vacuity: guard_offered = %d after a repeat key, want 1", o)
	}
}

// ---- supersession and the counts of step 10 ----

// TestMergeSupersedeTouchesNothingAndCountsOnce: units_superseded_out_of_order
// rises by exactly one per Supersede (the guard does not count it; section 6
// erratum to V2:1267), and a superseded contribution leaves the entry, the
// rings and the bus exactly as they were. units_applied counts every unit;
// units_merged only the contributions that reached the image.
func TestMergeSupersedeTouchesNothingAndCountsOnce(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	slot := slotOf(t, s, securityMaster, 7305)
	unit := func(seq int32, strike uint32) model.UpdateUnit {
		u := model.UpdateUnit{Code: 7305, Flags: model.FlagNonRateLimited, Seq: seq, LogTime: 100, RecvMono: int64(seq)}
		put(t, s, &u, securityMaster, 42, slot, fv{3, be32(strike)})
		return u
	}
	s.merge(unit(100, 1))
	e := rowEntry(s, securityMaster, 42)
	s.bus.Taken(0)
	before, ring := *e, queued(s, securityMaster)

	const k = 5
	for i := range k {
		s.merge(unit(int32(90+i), 999)) // older BCSeqNo: superseded
		if got := s.inst.UnitsSupersededOutOfOrder.Load(); got != uint64(i+1) {
			t.Fatalf("after %d supersessions units_superseded_out_of_order = %d, want exactly %d", i+1, got, i+1)
		}
	}
	if _, sup := guardCounters(s, 7305); sup != k {
		t.Fatalf("guard_superseded = %d, want %d", sup, k)
	}
	if *e != before || !slices.Equal(queued(s, securityMaster), ring) || s.bus.Pending(0) {
		t.Fatal("a superseded contribution changed the entry, the ring or raised a flush")
	}
	if got := s.inst.FlushTriggered.Load(); got != 1 {
		t.Fatalf("flush_triggered = %d, want 1: only the first unit dirtied anything", got)
	}
	if a, m := s.inst.UnitsApplied.Load(), s.inst.UnitsMerged.Load(); a != k+1 || m != 1 {
		t.Fatalf("units_applied = %d, units_merged = %d; want %d units and 1 merged contribution", a, m, k+1)
	}
}

// ---- section 12.3 P1 + P2 ----

// TestMergeMidFlightKeepsMaskAndDigest is section 12.3's P1 and P2 together,
// through the real pump and completion: a row is batched (fillRow cleared its
// ColMask and the batch carries the image's digest) over an earlier commit,
// then a newer value of a column the batch also carries is merged mid-flight.
//
// ASSERTION, at the merge: the row is ReDirty, on no ring, with the column back
// in ColMask and the newer value in the image (P1); and the merge never wrote
// Digest or Written (P2). Then, after the batch completes Committed: Digest is
// the BATCH's, not the image's (P2); ColMask still names the column (P1); the
// row is requeued, and the next batch carries the newer value.
//
// The two are tested together because they are the same mistake about two
// fields: a fix to one that leaves the other must still fail here.
func TestMergeMidFlightKeepsMaskAndDigest(t *testing.T) {
	s, q, _ := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)
	x.send(7, fv{0, be32(500)}) // the earlier commit
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := writeBack(s, next(t, q), allCommitted()); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	e := rowEntry(s, tokenQuote, 7)
	prevDigest, prevWritten := e.Digest, e.Written

	x.send(7, fv{0, be32(1000)})
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	b := next(t, q)
	batchMask, batchDigest := b.Rows[0].Mask, b.Rows[0].Digest

	x.send(7, fv{0, be32(2000)})
	if !e.InFlight || e.Dirty || !e.ReDirty || s.Accum[tokenQuote].Len() != 0 {
		t.Fatalf("mid-flight merge: InFlight %v Dirty %v ReDirty %v queued %d; want in flight, re-dirtied, on no ring",
			e.InFlight, e.Dirty, e.ReDirty, s.Accum[tokenQuote].Len())
	}
	if e.ColMask != batchMask { // P1
		t.Fatalf("ColMask = %#x, want %#x: the re-dirtied column must survive to the next batch", e.ColMask, batchMask)
	}
	if got := binary.BigEndian.Uint32(e.Image[0:4]); got != 2000 {
		t.Fatalf("image holds %d, want the newer 2000", got)
	}
	if e.Digest != prevDigest || e.Written != prevWritten { // P2
		t.Fatalf("merge wrote Digest %#x / Written %#x; only the completion may", e.Digest, e.Written)
	}
	if imageDigest(s, tokenQuote, e) == batchDigest || batchDigest == prevDigest {
		t.Fatal("non-vacuity: the three images must digest apart, or P2 could not be told apart")
	}
	if got := s.inst.RedirtyTotal.Load(); got != 1 {
		t.Fatalf("redirty_total = %d, want 1", got)
	}

	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("second completion: %v", err)
	}
	if e.Digest != batchDigest { // P2
		t.Fatalf("Digest = %#x after the commit, want the batch's %#x (never recomputed)", e.Digest, batchDigest)
	}
	if e.ColMask != batchMask || !e.Dirty || !slices.Equal(queued(s, tokenQuote), []int32{s.Index[tokenQuote][7]}) { // P1
		t.Fatalf("after the commit: ColMask %#x Dirty %v queued %v; want the column kept and the row requeued",
			e.ColMask, e.Dirty, queued(s, tokenQuote))
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("third open: %v", err)
	}
	if b = next(t, q); b.Rows[0].Mask != batchMask || ltp(&b.Rows[0].Image) != 2000 {
		t.Fatalf("next batch: Mask %#x ltp %d; want the column at 2000", b.Rows[0].Mask, ltp(&b.Rows[0].Image))
	}
}

// TestMergeMidFlightReturnToCommittedValueIsRequeued: A committed, B pumped,
// then a rate-limited A merged while B is in flight. Every suppression clause
// holds at that moment -- Digest is still D(A), ColMask is inside Written, the
// image hashes to D(A) again -- but Digest describes the last COMMIT, not the
// batch, so suppressing would leave Oracle holding B and the mirror A with
// nothing to requeue the row (Figure 2.4 has no suppression arc out of
// InFlight). The merge must set ReDirty, and the real Committed arm (section
// 8.4) must record B's digest and put the row back on the ring, so the next
// batch writes A over B.
func TestMergeMidFlightReturnToCommittedValueIsRequeued(t *testing.T) {
	const d = tokenQuote
	s, q, _ := newEpochShard(t, 8, 1000)
	x := newSender7208(t, s)

	x.send(1, fv{0, be32(500)}) // A, committed
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := writeBack(s, next(t, q), allCommitted()); err != nil {
		t.Fatalf("A's completion: %v", err)
	}
	e := rowEntry(s, d, 1)
	committedA := e.Digest

	x.send(1, fv{0, be32(600)}) // B, pumped
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	b := next(t, q)
	batchB := b.Rows[0].Digest

	x.send(1, fv{0, be32(500)}) // A again, mid-flight
	if e.Digest != committedA || e.ColMask&^e.Written != 0 || imageDigest(s, d, e) != committedA || batchB == committedA {
		t.Fatal("non-vacuity: the mid-flight merge did not meet every other suppression clause")
	}
	if !e.ReDirty || s.inst.ChangeSuppressed.Load() != 0 || s.inst.RedirtyTotal.Load() != 1 {
		t.Fatalf("mid-flight return to the committed value: ReDirty %v change_suppressed %d redirty_total %d; "+
			"want ReDirty, not suppressed", e.ReDirty, s.inst.ChangeSuppressed.Load(), s.inst.RedirtyTotal.Load())
	}

	if err := writeBack(s, b, allCommitted()); err != nil {
		t.Fatalf("B's completion: %v", err)
	}
	if e.Digest != batchB {
		t.Fatalf("Digest = %#x after B committed, want B's %#x", e.Digest, batchB)
	}
	if !e.Dirty || !slices.Equal(queued(s, d), []int32{s.Index[d][1]}) {
		t.Fatal("after B committed the row is clean and unqueued, but the mirror holds A: Oracle stays stale")
	}
	if err := s.maybeOpenEpoch(); err != nil {
		t.Fatalf("third open: %v", err)
	}
	if b = next(t, q); ltp(&b.Rows[0].Image) != 500 {
		t.Fatalf("next batch writes ltp %d, want A's 500 over B", ltp(&b.Rows[0].Image))
	}
}

// ---- INV-STATE ----

// TestMergeKeepsINVSTATE is a property test: random merges -- rate-limited or
// not, newer or superseded, onto clean, dirty, in-flight and quarantined rows
// -- interleaved with a modelled pump and completion that toggle InFlight.
// After every step: Dirty && InFlight is never both true, ReDirty implies
// InFlight, and Accum holds exactly the Dirty rows, each once (section 4.3).
func TestMergeKeepsINVSTATE(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	const d, rows = securityMaster, 8
	slot := slotOf(t, s, securityMaster, 7305)
	imgLen := s.dst.ByID(d).ImageLen
	rng := rand.New(rand.NewSource(7))
	seq := int32(0)
	recv := int64(0)

	check := func(step int, op string) {
		q := queued(s, d)
		for i := range s.NextSlot[d] {
			e := s.entry(d, i)
			if e.Dirty && e.InFlight || e.ReDirty && !e.InFlight {
				t.Fatalf("step %d (%s): slot %d Dirty %v InFlight %v ReDirty %v", step, op, i, e.Dirty, e.InFlight, e.ReDirty)
			}
			n := 0
			for _, x := range q {
				if x == i {
					n++
				}
			}
			if e.Dirty && n != 1 || !e.Dirty && n != 0 {
				t.Fatalf("step %d (%s): slot %d Dirty %v is queued %d times", step, op, i, e.Dirty, n)
			}
		}
	}

	nDirty := func() (n uint64) {
		for i := range s.NextSlot[d] {
			if s.entry(d, i).Dirty {
				n++
			}
		}
		return n
	}
	var dirtied uint64 // clean -> Dirty transitions made by merge

	for step := range 20_000 {
		var op string
		switch r := rng.Intn(10); {
		case r < 6:
			op = "merge"
			before := nDirty()
			seq++
			recv++
			u := model.UpdateUnit{Code: 7305, Seq: seq, LogTime: 100, RecvMono: recv}
			if rng.Intn(2) == 0 {
				u.Flags |= model.FlagNonRateLimited
			}
			if rng.Intn(8) == 0 {
				u.Seq -= 1000 // older: superseded once the slot is seen
			}
			put(t, s, &u, d, model.RowKey(rng.Intn(rows)), slot, fv{3, be32(uint32(rng.Intn(3)))})
			s.merge(u)
			dirtied += nDirty() - before // a merge never cleans a row
		case r < 8:
			op = "pump"
			for {
				i, ok := s.Accum[d].PopFront()
				if !ok {
					break
				}
				e := s.entry(d, i)
				e.Dirty, e.InFlight, e.ColMask = false, true, 0
			}
		case r < 9:
			op = "complete"
			for i := range s.NextSlot[d] {
				e := s.entry(d, i)
				if !e.InFlight {
					continue
				}
				e.InFlight = false
				e.Digest = digest(s.seed, e.Image[:imgLen])
				e.Written = e.Written.Union(s.dst.ByID(d).FullMask())
				if e.ReDirty {
					e.ReDirty, e.Dirty = false, true
					s.Accum[d].PushBack(i)
				}
			}
		default:
			op = "quarantine toggle"
			i := int32(rng.Intn(int(max(s.NextSlot[d], 1))))
			if i < s.NextSlot[d] {
				e := s.entry(d, i)
				if !e.Dirty && !e.InFlight {
					e.Quarantined = !e.Quarantined
				}
			}
		}
		check(step, op)
	}
	if s.inst.RedirtyTotal.Load() == 0 || s.inst.ChangeSuppressed.Load() == 0 ||
		s.inst.MergeIntoQuarantined[d].Load() == 0 || s.inst.UnitsSupersededOutOfOrder.Load() == 0 {
		t.Fatal("non-vacuity: the random walk never reached one of the in-flight, suppressed, " +
			"quarantined or superseded arms")
	}
	// rows_dirtied is ASM-17's numerator (step 10): exactly the merge's own
	// clean -> Dirty transitions, not the completion's requeues.
	if got := s.inst.RowsDirtied.Load(); got != dirtied || dirtied == 0 {
		t.Fatalf("rows_dirtied = %d, want the %d clean -> Dirty transitions observed", got, dirtied)
	}
	if got := s.inst.RingGrown[d].Load(); got != 0 {
		t.Fatalf("ring_grown = %d on a ring pre-sized past %d rows, want 0", got, rows)
	}
}

// ---- adversarial ----

// TestMergeBlobLenMismatchMergesNothing: a contribution whose BlobLen is one
// byte short of its mask's declared widths, whose mask names a bit the
// destination never declared, or whose run lies outside the unit's packed
// bytes (u.BlobLen) or outside u.Blob itself -- leaves the image, mask and rings untouched,
// counts blob_len_mismatch{code} and logs the ERROR class, and raises no flush.
func TestMergeBlobLenMismatchMergesNothing(t *testing.T) {
	s, buf := newMergeShard(t, GuardAuto)
	slot := slotOf(t, s, securityMaster, 7305)
	pos := s.guards.index(7305)
	recv := int64(0)
	unit := func(strike uint32) model.UpdateUnit {
		recv++
		u := model.UpdateUnit{Code: 7305, Flags: model.FlagNonRateLimited, Seq: int32(recv), LogTime: 100, RecvMono: recv}
		put(t, s, &u, securityMaster, 9, slot, fv{3, be32(strike)}, fv{4, be32(10)})
		return u
	}
	s.merge(unit(1))
	e := rowEntry(s, securityMaster, 9)
	e.Dirty = false
	s.Accum[securityMaster].Reset()
	s.bus.Taken(0)
	e.ColMask = 0
	img := e.Image
	seq, lt, lr := e.LastSeq, e.LastTime, e.LastRecv

	short := unit(2)
	short.Contribs[securityMaster].BlobLen--
	undeclared := unit(3)
	undeclared.Contribs[securityMaster].Mask = undeclared.Contribs[securityMaster].Mask.Set(40)
	// Right width, but the run starts where the unit's packed bytes end: it
	// would copy the blob's unused tail, which the decoder never packed.
	pastInUse := unit(5)
	pastInUse.Contribs[securityMaster].BlobOff = pastInUse.BlobLen
	// Right width, and a corrupt u.BlobLen claiming the whole array: only the
	// array bound stands between this run and an index panic.
	pastArray := unit(6)
	pastArray.BlobLen = 0xFFFF
	pastArray.Contribs[securityMaster].BlobOff = model.UnitBlobSize - 1

	for i, u := range []model.UpdateUnit{short, undeclared, pastInUse, pastArray} {
		s.merge(u)
		if e.Image != img || e.ColMask != 0 || e.Dirty || s.Accum[securityMaster].Len() != 0 || s.bus.Pending(0) {
			t.Fatalf("case %d: a mismatched contribution changed the row or raised a flush", i)
		}
		// Rejected whole, so no ordering slot moved either: otherwise a corrupt
		// unit would make later valid ones look out of order.
		if e.LastSeq != seq || e.LastTime != lt || e.LastRecv != lr {
			t.Fatalf("case %d: a mismatched contribution advanced the ordering slots", i)
		}
		if got := s.inst.Transcode.BlobLenMismatch[pos].Load(); got != uint64(i+1) {
			t.Fatalf("case %d: blob_len_mismatch{7305} = %d, want %d", i, got, i+1)
		}
	}
	if !strings.Contains(buf.String(), "ERROR blob_len_mismatch dest=MD_SECURITY_MASTER code=7305") {
		t.Fatalf("no blob_len_mismatch ERROR logged:\n%s", buf.String())
	}

	// Non-vacuity: the same unit at its true length merges.
	s.merge(unit(4))
	if binary.BigEndian.Uint32(e.Image[s.dst.ByID(securityMaster).Column(3).Offset:]) != 4 || !e.Dirty {
		t.Fatal("non-vacuity: a well-formed contribution did not merge")
	}
}

// TestMergeIntoQuarantinedRow: the image and ColMask stay current, the row is
// not queued, merge_into_quarantined counts it, and -- being the unit's only
// contribution -- it raises no flush even though the unit is non-rate-limited.
// The same unit on an unquarantined row DOES raise one.
func TestMergeIntoQuarantinedRow(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	slot := slotOf(t, s, securityMaster, 7305)
	recv := int64(0)
	send := func(row model.RowKey, strike uint32) {
		recv++
		u := model.UpdateUnit{Code: 7305, Flags: model.FlagNonRateLimited, Seq: int32(recv), LogTime: 100, RecvMono: recv}
		put(t, s, &u, securityMaster, row, slot, fv{3, be32(strike)})
		s.merge(u)
	}
	slotQ := s.allocEntry(securityMaster)
	s.Index[securityMaster][5] = slotQ
	e := s.entry(securityMaster, slotQ)
	e.Row, e.Quarantined = 5, true

	send(5, 77)
	off := s.dst.ByID(securityMaster).Column(3).Offset
	if binary.BigEndian.Uint32(e.Image[off:]) != 77 || !e.ColMask.Has(3) {
		t.Fatal("a quarantined row's image or ColMask was not kept current")
	}
	if e.Dirty || s.Accum[securityMaster].Len() != 0 || s.inst.MergeIntoQuarantined[securityMaster].Load() != 1 {
		t.Fatal("a quarantined row was queued, or merge_into_quarantined was not counted")
	}
	if s.bus.Pending(0) || s.inst.FlushTriggered.Load() != 0 {
		t.Fatal("a unit that only landed on a quarantined row raised a flush")
	}
	send(6, 77)
	if !s.bus.Pending(0) || s.inst.FlushTriggered.Load() != 1 {
		t.Fatal("non-vacuity: the same unit on a normal row did not raise a flush")
	}
}

// TestMergeAllSupersededRaisesNoFlush: a non-rate-limited unit whose only
// contribution is superseded dirties nothing, so it must not Trigger.
func TestMergeAllSupersededRaisesNoFlush(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	slot := slotOf(t, s, securityMaster, 7305)
	send := func(seq int32, recv int64) {
		u := model.UpdateUnit{Code: 7305, Flags: model.FlagNonRateLimited, Seq: seq, LogTime: 100, RecvMono: recv}
		put(t, s, &u, securityMaster, 3, slot, fv{3, be32(uint32(seq))})
		s.merge(u)
	}
	send(10, 1)
	s.bus.Taken(0)
	send(9, 2) // superseded
	if s.bus.Pending(0) || s.inst.FlushTriggered.Load() != 1 {
		t.Fatal("an all-superseded non-rate-limited unit raised a flush")
	}
	if s.inst.UnitsSupersededOutOfOrder.Load() != 1 {
		t.Fatal("non-vacuity: the unit was not superseded")
	}
}

// TestMergeSuppressionIsRateLimitedOnly: on a clean, written row whose image
// already digests to Entry.Digest, a rate-limited unit is suppressed and a
// non-rate-limited one is STILL dirtied (NFR13's carve-out). And a field set
// for the first time to its zero fill -- its bit not in Written -- is never
// suppressed (ADR-L04: otherwise it stays NULL in Oracle forever).
func TestMergeSuppressionIsRateLimitedOnly(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	const d = tokenQuote
	slot := slotOf(t, s, d, 7208)
	imgLen := s.dst.ByID(d).ImageLen
	seq := int32(0)
	send := func(flags model.UnitFlags, fs ...fv) {
		seq++
		u := model.UpdateUnit{Code: 7208, Flags: flags, Seq: seq, LogTime: 100, RecvMono: int64(seq)}
		put(t, s, &u, d, 1, slot, fs...)
		s.merge(u)
	}
	// commit models section 8.3's Committed arm: clean, digest and Written set.
	commit := func(written model.ColMask) *model.Entry {
		e := rowEntry(s, d, 1)
		s.Accum[d].Reset()
		e.Dirty = false
		e.Digest = digest(s.seed, e.Image[:imgLen])
		e.Written, e.ColMask = written, 0
		return e
	}

	send(0, fv{0, be32(500)})
	e := commit(model.Bit(0))
	send(0, fv{0, be32(500)}) // rate-limited, same value
	if e.Dirty || s.inst.ChangeSuppressed.Load() != 1 {
		t.Fatal("a rate-limited unchanged value was not suppressed")
	}
	if !e.ColMask.Has(0) {
		t.Fatal("suppression cleared ColMask; it must stay set (Vol 0 section 6.2 step 3)")
	}

	commit(model.Bit(0))
	send(model.FlagNonRateLimited, fv{0, be32(500)}) // non-rate-limited, same value
	if !e.Dirty || s.inst.ChangeSuppressed.Load() != 1 {
		t.Fatal("a non-rate-limited unit whose digest matches was suppressed")
	}

	commit(model.Bit(0))
	send(0, fv{1, be32(0)}) // volume_traded_today: first ever value, equal to the zero fill
	if !e.Dirty || s.inst.ChangeSuppressed.Load() != 1 {
		t.Fatal("a first-ever zero value of an unwritten column was suppressed (ADR-L04)")
	}
}

// ---- allocation ----

// TestMergeOnAnExistingRowAllocatesNothing: AllocsPerRun = 0 for merge on an
// existing row (V2:1138-1141) -- along the coalescing, flush-triggering,
// suppressed and in-flight paths, with destination 0's full 16-bit, 181-byte
// contribution.
func TestMergeOnAnExistingRowAllocatesNothing(t *testing.T) {
	// path is the counter only that case's path moves. AllocsPerRun calls the
	// function 1001 times (one warm-up), so it must rise by at least 1000, or
	// the case measured some other path. Coalescing has no counter of its own:
	// units_merged moving while rows_dirtied does not is its proof.
	cases := []struct {
		name  string
		flags model.UnitFlags
		path  func(s *Shard) uint64
		prep  func(s *Shard, e *model.Entry)
	}{
		{"coalescing on a dirty row", 0, func(s *Shard) uint64 { return s.inst.UnitsMerged.Load() },
			func(*Shard, *model.Entry) {}},
		{"non-rate-limited, Trigger", model.FlagNonRateLimited, func(s *Shard) uint64 { return s.inst.FlushTriggered.Load() },
			func(*Shard, *model.Entry) {}},
		{"suppressed", 0, func(s *Shard) uint64 { return s.inst.ChangeSuppressed.Load() }, func(s *Shard, e *model.Entry) {
			s.Accum[tokenQuote].Reset()
			e.Dirty, e.ColMask = false, 0
			e.Written = s.dst.ByID(tokenQuote).FullMask()
			e.Digest = digest(s.seed, e.Image[:s.dst.ByID(tokenQuote).ImageLen])
		}},
		{"in flight", 0, func(s *Shard) uint64 { return s.inst.RedirtyTotal.Load() }, func(s *Shard, e *model.Entry) {
			s.Accum[tokenQuote].Reset()
			e.Dirty, e.InFlight = false, true
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newMergeShard(t, GuardAuto)
			slot := slotOf(t, s, tokenQuote, 7208)
			var u model.UpdateUnit
			var fs []fv
			for bit := range uint8(16) {
				// A small value in the field's LAST byte, and '+' for the
				// flag, so pre-validation (P3-07b) passes every field and
				// the whole 181 bytes are copied.
				val := make([]byte, s.dst.ByID(tokenQuote).Column(bit).Width)
				val[len(val)-1] = bit + 1
				if bit == 7 {
					val[0] = '+'
				}
				fs = append(fs, fv{bit, val})
			}
			put(t, s, &u, tokenQuote, 99, slot, fs...)
			if u.BlobLen != 181 {
				t.Fatalf("blob %d bytes, want destination 0's 181", u.BlobLen)
			}
			u.Code, u.Flags, u.LogTime = 7208, tc.flags, 100
			next := func() {
				u.Seq++
				u.RecvMono++
				s.merge(u)
			}
			next()
			tc.prep(s, rowEntry(s, tokenQuote, 99))
			before, pathBefore, dirtiedBefore := s.inst.UnitsMerged.Load(), tc.path(s), s.inst.RowsDirtied.Load()
			if got := testing.AllocsPerRun(1000, next); got != 0 {
				t.Fatalf("merge allocated %.1f times per call, want 0", got)
			}
			if s.inst.UnitsMerged.Load()-before < 1000 {
				t.Fatal("non-vacuity: the measured merges did not reach the image")
			}
			if got := tc.path(s) - pathBefore; got < 1000 {
				t.Fatalf("non-vacuity: the path's own counter rose by %d, want >= 1000: "+
					"the case measured some other path", got)
			}
			if got := s.inst.Transcode.PrevalidateRejectedTC[s.guards.index(7208)].Load(); got != 0 {
				t.Fatalf("pre-validation refused %d fields: the case did not copy the full contribution", got)
			}
			if got := s.inst.RowsDirtied.Load() - dirtiedBefore; got != 0 {
				t.Fatalf("rows_dirtied rose by %d: a measured merge took the clean -> Dirty arm", got)
			}
		})
	}
}

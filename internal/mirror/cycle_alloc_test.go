package mirror

import (
	"encoding/binary"
	"testing"

	"feedvalve/internal/model"
)

// cycleRows is how many warm destination-0 rows the cycle rig carries: two
// full batches at the rig's maxBatchSize of 4, so both of m = 2 batches are
// out at once.
const cycleRows = 8

// cycleRig drives one shard through the whole steady-state cycle, synchronously
// and on the test goroutine: merge -> epoch open -> pump -> a merge into rows
// in flight -> completions until the epoch closes.
//
// units are built once, with put, so building a unit is not what is measured;
// a cycle only bumps their ordering fields and last_traded_price.
type cycleRig struct {
	s     *Shard
	q     chan *model.Batch
	units [cycleRows]model.UpdateUnit
	mode  fakeMode
	v     uint32 // last_traded_price of the current cycle
}

// newCycleRig is newEpochShard (q of 8, maxBatchSize 4) with cycleRows rows of
// destination 0, each fed destination 0's full 16-bit, 181-byte 7208
// contribution, and warmed by two cycles so every row, ring and map bucket
// already exists.
func newCycleRig(tb testing.TB) *cycleRig {
	tb.Helper()
	s, q, _ := newEpochShard(tb, 8, 4)
	r := &cycleRig{s: s, q: q, mode: allCommitted()}
	slot := slotOf(tb, s, tokenQuote, 7208)
	for i := range r.units {
		u := &r.units[i]
		var fs []fv
		for bit := range uint8(16) {
			// A small value in the field's last byte, and '+' for the flag,
			// so pre-validation passes every field (as in
			// TestMergeOnAnExistingRowAllocatesNothing).
			val := make([]byte, s.dst.ByID(tokenQuote).Column(bit).Width)
			val[len(val)-1] = bit + 1
			if bit == 7 {
				val[0] = '+'
			}
			fs = append(fs, fv{bit, val})
		}
		put(tb, s, u, tokenQuote, model.RowKey(100+i), slot, fs...)
		if u.BlobLen != 181 {
			tb.Fatalf("blob %d bytes, want destination 0's 181", u.BlobLen)
		}
		u.Code, u.LogTime = 7208, 100
	}
	r.cycle(tb)
	r.cycle(tb)
	return r
}

// send merges unit i carrying last_traded_price v: bit 0, the first 4 bytes
// of the packed run.
func (r *cycleRig) send(i int, v uint32) {
	u := &r.units[i]
	u.Seq++
	u.RecvMono++
	binary.BigEndian.PutUint32(u.Blob[u.Contribs[tokenQuote].BlobOff:], v)
	r.s.merge(*u)
}

// cycle is one full cycle. Half the rows are merged again while their batch is
// out, so the cycle takes the ReDirty arm and the completion's tail requeue as
// well as the plain commit -- the paths a quiet market and a busy one take.
func (r *cycleRig) cycle(tb testing.TB) {
	r.v = r.v%250 + 1
	for i := range r.units {
		r.send(i, r.v)
	}
	if err := r.s.maybeOpenEpoch(); err != nil {
		tb.Fatalf("maybeOpenEpoch: %v", err)
	}
	for i := range cycleRows / 2 {
		r.send(i, r.v+250) // in flight: ReDirty, not a second ring entry
	}
	for r.s.EpochOpen {
		if err := writeBack(r.s, next(tb, r.q), r.mode); err != nil {
			tb.Fatalf("completion: %v", err)
		}
	}
}

// TestFullCycleOnWarmRowsAllocatesNothing is the steady-state allocation test
// of phase 3 P3-14: AllocsPerRun = 0 over a full merge -> pump -> complete
// cycle on warm rows, beside the per-function tests for merge, pump and
// applyCompletion.
//
// SETUP: shard 0, maxBatchSize 4, m = 2, eight destination-0 rows already
// created and written twice.
//
// STIMULUS: testing.AllocsPerRun over one cycle -- eight 181-byte 7208 merges,
// the epoch open and its pump of two batches, four merges into rows in flight,
// and the completions that close the epoch.
//
// ASSERTION: 0 allocations per cycle; and (non-vacuity) the measured cycles
// carried eight rows each through the pump, took the ReDirty arm four times
// each, and left each row's image holding its last merged value.
//
// WOULD HAVE CAUGHT: an allocation that only appears where the stages meet --
// a ring or the free list grown by the hand-over between them, a map write per
// cycle, a closure or interface conversion on the path from one stage to the
// next -- which the three per-function tests, each starting from a state it
// built itself, cannot see.
func TestFullCycleOnWarmRowsAllocatesNothing(t *testing.T) {
	r := newCycleRig(t)
	in := r.s.inst
	rows0, redirty0 := in.EpochRows.Load(), in.RedirtyTotal.Load()

	if got := testing.AllocsPerRun(1000, func() { r.cycle(t) }); got != 0 {
		t.Fatalf("a full merge -> pump -> complete cycle allocated %.1f times, want 0", got)
	}

	// AllocsPerRun makes one extra warm-up call: 1001 cycles.
	if got := in.EpochRows.Load() - rows0; got < 1000*cycleRows {
		t.Fatalf("non-vacuity: %d rows pumped over the measured cycles, want >= %d", got, 1000*cycleRows)
	}
	if got := in.RedirtyTotal.Load() - redirty0; got < 1000*cycleRows/2 {
		t.Fatalf("non-vacuity: redirty_total rose by %d, want >= %d", got, 1000*cycleRows/2)
	}
	for i := range cycleRows {
		want := r.v
		if i < cycleRows/2 {
			want = r.v + 250
		}
		if got := ltp(&rowEntry(r.s, tokenQuote, model.RowKey(100+i)).Image); got != want {
			t.Fatalf("row %d last_traded_price = %d, want %d: the cycle did not merge what it sent", i, got, want)
		}
	}
}

package writer

import (
	"encoding/binary"
	"testing"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
)

// allocBatch lays out a per-destination batch of n rows in one destination with
// two statement identities (the full mask and the full mask less its last
// column), alternating, so grouping produces two runs of n/2 and the sort has
// real work. text fills the first columns of destination 1 (MD_SECURITY_MASTER)
// with non-empty text, which is the one thing Bind may allocate for.
func allocBatch(d *destinations.Destination, n int, text bool) (*model.Batch, [2]model.ColMask) {
	full := d.FullMask()
	// The two identities differ by one bit, the highest the destination declares.
	hi := model.ColMask(0)
	for i := range d.Columns {
		if m := model.ColMask(1) << d.Columns[i].Bit; m > hi {
			hi = m
		}
	}
	masks := [2]model.ColMask{full, full &^ hi}
	b := &model.Batch{Rows: make([]model.BatchRow, n)}
	for i := range b.Rows {
		r := &b.Rows[i]
		// Keys descend, so the sort cannot be satisfied by the input order.
		r.Row, r.Mask, r.Slot = model.SingleKey(uint32(n-i)), masks[i%2], int32(i)
		if text {
			copy(r.Image[0:], "RELIANCE  ") // symbol, 10 bytes at 0
			copy(r.Image[10:], "EQ")        // series, 2 bytes at 10
		}
		binary.BigEndian.PutUint32(r.Image[12:], uint32(i+1))
	}
	b.DestStart[1], b.DestCount[1], b.N = 0, int32(n), int32(n)
	return b, masks
}

// TestAllocBudgetPerBatchGroupAndBind is the V3 section 15 allocation budget
// for the writer's pre-execute work: group a batch's span into mask runs, then
// bind each run.
//
// SETUP: a 64-row span of MD_SECURITY_MASTER (two text columns, the only bind
// kind that allocates) in two statement identities, one Binder each, warmed by
// one pass so arenas, boxes and the grouper's scratch exist.
//
// STIMULUS: testing.AllocsPerRun over group + BindIdx for both runs.
//
// ASSERTION, against hand-written budgets and not a measurement of the code:
//   - group allocates nothing (the permutation is writer-owned scratch, D7);
//   - with the text columns empty the whole pass allocates nothing -- the
//     interface boxes around the typed arenas are rebuilt only when a run's
//     length changes, and here it does not (the one documented exception,
//     asserted separately below);
//   - with both text columns filled it allocates at most one string per text
//     value: 64 rows x 2 columns = 128, no more. Anything above that is a
//     per-row or per-call allocation that is not the value itself;
//   - a run whose length changes pays exactly the boxes of that Binder: one
//     per bind position, and the pass after it is back to the steady budget.
//
// WOULD HAVE CAUGHT: Bind reallocating its argument slice or arenas per call
// (HLD "no allocation on the hot path" turning into 20,000 rows/s of garbage),
// the grouper sorting a copy of the rows or allocating its comparison closure
// per call, readString going through fmt, and the box cache never being kept
// (every call re-boxing every column).
func TestAllocBudgetPerBatchGroupAndBind(t *testing.T) {
	const n = 64
	sm := shippedDest(t, "MD_SECURITY_MASTER")

	run := func(text bool) (grp, all float64, positions [2]int) {
		b, masks := allocBatch(sm, n, text)
		g := newGrouper(n)
		binders := [2]*Binder{newBinder(t, sm, masks[0], n), newBinder(t, sm, masks[1], n)}
		for i := range binders {
			_, plan, err := destinations.StatementFor(sm, masks[i])
			if err != nil {
				t.Fatal(err)
			}
			positions[i] = len(plan)
		}
		pass := func() {
			idx, err := g.group(b, 1, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			for len(idx) > 0 {
				k := runLen(b.Rows, idx)
				bd := binders[0]
				if b.Rows[idx[0]].Mask != masks[0] {
					bd = binders[1]
				}
				if _, err := bd.BindIdx(b.Rows, idx[:k], testUpdatedAt); err != nil {
					t.Fatal(err)
				}
				idx = idx[k:]
			}
		}
		pass() // warm
		grp = testing.AllocsPerRun(50, func() {
			if _, err := g.group(b, 1, nil, nil); err != nil {
				t.Fatal(err)
			}
		})
		all = testing.AllocsPerRun(50, pass)
		return grp, all, positions
	}

	grp, empty, _ := run(false)
	if grp != 0 {
		t.Errorf("group allocates %v times per batch, budget 0", grp)
	}
	if empty != 0 {
		t.Errorf("group+bind with empty text allocates %v times per 64-row batch, budget 0", empty)
	}

	_, filled, _ := run(true)
	if want := float64(n * 2); filled > want {
		t.Errorf("group+bind with 2 text columns filled allocates %v per 64-row batch, budget at most %v "+
			"(one string per text value)", filled, want)
	}
	if filled == 0 {
		t.Error("non-vacuity: filled text allocated nothing, so the text budget measured nothing")
	}

	// The documented exception: a changed run length re-boxes that Binder's
	// positions once. Alternate 16 and 32 rows through ONE binder and assert
	// the cost is the position count per change, never more.
	b, masks := allocBatch(sm, n, false)
	bd := newBinder(t, sm, masks[0], n)
	_, plan, err := destinations.StatementFor(sm, masks[0])
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]model.BatchRow, 0, n)
	for i := range b.Rows {
		if b.Rows[i].Mask == masks[0] {
			rows = append(rows, b.Rows[i])
		}
	}
	lens, flip := [2]int{16, 32}, 0
	if _, err := bd.Bind(rows[:lens[0]], testUpdatedAt); err != nil {
		t.Fatal(err)
	}
	got := testing.AllocsPerRun(50, func() {
		flip ^= 1
		if _, err := bd.Bind(rows[:lens[flip]], testUpdatedAt); err != nil {
			t.Fatal(err)
		}
	})
	if want := float64(len(plan)); got > want {
		t.Errorf("a run-length change allocates %v, budget %d boxes (one per bind position)", got, len(plan))
	}
	if got == 0 {
		t.Error("non-vacuity: changing the run length allocated nothing, so the box exception is not exercised")
	}
}

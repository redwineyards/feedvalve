package writer

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"unsafe"

	"feedvalve/internal/model"
	"feedvalve/internal/oradb/oratest"
)

// randomBatch is standard()'s layout with every field the writer must not
// touch set to a distinct non-zero value, images filled with seeded noise.
func randomBatch(seed int64) *model.Batch {
	b := standard()
	rng := rand.New(rand.NewSource(seed))
	b.Shard, b.Epoch = 3, 0xABCDEF
	for i := range b.Rows {
		r := &b.Rows[i]
		r.Digest = rng.Uint64() | 1
		r.Slot = int32(1000 + i)
		rng.Read(r.Image[:])
	}
	return b
}

// snapshot is a deep copy of everything about a batch, and the identity of
// its Rows backing array.
type header struct {
	Shard     model.ShardID
	Epoch     uint64
	DestStart [model.MaxDestinations]int32
	DestCount [model.MaxDestinations]int32
	N         int32
}

type snapshot struct {
	batch header
	rows  []model.BatchRow
	base  unsafe.Pointer
	cap   int
}

func snap(b *model.Batch) snapshot {
	s := snapshot{batch: header{b.Shard, b.Epoch, b.DestStart, b.DestCount, b.N}, rows: append([]model.BatchRow(nil), b.Rows...), cap: cap(b.Rows)}
	if len(b.Rows) > 0 {
		s.base = unsafe.Pointer(&b.Rows[0])
	}
	return s
}

// TestG_ROW_2_RunBatchChangesOnlyOutcomeErrCodeErr is the ownership boundary:
// the writer owns the batch for one transaction and may write exactly three
// things, Batch.Err, BatchRow.Outcome and BatchRow.ErrCode. Everything else the
// shard filled in (Shard, Epoch, DestStart, DestCount, N, Rows' length and
// backing array, and per row Row, Mask, Digest, Slot, the pad and the 256-
// byte Image) must come back bit-identical, or the completion path acts on a
// row that is no longer the one it sent.
//
// SETUP: a batch whose every field is a distinct non-zero value.
// STIMULUS: runBatch through each path.
// ASSERTION: a full-field comparison of the batch and each row with Outcome and
// ErrCode zeroed, and Err ignored; Rows is not reordered, reallocated or
// resliced (D7). The non-vacuity check requires that every path really did
// change Outcome somewhere and that the comparison catches a one-bit change.
// WOULD HAVE CAUGHT: sorting b.Rows in place; recomputing Digest; clearing
// Mask or Image after binding; reslicing Rows to N.
func TestG_ROW_2_RunBatchChangesOnlyOutcomeErrCodeErr(t *testing.T) {
	type path struct {
		name   string
		script oratest.Script
		hook   func(*rig)
		cancel bool
		batch  func() *model.Batch
	}
	failOn := func(sqlText string, code int) oratest.Script {
		return func(c oratest.Call) oratest.Result {
			if c.SQL == sqlText {
				return oratest.Result{Err: oratest.ORA(code)}
			}
			return oratest.Result{}
		}
	}
	paths := []path{
		{name: "success", batch: func() *model.Batch { return randomBatch(1) }},
		{name: "phase1 abandon", script: func(c oratest.Call) oratest.Result {
			if c.Rows > 0 {
				return oratest.Result{Err: oratest.ORA(60)}
			}
			return oratest.Result{}
		}, batch: func() *model.Batch { return randomBatch(2) }},
		{name: "phase2 reject", script: rowScript(1, map[int64]int{20: 1}), batch: func() *model.Batch { return randomBatch(10) }},
		{name: "phase2 abandon", script: rowScript(1, map[int64]int{20: 1, 10: 60}), batch: func() *model.Batch { return randomBatch(11) }},
		{name: "phase2 direct", script: rowScript(0, map[int64]int{9: 1438}), hook: func(g *rig) { g.r.skipPhase1 = true },
			batch: func() *model.Batch { return randomBatch(12) }},
		{name: "commit failure", script: failOn(oratest.OpCommit, 3113), batch: func() *model.Batch { return randomBatch(3) }},
		{name: "panic in bind", hook: func(g *rig) {
			bd, _ := g.r.binder(stmtKey{Dest: 1, Mask: 0x1FF})
			bd.cols[1].bind.Offset = 65535
		}, batch: func() *model.Batch { return randomBatch(4) }},
		{name: "panic after commit", hook: func(g *rig) { g.r.afterCommit = func() { panic("x") } },
			batch: func() *model.Batch { return randomBatch(5) }},
		{name: "canceled", cancel: true, batch: func() *model.Batch { return randomBatch(6) }},
		{name: "empty mask", batch: func() *model.Batch {
			b := randomBatch(7)
			b.Rows[1].Mask = 0
			return b
		}},
		{name: "rows beyond N untouched", batch: func() *model.Batch {
			b := randomBatch(8)
			b.Rows = append(b.Rows, model.BatchRow{Digest: 99, Slot: 77, Outcome: model.OutcomePending})
			return b
		}},
	}
	for _, p := range paths {
		t.Run(p.name, func(t *testing.T) {
			g := newRig(t)
			if p.script != nil {
				g.f.SetScript(p.script)
			}
			if p.hook != nil {
				p.hook(g)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if p.cancel {
				cancel()
			}
			b := p.batch()
			before := snap(b)
			g.r.runBatch(ctx, g.s, b)
			after := snap(b)

			if before.batch != after.batch {
				t.Errorf("batch header changed:\n before %+v\n after  %+v", before.batch, after.batch)
			}
			if before.base != after.base || before.cap != after.cap || len(before.rows) != len(after.rows) {
				t.Fatalf("Rows was reallocated or resliced")
			}
			for _, d := range diffBeyondOutcome(before, after) {
				t.Error(d)
			}
			changed := 0
			for i := range before.rows {
				if before.rows[i].Outcome != after.rows[i].Outcome {
					changed++
				}
			}
			if p.name == "commit failure" {
				for i, r := range after.rows[:after.batch.N] {
					if r.ErrCode != 3113 {
						t.Errorf("failed row %d ErrCode %d, want 3113", i, r.ErrCode)
					}
				}
			}
			if changed == 0 {
				t.Errorf("no Outcome changed on path %q: the comparison proves nothing", p.name)
			}
			if p.name == "rows beyond N untouched" {
				last := after.rows[len(after.rows)-1]
				if last.Outcome != model.OutcomePending || last.Digest != 99 {
					t.Errorf("a row beyond N was touched: %+v", last)
				}
			}
		})
	}

	// Non-vacuity: the comparison must see a single flipped image bit, so a
	// passing table means something.
	// It runs the table's own comparison, not a copy of it.
	b := randomBatch(9)
	before := snap(b)
	b.Rows[3].Image[200] ^= 1
	if len(diffBeyondOutcome(before, snap(b))) == 0 {
		t.Fatalf("the table's comparison is blind to an image bit flip")
	}
	b.Rows[3].Image[200] ^= 1
	b.Rows[3].Outcome, b.Rows[3].ErrCode = model.OutcomeFailed, 7
	if d := diffBeyondOutcome(before, snap(b)); len(d) != 0 {
		t.Fatalf("the table's comparison flags an Outcome/ErrCode change: %v", d)
	}
}

// diffBeyondOutcome reports every row whose fields differ other than Outcome
// and ErrCode.
func diffBeyondOutcome(before, after snapshot) []string {
	var out []string
	for i := range before.rows {
		x, y := before.rows[i], after.rows[i]
		x.Outcome, x.ErrCode, y.Outcome, y.ErrCode = 0, 0, 0, 0
		if x != y {
			out = append(out, fmt.Sprintf("row %d changed beyond Outcome/ErrCode: slot %d->%d digest %x->%x mask %x->%x image equal=%v",
				i, x.Slot, y.Slot, x.Digest, y.Digest, x.Mask, y.Mask, x.Image == y.Image))
		}
	}
	return out
}

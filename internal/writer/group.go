package writer

import (
	"fmt"
	"slices"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// grouper turns one destination's span of a batch into runs of rows that share
// a statement identity (V3 section 4.2, ADR-W07). It sorts a writer-owned
// permutation of indices, never the rows: Batch.Rows keeps the order the shard
// gave it (ruling D7) and a sort of 288-byte rows is avoided. Not safe for
// concurrent use; each writer owns one.
type grouper struct {
	perm []int32
}

// newGrouper sizes the scratch for the largest span group will be given,
// maxBatchSizeCeiling.
func newGrouper(ceiling int) *grouper {
	return &grouper{perm: make([]int32, 0, ceiling)}
}

// group returns the indices into b.Rows of destination d's rows, ordered by
// (Mask ascending, Row ascending), which is the lock order of V3 section 7.2.
// Ties break on index, so the order is deterministic.
//
// A row with an empty mask cannot name a SET list (W-4): it is marked
// OutcomeRejected, counted in batch_row_empty_mask{d}, logged under the
// writer's rate limit and left out of the result; the batch continues. inst
// and log may be nil.
//
// Seam for P4-07a: the result holds absolute indices into b.Rows, while
// Binder.Bind takes a contiguous []BatchRow. Rows must not be copied or
// reordered on the hot path (D7), so P4-07a needs an index-based Bind variant
// (BindIdx(rows, idx, updatedAt)) in bind.go.
//
// The result aliases the grouper's scratch and is valid until the next call.
func (g *grouper) group(b *model.Batch, d model.DestID, inst *Instruments, log *obs.AppLogger) ([]int32, error) {
	start, n := int(b.DestStart[d]), int(b.DestCount[d])
	if n > cap(g.perm) || start < 0 || n < 0 || start+n > len(b.Rows) {
		return nil, fmt.Errorf("writer: destination %d span [%d,+%d) outside ceiling %d or %d rows",
			d, start, n, cap(g.perm), len(b.Rows))
	}
	g.perm = g.perm[:0]
	for i := start; i < start+n; i++ {
		r := &b.Rows[i]
		if r.Mask == 0 {
			r.Outcome = model.OutcomeRejected
			if inst != nil && int(d) < len(inst.cache.BatchRowEmptyMask) {
				add(inst.cache.BatchRowEmptyMask[d])
			}
			if log != nil {
				log.Warnf(obs.ClassWriteFailed, "writer: batch row empty mask dest=%d row=%d slot=%d rejected", d, r.Row, r.Slot)
			}
			continue
		}
		g.perm = append(g.perm, int32(i))
	}
	rows := b.Rows
	slices.SortFunc(g.perm, func(a, c int32) int {
		ra, rc := &rows[a], &rows[c]
		switch {
		case ra.Mask != rc.Mask:
			if ra.Mask < rc.Mask {
				return -1
			}
			return 1
		case ra.Row != rc.Row:
			if ra.Row < rc.Row {
				return -1
			}
			return 1
		}
		return int(a - c)
	})
	return g.perm, nil
}

// runLen is the length of the leading run of idx whose rows share one mask.
// idx must be non-empty and sorted by group.
func runLen(rows []model.BatchRow, idx []int32) int {
	m := rows[idx[0]].Mask
	n := 1
	for n < len(idx) && rows[idx[n]].Mask == m {
		n++
	}
	return n
}

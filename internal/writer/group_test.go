package writer

import (
	"bytes"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// mkBatch lays rows out as [dest0..., dest1..., dest2...] with DestStart and
// DestCount set from the per-destination slices of (mask, row) pairs.
func mkBatch(per [3][][2]uint64) *model.Batch {
	b := &model.Batch{}
	for d := 0; d < 3; d++ {
		b.DestStart[d] = int32(len(b.Rows))
		for _, p := range per[d] {
			b.Rows = append(b.Rows, model.BatchRow{Mask: model.ColMask(p[0]), Row: model.RowKey(p[1]),
				Slot: int32(len(b.Rows))})
		}
		b.DestCount[d] = int32(len(per[d]))
	}
	b.N = int32(len(b.Rows))
	return b
}

func cloneRows(rows []model.BatchRow) []model.BatchRow { return append([]model.BatchRow(nil), rows...) }

// TestGroupMatchesStableSort is a property test: for seeded random spans the
// permutation must equal what sort.SliceStable gives on (Mask, Row), and
// Batch.Rows must be unchanged (ruling D7).
//
// WOULD HAVE CAUGHT: sorting Rows in place; sorting by Row only; a descending
// comparator.
func TestGroupMatchesStableSort(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	g := newGrouper(1000)
	for _, n := range []int{1, 2, 250, 1000} {
		var per [3][][2]uint64
		for i := 0; i < n; i++ {
			// few masks and few keys, so ties on both exist
			per[1] = append(per[1], [2]uint64{uint64(1 + rng.Intn(4)), uint64(rng.Intn(40))})
		}
		b := mkBatch(per)
		before := cloneRows(b.Rows)

		got, err := g.group(b, 1, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		ref := make([]int32, n)
		for i := range ref {
			ref[i] = int32(i)
		}
		sort.SliceStable(ref, func(x, y int) bool {
			a, c := before[ref[x]], before[ref[y]]
			if a.Mask != c.Mask {
				return a.Mask < c.Mask
			}
			return a.Row < c.Row
		})
		if len(got) != n {
			t.Fatalf("n=%d: %d indices", n, len(got))
		}
		for i := range ref {
			if got[i] != ref[i] {
				t.Fatalf("n=%d: position %d = %d, want %d", n, i, got[i], ref[i])
			}
		}
		for i := range before {
			if b.Rows[i] != before[i] {
				t.Fatalf("n=%d: Rows[%d] changed by grouping", n, i)
			}
		}
	}
}

// TestG_ORD_1_WriterLockOrderAscending is the writer half of gate G-ORD-1:
// walking destinations 0,1,2 and each one's grouped indices must visit rows in
// ascending (DestID, Mask, Row) order, so two writers can never hold locks in
// opposite order.
//
// SETUP: rows deliberately given in descending order in every destination.
// STIMULUS: group each destination in the fixed order.
// ASSERTION: the concatenated visit sequence is strictly ascending by
// (dest, mask, row), and every row was visited exactly once.
// WOULD HAVE CAUGHT: grouping in input order; destinations walked 2,1,0;
// mask-major vs row-major confusion.
func TestG_ORD_1_WriterLockOrderAscending(t *testing.T) {
	var per [3][][2]uint64
	for d := 0; d < 3; d++ {
		for m := uint64(3); m >= 1; m-- {
			for r := uint64(5); r >= 1; r-- {
				per[d] = append(per[d], [2]uint64{m, r * 10})
			}
		}
	}
	b := mkBatch(per)

	type k struct{ d, m, r uint64 }
	check := func(visit []k) (bad int) {
		for i := 1; i < len(visit); i++ {
			a, c := visit[i-1], visit[i]
			if a.d > c.d || (a.d == c.d && (a.m > c.m || (a.m == c.m && a.r >= c.r))) {
				bad++
			}
		}
		return
	}

	// non-vacuity: the raw input order violates the rule, so the checker bites.
	var raw []k
	for d := 0; d < 3; d++ {
		for _, r := range b.Dest(model.DestID(d)) {
			raw = append(raw, k{uint64(d), uint64(r.Mask), uint64(r.Row)})
		}
	}
	if check(raw) == 0 {
		t.Fatal("vacuous: unsorted input passes the order check")
	}

	g := newGrouper(1000)
	var visit []k
	seen := map[int32]bool{}
	for d := 0; d < 3; d++ {
		idx, err := g.group(b, model.DestID(d), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range idx {
			if seen[i] {
				t.Fatalf("row %d visited twice", i)
			}
			seen[i] = true
			visit = append(visit, k{uint64(d), uint64(b.Rows[i].Mask), uint64(b.Rows[i].Row)})
		}
	}
	if len(visit) != 45 {
		t.Fatalf("visited %d rows, want 45", len(visit))
	}
	if n := check(visit); n != 0 {
		t.Errorf("%d order violations in %v", n, visit)
	}
}

// TestRunWalk: runLen splits the sorted indices into runs of one mask each,
// in ascending mask order, with hand-written expected run lengths.
func TestRunWalk(t *testing.T) {
	b := mkBatch([3][][2]uint64{{{9, 3}, {2, 5}, {9, 1}, {2, 1}, {4, 7}, {9, 2}}})
	g := newGrouper(10)
	idx, err := g.group(b, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var masks, lens []int
	for len(idx) > 0 {
		n := runLen(b.Rows, idx)
		masks = append(masks, int(b.Rows[idx[0]].Mask))
		lens = append(lens, n)
		idx = idx[n:]
	}
	wantM, wantL := []int{2, 4, 9}, []int{2, 1, 3}
	for i := range wantM {
		if i >= len(masks) || masks[i] != wantM[i] || lens[i] != wantL[i] {
			t.Fatalf("runs masks=%v lens=%v, want %v %v", masks, lens, wantM, wantL)
		}
	}
	if len(masks) != 3 {
		t.Fatalf("runs masks=%v", masks)
	}
}

// TestEmptyMaskRowIsRejectedAndBatchContinues covers W-4.
//
// SETUP: destination 2 holds one empty-mask row between two good ones.
// ASSERTION: that row is Rejected with no error code; the others stay Pending
// and are returned; batch_row_empty_mask{2} is 1 and no other destination's is.
// WOULD HAVE CAUGHT: building an empty SET list; failing the whole batch;
// counting under the wrong destination.
func TestEmptyMaskRowIsRejectedAndBatchContinues(t *testing.T) {
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: 1})
	in := NewInstruments(reg, 3)
	in.ResolveCache(reg, 1, 3)
	reg.Freeze()

	b := mkBatch([3][][2]uint64{{}, {}, {{3, 20}, {0, 15}, {3, 10}}})
	g := newGrouper(10)
	var logbuf bytes.Buffer
	log := obs.NewAppLogger(&logbuf, obs.LevelWarn, 60, nil)
	idx, err := g.group(b, 2, in, log)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 2 || idx[0] != 2 || idx[1] != 0 {
		t.Errorf("indices = %v, want [2 0]", idx)
	}
	if got := b.Rows[1].Outcome; got != model.OutcomeRejected {
		t.Errorf("empty-mask row outcome = %v, want Rejected", got)
	}
	if b.Rows[0].Outcome != model.OutcomePending || b.Rows[2].Outcome != model.OutcomePending {
		t.Error("good rows must stay Pending")
	}
	if b.Rows[1].ErrCode != 0 {
		t.Errorf("rejected row carries error code %d, want none", b.Rows[1].ErrCode)
	}
	if out := logbuf.String(); strings.Count(out, "WARN") != 1 || !strings.Contains(out, "empty mask") || !strings.Contains(out, "dest=2") {
		t.Errorf("want exactly one WARN naming empty mask and dest=2, got %q", out)
	}
	if got := in.cache.BatchRowEmptyMask[2].Load(); got != 1 {
		t.Errorf("batch_row_empty_mask{2} = %d, want 1", got)
	}
	if in.cache.BatchRowEmptyMask[0].Load() != 0 || in.cache.BatchRowEmptyMask[1].Load() != 0 {
		t.Error("other destinations counted")
	}
}

func TestGroupRejectsSpanBeyondCeiling(t *testing.T) {
	b := mkBatch([3][][2]uint64{{{1, 1}, {1, 2}, {1, 3}}})
	if _, err := newGrouper(2).group(b, 0, nil, nil); err == nil {
		t.Error("span of 3 over ceiling 2 accepted")
	}
}

// TestAllEmptyMaskSpanReturnsEmpty: a span of only empty-mask rows yields no
// indices (so the caller never calls runLen on it) and rejects every row.
func TestAllEmptyMaskSpanReturnsEmpty(t *testing.T) {
	b := mkBatch([3][][2]uint64{{{0, 1}, {0, 2}}})
	idx, err := newGrouper(4).group(b, 0, nil, nil)
	if err != nil || len(idx) != 0 {
		t.Fatalf("idx=%v err=%v, want empty", idx, err)
	}
	for i, r := range b.Rows {
		if r.Outcome != model.OutcomeRejected {
			t.Errorf("row %d outcome %v, want Rejected", i, r.Outcome)
		}
	}
}

// TestGroupRejectsBadSpans: a span outside Rows is an error, never a panic.
func TestGroupRejectsBadSpans(t *testing.T) {
	cases := []struct {
		name         string
		start, count int32
	}{
		{"start past end", 5, 1},
		{"negative count", 0, -1},
		{"negative start", -1, 1},
		{"length overruns rows", 1, 3},
	}
	for _, c := range cases {
		b := mkBatch([3][][2]uint64{{{1, 1}, {1, 2}}})
		b.DestStart[0], b.DestCount[0] = c.start, c.count
		if idx, err := newGrouper(8).group(b, 0, nil, nil); err == nil {
			t.Errorf("%s: accepted, idx=%v", c.name, idx)
		}
	}
}

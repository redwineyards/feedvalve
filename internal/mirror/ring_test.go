package mirror

import (
	"math/rand"
	"slices"
	"testing"
)

// checkRing asserts r holds exactly want, in order, without disturbing it, and
// that its capacity is a power of two.
func checkRing(t *testing.T, r *ring, want []int32) {
	t.Helper()
	if r.Len() != len(want) {
		t.Fatalf("Len() = %d, want %d", r.Len(), len(want))
	}
	if c := len(r.buf); c&(c-1) != 0 {
		t.Fatalf("capacity %d is not a power of two", c)
	}
	mask := len(r.buf) - 1
	for i, w := range want {
		if got := r.buf[(r.head+i)&mask]; got != w {
			t.Fatalf("position %d = %d, want %d (want %v)", i, got, w, want)
		}
	}
}

// drain pops everything and returns it in pop order.
func drain(r *ring) []int32 {
	var out []int32
	for {
		s, ok := r.PopFront()
		if !ok {
			return out
		}
		out = append(out, s)
	}
}

// TestRingMatchesASliceReference drives 100,000 seeded random operations
// against a ring and a plain-slice model of the same queue and requires them to
// agree after every step.
//
// The ring starts at capacity 4 and pops roughly as often as it pushes, so head
// wraps continuously and both growth paths -- PushBack and PushFrontBlock --
// fire while the queued run straddles the end of buf. That is the case a
// re-linearising copy gets wrong (copying [head, head+n) as if it never
// wrapped), and it decodes nothing wrongly until a slot is popped.
func TestRingMatchesASliceReference(t *testing.T) {
	rng := rand.New(rand.NewSource(20260212))
	r := newRing(4)
	var ref []int32
	next := int32(1)
	var grewWrapped, blockGrewWrapped, pushes, pops, blocks int

	for op := 0; op < 100_000; op++ {
		wrapped := r.n > 0 && r.head+r.n > len(r.buf)
		capBefore := len(r.buf)
		switch k := rng.Intn(10); {
		case k < 4: // PushBack
			grew := r.PushBack(next)
			ref = append(ref, next)
			next++
			pushes++
			if grew != (len(r.buf) != capBefore) {
				t.Fatalf("op %d: PushBack reported grew=%v but capacity went %d -> %d",
					op, grew, capBefore, len(r.buf))
			}
			if grew && wrapped {
				grewWrapped++
			}
		case k < 8: // PopFront
			s, ok := r.PopFront()
			if ok != (len(ref) > 0) {
				t.Fatalf("op %d: PopFront ok=%v with reference length %d", op, ok, len(ref))
			}
			if ok {
				if s != ref[0] {
					t.Fatalf("op %d: PopFront = %d, want %d", op, s, ref[0])
				}
				ref = ref[1:]
				pops++
			}
		case k < 9: // PushFrontBlock of 0..7 fresh slots
			blk := make([]int32, rng.Intn(8))
			for i := range blk {
				blk[i] = next
				next++
			}
			grew := r.PushFrontBlock(blk)
			ref = append(slices.Clone(blk), ref...)
			blocks++
			if grew != (len(r.buf) != capBefore) {
				t.Fatalf("op %d: PushFrontBlock reported grew=%v but capacity went %d -> %d",
					op, grew, capBefore, len(r.buf))
			}
			if grew && wrapped {
				blockGrewWrapped++
			}
		default: // Peek
			s, ok := r.Peek()
			if ok != (len(ref) > 0) || (ok && s != ref[0]) {
				t.Fatalf("op %d: Peek = (%d, %v), reference head %v", op, s, ok, ref[:min(1, len(ref))])
			}
		}
		checkRing(t, &r, ref)
		// Keep the queue short enough that it keeps wrapping rather than
		// settling into one huge buffer that never grows again.
		if len(ref) > 600 {
			r.Reset()
			ref = ref[:0]
			r = newRing(4)
		}
	}

	// Non-vacuity: the sequence must have exercised what it claims to.
	if grewWrapped == 0 || blockGrewWrapped == 0 || pushes == 0 || pops == 0 || blocks == 0 {
		t.Fatalf("the seed did not cover the cases: grewWrapped=%d blockGrewWrapped=%d "+
			"pushes=%d pops=%d blocks=%d", grewWrapped, blockGrewWrapped, pushes, pops, blocks)
	}
}

// TestRingPushFrontBlockGrowsMidBlock: a full ring with head at 0 receives a
// block bigger than the free space, so the block cannot be written without
// growing -- and the result must still be the block, in order, then the old
// contents.
func TestRingPushFrontBlockGrowsMidBlock(t *testing.T) {
	r := newRing(4)
	for s := int32(1); s <= 4; s++ {
		r.PushBack(s)
	}
	if r.head != 0 || r.Len() != len(r.buf) {
		t.Fatalf("setup: head=%d len=%d cap=%d, want a full ring at head 0", r.head, r.Len(), len(r.buf))
	}
	if !r.PushFrontBlock([]int32{10, 11, 12, 13, 14, 15}) {
		t.Fatal("PushFrontBlock into a full ring did not report growth")
	}
	if len(r.buf) != 16 {
		t.Fatalf("capacity = %d, want 16 (4 doubled until 10 fit)", len(r.buf))
	}
	if got, want := drain(&r), []int32{10, 11, 12, 13, 14, 15, 1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Fatalf("drained %v, want %v", got, want)
	}
}

// TestRingBlockPreservesOrderWhereOneAtATimeReverses pins section 8.6's reason
// for the block form: a failed batch's rows, pushed to the front one at a time
// in forward order, come out reversed -- inverting FIFO among the rows that have
// waited longest. The block form keeps them in order.
func TestRingBlockPreservesOrderWhereOneAtATimeReverses(t *testing.T) {
	failed := []int32{7, 8, 9}

	block := newRing(8)
	block.PushBack(100) // a row dirtied after the failed batch was drawn
	block.PushFrontBlock(failed)
	if got, want := drain(&block), []int32{7, 8, 9, 100}; !slices.Equal(got, want) {
		t.Fatalf("block form drained %v, want %v", got, want)
	}

	// Non-vacuity: the one-at-a-time form really does reverse, so the
	// assertion above is distinguishing something.
	single := newRing(8)
	single.PushBack(100)
	for _, s := range failed {
		single.PushFrontBlock([]int32{s})
	}
	if got, want := drain(&single), []int32{9, 8, 7, 100}; !slices.Equal(got, want) {
		t.Fatalf("one-at-a-time drained %v, want the reversal %v", got, want)
	}
}

// TestRingCapacityIsAlwaysAPowerOfTwo: pre-sizing rounds up (50,000 is the
// default per-ring figure and must become 65,536), and every growth doubles.
func TestRingCapacityIsAlwaysAPowerOfTwo(t *testing.T) {
	for _, tc := range []struct{ n, want int }{
		{0, 1}, {1, 1}, {2, 2}, {3, 4}, {4, 4}, {5, 8}, {50_000, 65_536}, {65_536, 65_536},
	} {
		if r := newRing(tc.n); len(r.buf) != tc.want {
			t.Errorf("newRing(%d) capacity = %d, want %d", tc.n, len(r.buf), tc.want)
		}
	}

	var r ring // the zero value grows from nothing
	for s := int32(0); s < 1000; s++ {
		r.PushBack(s)
		if c := len(r.buf); c&(c-1) != 0 {
			t.Fatalf("capacity %d after %d pushes is not a power of two", c, s+1)
		}
	}
	if got := drain(&r); len(got) != 1000 || got[0] != 0 || got[999] != 999 {
		t.Fatalf("zero-value ring drained %d slots, first/last wrong", len(got))
	}
}

// TestRingSwapAndReset: Swap exchanges whole rings in O(1) and Reset empties
// one while keeping its buffer.
func TestRingSwapAndReset(t *testing.T) {
	a, b := newRing(4), newRing(16)
	a.PushBack(1)
	a.PushBack(2)
	b.PushBack(3)
	a.Swap(&b)
	checkRing(t, &a, []int32{3})
	checkRing(t, &b, []int32{1, 2})
	if len(a.buf) != 16 || len(b.buf) != 4 {
		t.Fatalf("Swap did not exchange buffers: caps %d, %d", len(a.buf), len(b.buf))
	}

	buf := &a.buf[0]
	a.Reset()
	checkRing(t, &a, nil)
	if &a.buf[0] != buf {
		t.Fatal("Reset dropped the buffer; it must be retained")
	}
	if _, ok := a.PopFront(); ok {
		t.Fatal("PopFront on a reset ring reported a slot")
	}
	if _, ok := a.Peek(); ok {
		t.Fatal("Peek on a reset ring reported a slot")
	}
}

// TestRingOperationsBelowCapacityAllocateNothing: the ring lives on the shard
// goroutine, where every operation below capacity must be allocation-free.
func TestRingOperationsBelowCapacityAllocateNothing(t *testing.T) {
	a, b := newRing(64), newRing(64)
	blk := []int32{1, 2, 3, 4}
	n := testing.AllocsPerRun(1000, func() {
		a.PushBack(5)
		a.PushFrontBlock(blk)
		_, _ = a.Peek()
		for a.Len() > 0 {
			a.PopFront()
		}
		a.Swap(&b)
		a.Reset()
	})
	if n != 0 {
		t.Fatalf("ring operations below capacity allocated %v times per run, want 0", n)
	}
}

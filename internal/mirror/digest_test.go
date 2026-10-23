package mirror

import (
	"hash/maphash"
	"testing"

	"feedvalve/internal/model"
)

// TestNonZeroRemapsOnlyZero: zero is Entry.Digest's "never written" sentinel,
// so a hash of 0 must become 1 -- and nothing else may move. Tested on the
// function directly, because no image can be found that hashes to 0.
func TestNonZeroRemapsOnlyZero(t *testing.T) {
	for _, tc := range []struct{ in, want uint64 }{
		{0, 1}, {1, 1}, {2, 2}, {1 << 63, 1 << 63}, {^uint64(0), ^uint64(0)},
	} {
		if got := nonZero(tc.in); got != tc.want {
			t.Errorf("nonZero(%#x) = %#x, want %#x", tc.in, got, tc.want)
		}
	}
}

// TestDigestDependsOnlyOnTheGivenSlice: the digest of img[:n] is a function of
// those n bytes alone. Whatever lies past n in the backing array -- a longer
// previous value's tail -- must not reach it. This is G-DIG-3's precondition;
// the gate itself, that the merge rewrites the whole region, is the merge's.
func TestDigestDependsOnlyOnTheGivenSlice(t *testing.T) {
	seed := maphash.MakeSeed()
	var a, b [model.ImageSize]byte
	for i := range a {
		a[i] = byte(i)
		b[i] = byte(i)
	}
	const n = 181
	for i := n; i < len(b); i++ {
		b[i] ^= 0xFF // a different tail past n
	}
	if digest(seed, a[:n]) != digest(seed, b[:n]) {
		t.Fatal("two images equal in [:n] digest differently: bytes past n reached the hash")
	}

	// Non-vacuity: the digest does see every byte inside the slice, including
	// the last one, and is never the zero sentinel.
	b[n-1] ^= 0x01
	if digest(seed, a[:n]) == digest(seed, b[:n]) {
		t.Fatal("changing the last byte inside the slice did not change the digest")
	}
	if digest(seed, a[:n]) == 0 || digest(seed, nil) == 0 {
		t.Fatal("digest returned the never-written sentinel 0")
	}
}

// TestG_ALLOC_2_ARowDigestAllocatesNothing is gate G-ALLOC-2: a row digest
// allocates nothing.
//
// SETUP: one Entry.Image holding destination 0's 181 bytes and a process seed.
//
// STIMULUS: testing.AllocsPerRun over digest(seed, img[:181]) -- the function
// the merge and fillRow call, which is maphash.Bytes plus the 0 -> 1 remap.
//
// ASSERTION: 0 allocs/op, and (non-vacuity) the measured call really hashes:
// its result moves when a byte inside the slice moves.
//
// WOULD HAVE CAUGHT: a digest implementation that hashes through an io.Writer
// or an interface (hash.Hash64, a *maphash.Hash created per call), allocating
// per merge at 20,000 merges/s on the shard goroutine (Vol 0 section 5.5,
// OQ-L3).
func TestG_ALLOC_2_ARowDigestAllocatesNothing(t *testing.T) {
	seed := maphash.MakeSeed()
	var e model.Entry
	for i := range e.Image {
		e.Image[i] = byte(i * 7)
	}
	img := e.Image[:]

	var sink uint64
	if n := testing.AllocsPerRun(1000, func() { sink = digest(seed, img[:181]) }); n != 0 {
		t.Fatalf("digest allocated %v times per run, want 0", n)
	}

	before := sink
	img[180] ^= 0xFF
	if digest(seed, img[:181]) == before {
		t.Fatal("the measured digest ignores its input, so the gate measured nothing")
	}
}

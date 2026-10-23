package mirror

import "hash/maphash"

// digest is the change-suppression hash of a row image (Vol 0 section 5.2,
// Vol 2 section 4 step 7): maphash under the one process-wide seed, with 0
// remapped to 1.
//
// The remap is load-bearing, not cosmetic. Entry.Digest == 0 is the sentinel
// "never successfully written", so a real image that happened to hash to 0
// would be indistinguishable from a row Oracle has never seen.
//
// It hashes exactly the slice it is given -- the caller passes
// img[:ImageLen] -- and allocates nothing (gate G-ALLOC-2).
func digest(seed maphash.Seed, img []byte) uint64 {
	return nonZero(maphash.Bytes(seed, img))
}

// nonZero maps 0 to 1 and leaves every other value alone. Split out so the
// remap can be tested directly: finding a real image that hashes to 0 is a 2^64
// search.
func nonZero(h uint64) uint64 {
	if h == 0 {
		return 1
	}
	return h
}

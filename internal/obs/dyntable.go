package obs

import "sync/atomic"

// codeTable resolves a runtime code to a fixed slot, with atomic CAS insert
// and no allocation.
//
// This is the mechanism behind the ledger's only dynamic label. Vol 0 section
// 8.1: "unknown_transcode{code} is the one dynamic label and it is on the
// receive path, so it is a 64-slot open-addressed table with atomic CAS insert
// and an unknown_transcode_other overflow counter -- NEVER a mutex and never a
// map allocation". The reason is F2: req section 12.1 says unknown codes will
// occur, so this path is reached by definition, and a mutex at 10,000
// messages/s would turn an inventory gap into an availability problem.
//
// oracle_error{code} uses a second instance. Section 8.2a calls that one a
// "fixed 24-slot table", and it would be defensible as a plain array -- except
// that the 24 codes are oradb's taxonomy and obs may not import oradb, so
// enumerating them here would either duplicate Phase 3 data in a Phase 1
// constructor or invent it. Probing for them costs nothing on an error path.
type codeTable struct {
	// code holds the occupant of each slot, 0 meaning free. int32 because a
	// transaction code and an ORA number both fit, and because CAS on an
	// int32 is one instruction on every target.
	code []atomic.Int32

	// n is the number of probe slots.
	n uint32

	// pow2 selects masking over modulo. unknown_transcode's 64 slots are a
	// power of two and it is the one table on the receive path, so it gets
	// the mask; the Oracle table is 25 and takes the divide, on a path that
	// runs at error rate.
	pow2 bool
}

func newCodeTable(n uint32) codeTable {
	return codeTable{
		code: make([]atomic.Int32, n),
		n:    n,
		pow2: n != 0 && n&(n-1) == 0,
	}
}

// slot returns the slot holding code, claiming a free one if the code is not
// present yet. ok is false when the table is full, and the caller must fall
// back to its overflow bucket.
//
// Linear probing from the code's own hash. Linear rather than quadratic
// because the table is tiny and contiguous -- a miss walks one cache line, and
// the occupancy this is designed for is a handful of codes in 64 slots.
func (t *codeTable) slot(code int32) (uint32, bool) {
	// Zero is the free marker, so it cannot also be a stored code. No
	// transaction code and no ORA number is zero, so this costs nothing real;
	// sending such a code to overflow is still accounted, just not separated.
	if code == 0 || t.n == 0 {
		return 0, false
	}

	h := uint32(code)
	if t.pow2 {
		h &= t.n - 1
	} else {
		h %= t.n
	}

	for i := uint32(0); i < t.n; i++ {
		j := h + i
		if j >= t.n {
			j -= t.n
		}
		switch cur := t.code[j].Load(); {
		case cur == code:
			return j, true
		case cur == 0:
			if t.code[j].CompareAndSwap(0, code) {
				return j, true
			}
			// Losing the CAS means another goroutine claimed this slot
			// between the load and the swap -- and it may have claimed it for
			// the SAME code, which is the common case under a burst of one new
			// code. Re-read before advancing, or two goroutines seeing the
			// same new code would occupy two slots and split its count.
			if t.code[j].Load() == code {
				return j, true
			}
		}
	}
	return 0, false
}

// at returns the code occupying a slot, or 0 if it is free. Read by emission
// to render the label of a dynamic dimension.
func (t *codeTable) at(j uint32) int32 {
	if j >= t.n {
		return 0
	}
	return t.code[j].Load()
}

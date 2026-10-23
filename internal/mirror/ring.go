package mirror

// ring is a circular buffer of int32 arena slots: the type of Accum[d] and
// Cohort[d] (Vol 2 section 1.3, ADR-L21).
//
// It is a genuine ring and not a slice because the failure path inserts at the
// head. A slice makes a head insert O(n), and section 11.4 works the arithmetic:
// during a database outage that is ~200 ms of memmove per epoch, which trips the
// stall budget and turns the outage into a process crash through its own retry
// path. Here PushFrontBlock is O(k) in the rows inserted.
//
// len(buf) is always a power of two, so a position is (head+i)&mask. The zero
// value is an empty ring of capacity zero; newRing pre-sizes one.
//
// The ring has no instruments. Growth is REPORTED to the caller, which counts
// ring_grown{k,d}; a container does not log or count.
type ring struct {
	buf  []int32
	head int
	n    int
}

// newRing returns an empty ring whose capacity is n rounded up to a power of
// two (and at least 1). 50,000 becomes 65,536.
func newRing(n int) ring {
	c := 1
	for c < n {
		c <<= 1
	}
	return ring{buf: make([]int32, c)}
}

// Len is the number of slots queued.
func (r *ring) Len() int { return r.n }

// Reset empties the ring and keeps its buffer.
func (r *ring) Reset() { r.head, r.n = 0, 0 }

// Swap exchanges the contents of r and o in O(1): the epoch's ring swap
// (section 6.3) exchanges the three fields (buf, head, n) per side, never the
// slots.
func (r *ring) Swap(o *ring) { *r, *o = *o, *r }

// Peek returns the head slot without removing it; ok is false when empty.
func (r *ring) Peek() (s int32, ok bool) {
	if r.n == 0 {
		return 0, false
	}
	return r.buf[r.head], true
}

// PopFront removes and returns the head slot -- the oldest -- or reports false
// when the ring is empty. The pump's only read (section 7.2).
func (r *ring) PopFront() (s int32, ok bool) {
	if r.n == 0 {
		return 0, false
	}
	s = r.buf[r.head]
	r.head = (r.head + 1) & (len(r.buf) - 1)
	r.n--
	return s, true
}

// PushBack appends s at the tail and reports whether the buffer had to grow.
func (r *ring) PushBack(s int32) (grew bool) {
	grew = r.reserve(r.n + 1)
	r.buf[(r.head+r.n)&(len(r.buf)-1)] = s
	r.n++
	return grew
}

// PushFrontBlock inserts ss ahead of everything queued, as one run that keeps
// ss's order: afterwards PopFront yields ss[0], ss[1], ... and then the rows that
// were already there. It is O(len(ss)) and reports whether the buffer grew.
//
// Pushing one at a time to the front would REVERSE ss -- and ss is a failed
// batch drawn oldest-first, so that would invert FIFO among exactly the rows
// that have waited longest (section 8.6).
func (r *ring) PushFrontBlock(ss []int32) (grew bool) {
	k := len(ss)
	if k == 0 {
		return false
	}
	grew = r.reserve(r.n + k)
	mask := len(r.buf) - 1
	r.head = (r.head - k) & mask
	// ss lands at [head, head+k), which may wrap once past the end of buf.
	c := copy(r.buf[r.head:], ss)
	copy(r.buf, ss[c:])
	r.n += k
	return grew
}

// reserve makes room for need slots, doubling as often as necessary and
// re-linearising so the head lands at index 0. It reports whether it grew.
func (r *ring) reserve(need int) bool {
	if need <= len(r.buf) {
		return false
	}
	c := len(r.buf)
	if c == 0 {
		c = 1
	}
	for c < need {
		c <<= 1
	}
	nb := make([]int32, c)
	if r.n > 0 {
		// The queued run is [head, head+n), possibly wrapped.
		m := copy(nb, r.buf[r.head:min(r.head+r.n, len(r.buf))])
		copy(nb[m:r.n], r.buf)
	}
	r.buf, r.head = nb, 0
	return true
}

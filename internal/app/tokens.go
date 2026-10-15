package app

import "math/bits"

// tokenSet is req section 12.4's per-second distinct-token set: an
// open-addressed table cleared by incrementing a generation counter
// (ADR-L09, Vol 1 section 11.3).
//
// WHY NOT A MAP. HLD section 6.5 asks for a set "allocated once at startup,
// sized from the configured token universe, and cleared rather than rebuilt",
// because a fresh map each second would allocate on the one path ADR-006 keeps
// allocation-free. clear(m) on a reused map is the obvious answer and it very
// probably allocates nothing -- the current runtime reuses the bucket array.
// "Very probably" is the problem: that is an unspecified implementation
// property of a runtime we cross-compile to an architecture we cannot easily
// profile, and depending on it would make gate G-ALLOC-1 an assertion about a
// runtime internal rather than about our design. A plain slice cleared with
// clear() is definitively allocation-free but costs 4 MB of memclr every
// second, against one integer increment here.
//
// Owned by the decoder goroutine alone: plain memory, no atomics and no locks,
// the same discipline as decode.GapDetector (Vol 0 section 9.1).
type tokenSet struct {
	slots []tokenSlot
	mask  uint32

	// gen is the current generation and is NEVER 0, so a zero-valued slot --
	// which is what make() hands back -- reads as stale in generation 1 rather
	// than as a planted token 0.
	gen uint32

	// count is the distinct tokens seen in the current generation.
	count int
}

// tokenSlot is 8 bytes, so the whole table is one allocation the collector
// never has to trace: no pointers, nothing to scan.
type tokenSlot struct {
	token uint32
	stamp uint32
}

// newTokenSet sizes the table at 1 << ceil(log2(2 * expectedRows)) slots --
// 524,288 slots and 4 MiB at the default expectedRows of 200,000.
//
// Sized from runtime.expectedRows rather than from a key of its own, which is
// deliberate: it is already the configured expression of "how many rows/tokens
// this deployment expects" (Vol 0 section 7.2), and a parallel key would let
// the two disagree.
//
// THE TABLE CANNOT SATURATE, and that is what the 4 MiB buys. The token
// universe is 150,000-200,000 (HLD section 4.1) against 524,288 slots, so even
// if every token in existence traded in one second the load factor is 38%; at
// ASM-V6's pessimistic 20,000 distinct tokens per second it is 3.8% and linear
// probing essentially never probes. So there is no truncation case to specify,
// instrument or test -- which is worth more than the memory.
//
// At the configured maximum expectedRows of 5,000,000 the table is 1<<24 slots
// = 128 MiB, against Vol 0 section 3.7's ~189 MB process total. ADR-L09 already
// names ~2 million as the point to revisit the structure; the arithmetic is
// recorded here rather than clamped, because a clamp would be a policy nobody
// asked for and would reintroduce the saturation case this design removes.
func newTokenSet(expectedRows int) *tokenSet {
	if expectedRows < 1 {
		expectedRows = 1
	}
	slots := 1 << bits.Len(uint(2*expectedRows-1))
	return &tokenSet{
		slots: make([]tokenSlot, slots),
		mask:  uint32(slots - 1),
		gen:   1,
	}
}

// Insert records one token in the current generation, counting it only the
// first time it is seen.
//
// WHY CLAIMING THE FIRST STALE SLOT IS CORRECT. Within one generation, stamps
// are only ever SET to gen and never unset. So if t was already inserted this
// generation at position p in its probe chain, every slot before p in that
// chain held stamp == gen at insertion time and still does -- the re-probe
// walks the same prefix, finds none equal to t, and reaches p. The lookup
// cannot miss a token it inserted, so a stale slot is always genuinely free.
//
// Reuses mix32, the fan-out's shard finaliser: it is already the fmix32-shaped
// avalanche this needs, already in this package, and its properties are
// normative there (shard.go) for the same reason they matter here -- the low
// bits must avalanche, because the index is a mask of them.
func (s *tokenSet) Insert(t uint32) {
	i := mix32(t) & s.mask
	for {
		slot := &s.slots[i]
		if slot.stamp != s.gen {
			// Stale, therefore free this generation.
			slot.token = t
			slot.stamp = s.gen
			s.count++
			return
		}
		if slot.token == t {
			return // already counted this second
		}
		i = (i + 1) & s.mask
	}
}

// Count is the distinct tokens seen in the current generation.
func (s *tokenSet) Count() int { return s.count }

// Roll ends the current generation and returns its distinct count, clearing the
// table in O(1).
//
// No memclr and no allocation: incrementing gen makes every slot stale at once.
// The one exception is the wrap, which takes 4 MiB of memclr ONCE PER 2^32
// generations -- one second each, so once per ~136 years. It is written out
// because a wrap that was merely improbable rather than handled would plant
// every token in the table permanently: with gen back at a value slots still
// carry, stale entries would read as live and the count would collapse to
// near-zero for the rest of the process.
func (s *tokenSet) Roll() int {
	n := s.count
	s.count = 0
	s.gen++
	if s.gen == 0 {
		s.gen = 1
		clear(s.slots)
	}
	return n
}

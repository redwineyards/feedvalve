package app

import (
	"math/rand"
	"testing"
)

// The token set is a hand-written probe loop standing in for a language
// builtin (ADR-L09), so it carries the tests the builtin would not have needed:
// that it counts distinct tokens, that the generation stamp really does clear
// it, and that the once-per-136-years wrap is handled rather than merely
// improbable.

func TestTokenSetSizesToAPowerOfTwoAboveTwiceExpectedRows(t *testing.T) {
	// The default is the row that matters: ADR-L09's arithmetic says 200,000
	// expected rows gives 524,288 slots and 4 MiB, and the memory figure in
	// Vol 1 section 11.2's inventory is derived from exactly that.
	for _, c := range []struct {
		expectedRows int
		wantSlots    int
	}{
		{1, 2},
		{1000, 2048},         // the configured minimum (rule V-RANGE)
		{200_000, 524_288},   // the default: 4 MiB
		{262_144, 524_288},   // 2n exactly a power of two -- must not round up again
		{262_145, 1 << 20},   // one over
		{5_000_000, 1 << 24}, // the configured maximum: 128 MiB
	} {
		s := newTokenSet(c.expectedRows)
		if got := len(s.slots); got != c.wantSlots {
			t.Errorf("newTokenSet(%d) has %d slots, want %d", c.expectedRows, got, c.wantSlots)
		}
		if got := int(s.mask); got != c.wantSlots-1 {
			t.Errorf("newTokenSet(%d) mask = %d, want %d", c.expectedRows, got, c.wantSlots-1)
		}
		// Never 0, or a zero-valued slot would read as a planted token 0 in
		// the first generation.
		if s.gen == 0 {
			t.Errorf("newTokenSet(%d) starts at generation 0", c.expectedRows)
		}
	}
}

// TestTokenSetCountsDistinctTokens is ADR-L09's property test: insert N random
// tokens with duplicates, assert the count is the distinct input count.
//
// The oracle is a map, which is the structure ADR-L09 rejected for the hot path
// -- and that is exactly why it belongs here. A test is allowed to allocate.
func TestTokenSetCountsDistinctTokens(t *testing.T) {
	const (
		inserts       = 50_000
		tokenUniverse = 20_000 // ASM-V6's pessimistic distinct count, so duplicates are certain
	)

	s := newTokenSet(200_000)
	want := make(map[uint32]struct{}, tokenUniverse)

	r := rand.New(rand.NewSource(20260215))
	for range inserts {
		tok := uint32(r.Intn(tokenUniverse)) + 1
		want[tok] = struct{}{}
		s.Insert(tok)
	}

	if got := s.Count(); got != len(want) {
		t.Errorf("Count() = %d after %d inserts, want %d distinct", got, inserts, len(want))
	}
}

// TestTokenSetGenerationClearsWithoutForgettingWithinOne pins the two halves of
// the generation stamp that have to hold together: a token is counted ONCE
// within a generation, and counted AGAIN in the next one.
//
// Getting the first wrong double-counts every repeat trade and makes
// active_tokens_1s_max read as the message rate; getting the second wrong makes
// it monotonic across the whole session, so it answers a question nobody asked.
func TestTokenSetGenerationClearsWithoutForgettingWithinOne(t *testing.T) {
	s := newTokenSet(1000)

	for range 10 {
		s.Insert(4242)
	}
	if got := s.Count(); got != 1 {
		t.Errorf("Count() = %d after inserting one token ten times in one generation, want 1", got)
	}

	if got := s.Roll(); got != 1 {
		t.Errorf("Roll() returned %d, want the generation's count of 1", got)
	}
	if got := s.Count(); got != 0 {
		t.Errorf("Count() = %d immediately after Roll, want 0", got)
	}

	// The same token in the NEXT generation is a new distinct token.
	s.Insert(4242)
	if got := s.Count(); got != 1 {
		t.Errorf("Count() = %d after re-inserting the same token in the next generation, "+
			"want 1 -- the generation stamp is not clearing", got)
	}
}

// TestTokenSetProbesPastACollision covers the linear probe, which the load
// factor means production essentially never exercises (3.8% at ASM-V6) -- so
// the only place it runs is here.
//
// The table is forced to 2 slots so a third distinct token must probe, and
// inserting past capacity is deliberately NOT tested: the table cannot
// saturate at any configured size (524,288 slots against a 150,000-200,000
// token universe), which is why ADR-L09 specifies no truncation behaviour.
func TestTokenSetProbesPastACollision(t *testing.T) {
	s := newTokenSet(1) // 2 slots
	s.Insert(1)
	s.Insert(2)
	if got := s.Count(); got != 2 {
		t.Fatalf("Count() = %d after two tokens into a 2-slot table, want 2", got)
	}
	// Both slots now hold the current generation, so re-inserting either must
	// still find it rather than walking forever or counting it twice.
	s.Insert(1)
	s.Insert(2)
	if got := s.Count(); got != 2 {
		t.Errorf("Count() = %d after re-inserting both, want 2 -- a full generation's probe "+
			"chain is not finding a token it already holds", got)
	}
}

// TestTokenSetSurvivesTheGenerationWrap exercises the gen == 0 case directly
// rather than waiting the ~136 years it would take to reach it.
//
// Unhandled, the wrap is not a cosmetic bug: with gen back at a value the slots
// still carry, every stale entry reads as live, so the count collapses to
// near-zero for the remaining life of the process and the instrument silently
// stops measuring.
func TestTokenSetSurvivesTheGenerationWrap(t *testing.T) {
	s := newTokenSet(1000)

	// Plant a token under a generation the wrap will come back around to.
	s.gen = 1
	s.Insert(777)

	// Step to the generation before the wrap, then roll through it.
	s.gen = ^uint32(0)
	s.count = 0
	s.Insert(888)
	if got := s.Roll(); got != 1 {
		t.Fatalf("Roll() = %d in the generation before the wrap, want 1", got)
	}

	if s.gen != 1 {
		t.Errorf("generation is %d after the wrap, want 1 -- 0 would make a zero-valued "+
			"slot read as a planted token", s.gen)
	}
	// The memclr on wrap is what makes this hold: 777 was planted under
	// generation 1 and must NOT still be there now that gen is 1 again.
	s.Insert(777)
	if got := s.Count(); got != 1 {
		t.Errorf("Count() = %d after the wrap, want 1 -- the table was not cleared, so a "+
			"token planted in the pre-wrap generation 1 is being read as live", got)
	}
}

// TestTokenSetAllocatesNothing is the token set's own share of G-ALLOC-1,
// alongside the whole-iteration gate in alloc_gate_test.go.
//
// Insert and Roll separately, because they fail for different reasons: an
// Insert that allocated would mean the slots slice is being grown, and a Roll
// that allocated would mean the clear became a reallocation -- which is
// precisely the "rebuilt rather than cleared" row of section 11.4's table.
func TestTokenSetAllocatesNothing(t *testing.T) {
	s := newTokenSet(200_000)

	tok := uint32(0)
	if n := testing.AllocsPerRun(1000, func() {
		tok++
		s.Insert(tok)
	}); n != 0 {
		t.Errorf("Insert allocated %v times, want 0", n)
	}

	if n := testing.AllocsPerRun(1000, func() {
		s.Roll()
	}); n != 0 {
		t.Errorf("Roll allocated %v times, want 0 -- the generation stamp exists so the "+
			"boundary is O(1) with no memclr and no allocation (ADR-L09)", n)
	}
}

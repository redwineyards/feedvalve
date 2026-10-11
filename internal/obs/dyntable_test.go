package obs

import (
	"sync"
	"testing"
)

func TestCodeTableReturnsTheSameSlotForTheSameCode(t *testing.T) {
	tb := newCodeTable(64)
	a, ok := tb.slot(7208)
	if !ok {
		t.Fatal("first insert into an empty table was refused")
	}
	b, ok := tb.slot(7208)
	if !ok || a != b {
		t.Errorf("slot(7208) returned %d then %d; one code must occupy one slot or its "+
			"count is split across two", a, b)
	}
	if tb.at(a) != 7208 {
		t.Errorf("at(%d) = %d, want 7208", a, tb.at(a))
	}
}

func TestCodeTableSeparatesDistinctCodes(t *testing.T) {
	tb := newCodeTable(64)
	seen := map[uint32]int32{}
	for _, code := range []int32{7208, 7305, 7200, 6541, 6503, 1, 63, 64, 65, 128} {
		s, ok := tb.slot(code)
		if !ok {
			t.Fatalf("slot(%d) refused in a table with room", code)
		}
		if prev, dup := seen[s]; dup {
			t.Errorf("codes %d and %d share slot %d", prev, code, s)
		}
		seen[s] = code
	}
}

// TestCodeTableProbesPastACollision is the point of open addressing: 1 and 65
// hash to the same slot in a 64-entry masked table, and the second must land
// next door rather than overwrite or be refused.
func TestCodeTableProbesPastACollision(t *testing.T) {
	tb := newCodeTable(64)
	a, _ := tb.slot(1)
	b, ok := tb.slot(65) // 65 & 63 == 1, the same starting probe
	if !ok {
		t.Fatal("a colliding code was refused in an almost-empty table")
	}
	if a == b {
		t.Fatal("two codes with the same hash were given the same slot")
	}
	if tb.at(a) != 1 || tb.at(b) != 65 {
		t.Errorf("after probing, slots hold %d and %d, want 1 and 65", tb.at(a), tb.at(b))
	}
}

func TestCodeTableRefusesWhenFull(t *testing.T) {
	const n = 8
	tb := newCodeTable(n)
	for code := int32(1); code <= n; code++ {
		if _, ok := tb.slot(code); !ok {
			t.Fatalf("code %d refused before the table was full", code)
		}
	}
	if _, ok := tb.slot(999); ok {
		t.Error("a full table accepted another code; the caller's overflow bucket " +
			"would then never be used and the count would be silently wrong")
	}
	// A code already present is still found in a full table -- the probe must
	// not give up early just because there are no free slots.
	if _, ok := tb.slot(3); !ok {
		t.Error("a code already in a full table was reported missing")
	}
}

// TestCodeTableRefusesZero documents the one code the table cannot hold, and
// why that is acceptable: zero is the free marker, and no transaction code or
// ORA number is zero.
func TestCodeTableRefusesZero(t *testing.T) {
	tb := newCodeTable(64)
	if _, ok := tb.slot(0); ok {
		t.Error("zero was stored; it is the free marker and would make the slot " +
			"indistinguishable from an empty one")
	}
}

// TestCodeTableUnderConcurrentInsertGivesOneCodeOneSlot is the test that
// matters, because this table is reached from the receive goroutine the moment
// a new transaction code appears -- which F2 says will happen.
//
// The defect it catches is the one the CAS loop re-reads for: two goroutines
// both seeing a new code, both finding the slot free, one losing the CAS and
// then advancing to a second slot. The code would occupy two slots, its count
// would be split between them, and the emission would show the same unknown
// code twice with half the traffic each.
//
// There is no race detector on this host or on the target, so the assertion is
// on the outcome: exactly one slot holds the code, and its count is the total.
func TestCodeTableUnderConcurrentInsertGivesOneCodeOneSlot(t *testing.T) {
	const goroutines, each = 16, 500
	tb := newCodeTable(64)
	counts := make([]int64, 64)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				s, ok := tb.slot(7208)
				if !ok {
					t.Error("slot refused in an empty table")
					return
				}
				mu.Lock()
				counts[s]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	occupied := 0
	total := int64(0)
	for i, c := range counts {
		if c == 0 {
			continue
		}
		occupied++
		total += c
		if tb.at(uint32(i)) != 7208 {
			t.Errorf("slot %d was counted but holds %d", i, tb.at(uint32(i)))
		}
	}
	if occupied != 1 {
		t.Errorf("code 7208 occupies %d slots, want 1. A lost CAS must re-read the "+
			"slot before advancing, because the winner may have claimed it for the "+
			"same code", occupied)
	}
	if want := int64(goroutines * each); total != want {
		t.Errorf("counted %d resolutions, want %d", total, want)
	}
}

func BenchmarkCodeTableHit(b *testing.B) {
	tb := newCodeTable(64)
	tb.slot(7208)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tb.slot(7208)
	}
}

// BenchmarkCodeTableProbe is the cost of a lookup that has to walk past
// collisions -- a nearly full table, which is the worst case the receive
// goroutine can meet. It must allocate nothing: Vol 0 section 8.1 forbids a map
// here precisely because the insert would.
func BenchmarkCodeTableProbe(b *testing.B) {
	tb := newCodeTable(64)
	for code := int32(1); code < 60; code++ {
		tb.slot(code * 64) // every one hashes to slot 0, filling by probe
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tb.slot(59 * 64)
	}
}

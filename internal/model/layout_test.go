package model

import (
	"reflect"
	"testing"
	"unsafe"
)

// TestArenaTypesArePointerFree is gate G-PTR-1.
//
// Entry, UpdateUnit, BatchRow and Datagram live in pre-allocated arenas and
// rings -- roughly 200 MB of them. One pointer anywhere inside, at any depth,
// puts all of it on the collector's mark path on every cycle, on a partition
// with four entitled cores. The cost would not show up as a crash; it would
// show up as flush latency nobody could explain.
//
// Batch is deliberately absent. Vol 0 section 3.8 exempts it: it holds the one
// slice and the one error in these types, it is pooled per shard rather than
// per row, and it never enters an arena.
func TestArenaTypesArePointerFree(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
	}{
		{"Entry", reflect.TypeOf(Entry{})},
		{"UpdateUnit", reflect.TypeOf(UpdateUnit{})},
		{"BatchRow", reflect.TypeOf(BatchRow{})},
		{"Datagram", reflect.TypeOf(Datagram{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertPointerFree(t, tc.typ, tc.name)
		})
	}
}

// assertPointerFree walks a type transitively, failing on any kind the
// collector would have to trace.
func assertPointerFree(t *testing.T, typ reflect.Type, path string) {
	t.Helper()

	switch typ.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Slice, reflect.Map,
		reflect.String, reflect.Chan, reflect.Func, reflect.Interface:
		t.Errorf("%s is a %s. Arena-resident types must be pointer-free so the "+
			"collector never traces the arenas (ADR-006).", path, typ.Kind())

	case reflect.Array:
		assertPointerFree(t, typ.Elem(), path+"[]")

	case reflect.Struct:
		for i := range typ.NumField() {
			f := typ.Field(i)
			assertPointerFree(t, f.Type, path+"."+f.Name)
		}
	}
}

// TestBatchIsTheDeliberateException pins the exemption above, so that "Batch
// has a slice" stays a recorded decision rather than something a later reader
// assumes is an oversight in the gate.
func TestBatchIsTheDeliberateException(t *testing.T) {
	typ := reflect.TypeOf(Batch{})
	rows, ok := typ.FieldByName("Rows")
	if !ok || rows.Type.Kind() != reflect.Slice {
		t.Error("Batch.Rows should be a slice; if that changed, Vol 0 section 3.8's " +
			"rationale for exempting Batch from G-PTR-1 needs rereading")
	}
	if err, ok := typ.FieldByName("Err"); !ok || err.Type.Kind() != reflect.Interface {
		t.Error("Batch.Err should be an error interface")
	}
}

// TestCounterIsTotal checks the promise Vol 0 section 3.11 makes by putting
// Counter in this package at all: every rejection maps to exactly one counter
// name, so a rejection cannot be counted under two names in two volumes.
func TestCounterIsTotal(t *testing.T) {
	seen := map[string]Reject{}

	for r := RejectNone; r <= RejectPanic; r++ {
		name := r.Counter()

		if r == RejectNone {
			if name != "" {
				t.Errorf("RejectNone should have no counter, got %q", name)
			}
			continue
		}

		if name == "" {
			t.Errorf("Reject(%d) has no counter name. Every value except RejectNone "+
				"must map to a counter in the Vol 0 section 8.2 ledger.", r)
			continue
		}
		if prev, dup := seen[name]; dup {
			t.Errorf("Reject(%d) and Reject(%d) both report counter %q; a rejection "+
				"must not be counted under one name from two causes", prev, r, name)
		}
		seen[name] = r
	}

	if len(seen) != int(RejectPanic) {
		t.Errorf("mapped %d counters for %d rejection values", len(seen), int(RejectPanic))
	}
}

// TestMemoryFigures ties the struct sizes to the capacity planning that depends
// on them. A size change is not a failed constant, it is a change to how much
// of the 40 GB partition this process needs, and it should read that way.
func TestMemoryFigures(t *testing.T) {
	const (
		expectedRows        = 400_000 // ASM-7
		qdCapacity          = 8192    // Vol 0 section 9.5.3
		shards              = 4
		shardQueueCapacity  = 4096
		maxInFlightPerShard = 2
		maxBatchSizeCeiling = 1000
	)

	for _, tc := range []struct {
		what string
		got  uintptr
		want uintptr
	}{
		{"arenas (400k x Entry)", expectedRows * unsafe.Sizeof(Entry{}), 169_600_000},
		{"QD ring (8192 x Datagram)", qdCapacity * unsafe.Sizeof(Datagram{}), 8_519_680},
		{"Q1 (4 x 4096 x UpdateUnit)", shards * shardQueueCapacity * unsafe.Sizeof(UpdateUnit{}), 5_898_240},
		{"batch rows (4 x 2 x 1000 x BatchRow)",
			shards * maxInFlightPerShard * maxBatchSizeCeiling * unsafe.Sizeof(BatchRow{}), 2_304_000},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d bytes, want %d. A struct size moved; the resident-memory "+
				"figure of about 220.8 MB in Vol 0 section 3.7 needs re-deriving.",
				tc.what, tc.got, tc.want)
		}
	}
}

// TestRowKeyIsZeroExtended pins the encoding hazard Vol 0 section 3.2 warns
// about. A token arrives as a LONG, is held in an int32, and a direct
// conversion SIGN-extends: -1 becomes 0xFFFFFFFFFFFFFFFF, a different row from
// 0x00000000FFFFFFFF. Going through uint32 is what makes the encoding the
// bijection onto the low 32 bits that the design assumes.
func TestRowKeyIsZeroExtended(t *testing.T) {
	token := int32(-1)

	if got := SingleKey(uint32(token)); got != 0x00000000FFFFFFFF {
		t.Errorf("SingleKey(uint32(-1)) = %#016x, want 0x00000000ffffffff", uint64(got))
	}
	if got := RowKey(token); got == SingleKey(uint32(token)) {
		t.Error("direct conversion and SingleKey agree for a negative token; the " +
			"sign-extension hazard this constructor exists for has gone away, " +
			"or the test is wrong")
	}

	if got := CompositeKey(0xDEADBEEF, 0x0000002A); got != 0xDEADBEEF0000002A {
		t.Errorf("CompositeKey = %#016x, want 0xdeadbeef0000002a", uint64(got))
	}
	k := CompositeKey(7, 9)
	if k.Hi() != 7 || k.Lo() != 9 {
		t.Errorf("Hi/Lo round trip = (%d, %d), want (7, 9)", k.Hi(), k.Lo())
	}
}

func TestColMask(t *testing.T) {
	var m ColMask

	m = m.Set(0).Set(63).Set(7)
	if !m.Has(0) || !m.Has(63) || !m.Has(7) {
		t.Errorf("Set/Has disagree: %#016x", uint64(m))
	}
	if m.Has(1) {
		t.Error("bit 1 should not be set")
	}
	if m.Count() != 3 {
		t.Errorf("Count = %d, want 3", m.Count())
	}

	// Bit 63 must be reachable: ColMask is exactly MaxColumns wide, and a
	// mask built with a signed shift would overflow here rather than set it.
	if m&(1<<63) == 0 {
		t.Error("bit 63 is not reachable; ColMask must cover all MaxColumns fields")
	}

	if got := m.Clear(7); got.Has(7) || got.Count() != 2 {
		t.Errorf("Clear(7) = %#016x", uint64(got))
	}
	if got := m.Without(Bit(0).Set(63)); got.Count() != 1 || !got.Has(7) {
		t.Errorf("Without = %#016x, want only bit 7", uint64(got))
	}
	if !m.Intersects(Bit(7)) || m.Intersects(Bit(1)) {
		t.Error("Intersects disagrees with Has")
	}
	if ColMask(0).Empty() != true || m.Empty() {
		t.Error("Empty disagrees with the zero value")
	}
}

func TestEntrySlotSeen(t *testing.T) {
	var e Entry

	for i := uint8(0); i < MaxContributors; i++ {
		if e.SeenSlot(i) {
			t.Fatalf("slot %d seen before any contribution", i)
		}
	}
	e.MarkSlotSeen(5)
	if !e.SeenSlot(5) {
		t.Error("slot 5 not recorded")
	}
	if e.SeenSlot(4) || e.SeenSlot(6) {
		t.Error("marking slot 5 affected a neighbour")
	}

	// SlotSeen is a uint8, so it has room for exactly MaxContributors slots.
	// Raising MaxContributors past 8 without widening it would silently stop
	// recording the new slots, and the ordering guard's denominator rule
	// would then be wrong for them rather than obviously broken.
	if MaxContributors > 8 {
		t.Error("MaxContributors exceeds the width of Entry.SlotSeen (uint8); " +
			"widen SlotSeen and re-derive Entry's layout")
	}
}

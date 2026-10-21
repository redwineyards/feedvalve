package mirror

import (
	"hash/maphash"
	"math"
	"math/rand"
	"runtime"
	"testing"
	"unsafe"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// defaultSizing is Vol 0 section 7.2's defaults: four shards, 200,000 expected
// rows (so 50,000 per shard), 4,096-row chunks, m = 2, a 1,000-row ceiling.
func defaultSizing() Sizing {
	return Sizing{
		Shards:              4,
		ExpectedRows:        200_000,
		ArenaChunkRows:      4096,
		MaxInFlightPerShard: 2,
		MaxBatchSizeCeiling: 1000,
		Seed:                maphash.MakeSeed(),
	}
}

// testInstruments is a ShardInstruments whose handles NewShard and allocEntry
// touch are fresh, unregistered instruments. The rest stay nil, so a
// construction path that starts writing an instrument this card did not give
// it fails loudly with a nil dereference instead of passing silently.
func testInstruments(shards int) *ShardInstruments {
	in := &ShardInstruments{FlushCoalesced: &obs.Counter{}, FlushWakeDropped: make([]*obs.Counter, shards)}
	for j := range in.FlushWakeDropped {
		in.FlushWakeDropped[j] = &obs.Counter{}
	}
	for d := range in.ArenaChunks {
		in.ArenaChunks[d] = &obs.Gauge{}
	}
	return in
}

// newTestShard builds shard 0 at sz with a Params of generation gen.
func newTestShard(t testing.TB, sz Sizing, gen uint64) *Shard {
	t.Helper()
	dst, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	p := &Params{Gen: gen, MaxBatchSize: sz.MaxBatchSizeCeiling}
	return NewShard(0, sz, func() *Params { return p }, dst, NewFlushBus(sz.Shards),
		testInstruments(sz.Shards), Edges{})
}

// smallSizing is a sizing with tiny chunks, so chunk growth is cheap to reach.
func smallSizing(chunkRows int) Sizing {
	sz := defaultSizing()
	sz.ArenaChunkRows = chunkRows
	sz.ExpectedRows = 64
	return sz
}

// TestNewShardConstruction checks every structure section 1.3 says exists
// before the first unit arrives.
func TestNewShardConstruction(t *testing.T) {
	sz := defaultSizing()
	s := newTestShard(t, sz, 7)

	if s.cfgGen != 7 {
		t.Errorf("cfgGen = %d, want 7: it must start at the first Params' generation, or the "+
			"first epoch releases an empty quarantine", s.cfgGen)
	}
	if s.wake != s.bus.Wake(0) {
		t.Error("the wake edge is not the bus's wake[0]")
	}
	for d := range model.MaxDestinations {
		if len(s.Arena[d]) != 1 || len(s.Arena[d][0]) != sz.ArenaChunkRows {
			t.Errorf("Arena[%d]: %d chunks, want chunk 0 of %d rows allocated with the shard",
				d, len(s.Arena[d]), sz.ArenaChunkRows)
		}
		if got := s.inst.ArenaChunks[d].Load(); got != 1 {
			t.Errorf("arena_chunks{0,%d} = %d, want 1", d, got)
		}
		// 50,000 rounds to the next power of two (the V2 section 10.3 erratum).
		for _, r := range []*ring{&s.Accum[d], &s.Cohort[d]} {
			if len(r.buf) != 65536 || r.Len() != 0 {
				t.Errorf("ring of destination %d: cap %d len %d, want 65536 and 0", d, len(r.buf), r.Len())
			}
		}
		if cap(s.failedRows[d]) != sz.MaxBatchSizeCeiling || len(s.failedRows[d]) != 0 {
			t.Errorf("failedRows[%d]: len %d cap %d, want 0 and the ceiling",
				d, len(s.failedRows[d]), cap(s.failedRows[d]))
		}
		if s.Index[d] == nil || s.NextSlot[d] != 0 {
			t.Errorf("Index[%d] nil or NextSlot non-zero", d)
		}
	}

	if len(s.Free) != sz.MaxInFlightPerShard || len(s.all) != sz.MaxInFlightPerShard ||
		len(s.sentAt) != sz.MaxInFlightPerShard {
		t.Fatalf("Free %d, all %d, sentAt %d; want %d each",
			len(s.Free), len(s.all), len(s.sentAt), sz.MaxInFlightPerShard)
	}
	seen := map[*model.Batch]bool{}
	for i, b := range s.Free {
		if b != s.all[i] || seen[b] {
			t.Errorf("Free[%d] is not all[%d], or is a duplicate", i, i)
		}
		seen[b] = true
		if len(b.Rows) != sz.MaxBatchSizeCeiling || b.Shard != 0 || b.N != 0 {
			t.Errorf("batch %d: %d rows, shard %d, N %d; want the ceiling, 0, 0",
				i, len(b.Rows), b.Shard, b.N)
		}
	}
}

// TestAllocEntryZeroesEvenADirtiedSlot is INV-ZERO (Vol 2 section 1.3): a
// freshly allocated entry has an all-zero Image, Digest, Written, SlotSeen and
// Quarantined, whatever bytes the chunk held at that position.
//
// SETUP: a shard; slot 0 allocated and dirtied; then the bytes at slot 1's
// position in the chunk dirtied too -- the state a future slot-reuse feature
// would leave behind, and the only way to reach a non-zero fresh slot today.
//
// STIMULUS: allocEntry.
//
// ASSERTION: slot 1 comes back entirely zero, and slot 0 is untouched.
//
// WOULD HAVE CAUGHT: dropping step 5's explicit zero. A recycled slot carrying
// a previous row's bytes digests to a different row's history (Vol 0 section
// 5.2 normalisation rule 1), and change suppression then swallows a real write.
func TestAllocEntryZeroesEvenADirtiedSlot(t *testing.T) {
	s := newTestShard(t, smallSizing(256), 1)
	const d = model.DestID(1)

	if got := s.allocEntry(d); got != 0 {
		t.Fatalf("first slot = %d, want 0", got)
	}
	dirty := func(e *model.Entry) {
		e.Row, e.Digest, e.Written, e.ColMask = 99, 0xDEAD, 0xFF, 0xF0
		e.SlotSeen, e.Quarantined, e.Dirty, e.InFlight = 0xFF, true, true, true
		e.LastSeq[3], e.LastRecv[7] = 5, 6
		for i := range e.Image {
			e.Image[i] = 0xA5
		}
	}
	dirty(s.entry(d, 0))
	dirty(&s.Arena[d][0][1]) // slot 1's bytes, before slot 1 exists

	// Non-vacuity: slot 1's memory really is dirty going in.
	if s.Arena[d][0][1] == (model.Entry{}) {
		t.Fatal("setup: slot 1 is still zero, so this test would pass without the explicit zero")
	}

	slot := s.allocEntry(d)
	if slot != 1 {
		t.Fatalf("second slot = %d, want 1", slot)
	}
	if *s.entry(d, slot) != (model.Entry{}) {
		t.Errorf("a fresh entry is not zero: %+v", *s.entry(d, slot))
	}
	if e := s.entry(d, 0); e.Digest != 0xDEAD || e.Image[255] != 0xA5 {
		t.Error("allocating slot 1 disturbed slot 0")
	}
}

// TestArenaSlotsAreStableAcrossChunkGrowth is INV-SLOT: a slot addresses the
// same Entry for the life of the process, so BatchRow.Slot can be a bare int32
// the completion path dereferences without a map lookup.
//
// SETUP: 4-row chunks; every allocated entry marked with its own slot, and its
// address recorded at allocation time.
//
// STIMULUS: allocate past three chunk boundaries.
//
// ASSERTION: every slot still resolves to its original address and marker;
// arena_chunks{k,d} equals the chunk count after each growth -- STORED, so a
// gauge pre-loaded with garbage is overwritten rather than added to.
//
// WOULD HAVE CAUGHT: an arena grown by copying (append to one []model.Entry),
// which moves every entry and leaves in-flight batch slots pointing at stale
// copies; and an arena_chunks written with Add, which would publish a running
// total of growths rather than the count.
func TestArenaSlotsAreStableAcrossChunkGrowth(t *testing.T) {
	const rows = 4
	s := newTestShard(t, smallSizing(rows), 1)
	const d = model.DestID(2)
	g := s.inst.ArenaChunks[d]
	g.Store(1000) // garbage: a Store overwrites it, an Add would not

	var addr []*model.Entry
	for i := range 3*rows + 1 {
		slot := s.allocEntry(d)
		if int(slot) != i {
			t.Fatalf("allocation %d returned slot %d", i, slot)
		}
		e := s.entry(d, slot)
		e.Row = model.RowKey(1000 + i)
		addr = append(addr, e)

		wantChunks := i/rows + 1
		if len(s.Arena[d]) != wantChunks {
			t.Fatalf("after slot %d: %d chunks, want %d", i, len(s.Arena[d]), wantChunks)
		}
		if i%rows == 0 && i > 0 {
			if got := g.Load(); got != int64(wantChunks) {
				t.Errorf("arena_chunks after growing to %d chunks = %d, want %d stored",
					wantChunks, got, wantChunks)
			}
		}
	}
	// Non-vacuity: growth really happened, three times.
	if len(s.Arena[d]) != 4 {
		t.Fatalf("setup: %d chunks, want 4", len(s.Arena[d]))
	}
	for i, p := range addr {
		if s.entry(d, int32(i)) != p {
			t.Errorf("slot %d moved across chunk growth", i)
		}
		if p.Row != model.RowKey(1000+i) {
			t.Errorf("slot %d holds row %d, want %d", i, p.Row, 1000+i)
		}
	}
	if s.NextSlot[d] != 3*rows+1 || s.NextSlot[0] != 0 {
		t.Errorf("NextSlot = %v; the allocator is not per destination", s.NextSlot)
	}
}

// TestIndexPresizeAvoidsBucketEvacuation: Index[d] is pre-sized so filling it
// to ExpectedRows/Shards never evacuates buckets on the shard goroutine
// (Vol 2 section 1.3, HLD section 7.8).
//
// Not "zero allocations": a Go 1.23 map pre-allocates only 1/16 as many
// overflow buckets as buckets, and 50,000 keys at load factor 6.1 overflow
// more than that, so some small overflow-bucket allocations are expected
// (Vol 2 section 10.3). What must not happen is a GROWTH -- a new bucket array
// at least as large as the current one, plus the incremental evacuation that
// copies every key into it.
//
// SETUP: a shard at the default sizing; 50,000 distinct pseudo-random keys.
//
// STIMULUS: insert them all into Index[0].
//
// ASSERTION: the bytes allocated are below half of one 8,192-bucket array
// (917,504 B) -- a growth would allocate at least the whole array.
//
// WOULD HAVE CAUGHT: dropping the size hint, or hinting with the wrong figure
// (the whole expectedRows over one shard is too large; per destination of one
// shard too small would grow). The unhinted control below proves the measure
// can see a growth.
func TestIndexPresizeAvoidsBucketEvacuation(t *testing.T) {
	sz := defaultSizing()
	perShard := sz.ExpectedRows / sz.Shards
	keys := make([]model.RowKey, perShard)
	rng := rand.New(rand.NewSource(1))
	for i := range keys {
		keys[i] = model.RowKey(rng.Uint64())
	}
	const oneBucketArray = 8192 * 112

	fill := func(m map[model.RowKey]int32) uint64 {
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		for i, k := range keys {
			m[k] = int32(i)
		}
		runtime.ReadMemStats(&b)
		return b.TotalAlloc - a.TotalAlloc
	}

	s := newTestShard(t, sz, 1)
	got := fill(s.Index[0])
	if len(s.Index[0]) != perShard {
		t.Fatalf("setup: %d keys landed, want %d (a duplicate pseudo-random key?)", len(s.Index[0]), perShard)
	}
	if got >= oneBucketArray/2 {
		t.Errorf("filling the pre-sized index to %d keys allocated %d B, at least half a "+
			"bucket array: the map grew on the shard goroutine", perShard, got)
	}

	// Non-vacuity: the same fill into an unhinted map is seen to grow.
	ctl := fill(map[model.RowKey]int32{})
	t.Logf("pre-sized fill allocated %d B; unhinted control %d B", got, ctl)
	if ctl < oneBucketArray {
		t.Fatalf("control: an unhinted map allocated only %d B for %d keys; the measure "+
			"cannot see a growth, so the assertion above proves nothing", ctl, perShard)
	}
}

// TestNewShardAllocatesTheDerivedFigure measures what NewShard allocates at
// the default sizing against the figure derived from Vol 2 section 10.3 (with
// the rings at the power of two they really are -- the section 10.3 erratum).
//
// Per shard: 3 arena chunks of 4,096 x 424 B; 6 rings of 65,536 x 4 B; 3
// indexes of 8,192 buckets + 512 pre-allocated overflow buckets at 112 B; 2
// batches of 1,000 x 288 B rows; 3 failed-row scratches of 1,000 x 4 B. Each
// is counted as the allocator really rounds it: a large object (over 32 KiB) to
// whole 8 KiB pages -- the same on every GOARCH -- which costs the two Rows
// arrays 6,912 B each; a small one to its size class, 4,000 B to 4,096 B. The
// chunks, rings and index arrays are already page multiples. The rest -- the
// Shard struct, batch headers, slice headers, about 1.3 KiB -- is what the
// allowance covers. The allowance is one scratch's 4,096 B: a missing structure
// falls below the derived figure, and an extra one, even the smallest, lands
// above it.
//
// TotalAlloc is process-wide, so one sample also counts whatever the runtime
// and the test framework allocated meanwhile (up to ~30 KiB seen). That noise
// only ever adds, so the measure is the minimum of several samples.
func TestNewShardAllocatesTheDerivedFigure(t *testing.T) {
	sz := defaultSizing()
	const (
		page    = 8 << 10
		chunks  = 3 * 4096 * 424                            // 5,210,112 (212 pages each)
		rings   = 6 * 65536 * 4                             // 1,572,864 (32 pages each)
		indexes = 3 * (8192 + 512) * 112                    // 2,924,544
		rows    = 2 * ((1000*288 + page - 1) / page * page) // 2 x 294,912 = 589,824
		scratch = 3 * 4096                                  // 3 x 1,000 x 4 B, size class 4,096
		derived = chunks + rings + indexes + rows + scratch
		slack   = 4 << 10 // the small headers; see above
		samples = 5
	)
	if unsafe.Sizeof(model.Entry{}) != 424 || unsafe.Sizeof(model.BatchRow{}) != 288 {
		t.Fatal("Entry or BatchRow changed size; re-derive this figure")
	}

	dst, err := destinations.Default()
	if err != nil {
		t.Fatal(err)
	}
	p := &Params{Gen: 1}
	bus, inst := NewFlushBus(sz.Shards), testInstruments(sz.Shards)

	got := uint64(math.MaxUint64)
	for range samples {
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		s := NewShard(0, sz, func() *Params { return p }, dst, bus, inst, Edges{})
		runtime.ReadMemStats(&b)
		runtime.KeepAlive(s)
		got = min(got, b.TotalAlloc-a.TotalAlloc)
	}
	t.Logf("NewShard at defaults allocated %d B (least of %d); derived %d B", got, samples, derived)
	if got < derived || got > derived+slack {
		t.Errorf("NewShard allocated %d B, want %d B (+ up to %d B of rounding). A structure "+
			"is sized differently from Vol 2 section 10.3's arithmetic", got, derived, slack)
	}
}

// TestBatchIndexFindsEachBatchsOwnSentAt is section 8.5's batchSentAt, defined
// (phase 3 seam 3): completions return out of order and Free is a stack, so a
// batch's free-list position is not its identity. Its index in all is.
//
// SETUP: m = 4; every batch taken off Free the way the pump does (from the
// end) and given a distinct send time at its batchIndex.
//
// STIMULUS: the batches come back in the order they were sent, which reverses
// them on the free list.
//
// ASSERTION: each returned batch resolves, through batchIndex, to the send time
// it was given; a batch this shard does not own resolves to -1.
//
// WOULD HAVE CAUGHT: indexing sentAt by free-list position, which after the
// reversal attributes every batch's completion lag to another batch.
func TestBatchIndexFindsEachBatchsOwnSentAt(t *testing.T) {
	sz := defaultSizing()
	sz.MaxInFlightPerShard = 4
	s := newTestShard(t, sz, 1)

	var sent []*model.Batch
	want := map[*model.Batch]int64{}
	for i := 0; len(s.Free) > 0; i++ {
		b := s.Free[len(s.Free)-1]
		s.Free = s.Free[:len(s.Free)-1]
		at := int64(1000 * (i + 1))
		s.sentAt[s.batchIndex(b)] = at
		want[b] = at
		sent = append(sent, b)
	}
	// Completions in send order -- first sent, first back -- which, against a
	// stack, leaves Free in the REVERSE of its original order.
	s.Free = append(s.Free, sent...)

	// Non-vacuity: after that return no batch sits at its own index in all, so
	// a lookup by free-list position would be wrong for every one.
	for i, b := range s.Free {
		if b == s.all[i] {
			t.Fatalf("setup: Free[%d] is all[%d]; the return order does not discriminate", i, i)
		}
	}
	for i, b := range s.Free {
		if got := s.sentAt[s.batchIndex(b)]; got != want[b] {
			t.Errorf("Free[%d] resolves to sentAt %d, want its own %d", i, got, want[b])
		}
	}
	if s.batchIndex(&model.Batch{}) != -1 {
		t.Error("a batch this shard does not own resolved to an index")
	}
}

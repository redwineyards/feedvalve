package mirror

import (
	"hash/maphash"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
)

// Shard owns one partition of all three destination mirrors (Vol 2 section
// 1.1). Exactly one goroutine -- the one that will run Shard.Run -- reads or
// writes every field below except the wiring (params, dst, inst, bus, seed and
// the channels), which NewShard sets once and nothing changes afterwards. The
// one cross-shard signal lives in FlushBus, which the shard only reads.
//
// Allocated once at startup, never copied. Construction is the only thing that
// allocates; nothing in a normal session grows (section 1.3).
type Shard struct {
	ID model.ShardID // 0..shards-1; stamped into every Batch it emits

	// ---- the mirror ----
	// Index[d] maps a destination-d row key to an arena slot, pre-sized at
	// ExpectedRows/Shards so bucket evacuation does not land on Q1's sole
	// consumer (HLD section 7.8).
	Index [model.MaxDestinations]map[model.RowKey]int32

	// Arena[d] holds destination d's entries in chunks of chunkRows. Growth
	// appends a chunk and copies nothing, so a slot is a stable address for the
	// life of the process (INV-SLOT). Entry is pointer-free, so a chunk's
	// backing array is never traced (ADR-006).
	Arena [model.MaxDestinations][]arenaChunk

	NextSlot [model.MaxDestinations]int32 // the bump allocator; == live entries

	// ---- the epoch machinery ----
	// Accum[d] accumulates rows dirtied now, for the NEXT epoch; Cohort[d] is
	// this epoch's snapshot, drained oldest-first. Genuine rings (ADR-L21).
	Accum  [model.MaxDestinations]ring
	Cohort [model.MaxDestinations]ring

	EpochOpen     bool   // at most one epoch open per shard, ever
	Epoch         uint64 // monotonic, stamped into every Batch
	CohortOut     int32  // batches of this epoch emitted and not yet returned
	TickMissed    bool   // a W tick fired while the epoch was open
	epochOpenedAt int64  // monotonic ns

	// tickPending is a tick seen and not yet acted on: the tick arm sets it
	// and stage B opens the epoch (section 2.3). maxBatchSize is the value
	// loaded at epoch open and held for the whole epoch (section 6.3 step 7).
	// Both are used by section 2 and 6 and omitted from section 1.1's struct.
	tickPending  bool
	maxBatchSize int

	// Free holds this shard's batches not currently on batchQ or with a
	// writer. Its length is the remaining write concurrency, and an empty Free
	// is the ONLY thing that stops the pump (section 7.2). A stack: batches
	// come back out of order.
	Free []*model.Batch

	// all is every batch this shard owns, in a fixed order, and sentAt[i] is
	// when all[i] was last sent on batchQ -- section 8.5's batchSentAt. Free's
	// position is not an identity (it is a stack, and completions arrive out of
	// order), and model.Batch gains no field for it, so a batch's index is found
	// by a linear scan of all: m is 2 by default.
	all    []*model.Batch
	sentAt []int64

	// Quarantine[d] is the slots of destination-d rows Oracle has permanently
	// refused (section 9). Expected empty; walked only on a reload.
	Quarantine [model.MaxDestinations][]int32

	// failedRows[d] is section 8.4's scratch for a failed batch's slots,
	// pre-allocated at the batch ceiling so the completion path never grows it.
	failedRows [model.MaxDestinations][]int32

	// cfgGen is the configuration generation last observed. A change at epoch
	// open releases the quarantine (section 9.5). Initialised from the first
	// Params, so the first epoch does not release an empty set.
	cfgGen uint64

	chunkRows int32 // Sizing.ArenaChunkRows

	// ---- wiring: set by NewShard, read-only thereafter ----
	in         <-chan model.UpdateUnit // Q1, this shard's ingest
	completion <-chan *model.Batch     // Q2c, this shard's completions
	tick       <-chan struct{}         // T, from the shared ticker
	wake       <-chan struct{}         // W, the L1 wakeup, cap 1
	batchQ     chan<- *model.Batch     // Q2, shared with every shard

	params  func() *Params // the published reloadable configuration (ruling D1)
	bus     *FlushBus      // the only cross-shard surface
	dst     *destinations.Set
	inst    *ShardInstruments
	busInst busInstruments // inst's two flush-bus handles, in the form Trigger takes
	seed    maphash.Seed
}

// arenaChunk is one contiguous run of entries, allocated whole and never freed,
// moved or resized.
//
// A slice rather than section 1.1's [arenaChunkRows]model.Entry array, because
// runtime.arenaChunkRows is configuration (256-65,536, Vol 0 section 7.2), not
// a constant, and an array length must be one. The property the array was for
// survives: the backing array holds no pointer, so the collector traces one
// slice header per chunk and never the entries.
type arenaChunk []model.Entry

// Edges carries three of the four inbound channel edges of Vol 2 section 1.4
// (shardCh, completionCh, tickCh) plus the outbound batchQ. The fourth inbound
// edge, wake, comes from the FlushBus (FlushBus.Wake(k)).
type Edges struct {
	In         <-chan model.UpdateUnit // shardCh[k]
	Completion <-chan *model.Batch     // completionCh[k]
	Tick       <-chan struct{}         // tickCh[k]
	BatchQ     chan<- *model.Batch     // batchQ, shared
}

// NewShard builds shard k, performing every allocation the shard will make in
// a normal session (Vol 2 section 1.3, Vol 4 startup step 12): the indexes and
// rings pre-sized at ExpectedRows/Shards, arena chunk 0 for every destination,
// m batches with their Rows at the ceiling, and the failed-row scratch.
//
// params is called once here, so cfgGen starts at the first published
// generation. It starts no goroutine.
func NewShard(k model.ShardID, sz Sizing, params func() *Params, dst *destinations.Set,
	bus *FlushBus, inst *ShardInstruments, e Edges) *Shard {
	s := &Shard{
		ID:         k,
		chunkRows:  int32(sz.ArenaChunkRows),
		in:         e.In,
		completion: e.Completion,
		tick:       e.Tick,
		wake:       bus.Wake(k),
		batchQ:     e.BatchQ,
		params:     params,
		bus:        bus,
		dst:        dst,
		inst:       inst,
		busInst:    busInstruments{coalesced: inst.FlushCoalesced, wakeDropped: inst.FlushWakeDropped},
		seed:       sz.Seed,
		cfgGen:     params().Gen,
	}

	perShard := sz.ExpectedRows / sz.Shards
	for d := range model.MaxDestinations {
		s.Index[d] = make(map[model.RowKey]int32, perShard)
		s.Arena[d] = append(make([]arenaChunk, 0, 1), make(arenaChunk, sz.ArenaChunkRows))
		inst.ArenaChunks[d].Store(1)
		s.Accum[d] = newRing(perShard)
		s.Cohort[d] = newRing(perShard)
		s.failedRows[d] = make([]int32, 0, sz.MaxBatchSizeCeiling)
	}

	m := sz.MaxInFlightPerShard
	s.all = make([]*model.Batch, m)
	s.sentAt = make([]int64, m)
	s.Free = make([]*model.Batch, 0, m)
	for i := range m {
		b := &model.Batch{Shard: k, Rows: make([]model.BatchRow, sz.MaxBatchSizeCeiling)}
		s.all[i] = b
		s.Free = append(s.Free, b)
	}
	return s
}

// entry is the Entry at slot of destination d -- a pointer into its chunk,
// never a copy.
func (s *Shard) entry(d model.DestID, slot int32) *model.Entry {
	return &s.Arena[d][slot/s.chunkRows][slot%s.chunkRows]
}

// allocEntry hands out the next slot of destination d (Vol 2 section 1.3), a
// bump allocator: slots are never freed, compacted or moved (INV-SLOT).
//
// When the last chunk is full it appends a new one and STORES the chunk count
// in arena_chunks{k,d} -- a gauge, so the value is the count rather than a
// running total of growths. The entry is then zeroed explicitly (INV-ZERO):
// a fresh chunk is already zero, so this is belt-and-braces against a future
// slot-reuse feature, at 424 bytes per new row rather than per update.
func (s *Shard) allocEntry(d model.DestID) int32 {
	slot := s.NextSlot[d]
	if int(slot/s.chunkRows) == len(s.Arena[d]) {
		s.Arena[d] = append(s.Arena[d], make(arenaChunk, s.chunkRows))
		s.inst.ArenaChunks[d].Store(int64(len(s.Arena[d])))
	}
	s.NextSlot[d]++
	*s.entry(d, slot) = model.Entry{}
	return slot
}

// batchIndex is b's position in all, found by a linear scan -- which is the
// index of its sentAt. -1 for a batch this shard does not own, which would be
// a routing defect upstream.
func (s *Shard) batchIndex(b *model.Batch) int {
	for i, x := range s.all {
		if x == b {
			return i
		}
	}
	return -1
}

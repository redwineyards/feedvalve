package model

// BatchRow is one row's snapshot, copied out of its Entry at pump time so the
// Entry is free to be re-merged while the batch is in flight (HLD section 6.7).
type BatchRow struct {
	Row  RowKey  // the row key, bound as :1 of the MERGE's ON clause
	Mask ColMask // THE MASK THE STATEMENT'S SET LIST IS BUILT FROM.
	// Never the entry's live ColMask, which was zeroed at pump time and
	// has since re-accumulated mid-flight changes. Getting this wrong
	// writes the wrong columns, silently
	Digest uint64 // digest of Image at pump time; the value the shard stores into
	// Entry.Digest on OutcomeCommitted -- never recomputed
	Slot int32 // arena slot of the source Entry, so the completion path needs no
	// map lookup and cannot look up the wrong row after a reload
	Outcome RowOutcome // filled by the writer, read by the owning shard
	_       [1]byte    // explicit pad -- was [3]byte before ErrCode
	ErrCode uint16     // the bare ORA number for a Rejected or Failed row, 0 otherwise.
	// A uint16 and not an oradb.ErrClass because mirror may not import
	// oradb, and the shard never classifies -- it only logs. Without it
	// the quarantine ERROR line names no cause and is undiagnosable
	Image [ImageSize]byte // the row image as it was at pump time, wire form
}

// Batch is one transaction: up to maxBatchSize rows spanning all three
// destinations, committed in destination order 0, 1, 2 with rows sorted by key
// inside each (HLD section 8.3, 8.4).
//
// Ownership crosses goroutines twice and is strictly sequential: the owning
// shard fills it and sends it on batchQ; exactly one writer holds it for the
// duration of one transaction and fills every Outcome; the writer returns it on
// the shard's completion channel; the shard applies the outcomes and returns it
// to its own free list. At no instant do two goroutines hold it.
//
// Batch is the only heap object on the hot path and the only thing passed by
// pointer. It is pre-allocated per shard and reused forever, so a *Batch on a
// channel costs no allocation. Unlike UpdateUnit and Entry it is NOT required
// to be pointer-free -- it never enters an arena and is never traced at
// arena scale -- which is why Rows and Err are allowed here and nowhere else.
type Batch struct {
	Shard     ShardID                // routes the completion back to the owning shard
	Epoch     uint64                 // the shard's epoch counter, for logging and the epoch-completeness gate
	DestStart [MaxDestinations]int32 // first index in Rows belonging to destination d
	DestCount [MaxDestinations]int32 // number of rows in Rows belonging to destination d
	N         int32                  // total rows in use; equals the sum of DestCount
	Err       error                  // batch-level (not row-level) failure: the connection died, the
	// transaction could not begin, or it could not commit. Nil when every
	// verdict is per row.
	//
	// Err and the per-row outcomes are NOT alternatives. A batch that never
	// reached the server has Err set and every row still OutcomePending;
	// the completion path must read that as "every Pending row is
	// OutcomeFailed", and the writer must never return Err == nil with a
	// row left Pending.
	Rows []BatchRow // len is maxBatchSizeCeiling, allocated once at startup and reused;
	// only Rows[:N] is meaningful. This is the one slice in the hot-path
	// types and it is deliberate -- which is exactly why maxBatchSize is
	// bounded by a startup-only ceiling
}

// Dest returns the rows belonging to destination d. The flat array plus
// DestStart/DestCount replaces HLD section 8.3's [3]map[ColMask][]row: a map
// rebuilt per batch allocates on the writer path for no benefit. The writer
// groups by (destination, mask) by sorting this span and walking the runs.
func (b *Batch) Dest(d DestID) []BatchRow {
	start := b.DestStart[d]
	return b.Rows[start : start+b.DestCount[d]]
}

// Active returns the rows in use.
func (b *Batch) Active() []BatchRow { return b.Rows[:b.N] }

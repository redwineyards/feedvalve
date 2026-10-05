package model

// Entry is one row of one destination, living in a shard's arena. Owned
// exclusively by its shard goroutine; never shared, never pointed at.
//
// It contains no pointers and no slices, so the collector never traces the
// roughly 188 MB of arenas that hold these (ADR-006, HLD section 7.8).
//
// All three ordering components are ARRAYS indexed by contributor slot, not
// scalars. Two transaction codes writing disjoint columns of one row have
// incomparable ordering values, so a single per-row counter of any of the three
// lets the faster one permanently suppress the slower (HLD section 6.5).
type Entry struct {
	Row     RowKey  // this destination's row key
	ColMask ColMask // fields dirtied SINCE this row was last batched
	Written ColMask // union of every field ever SUCCESSFULLY committed for this row.
	// The second half of the suppression predicate: without it, a field
	// set for the first time to a value that happens to equal the zero
	// fill is suppressed and stays NULL in Oracle forever
	Digest uint64 // digest of the row image successfully written (NFR13).
	// Zero is the sentinel "never successfully written"
	LastSeq  [MaxContributors]int32 // last applied BCSeqNo, per contributor slot
	LastTime [MaxContributors]int32 // last applied LogTime, SECONDS since 1980, per
	// contributor slot -- wire width, not widened to int64
	LastRecv [MaxContributors]int64 // receipt monotonic ns, per contributor slot --
	// the tiebreak, stored always rather than as a fallback

	Dirty    bool
	InFlight bool // batched, not yet confirmed -- the in-flight rule, HLD section 6.7
	ReDirty  bool // merged again while in flight; requeued by the completion

	Quarantined bool // Oracle refused this row and will refuse it identically forever;
	// off every ring, zero retries, released only by a config fix or a
	// merge touching a disjoint set of columns
	SlotSeen uint8 // bit i: has contributor slot i ever been applied? The ordering
	// guard's denominator rule -- a contribution that initialises a slot
	// has nothing to compare against and must count in neither
	// guard_offered nor guard_superseded

	_ [3]byte // explicit pad to the 8-byte boundary.
	// Was [5]byte before Quarantined and SlotSeen; both landed here, so
	// Entry is still 424 B and no memory figure moved. That is the
	// explicit-padding rule earning its keep -- the free space was visible
	// as a budget and could be spent deliberately, instead of growing the
	// struct by eight bytes across 400,000 rows. Three bytes remain.
	Image [ImageSize]byte // the current column image of this row, wire form
}

// SeenSlot reports whether contributor slot i has ever been applied to this
// row. A contribution initialising a slot has nothing to compare against, so it
// must count in neither guard numerator nor denominator.
func (e *Entry) SeenSlot(i uint8) bool { return e.SlotSeen&(1<<i) != 0 }

// MarkSlotSeen records that contributor slot i has now been applied.
func (e *Entry) MarkSlotSeen(i uint8) { e.SlotSeen |= 1 << i }

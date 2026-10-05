package model

// Contribution describes the columns one UpdateUnit sets in one destination row.
//
// The values themselves live in UpdateUnit.Blob as a PACKED RUN: the bytes for
// the fields named by Mask, in ASCENDING BIT ORDER, each at its declared width,
// with no padding, starting at BlobOff and running BlobLen bytes.
//
// The merge is therefore a single deterministic loop -- walk Mask's set bits
// low to high, copy width(bit) bytes from the blob cursor to imageOffset(bit)
// -- and BlobLen is exactly the sum of those widths, which is what ASM-16a
// bounds.
type Contribution struct {
	Row     RowKey  // the row this contribution targets, within its destination
	Mask    ColMask // fields set; zero means "no contribution to this destination"
	BlobOff uint16  // start of the packed values in UpdateUnit.Blob
	BlobLen uint16  // total packed bytes; equals the sum of the widths of Mask's set bits
	Slot    uint8   // contributor slot of the unit's TransCode WITHIN THIS destination,
	// assigned at startup by ascending transaction code. Per-contribution
	// and not per-unit: one transaction code may occupy different slots in
	// different destinations.
	_ [3]byte // explicit pad to the 8-byte boundary
}

// Present reports whether this unit contributes to its destination at all.
// Contributions are indexed BY destination, so an absent one is an empty mask
// rather than a missing element -- which removes both a destination field and a
// count field, and makes the test one comparison.
func (c *Contribution) Present() bool { return c.Mask != 0 }

// UpdateUnit is one token's worth of one decoded message, with its destination
// contributions already resolved. It is the only thing that crosses the
// receive-to-shard boundary, and it crosses BY VALUE (ADR-006).
//
// Pointer-free and slice-free, and not merely as an optimisation: a []byte here
// would alias the shared decompression buffer, which the receive goroutine
// overwrites on the very next packet (HLD section 6.1).
//
// INVARIANT, gated by the reflection test in layout_test.go: no field of this
// type, transitively, is a pointer, slice, map, string, channel, func or
// interface.
type UpdateUnit struct {
	Token int32 // shard key for token-scoped units (bible section 4.3, 4.5, 4.8 Token,
	// LONG); zero for general units, which are pinned to the designated
	// shard (FR8)
	Code  TransCode // the contributing transaction code
	Flags UnitFlags
	_     [1]byte // explicit pad
	Seq   int32   // BCSeqNo as read (bible section 2.8, header offset 14). Compared with
	// serial-number arithmetic, never with '>' -- ASM-11
	LogTime int32 // BCAST_HEADER.LogTime, SECONDS since 1 Jan 1980 (bible section 2.5,
	// 2.8). Compared as a plain signed timestamp, not serially
	RecvMono int64 // receipt monotonic nanoseconds since process start. Always
	// recorded, never a fallback -- the tiebreak that lets
	// 6531 -> 6571 -> 6511 inside one second order correctly (bible section 4.5)
	Contribs [MaxDestinations]Contribution // indexed by DestID; Mask == 0 means absent
	BlobLen  uint16                        // total bytes of Blob in use; at most UnitBlobSize. The runtime
	// side of the ASM-16a gate
	_    [6]byte            // explicit pad
	Blob [UnitBlobSize]byte // packed contribution values, wire form
}

// General reports whether this unit is non-token-scoped and therefore pinned to
// the designated general shard (FR8).
func (u *UpdateUnit) General() bool { return u.Flags.Has(FlagGeneral) }

// Contribution returns the unit's contribution to destination d. The result is
// always non-nil; use Contribution.Present to tell whether it is populated.
func (u *UpdateUnit) Contribution(d DestID) *Contribution { return &u.Contribs[d] }

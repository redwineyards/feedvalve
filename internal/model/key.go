package model

import "math/bits"

// RowKey identifies one row within one destination.
//
// It is unique within a destination only: each destination has its own
// index keyed by RowKey (HLD section 7.3), so two destinations may
// legitimately use the same value.
//
// Eight bytes, comparable, hashable by Go's map without a pointer chase, and
// pointer-free -- which is what keeps the arenas out of the collector's mark
// phase (ADR-006). A destination whose natural key does not fit 64 bits is
// rejected by -selfcheck rather than silently truncated.
//
// Build one with SingleKey or CompositeKey. Converting directly is how the
// sign-extension bug gets in; see SingleKey.
type RowKey uint64

// SingleKey encodes a single 32-bit key -- a token, a market type -- into the
// low 32 bits.
//
// The parameter is uint32 on purpose. The wire values this wraps are LONG, read
// into int32, and RowKey(someInt32) SIGN-extends: a token of -1 becomes
// 0xFFFFFFFFFFFFFFFF rather than 0x00000000FFFFFFFF, which is a different row.
// Forcing the caller through uint32 makes the conversion explicit at the call
// site and keeps the encoding a bijection onto the low 32 bits, as Vol 0
// section 3.2 requires.
func SingleKey(v uint32) RowKey { return RowKey(v) }

// CompositeKey encodes two 32-bit components, a in the high half and b in the
// low half. Same zero-extension rule as SingleKey.
func CompositeKey(a, b uint32) RowKey { return RowKey(a)<<32 | RowKey(b) }

// Hi returns the high 32 bits -- the first component of a composite key.
func (k RowKey) Hi() uint32 { return uint32(k >> 32) }

// Lo returns the low 32 bits: the whole of a single key, or the second
// component of a composite one.
func (k RowKey) Lo() uint32 { return uint32(k) }

// ColMask is a bitset over one destination's maskable fields.
//
// Bit i corresponds to image field i of that destination, in the destination's
// declared column order. Bit indices are assigned at startup from the declared
// column list and are stable for the life of the process; the column list is
// startup-only configuration precisely so that they are.
//
// A ColMask is meaningful only together with its DestID: bit 3 of destination 0
// and bit 3 of destination 1 are unrelated.
//
// One bit may map to MORE THAN ONE SQL column. An image field is the unit of
// masking, and a field such as destination 0's 120-byte MBP ladder expands to
// 40 bound columns. The mapping from bit to columns and bind order lives in the
// destinations table and is fixed at startup.
type ColMask uint64

// Bit returns the mask containing only bit i. Callers in other packages
// manipulate masks constantly; doing the shift here once means a bit index can
// be wrong in one place rather than six.
func Bit(i uint8) ColMask { return ColMask(1) << i }

// Has reports whether bit i is set.
func (m ColMask) Has(i uint8) bool { return m&Bit(i) != 0 }

// Set returns m with bit i set.
func (m ColMask) Set(i uint8) ColMask { return m | Bit(i) }

// Clear returns m with bit i cleared.
func (m ColMask) Clear(i uint8) ColMask { return m &^ Bit(i) }

// Union returns the fields set in either mask.
func (m ColMask) Union(o ColMask) ColMask { return m | o }

// Without returns the fields of m that are not in o. This is the shape the
// completion path needs: the columns still dirty after a batch committed some
// of them.
func (m ColMask) Without(o ColMask) ColMask { return m &^ o }

// Intersects reports whether the two masks share any field. The quarantine
// release rule turns on a merge touching a DISJOINT set of columns, so this
// being false is what lets a quarantined row back onto the ring.
func (m ColMask) Intersects(o ColMask) bool { return m&o != 0 }

// Count is the number of fields set. The merge walks this many bits, and
// Contribution.BlobLen is the sum of their widths.
func (m ColMask) Count() int { return bits.OnesCount64(uint64(m)) }

// Empty reports whether no field is set. For a Contribution it means "this unit
// does not touch this destination"; for a batch row it is a defect, counted as
// batch_row_empty_mask.
func (m ColMask) Empty() bool { return m == 0 }

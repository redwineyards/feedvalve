package wire

// The protocol constants, and gate G-BLD-1 that pins them.
//
// Every value here is a fact about the exchange's wire format, taken from the
// protocol bible rather than derived from a Go struct. That distinction is the
// whole point of the gate: Vol 4 section 7.1 names the defect it catches as "a
// tidy-up that computes the prefix length from a struct instead of pinning it
// to 8", and bible section 3.4 calls the 8-byte prefix "the single most common
// decoding error". Getting it wrong shifts EVERY field by eight bytes, and the
// symptom is plausible-looking data rather than a crash.
//
// Exported, because framing and decode read them in Phase 2. The gate's own
// table spells them lower-case; they are capitalised here because they cross a
// package boundary, which is the same choice model made for MaxDestinations.
const (
	// BroadcastDataPrefixLen is the prefix that precedes the broadcast header
	// in every broadcast data payload -- for compressed sub-packets, AFTER
	// decompression. Byte 0 repeats the segment netId, bytes 1 to 7 are
	// ignored, and the header starts at the ninth byte (bible section 3.4).
	BroadcastDataPrefixLen = 8

	// BroadcastHeaderSize is the wire size of BCAST_HEADER, excluding the
	// prefix above it.
	//
	// Named in full rather than as the gate table's bcastHeaderSize: poc uses
	// the long form and so does this package's own doc comment, and matching
	// the package beats matching the table's abbreviation.
	BroadcastHeaderSize = 40

	// MessageHeaderSize is MESSAGE_HEADER, which broadcast codes 6503 and 6541
	// carry INSTEAD of a BCAST_HEADER (bible section 4.12, section 4.15). Same
	// size as the broadcast header and a different layout, which is exactly
	// why both are pinned separately.
	MessageHeaderSize = 40

	// InnerMessageHeaderSize is INNER_MESSAGE_HEADER. Also 40, also a
	// DIFFERENT layout again (bible section 2.x, marked there with a warning).
	// Three distinct forty-byte headers is the kind of coincidence that makes
	// a single shared constant tempting and wrong.
	InnerMessageHeaderSize = 40

	// MaxPackedDataLen is cPackData in the BcastPackData envelope: the
	// compressed or uncompressed sub-packet area (bible section 3.2).
	MaxPackedDataLen = 512

	// MaxDatagramLen is the full wire size of BcastPackData:
	// cNetId[2] + iNoPackets[2] + cPackData[512].
	//
	// DERIVED from its own fields rather than written as 516, so the
	// arithmetic and the declared total have to agree -- and the gate below
	// asserts they do. A datagram larger than this is an envelope-assumption
	// break (F6) and must be reported intact, never trimmed into looking
	// valid.
	MaxDatagramLen = 2 + 2 + MaxPackedDataLen

	// MSErrorResponseLen is MS_ERROR_RESPONSE, which arrives instead of the
	// expected body whenever the header's ErrorCode is non-zero -- 182 bytes
	// regardless of the transaction code (bible section 2.10).
	MSErrorResponseLen = 182
)

// Gate G-BLD-1 (Vol 4 section 7.1), in the two-sided form of Vol 0 section 2
// rule 7: each line compiles only when the constant holds its declared value,
// because otherwise one of the two terms is negative and converting a negative
// constant to uint is a compile-time error.
//
// Nothing here runs. The cost is one unused constant per line and the benefit
// is that a protocol value cannot be changed by accident -- only deliberately,
// by editing the number the gate is written against as well.
const (
	_ = uint(BroadcastDataPrefixLen-8) + uint(8-BroadcastDataPrefixLen)
	_ = uint(BroadcastHeaderSize-40) + uint(40-BroadcastHeaderSize)
	_ = uint(MessageHeaderSize-40) + uint(40-MessageHeaderSize)
	_ = uint(InnerMessageHeaderSize-40) + uint(40-InnerMessageHeaderSize)
	_ = uint(MaxPackedDataLen-512) + uint(512-MaxPackedDataLen)
	_ = uint(MaxDatagramLen-516) + uint(516-MaxDatagramLen)
	_ = uint(MSErrorResponseLen-182) + uint(182-MSErrorResponseLen)
)

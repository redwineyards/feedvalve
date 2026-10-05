package model

// MsgHeader is the decoded 40-byte message header, whichever of the two
// broadcast layouts it came from (bible section 2.8).
//
// This is NOT a wire layout. It is the union of the fields we use from both
// header shapes, already byte-swapped; the wire structs live in internal/wire.
//
// Kind is resolved BEFORE the registry is consulted and BEFORE the gap detector
// runs (HLD section 6.2, 6.4). BCSeqNo exists only in BCAST_HEADER, and at
// offset 14 of a MESSAGE_HEADER sits the top half of a nanosecond Timestamp,
// which would fabricate a sequence gap on every 9-second heartbeat
// (bible section 4.12).
type MsgHeader struct {
	TimestampNs int64 // MESSAGE_HEADER.Timestamp, ns since 1 Jan 1980 (bible section 2.5,
	// 2.8). Zero and meaningless when Kind is HeaderBcast
	LogTime int32 // present in both layouts but at DIFFERENT offsets: header offset 4
	// in BCAST_HEADER, offset 2 in MESSAGE_HEADER. Seconds since
	// 1 Jan 1980. Read it at a single fixed offset and the other layout
	// yields AlphaChar -- two characters of Symbol construed as a
	// timestamp. Resolve Kind first, then pick the offset
	BCSeqNo   int32     // BCAST_HEADER only, header offset 14. Zero when Kind is HeaderMessage
	Code      TransCode // BCAST_HEADER offset 10 / MESSAGE_HEADER offset 0
	ErrorCode int16     // offset 12 in BOTH layouts -- checked before dispatch (bible section 2.10)
	MsgLen    int16     // offset 38 in both; counts from the header onward, NOT including
	// the 8-byte prefix. Below 40 is rejected, never clamped
	Kind       HeaderKind
	_          [1]byte // explicit pad
	PayloadLen int32   // length of the whole broadcast data INCLUDING the 8-byte prefix,
	// as framed -- the quantity the 48/60 test in HLD section 6.4 compares
	_ [4]byte // explicit trailing pad.
	// Go's own rounding would supply these four bytes, which makes this the
	// one place Vol 0 section 2 rule 6 could be skipped. It is declared
	// anyway so the offset table and the compiler cannot diverge: a field
	// added here must visibly spend the padding rather than silently grow
	// the struct.
}

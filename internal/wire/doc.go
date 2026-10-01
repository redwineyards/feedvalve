// Package wire owns the protocol surface: the constants (broadcastDataPrefixLen
// 8, broadcastHeaderSize 40, maxPackedDataLen 512, maxDatagramLen 516, the
// per-segment netIds), the wire struct declarations, the fieldReader cursor,
// hexDump, and the pack(2) compile-time layout assertions.
//
// wire MUST NOT import oradb, writer, mirror, config or database/sql.
//
// The structs describe the wire layout under #pragma pack(2), where a word is
// two bytes: CHAR and CHAR arrays are byte aligned, every other member is word
// aligned, and a struct of odd size is padded to an even length -- which also
// sets the stride of an array of them. Several of the specification's own offset
// tables are written as if fully packed and come out one or two bytes short of
// the packet length declared beside them. That gap is real interior padding, not
// trailing slack.
//
// The structs are documentation of field names and order. They are never decoded
// through encoding/binary or reflection; parsing goes through fieldReader, so
// field order in the reader calls is the wire order and there are no offset
// constants to keep in sync. Padding is read explicitly so it stays visible in
// the debug trace and the logged offsets line up with a hex dump byte for byte.
//
// Carried over from poc/exchange_communication.go and poc/reader.go.
//
// Specified by docs/lld/01-receive-decode.md. Built in Phase 2.
package wire

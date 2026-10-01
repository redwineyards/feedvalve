// Package decode owns the decoder registry and the per-transaction-code
// decoders, one file per code, each with its Part V layout assertion.
//
// decode MUST NOT import oradb, writer, mirror or database/sql.
//
// Decoder selection is data-driven: every decoder is registered, and whichever
// transaction code arrives is decoded. Transaction codes 7200 and 7208 are
// mutually exclusive and the choice belongs to the exchange, not to us, so a
// configuration declaring both is accepted rather than rejected. A build that
// can only decode one is a build that goes blind the day the exchange switches.
//
// Each decoder carries a compile-time assertion that its pack(2) field
// arithmetic reconciles with the packet length the specification declares
// (gate G-LAY-1). The worked example is 7208: the record's fields sum to 213,
// but NetChangeIndicator is a CHAR at offset 16 and the only odd-sized member,
// so one pad byte sits at offset 17 and the stride is 214, making
// 40 + 2 + 2*214 the declared 470. That bug is subtly wrong rather than
// obviously wrong -- the token and everything before offset 17 still decodes
// correctly while the prices become noise and the second record starts a byte
// early. Nothing alarms.
//
// Record counts are not array sizes. A message has room for the maximum number
// of records, but the count field says how many actually carry data. Parsing
// every slot unconditionally decodes trailing padding into a fabricated security
// that looks real downstream.
//
// Carried over from poc/gobrdclnt.go (parseBroadcastHeader, the parseMS
// functions).
//
// Specified by docs/lld/01-receive-decode.md. Built in Phase 2.
package decode

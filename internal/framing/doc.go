// Package framing splits the BroadcastPackData envelope: the netId, the packet
// count, the FIFO loop over length-prefixed sub-packets, the compressed and
// uncompressed branches, the cursor arithmetic, and the 8-byte payload prefix
// rule.
//
// framing MUST NOT import oradb, writer, mirror or database/sql.
//
// Three things here are easy to get wrong and expensive to get wrong:
//
// Every broadcast data payload starts with an 8-byte prefix. Byte 0 repeats the
// segment netId, bytes 1 to 7 are ignored, and the broadcast header starts at the
// ninth byte -- for compressed sub-packets, after decompression. Getting this
// wrong shifts every field by eight bytes.
//
// messageLength counts from the broadcast header onward, so a value below the
// header size is rejected. Accepting one advances the cursor too little and
// misframes every later sub-packet in the same buffer.
//
// Byte 0 of the prefix is a free frame-alignment oracle. A netId mismatch on the
// second or later sub-packet of a buffer almost always means the previous
// sub-packet's length was computed wrongly. Mismatches are counted and logged,
// never dropped.
//
// Carried over from poc/gobrdclnt.go (processBroadcastPackData, handleSubPacket).
//
// Specified by docs/lld/01-receive-decode.md. Built in Phase 2.
package framing

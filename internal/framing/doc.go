// Package framing owns QD -- the buffered datagram queue between the receiver
// and the decoder -- and splits the BroadcastPackData envelope: the netId, the
// packet count, the FIFO loop over length-prefixed sub-packets, the compressed
// and uncompressed branches, the cursor arithmetic, and the 8-byte payload
// prefix rule.
//
// framing MUST NOT import oradb, writer, mirror or database/sql.
//
// QD is here because the contract never said where it goes: Vol 0 locates it
// only by goroutine -- the receiver publishes, the decoder drains -- and this is
// the receive-path package nearest the socket.
//
// QD is reached through a Producer and a Consumer handle. The one invariant that
// must not be relaxed is exactly one consumer: a second one reorders datagrams
// relative to arrival, and that order is what makes ordering structural instead
// of dependent on a guard that can disable itself. Drain refuses a second
// concurrent caller. See qd.go for why QD is a channel and not the lock-free
// ring Vol 0 section 9.5 first specified.
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
// The envelope half is carried over from poc/gobrdclnt.go
// (processBroadcastPackData, handleSubPacket). QD has no antecedent in poc,
// where decode ran inline on the receive goroutine -- which is the problem it
// exists to fix.
//
// QD specified by docs/lld/00-foundations.md section 9.5, built in P1-08 as a
// lock-free ring and replaced by a buffered channel under that section's own
// "a buffered channel is a correct drop-in" clause. The envelope splitting is specified by docs/lld/01-receive-decode.md and arrives
// in Phase 2.
package framing

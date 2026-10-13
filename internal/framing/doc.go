// Package framing owns QD -- the lock-free single-producer/single-consumer
// datagram ring between the receiver and the decoder -- and splits the
// BroadcastPackData envelope: the netId, the packet count, the FIFO loop over
// length-prefixed sub-packets, the compressed and uncompressed branches, the
// cursor arithmetic, and the 8-byte payload prefix rule.
//
// framing MUST NOT import oradb, writer, mirror or database/sql.
//
// QD is here because the contract never said where it goes. Vol 0 section 1.1's
// tree and section 1.2's ownership table name fourteen packages and none of
// them mentions a ring; the only identifier the documents ever give it is
// datagramRing, unexported, and they locate it only by goroutine -- the
// receiver owns the producer index, the decoder owns the consumer index. This is
// the receive-path package nearest the socket, and the receiver that owns the
// producer index is the goroutine running framing code. Sections 1.1 and 1.2 now
// say so.
//
// The ring is reached only through a Producer and a Consumer handle, which is
// what enforces its two non-negotiable invariants rather than merely documenting
// them: exactly one consumer, because a second one reorders datagrams relative
// to arrival and that order is what makes ordering structural instead of
// dependent on a guard that can disable itself; and exactly one producer, which
// the contract omits even though two producers would both store tail+1 and lose
// a datagram with no error and no counter.
//
// One rule governs every wakeup here: ANNOUNCE, THEN SIGNAL. A publish stores
// tail before it sends; a close stores the closed flag before it sends; the
// consumer observes tail, then closed, then parks. Reverse any announcement and
// its signal and a wakeup is lost -- which presents as a datagram arriving
// several hundred microseconds late, or at shutdown as one never arriving at
// all, and never as a crash.
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
// QD specified by docs/lld/00-foundations.md section 9.5, built in P1-08. The
// envelope splitting is specified by docs/lld/01-receive-decode.md and arrives
// in Phase 2.
package framing

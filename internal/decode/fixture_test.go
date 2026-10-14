package decode

import (
	"encoding/binary"
	"testing"
)

// Payload fixtures, built from ABSOLUTE OFFSETS transcribed by hand from bible
// section 2.8's two offset tables (reproduced side by side in Vol 1
// section 6.3).
//
// This is the one rule that matters about this file: a fixture builder that
// walks the parser's field order agrees with the parser EVEN WHEN BOTH ARE
// WRONG. poc's suite carries the scar -- TestMBPRecordWireOffsets is
// deliberately not built with buildMSBroadcastOnlyMBPMessage, because a shared
// field order is exactly how a 213-byte record stride survived for months
// (CLAUDE.md, HLD section 10.4, Vol 1 section 8.6 point 2).
//
// So nothing here appends. Every field is placed at a number, and that number
// came from the spec rather than from reader.go.

// field is one placement: size bytes of val at absolute payload offset off.
type field struct {
	off  int
	size int
	val  uint64
}

func u16(off int, v uint16) field { return field{off, 2, uint64(v)} }
func u32(off int, v uint32) field { return field{off, 4, uint64(v)} }
func u64(off int, v uint64) field { return field{off, 8, v} }

// payload builds a broadcast data payload of the given total length (prefix
// included) and writes each field at its absolute offset, big-endian.
//
// Unwritten bytes are 0xDD rather than zero: a parser that reads the wrong
// offset then returns a loud, recognisable value instead of a plausible zero,
// and a field the test forgot to place shows up as 0xDDDD rather than passing
// as "not set".
func payload(t *testing.T, total int, fields ...field) []byte {
	t.Helper()

	b := make([]byte, total)
	for i := range b {
		b[i] = 0xDD
	}
	// Byte 0 repeats the segment netId (commodity derivatives); bytes 1-7 are
	// ignored (bible section 3.4).
	b[0] = 8

	for _, f := range fields {
		if f.off+f.size > total {
			t.Fatalf("fixture field at offset %d (%d bytes) does not fit in %d", f.off, f.size, total)
		}
		switch f.size {
		case 2:
			binary.BigEndian.PutUint16(b[f.off:], uint16(f.val))
		case 4:
			binary.BigEndian.PutUint32(b[f.off:], uint32(f.val))
		case 8:
			binary.BigEndian.PutUint64(b[f.off:], f.val)
		default:
			t.Fatalf("fixture field size %d is not a wire width", f.size)
		}
	}
	return b
}

// Absolute payload offsets, transcribed from bible section 2.8 / Vol 1
// section 6.3. Written out as literals, NOT computed from the constants the
// code under test uses, so that a change to those constants cannot move the
// test and the code in the same direction at once.
const (
	// BCAST_HEADER (header offset + 8).
	bcastReserved1    = 8
	bcastReserved2    = 10
	bcastLogTime      = 12
	bcastAlphaChar    = 16
	bcastTransCode    = 18
	bcastErrorCode    = 20
	bcastBCSeqNo      = 22
	bcastReserved3    = 26
	bcastReserved4    = 27
	bcastTimeStamp2   = 30
	bcastFiller2      = 38
	bcastMessageLen   = 46
	bcastHeaderEndOff = 48

	// MESSAGE_HEADER (header offset + 8).
	msgTransCode    = 8
	msgLogTime      = 10
	msgAlphaChar    = 14
	msgTraderID     = 16
	msgErrorCode    = 20
	msgTimestamp    = 22
	msgTimeStamp1   = 30
	msgTimeStamp2   = 38
	msgMessageLen   = 46
	msgHeaderEndOff = 48
)

// bcastHeader builds a payload carrying a BCAST_HEADER. Values are distinct and
// recognisable so a one-field shift cannot produce a passing comparison.
func bcastHeader(t *testing.T, total int, code uint16, msgLen uint16, seqNo uint32) []byte {
	t.Helper()
	return payload(t, total,
		u16(bcastReserved1, 0),
		u16(bcastReserved2, 0),
		u32(bcastLogTime, 0x11223344),
		u16(bcastAlphaChar, 0xAAAA),
		u16(bcastTransCode, code),
		u16(bcastErrorCode, 0),
		u32(bcastBCSeqNo, seqNo),
		u16(bcastMessageLen, msgLen),
	)
}

// messageHeader builds a payload carrying a MESSAGE_HEADER.
//
// The Timestamp is deliberately large enough that its TOP FOUR BYTES are a
// huge int32: those four bytes sit at payload 22-26, which is exactly where
// BCAST_HEADER keeps BCSeqNo. Reading them as a sequence number on every
// nine-second heartbeat is the defect gate G-HDR-1 exists to catch, and the
// tests assert the value never appears.
func messageHeader(t *testing.T, total int, code uint16, msgLen uint16, ts uint64) []byte {
	t.Helper()
	return payload(t, total,
		u16(msgTransCode, code),
		u32(msgLogTime, 0x55667788),
		u16(msgAlphaChar, 0xBBBB),
		u32(msgTraderID, 0xCCCCCCCC),
		u16(msgErrorCode, 0),
		u64(msgTimestamp, ts),
		u16(msgMessageLen, msgLen),
	)
}

// heartbeatTimestamp is a plausible nanoseconds-since-1980 value (bible
// section 2.5): about 1.4e18, which is what makes its top half a large
// positive int32 rather than a small number that could pass for a sequence.
const heartbeatTimestamp uint64 = 1_400_000_000_000_000_000

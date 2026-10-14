package wire

import (
	"encoding/binary"
	"math"
)

// FieldReader is a sequential big-endian cursor over a byte slice. Every read
// advances the cursor by exactly the bytes consumed, so field order in the
// parser IS wire order and offsets cannot drift out of sync the way
// hand-written data[a:b] slices can (poc/reader.go:10-20, CLAUDE.md).
//
// Two differences from poc's fieldReader, both forced by requirements poc did
// not have (Vol 1 section 8.4):
//
//  1. It is RESET, not constructed. poc's newFieldReader returns a pointer that
//     escapes to the heap on every call -- one allocation per header parse and
//     one per message body, on the path ADR-006 requires to be allocation-free.
//     Here it is a value owned by the receiver and Reset per message.
//  2. Field names are FieldID constants, not strings (ADR-L07). poc formats
//     every field name with fmt.Sprintf whether or not tracing is on; at the
//     7,500 messages/s planning rate that is the largest single source of
//     garbage in the process, spent entirely on lines nobody asked for.
//
// This is the subset Vol 1 section 6 needs: the three integer widths a header
// contains, plus Skip. F64, Byte, Copy and Pad arrive with the decoders that
// need them (P2-05, P2-06), which is also when the name table below grows past
// the header fields.
type FieldReader struct {
	data []byte
	pos  int

	// base is the payload offset of data[0], added to pos when tracing so the
	// logged offsets are ABSOLUTE WITHIN THE BROADCAST DATA, INCLUDING THE
	// 8-BYTE PREFIX (Vol 0 section 10.5, bible section 3.4). That is what makes
	// a trace line up 1:1 against a hexdump -C and against a wire capture, and
	// it is what the differential harness of gate G-DIFF-1 diffs.
	base int

	// trace is nil in normal operation, which is the whole allocation story:
	// the value passed to a trace sink is an any, so the CALLER boxes it, and
	// boxing allocates for anything but a very small integer. poc checks its
	// debugEnabled flag inside logField -- after the call site has already
	// boxed -- so with -debug off it still allocates once per field per
	// message. Here the nil check happens BEFORE the box.
	trace TraceSink
}

// TraceSink receives one line per parsed field. It is declared here, by the
// consumer, rather than imported: obs.DebugLogger satisfies it structurally, so
// neither package needs to know the other exists -- which matters because obs
// MUST NOT import any other internal package (Vol 0 section 1.3).
//
// lo and hi are absolute offsets within the broadcast data, prefix included.
type TraceSink interface {
	Field(name string, lo, hi int, raw []byte, value any)
}

// Reset points the cursor at data, whose first byte sits at payload offset
// base, and enables tracing if trace is non-nil.
//
// Pass a nil trace unless an operator has asked for this transcode
// specifically (obs.DebugLogger.Enabled). Resetting is the only way to use a
// FieldReader; there is deliberately no constructor, because a constructor is
// what allocates.
func (r *FieldReader) Reset(data []byte, base int, trace TraceSink) {
	r.data = data
	r.pos = 0
	r.base = base
	r.trace = trace
}

// Remaining is the bytes left at the cursor. A parser asserts against it once,
// before its first read, and does not bounds-check again (HLD section 9.5
// layer 3).
func (r *FieldReader) Remaining() int { return len(r.data) - r.pos }

// Offset is the current cursor position as a payload offset -- base included,
// so it is directly comparable with the offsets in a trace and in bible
// section 2.8's tables.
func (r *FieldReader) Offset() int { return r.base + r.pos }

// I16 reads a SHORT (bible section 2.7).
func (r *FieldReader) I16(f FieldID) int16 {
	raw := r.take(2)
	v := int16(binary.BigEndian.Uint16(raw))
	if r.trace != nil {
		r.emit(f, raw, v)
	}
	return v
}

// I32 reads a LONG -- four bytes, never the host int.
func (r *FieldReader) I32(f FieldID) int32 {
	raw := r.take(4)
	v := int32(binary.BigEndian.Uint32(raw))
	if r.trace != nil {
		r.emit(f, raw, v)
	}
	return v
}

// I64 reads a LONG LONG.
func (r *FieldReader) I64(f FieldID) int64 {
	raw := r.take(8)
	v := int64(binary.BigEndian.Uint64(raw))
	if r.trace != nil {
		r.emit(f, raw, v)
	}
	return v
}

// F64 reads a DOUBLE: BigEndian.Uint64 then Float64frombits.
//
// Not math.Float64frombits(uint64(I64(...))) at the call site, because that
// reads the field twice in the trace and because a sign-extended intermediate
// is exactly the kind of conversion that turns a negative price into a
// different negative price.
func (r *FieldReader) F64(f FieldID) float64 {
	raw := r.take(8)
	v := math.Float64frombits(binary.BigEndian.Uint64(raw))
	if r.trace != nil {
		r.emit(f, raw, v)
	}
	return v
}

// Byte reads a CHAR.
func (r *FieldReader) Byte(f FieldID) byte {
	raw := r.take(1)
	if r.trace != nil {
		r.emit(f, raw, raw[0])
	}
	return raw[0]
}

// Copy copies len(dst) bytes at the cursor into dst and advances.
//
// This is the workhorse for stored fields, and the copy is the point: a stored
// field keeps its WIRE FORM (Vol 0 section 4.1 R1), so it is copied rather than
// decoded and re-encoded. Decoding a blank-padded CHAR array to a string and
// re-encoding it would allocate, lose the padding the exchange sent, and make
// two representations of one field that can disagree.
//
// A nil or empty dst advances nothing, which keeps the compile-time shape
// assertions in decode harmless.
func (r *FieldReader) Copy(f FieldID, dst []byte) {
	if len(dst) == 0 {
		return
	}
	raw := r.take(len(dst))
	copy(dst, raw)
	if r.trace != nil {
		r.emit(f, raw, raw)
	}
}

// Pad advances over ALIGNMENT padding.
//
// Semantically distinct from Skip even though the bytes move the same way, and
// the distinction is for the human reading a trace: a pad byte is a
// consequence of pack(2) arithmetic, while a skipped field is something the
// exchange sent and we chose not to keep. Telling them apart in the trace is
// how an offset bug gets diagnosed in minutes instead of hours
// (poc/reader.go:43-53, CLAUDE.md invariant 3).
func (r *FieldReader) Pad(f FieldID, n int) {
	if n == 0 {
		return
	}
	raw := r.take(n)
	if r.trace != nil {
		r.trace.Field(f.String(), r.base+r.pos-n, r.base+r.pos, raw, padValue)
	}
}

// padValue is what a pad read reports instead of a number, carried verbatim
// from poc/reader.go:52 so the trace a human reads is the trace they are used
// to -- and so the differential harness of gate G-DIFF-1 sees the same token.
const padValue = "(alignment padding, ignored)"

// Skip advances over a field that is read but not kept -- Reserved, Filler,
// AlphaChar, TraderId.
//
// Bible section 2.6 is explicit that reserved fields "still occupy bytes and
// must be read". Reading them through the cursor rather than assigning past
// them keeps the trace's offsets contiguous, so a missing byte shows up as a
// gap in the trace instead of silently shifting every field after it.
func (r *FieldReader) Skip(f FieldID, n int) {
	raw := r.take(n)
	if r.trace != nil {
		r.emit(f, raw, raw)
	}
}

// take returns the next n bytes and advances. An over-read panics on the slice
// bound, which is intentional: every caller has already checked Remaining
// against a length the protocol fixes, so reaching here with too few bytes is a
// parser defect, not bad input, and the receiver's recover boundary (Vol 1
// section 5.5) turns it into one lost sub-packet while the counter says which.
func (r *FieldReader) take(n int) []byte {
	raw := r.data[r.pos : r.pos+n]
	r.pos += n
	return raw
}

// emit writes one trace line. Split out so the allocation that resolving a name
// and boxing a value costs happens strictly behind the nil check in each read.
func (r *FieldReader) emit(f FieldID, raw []byte, value any) {
	lo := r.base + r.pos - len(raw)
	r.trace.Field(f.String(), lo, r.base+r.pos, raw, value)
}

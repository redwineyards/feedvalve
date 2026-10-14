package wire

import (
	"fmt"
	"strings"
	"testing"
)

// traceRecorder captures what a TraceSink receives, so the tests can assert on
// the stream a trace would print rather than on a formatted string.
//
// It records the FieldReader's own output, which is the half this package owns;
// the line FORMAT is obs.DebugLogger's and is pinned by its own test, verbatim
// from poc/reader.go:30 because gate G-DIFF-1 diffs against it.
type traceRecorder struct {
	lines []traceLine
}

type traceLine struct {
	name   string
	lo, hi int
	raw    string
	value  string
}

func (tr *traceRecorder) Field(name string, lo, hi int, raw []byte, value any) {
	tr.lines = append(tr.lines, traceLine{
		name:  name,
		lo:    lo,
		hi:    hi,
		raw:   fmt.Sprintf("%x", raw),
		value: fmt.Sprintf("%v", value),
	})
}

func TestFieldReaderReadsBigEndianAndAdvances(t *testing.T) {
	data := []byte{
		0x12, 0x34, // I16
		0x01, 0x02, 0x03, 0x04, // I32
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // I64
		0xAA, 0xBB, // Skip
	}

	var r FieldReader
	r.Reset(data, 0, nil)

	if got := r.Remaining(); got != len(data) {
		t.Fatalf("Remaining before any read = %d, want %d", got, len(data))
	}
	if got := r.I16(FieldTransactionCode); got != 0x1234 {
		t.Errorf("I16 = %#04x, want 0x1234 -- a little-endian read would give %#04x", got, 0x3412)
	}
	if got := r.I32(FieldLogTime); got != 0x01020304 {
		t.Errorf("I32 = %#08x, want 0x01020304", got)
	}
	if got := r.I64(FieldTimestamp); got != 7 {
		t.Errorf("I64 = %d, want 7", got)
	}
	if got := r.Offset(); got != 14 {
		t.Errorf("Offset after 2+4+8 bytes = %d, want 14", got)
	}
	r.Skip(FieldFiller2, 2)
	if got := r.Remaining(); got != 0 {
		t.Errorf("Remaining after reading everything = %d, want 0", got)
	}
}

// TestFieldReaderOffsetsAreAbsolute is the property the whole trace rests on.
//
// base is the payload offset of data[0], so a reader handed the header window
// (payload[8:48]) with base 8 must report its first field at 8-10, not 0-2.
// Vol 0 section 10.5 requires offsets "absolute within the broadcast data,
// including the 8-byte prefix" for two reasons that both bite at 03:00: a trace
// lines up 1:1 with a hexdump -C, and gate G-DIFF-1's harness diffs on the
// offset range. Getting it wrong shifts every logged offset by eight, which
// bible section 3.4 calls "the single most common decoding error".
func TestFieldReaderOffsetsAreAbsolute(t *testing.T) {
	payload := make([]byte, BroadcastDataPrefixLen+BroadcastHeaderSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	var tr traceRecorder
	var r FieldReader
	r.Reset(payload[BroadcastDataPrefixLen:], BroadcastDataPrefixLen, &tr)

	r.I16(FieldTransactionCode)
	r.I32(FieldLogTime)
	r.Skip(FieldFiller2, 2)

	want := []struct {
		name   string
		lo, hi int
	}{
		{"TransactionCode", 8, 10},
		{"LogTime", 10, 14},
		{"Filler2", 14, 16},
	}
	if len(tr.lines) != len(want) {
		t.Fatalf("traced %d lines, want %d: %+v", len(tr.lines), len(want), tr.lines)
	}
	for i, w := range want {
		got := tr.lines[i]
		if got.name != w.name || got.lo != w.lo || got.hi != w.hi {
			t.Errorf("line %d = %s offset=%d-%d, want %s offset=%d-%d",
				i, got.name, got.lo, got.hi, w.name, w.lo, w.hi)
		}
	}

	// Contiguity: a gap or an overlap between consecutive lines means a field
	// was skipped silently or read twice, which is precisely the failure an
	// explicit Skip exists to make visible.
	for i := 1; i < len(tr.lines); i++ {
		if tr.lines[i].lo != tr.lines[i-1].hi {
			t.Errorf("trace is not contiguous: line %d ends at %d, line %d starts at %d",
				i-1, tr.lines[i-1].hi, i, tr.lines[i].lo)
		}
	}
}

// TestFieldReaderTracesRawBytesAndValue pins the other two columns the
// differential harness compares: the raw hex at the field's offsets, and the
// decoded value.
func TestFieldReaderTracesRawBytesAndValue(t *testing.T) {
	var tr traceRecorder
	var r FieldReader
	r.Reset([]byte{0x1c, 0x89, 0xff, 0xff, 0xff, 0xfe}, 18, &tr)

	if got := r.I16(FieldTransactionCode); got != 7305 {
		t.Fatalf("I16 = %d, want 7305", got)
	}
	if got := r.I32(FieldBCSeqNo); got != -2 {
		t.Fatalf("I32 = %d, want -2 (LONG is SIGNED; an unsigned read gives 4294967294)", got)
	}

	if tr.lines[0].raw != "1c89" || tr.lines[0].value != "7305" {
		t.Errorf("line 0 hex=%s value=%s, want hex=1c89 value=7305", tr.lines[0].raw, tr.lines[0].value)
	}
	if tr.lines[1].raw != "fffffffe" || tr.lines[1].value != "-2" {
		t.Errorf("line 1 hex=%s value=%s, want hex=fffffffe value=-2", tr.lines[1].raw, tr.lines[1].value)
	}
}

// TestFieldReaderAllocatesNothingUntraced is ADR-L07 and the FieldReader's
// share of G-ALLOC-1.
//
// Two allocation sources are being held at zero. The reader itself is a value
// with a Reset rather than a constructor returning a pointer that escapes
// (poc/reader.go:22-24, one allocation per parse). And the trace's boxed value
// parameter is only reached behind the nil check, so with tracing off -- the
// normal state -- nothing is boxed. poc checks its flag INSIDE logField, after
// the call site has already boxed, which is why it allocates once per field per
// message with -debug off.
func TestFieldReaderAllocatesNothingUntraced(t *testing.T) {
	data := make([]byte, 64)
	var r FieldReader

	if n := testing.AllocsPerRun(1000, func() {
		r.Reset(data, BroadcastDataPrefixLen, nil)
		r.Skip(FieldReserved1, 2)
		_ = r.I32(FieldLogTime)
		_ = r.I16(FieldTransactionCode)
		_ = r.I64(FieldTimestamp)
	}); n != 0 {
		t.Fatalf("an untraced parse allocates %v times, want 0", n)
	}
}

// TestFieldIDNameTableIsComplete guards the one way a uint16 field identifier
// can go wrong: a constant added without its name, which would trace as the
// empty string and make a diff against poc's stream unreadable at exactly the
// field someone was chasing.
func TestFieldIDNameTableIsComplete(t *testing.T) {
	if len(fieldNames) != int(numFieldIDs) {
		t.Fatalf("fieldNames has %d entries, numFieldIDs is %d", len(fieldNames), numFieldIDs)
	}
	seen := map[string]FieldID{}
	for id := FieldID(0); id < numFieldIDs; id++ {
		name := id.String()
		if name == "" {
			t.Errorf("FieldID %d has no name", id)
			continue
		}
		if id == FieldInvalid {
			continue
		}
		if strings.ContainsAny(name, " \t") {
			t.Errorf("FieldID %d name %q contains whitespace; the trace format is column-aligned", id, name)
		}
		if prev, dup := seen[name]; dup {
			t.Errorf("FieldID %d and %d share the name %q", prev, id, name)
		}
		seen[name] = id
	}

	// Out of range resolves rather than panicking: the trace is a diagnostic
	// and must not be the thing that kills a parse.
	if got := numFieldIDs.String(); got != "<unknown field>" {
		t.Errorf("out-of-range FieldID resolved to %q", got)
	}
}

// TestFieldNamesKeepTheBibleSpelling pins the rename that Vol 1 section 8.6
// point 1 is built around: this design traces BCSeqNo where poc traces
// header.sequenceNumber, so the differential harness diffs on (offset, hex,
// value) and carries an explicit name-mapping table. If these names drifted
// back toward poc's, that mapping table would silently stop being needed in
// some places and still be applied -- so the names are pinned here, where the
// mapping's other half can point at them.
func TestFieldNamesKeepTheBibleSpelling(t *testing.T) {
	for id, want := range map[FieldID]string{
		FieldBCSeqNo:         "BCSeqNo",
		FieldLogTime:         "LogTime",
		FieldMessageLength:   "MessageLength",
		FieldTransactionCode: "TransactionCode",
		FieldErrorCode:       "ErrorCode",
	} {
		if got := id.String(); got != want {
			t.Errorf("FieldID %d traces as %q, want the bible's %q", id, got, want)
		}
	}
}

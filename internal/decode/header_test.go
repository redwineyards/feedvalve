package decode

import (
	"fmt"
	"testing"

	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// TestG_HDR_1_HeartbeatIsNotGapDetected is gate G-HDR-1 (Vol 4 section 7.3).
//
// A 6541 payload -- 8-byte prefix plus a 40-byte MESSAGE_HEADER, 48 total
// (bible section 4.12) -- must resolve as HeaderMessage with BCSeqNo == 0.
//
// The defect: resolving it as BCAST_HEADER and reading the HIGH FOUR BYTES of
// a nanosecond Timestamp (MESSAGE_HEADER offset 14) as BCSeqNo (BCAST_HEADER
// offset 14), fabricating a sequence gap every nine seconds and burying the
// real ones. The gate's remaining assertions -- heartbeat_seen++ and
// seq_gap_events unchanged -- belong to the gap detector and the receiver
// (P2-07, P2-04); what this task can pin is that the value which would
// fabricate the gap is never produced.
func TestG_HDR_1_HeartbeatIsNotGapDetected(t *testing.T) {
	p := messageHeader(t, 48, 6541, 40, heartbeatTimestamp)

	var r wire.FieldReader
	var h model.MsgHeader
	if rej := ReadHeader(&r, p, 48, nil, &h); rej != model.RejectNone {
		t.Fatalf("ReadHeader rejected a valid heartbeat: %v", rej)
	}

	if h.Kind != model.HeaderMessage {
		t.Errorf("Kind = %v, want HeaderMessage", h.Kind)
	}
	if h.Code != 6541 {
		t.Errorf("Code = %d, want 6541", h.Code)
	}
	if h.BCSeqNo != 0 {
		t.Errorf("BCSeqNo = %d, want 0 -- a MESSAGE_HEADER has no sequence number", h.BCSeqNo)
	}
	if h.TimestampNs != int64(heartbeatTimestamp) {
		t.Errorf("TimestampNs = %d, want %d", h.TimestampNs, heartbeatTimestamp)
	}

	// Name the value the defect would have produced, so the test says what it
	// is defending rather than only that it passed.
	wouldBe := int32(heartbeatTimestamp >> 32)
	if h.BCSeqNo == wouldBe {
		t.Errorf("BCSeqNo is the top half of the Timestamp (%d) -- the exact G-HDR-1 defect", wouldBe)
	}
	t.Logf("the BCAST_HEADER misread would have yielded BCSeqNo=%d from a %d ns timestamp, "+
		"on every nine-second heartbeat", wouldBe, heartbeatTimestamp)
}

// TestHeaderFieldsMatchTheOffsetTable parses both layouts and checks every
// field this design keeps against the hand-written absolute offsets in
// fixture_test.go.
//
// The point is the SECOND column: LogTime is at payload 12 in a BCAST_HEADER
// and payload 10 in a MESSAGE_HEADER. Vol 0 section 3.6 described it as "both
// layouts" without giving the two offsets, and Vol 1's V-D4 corrected it --
// this is, in the task card's words, "the reconciliation most likely to be
// implemented from the stale text". Read at one fixed offset, the other layout
// yields AlphaChar: two characters of a Symbol construed as a timestamp, which
// then feeds the ordering guard in Phase 3.
func TestHeaderFieldsMatchTheOffsetTable(t *testing.T) {
	t.Run("BCAST_HEADER", func(t *testing.T) {
		p := bcastHeader(t, 478, 7208, 470, 0x0000ABCD)

		var r wire.FieldReader
		var h model.MsgHeader
		if rej := ReadHeader(&r, p, 478, nil, &h); rej != model.RejectNone {
			t.Fatalf("ReadHeader = %v, want RejectNone", rej)
		}

		checkField(t, "Kind", int64(h.Kind), int64(model.HeaderBcast))
		checkField(t, "Code", int64(h.Code), 7208)
		checkField(t, "LogTime (payload 12, NOT 10)", int64(h.LogTime), 0x11223344)
		checkField(t, "ErrorCode", int64(h.ErrorCode), 0)
		checkField(t, "BCSeqNo (payload 22)", int64(h.BCSeqNo), 0x0000ABCD)
		checkField(t, "MsgLen (payload 46)", int64(h.MsgLen), 470)
		checkField(t, "PayloadLen", int64(h.PayloadLen), 478)
		checkField(t, "TimestampNs is meaningless here", h.TimestampNs, 0)

		// AlphaChar sits at payload 16 and is never kept. If LogTime were read
		// at the MESSAGE_HEADER offset it would pick up Reserved2 and the top
		// half of LogTime instead -- name that value too.
		if int64(h.LogTime) == 0x00001122 {
			t.Error("LogTime was read at payload 10 (the MESSAGE_HEADER offset)")
		}
	})

	t.Run("MESSAGE_HEADER", func(t *testing.T) {
		p := messageHeader(t, 60, 6503, 52, heartbeatTimestamp)

		var r wire.FieldReader
		var h model.MsgHeader
		if rej := ReadHeader(&r, p, 60, nil, &h); rej != model.RejectNone {
			t.Fatalf("ReadHeader = %v, want RejectNone", rej)
		}

		checkField(t, "Kind", int64(h.Kind), int64(model.HeaderMessage))
		checkField(t, "Code (payload 8)", int64(h.Code), 6503)
		checkField(t, "LogTime (payload 10, NOT 12)", int64(h.LogTime), 0x55667788)
		checkField(t, "ErrorCode", int64(h.ErrorCode), 0)
		checkField(t, "TimestampNs (payload 22)", h.TimestampNs, int64(heartbeatTimestamp))
		checkField(t, "MsgLen (payload 46)", int64(h.MsgLen), 52)
		checkField(t, "PayloadLen", int64(h.PayloadLen), 60)
		checkField(t, "BCSeqNo is not read for this kind", int64(h.BCSeqNo), 0)
	})
}

// TestG_FRM_7_NonZeroErrorCodeStopsBeforeDispatch is gate G-FRM-7.
//
// Bible section 2.10: a non-zero ErrorCode means the payload is
// MS_ERROR_RESPONSE, 182 bytes, REGARDLESS of the transaction code, and it must
// be checked before dispatching on that code. The defect is dispatching anyway
// and decoding 182 bytes of error response as whatever structure the code
// implies.
//
// ErrorCode is readable before resolution because it is at header offset 12 in
// both layouts -- which is the design reason the check can come first at all.
func TestG_FRM_7_NonZeroErrorCodeStopsBeforeDispatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
		total   int
	}{
		{"on a BCAST_HEADER", func() []byte {
			p := bcastHeader(t, 478, 7208, 470, 7)
			p[bcastErrorCode], p[bcastErrorCode+1] = 0x00, 0x29 // 41
			return p
		}(), 478},
		{"on a MESSAGE_HEADER shape", func() []byte {
			p := messageHeader(t, 48, 6541, 40, heartbeatTimestamp)
			p[msgErrorCode], p[msgErrorCode+1] = 0x00, 0x29
			return p
		}(), 48},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tr traceRecorder
			var r wire.FieldReader
			var h model.MsgHeader
			rej := ReadHeader(&r, tc.payload, tc.total, &tr, &h)

			if rej != model.RejectErrorCode {
				t.Fatalf("ReadHeader = %v, want RejectErrorCode", rej)
			}
			if h.ErrorCode != 41 {
				t.Errorf("ErrorCode = %d, want 41 -- the caller logs it", h.ErrorCode)
			}
			if got := rej.Counter(); got != "error_response" {
				t.Errorf("counter = %q, want error_response", got)
			}

			// NO HEADER FIELD IS PARSED. This is the assertion with teeth, and
			// it is stronger than "the check came first in the source": the
			// payload is a 182-byte MS_ERROR_RESPONSE, so every offset this
			// parser would read belongs to a different structure than the
			// transaction code implies. If any field reached h or the trace,
			// something downstream could consume it.
			//
			// (Checking ErrorCode after Resolve rather than before is NOT
			// observable here and is not what this guards -- Resolve is pure
			// and reads four bytes. The dispatch that bible section 2.10
			// forbids is the registry lookup and the decoder, which arrive in
			// P2-04 and P2-05 and are gated there.)
			if len(tr.lines) != 0 {
				t.Errorf("traced %d fields, want none: %+v", len(tr.lines), tr.lines)
			}
			if h.Code != 0 || h.MsgLen != 0 || h.BCSeqNo != 0 || h.LogTime != 0 || h.TimestampNs != 0 {
				t.Errorf("fields were parsed out of an error response: %+v", h)
			}
		})
	}
}

// TestReadHeaderRejections covers the two length checks that bracket the parse.
func TestReadHeaderRejections(t *testing.T) {
	t.Run("payload below 48 is short, and costs this sub-packet only", func(t *testing.T) {
		// Vol 1 section 6.1's single length precondition, checked once before
		// anything reads a header field.
		for _, n := range []int{0, 1, 40, 47} {
			p := bcastHeader(t, max(n, minPayloadLen), 7208, 470, 1)[:n]

			var r wire.FieldReader
			var h model.MsgHeader
			if rej := ReadHeader(&r, p, n, nil, &h); rej != model.RejectShortPayload {
				t.Errorf("payloadLen=%d: ReadHeader = %v, want RejectShortPayload", n, rej)
			}
		}
	})

	t.Run("exactly 48 is enough", func(t *testing.T) {
		p := bcastHeader(t, 48, 7213, 40, 1)
		var r wire.FieldReader
		var h model.MsgHeader
		if rej := ReadHeader(&r, p, 48, nil, &h); rej != model.RejectNone {
			t.Errorf("ReadHeader = %v, want RejectNone at the exact minimum", rej)
		}
	})

	t.Run("MsgLen below 40 is rejected, never clamped", func(t *testing.T) {
		// Vol 1 section 6.7 row 1. Reachable in the COMPRESSED branch only --
		// framing already rejected it in the uncompressed one -- and it costs
		// this message, which is why the counter is not framing_abandoned.
		p := bcastHeader(t, 478, 7208, 39, 1)

		var r wire.FieldReader
		var h model.MsgHeader
		rej := ReadHeader(&r, p, 478, nil, &h)
		if rej != model.RejectShortMessageLen {
			t.Fatalf("ReadHeader = %v, want RejectShortMessageLen", rej)
		}
		if h.MsgLen != 39 {
			t.Errorf("MsgLen = %d, want the declared 39 -- rejected, not clamped to 40", h.MsgLen)
		}
	})

	t.Run("MsgLen of exactly 40 is accepted", func(t *testing.T) {
		p := bcastHeader(t, 478, 7208, 40, 1)
		var r wire.FieldReader
		var h model.MsgHeader
		if rej := ReadHeader(&r, p, 478, nil, &h); rej != model.RejectNone {
			t.Errorf("ReadHeader = %v, want RejectNone at MsgLen 40", rej)
		}
	})
}

// TestReadHeaderTraceOffsetsAreAbsolute is the half of gate G-DIFF-1 that this
// task can close: the trace a header parse emits must carry offsets absolute
// within the broadcast data, prefix included, and must cover the header
// contiguously from 8 to 48 with no line for the prefix itself.
//
// poc emits no prefix line (it bases its reader past it,
// poc/gobrdclnt.go:309), so an extra line here would be as breaking to the
// differential harness as a missing one -- the harness diffs field STREAMS.
func TestReadHeaderTraceOffsetsAreAbsolute(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
		total   int
		fields  int
	}{
		{"BCAST_HEADER", bcastHeader(t, 478, 7208, 470, 5), 478, 12},
		{"MESSAGE_HEADER", messageHeader(t, 48, 6541, 40, heartbeatTimestamp), 48, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tr traceRecorder
			var r wire.FieldReader
			var h model.MsgHeader
			if rej := ReadHeader(&r, tc.payload, tc.total, &tr, &h); rej != model.RejectNone {
				t.Fatalf("ReadHeader = %v", rej)
			}

			if len(tr.lines) != tc.fields {
				t.Fatalf("traced %d fields, want %d (every byte of the header is read, "+
					"reserved fields included -- bible section 2.6)", len(tr.lines), tc.fields)
			}
			if tr.lines[0].lo != wire.BroadcastDataPrefixLen {
				t.Errorf("first traced field starts at %d, want %d: the prefix is skipped by basing "+
					"the reader past it, and gets no line of its own",
					tr.lines[0].lo, wire.BroadcastDataPrefixLen)
			}
			last := tr.lines[len(tr.lines)-1]
			if last.hi != minPayloadLen {
				t.Errorf("last traced field ends at %d, want %d", last.hi, minPayloadLen)
			}
			for i := 1; i < len(tr.lines); i++ {
				if tr.lines[i].lo != tr.lines[i-1].hi {
					t.Errorf("trace not contiguous between %q (ends %d) and %q (starts %d): "+
						"a silently skipped field shifts everything after it",
						tr.lines[i-1].name, tr.lines[i-1].hi, tr.lines[i].name, tr.lines[i].lo)
				}
			}
		})
	}
}

// TestReadHeaderAllocatesNothing is the header parse's share of G-ALLOC-1. The
// FieldReader is a value the caller owns and resets, and with no trace sink
// attached no value is boxed.
func TestReadHeaderAllocatesNothing(t *testing.T) {
	bcast := bcastHeader(t, 478, 7208, 470, 1)
	heartbeat := messageHeader(t, 48, 6541, 40, heartbeatTimestamp)

	var r wire.FieldReader
	var h model.MsgHeader

	if n := testing.AllocsPerRun(1000, func() {
		ReadHeader(&r, bcast, 478, nil, &h)
		ReadHeader(&r, heartbeat, 48, nil, &h)
	}); n != 0 {
		t.Fatalf("a header parse allocates %v times, want 0", n)
	}
}

// traceRecorder is decode's copy of the wire test's sink -- a few lines rather
// than an exported test helper, since exporting one would put test scaffolding
// in the production API of a package on the receive path.
type traceRecorder struct{ lines []traceLine }

type traceLine struct {
	name   string
	lo, hi int
	raw    string
}

func (tr *traceRecorder) Field(name string, lo, hi int, raw []byte, _ any) {
	tr.lines = append(tr.lines, traceLine{name: name, lo: lo, hi: hi, raw: fmt.Sprintf("%x", raw)})
}

func checkField(t *testing.T, name string, got, want int64) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %d (%#x), want %d (%#x)", name, got, got, want, want)
	}
}

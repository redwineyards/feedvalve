package framing

import (
	"encoding/binary"
	"testing"

	"feedvalve/internal/model"
)

// Envelope framing tests (Vol 1 section 4).
//
// The expected cursor positions below are written as LITERALS, taken from
// section 4.6's worked datagram and from hand arithmetic, never computed with
// the same expressions Split uses. A test that re-derives the offsets the way
// the code does agrees with the code even when both are wrong -- which is how
// a 213-byte record stride survived in poc for months (CLAUDE.md, HLD
// section 10.4).
//
// What is NOT tested here: decompression, header resolution and decoding.
// Split hands the handler the sub-packet's bytes and a compressed flag and goes
// no further, because the decompression buffer belongs to the receiver and is
// reused per sub-packet (section 5.2). The end-to-end "all three decoded" half
// of G-FRM-1 arrives with the receiver in P2-04. The half that lives here is
// the one the gate's defect column names -- "a cursor computed from the
// DECOMPRESSED length rather than iCompLen, misframing every later sub-packet"
// -- and that is caught in full by asserting where each later sub-packet
// starts.

// capture records what Split handed over, in order.
type capture struct {
	calls []call
}

type call struct {
	payload    []byte
	compressed bool
	idx, count int
}

func (c *capture) SubPacket(payload []byte, compressed bool, idx, count int) {
	c.calls = append(c.calls, call{payload: payload, compressed: compressed, idx: idx, count: count})
}

// nopHandler is for the allocation benchmark and the rejection cases, where
// recording would allocate.
type nopHandler struct{ n int }

func (h *nopHandler) SubPacket([]byte, bool, int, int) { h.n++ }

// --- fixture construction ------------------------------------------------
//
// Deliberately low level. These helpers place bytes at stated offsets and do
// no arithmetic the code also does; the one offset that matters -- where
// MessageLength lives inside an uncompressed sub-packet's payload -- is
// written as the literal 46 and cross-checked against the code's derived
// constant by TestMsgLenOffsetIsWhereTheBibleSaysItIs.

const msgLenLiteralOffset = 46 // payload offset: prefix 8 + header offset 38

// envelope wraps packed data in cNetId + iNoPackets. noPackets is written as
// declared, which may deliberately disagree with what packed contains.
func envelope(noPackets int, packed []byte) []byte {
	d := make([]byte, 4+len(packed))
	d[0] = 8 // commodity derivatives
	d[1] = 0
	binary.BigEndian.PutUint16(d[2:4], uint16(int16(noPackets)))
	copy(d[4:], packed)
	return d
}

// compressedSub builds a sub-packet with iCompLen = declared, followed by
// written bytes of filler. declared and written differ only in the malformed
// cases.
func compressedSub(declared, written int, fill byte) []byte {
	s := make([]byte, 2+written)
	binary.BigEndian.PutUint16(s[0:2], uint16(int16(declared)))
	for i := 2; i < len(s); i++ {
		s[i] = fill
	}
	return s
}

// uncompressedSub builds a sub-packet with iCompLen = 0 and a payload of
// 8 + msgLen bytes: the 8-byte prefix, then a 40-byte header carrying msgLen at
// payload offset 46, then the body.
//
// payloadLen defaults to 8 + msgLen; pass a different value to make the written
// length disagree with the declared one.
func uncompressedSub(t *testing.T, msgLen, payloadLen int, fill byte) []byte {
	t.Helper()
	if payloadLen == 0 {
		payloadLen = 8 + msgLen
	}

	s := make([]byte, 2+payloadLen)
	binary.BigEndian.PutUint16(s[0:2], 0) // iCompLen == 0: uncompressed
	payload := s[2:]
	for i := range payload {
		payload[i] = fill
	}
	payload[0] = 8 // the netId repeated in byte 0 (bible section 3.4)

	if len(payload) >= msgLenLiteralOffset+2 {
		binary.BigEndian.PutUint16(payload[msgLenLiteralOffset:msgLenLiteralOffset+2], uint16(int16(msgLen)))
	}
	return s
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// --- the gates -----------------------------------------------------------

// TestG_FRM_1_MixedSubPacketsFrameInFIFOOrder is gate G-FRM-1 (Vol 4
// section 7.3): a datagram with iNoPackets = 3 -- compressed, uncompressed,
// compressed, in that order, which bible section 3.2 permits -- framed in FIFO
// order, with the cursor advancing by iCompLen for the compressed ones and by
// 2 + 8 + MessageLength for the uncompressed one.
func TestG_FRM_1_MixedSubPacketsFrameInFIFOOrder(t *testing.T) {
	// Sub-packet 1: compressed, 47 bytes. 47 is chosen to be nothing like the
	// size it would decompress to -- a real 470-byte MBP message compresses to
	// about this -- so a cursor taking the decompressed length would land
	// hundreds of bytes away and the offsets below would not match.
	// Sub-packet 2: uncompressed, MessageLength 40, payload 48.
	// Sub-packet 3: compressed, 100 bytes.
	packed := concat(
		compressedSub(47, 47, 0xC1),
		uncompressedSub(t, 40, 0, 0x22),
		compressedSub(100, 100, 0xC3),
	)
	d := envelope(3, packed)

	var c capture
	res := Split(d, &c)

	if res.Abandoned() {
		t.Fatalf("framing abandoned at %v (sub-packet %d, offset %d)", res.Rule, res.Index, res.Offset)
	}
	if res.SubPackets != 3 || res.NoPackets != 3 {
		t.Fatalf("SubPackets=%d NoPackets=%d, want 3 and 3", res.SubPackets, res.NoPackets)
	}

	// Hand arithmetic, written out rather than computed:
	//   sub-packet 1: iCompLen at packed[0:2],   payload packed[2:49]    (47)
	//   sub-packet 2: iCompLen at packed[49:51], payload packed[51:99]   (48)
	//   sub-packet 3: iCompLen at packed[99:101],payload packed[101:201] (100)
	want := []struct {
		lo, hi     int
		compressed bool
		idx        int
	}{
		{2, 49, true, 0},
		{51, 99, false, 1},
		{101, 201, true, 2},
	}
	if len(c.calls) != len(want) {
		t.Fatalf("handler called %d times, want %d", len(c.calls), len(want))
	}
	packedArea := d[4:]
	for i, w := range want {
		got := c.calls[i]
		if got.idx != w.idx {
			t.Errorf("call %d: idx=%d, want %d -- FIFO order is a protocol requirement (bible section 3.2)",
				i, got.idx, w.idx)
		}
		if got.count != 3 {
			t.Errorf("call %d: count=%d, want 3", i, got.count)
		}
		if got.compressed != w.compressed {
			t.Errorf("call %d: compressed=%v, want %v", i, got.compressed, w.compressed)
		}
		if len(got.payload) != w.hi-w.lo {
			t.Errorf("call %d: payload is %d bytes, want %d", i, len(got.payload), w.hi-w.lo)
		}
		// Identity, not equality: the payload must ALIAS the caller's buffer.
		// A copy here would be an allocation per sub-packet, and it would
		// also hide the lifetime rule the decoders depend on (section 5.2).
		if &got.payload[0] != &packedArea[w.lo] {
			t.Errorf("call %d: payload does not start at packed[%d]", i, w.lo)
		}
	}
}

// TestG_FRM_1_Section4_6WorkedDatagram frames section 4.6's worked example
// exactly: 510 bytes, three sub-packets, compressed then uncompressed then
// uncompressed, both header kinds, no residue.
//
// This is the case the task card names, and it differs from Vol 4's G-FRM-1
// row, which asks for compressed/uncompressed/compressed. Both are framed by
// the same loop and both are tested, rather than picking one and calling the
// other a typo.
func TestG_FRM_1_Section4_6WorkedDatagram(t *testing.T) {
	packed := concat(
		compressedSub(150, 150, 0xA1),    // 7208, expands to 478 = 8 + 470
		uncompressedSub(t, 40, 0, 0x65),  // 6541 heartbeat: MESSAGE_HEADER, 48 bytes
		uncompressedSub(t, 294, 0, 0x73), // 7305: 8 + 294 = 302
	)
	d := envelope(3, packed)

	if len(d) != 510 {
		t.Fatalf("datagram is %d bytes, want section 4.6's 510", len(d))
	}
	if len(packed) != 506 {
		t.Fatalf("packed is %d bytes, want section 4.6's packedLen of 506", len(packed))
	}

	var c capture
	res := Split(d, &c)
	if res.Abandoned() {
		t.Fatalf("framing abandoned at %v (sub-packet %d, offset %d)", res.Rule, res.Index, res.Offset)
	}

	// Section 4.6's own cursor column: off = 152, then 202, then 506.
	wantLen := []int{150, 48, 302}
	wantCursorAfter := []int{152, 202, 506}
	off := 0
	for i, call := range c.calls {
		if len(call.payload) != wantLen[i] {
			t.Errorf("sub-packet %d is %d bytes, want %d", i, len(call.payload), wantLen[i])
		}
		off += 2 + len(call.payload)
		if off != wantCursorAfter[i] {
			t.Errorf("cursor after sub-packet %d is %d, want section 4.6's %d", i, off, wantCursorAfter[i])
		}
	}
	if off != 506 {
		t.Errorf("final cursor %d, want 506 == packedLen (section 4.6: no residue)", off)
	}

	// The second row is what makes section 4.1's coincidence load-bearing: the
	// framing loop read MessageLength out of a MESSAGE_HEADER and got the
	// right answer, before anything knew which header kind it was.
	if c.calls[1].compressed {
		t.Error("the 6541 heartbeat must frame through the uncompressed branch (bible section 4.12)")
	}
}

// TestG_FRM_2_ShortMessageLengthAbandonsTheRemainder is gate G-FRM-2: an
// uncompressed sub-packet declaring MessageLength = 39 abandons the REMAINDER
// of the datagram, framing_abandoned++, "and the earlier sub-packets of the
// same datagram were still processed".
//
// That last clause is the rule section 4.3 states rather than leaving as a
// property of call order: poc gets it right by accident, because it calls
// process before returning the error. Sub-packets already handled were framed
// from lengths read before the bad one and are independently valid.
func TestG_FRM_2_ShortMessageLengthAbandonsTheRemainder(t *testing.T) {
	packed := concat(
		uncompressedSub(t, 40, 0, 0x11),   // good: handed over
		uncompressedSub(t, 39, 100, 0x22), // MessageLength 39 -- R7
		uncompressedSub(t, 40, 0, 0x33),   // never reached
	)
	d := envelope(3, packed)

	var c capture
	res := Split(d, &c)

	if res.Rule != RuleShortMessageLength {
		t.Fatalf("Rule = %v, want R7", res.Rule)
	}
	if res.Reject != model.RejectShortMessageLen {
		t.Errorf("Reject = %v, want RejectShortMessageLen", res.Reject)
	}
	if got := res.Reject.Counter(); got != "framing_abandoned" {
		t.Errorf("counter = %q, want framing_abandoned", got)
	}
	if res.SubPackets != 1 || len(c.calls) != 1 {
		t.Errorf("SubPackets=%d, handler calls=%d, want 1 and 1: the earlier sub-packet is KEPT, "+
			"only the remainder is abandoned", res.SubPackets, len(c.calls))
	}
	if res.Index != 1 {
		t.Errorf("Index = %d, want 1 -- section 4.2 requires the ERROR to name the sub-packet", res.Index)
	}
	if res.Offset != 52 {
		t.Errorf("Offset = %d, want 52 (sub-packet 1 occupies packed[0:50], so the second "+
			"payload starts at 52)", res.Offset)
	}
	if c.calls[0].payload[8] != 0x11 {
		t.Error("the sub-packet that was kept is not the first one")
	}

	// MessageLength of exactly 40 is the boundary and must be accepted: the
	// header alone, with no body, is a legitimate message (6541).
	ok := envelope(1, uncompressedSub(t, 40, 0, 0x44))
	var c2 capture
	if res := Split(ok, &c2); res.Abandoned() || res.SubPackets != 1 {
		t.Errorf("MessageLength 40 was rejected: %v", res.Rule)
	}
}

// --- the remaining rules -------------------------------------------------

// TestSplitRejectionRules walks section 4.2's rules one at a time. Each case
// names the rule, which is the third thing the ERROR line needs after the index
// and the offset -- the counter cannot distinguish them, because all nine share
// framing_abandoned.
func TestSplitRejectionRules(t *testing.T) {
	for _, tc := range []struct {
		name      string
		datagram  []byte
		wantRule  Rule
		wantCalls int
		wantIndex int
	}{
		{
			name:     "R1: three bytes is not an envelope",
			datagram: []byte{8, 0, 0},
			wantRule: RuleShortDatagram, wantIndex: -1,
		},
		{
			name:     "R1: empty datagram",
			datagram: nil,
			wantRule: RuleShortDatagram, wantIndex: -1,
		},
		{
			name:     "R2: iNoPackets is negative, because SHORT is signed",
			datagram: envelope(-1, uncompressedSub(t, 40, 0, 0x55)),
			wantRule: RuleNegativeCount, wantIndex: -1,
		},
		{
			name:     "R4: declared three sub-packets, wrote two",
			datagram: envelope(3, concat(uncompressedSub(t, 40, 0, 1), uncompressedSub(t, 40, 0, 2))),
			wantRule: RuleTruncatedLength, wantCalls: 2, wantIndex: 2,
		},
		{
			name:     "R4: one trailing byte, not even a length prefix",
			datagram: envelope(2, concat(uncompressedSub(t, 40, 0, 1), []byte{0x00})),
			wantRule: RuleTruncatedLength, wantCalls: 1, wantIndex: 1,
		},
		{
			name:     "R5: iCompLen is negative",
			datagram: envelope(1, compressedSub(-5, 60, 0x66)),
			wantRule: RuleNegativeCompLen, wantIndex: 0,
		},
		{
			name:     "R6: uncompressed with fewer than 48 bytes left",
			datagram: envelope(1, uncompressedSub(t, 40, 47, 0x77)),
			wantRule: RuleTruncatedHeader, wantIndex: 0,
		},
		{
			name:     "R7: MessageLength below 40",
			datagram: envelope(1, uncompressedSub(t, 12, 100, 0x88)),
			wantRule: RuleShortMessageLength, wantIndex: 0,
		},
		{
			name:     "R7: MessageLength is negative",
			datagram: envelope(1, uncompressedSub(t, -1, 100, 0x99)),
			wantRule: RuleShortMessageLength, wantIndex: 0,
		},
		{
			name:     "R8: the declared compressed length overruns cPackData",
			datagram: envelope(1, compressedSub(400, 60, 0xAA)),
			wantRule: RuleLengthOverrun, wantIndex: 0,
		},
		{
			name:     "R8: the declared MessageLength overruns cPackData",
			datagram: envelope(1, uncompressedSub(t, 400, 60, 0xBB)),
			wantRule: RuleLengthOverrun, wantIndex: 0,
		},
		{
			name:     "R9: a one-byte compressed sub-packet",
			datagram: envelope(1, compressedSub(1, 60, 0xCC)),
			wantRule: RuleZeroAdvance, wantIndex: 0,
		},
		{
			name:     "R9: a two-byte compressed sub-packet",
			datagram: envelope(1, compressedSub(2, 60, 0xDD)),
			wantRule: RuleZeroAdvance, wantIndex: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c capture
			res := Split(tc.datagram, &c)

			if res.Rule != tc.wantRule {
				t.Fatalf("Rule = %v, want %v", res.Rule, tc.wantRule)
			}
			if res.Reject.Counter() != "framing_abandoned" {
				t.Errorf("counter = %q, want framing_abandoned", res.Reject.Counter())
			}
			if len(c.calls) != tc.wantCalls || res.SubPackets != tc.wantCalls {
				t.Errorf("handler calls=%d SubPackets=%d, want %d (earlier sub-packets are kept)",
					len(c.calls), res.SubPackets, tc.wantCalls)
			}
			if res.Index != tc.wantIndex {
				t.Errorf("Index = %d, want %d", res.Index, tc.wantIndex)
			}
		})
	}
}

// TestSplitAcceptsAnEmptyEnvelope is R3: iNoPackets == 0 is nothing to do, not
// an error, and not counted separately.
func TestSplitAcceptsAnEmptyEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name     string
		datagram []byte
	}{
		{"no packed data at all", envelope(0, nil)},
		{"a full buffer the envelope says is empty", envelope(0, make([]byte, 512))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c capture
			res := Split(tc.datagram, &c)
			if res.Abandoned() {
				t.Errorf("Rule = %v, want none: iNoPackets 0 is not an error", res.Rule)
			}
			if len(c.calls) != 0 {
				t.Errorf("handler called %d times for an empty envelope", len(c.calls))
			}
		})
	}
}

// TestSplitIgnoresResidue is section 4.5. iNoPackets is authoritative for how
// many sub-packets exist, so bytes after the last one carry no framing
// information -- and if the exchange writes the whole fixed-size struct, every
// single datagram has residue (OQ-V1). An instrument here would fire
// constantly; the thing it would be looking for is already caught for free by
// the netId oracle on the next sub-packet.
func TestSplitIgnoresResidue(t *testing.T) {
	packed := concat(uncompressedSub(t, 40, 0, 0x11), make([]byte, 200))
	d := envelope(1, packed)

	var c capture
	res := Split(d, &c)

	if res.Abandoned() {
		t.Fatalf("residue was treated as an error: %v", res.Rule)
	}
	if res.SubPackets != 1 {
		t.Fatalf("SubPackets = %d, want 1", res.SubPackets)
	}
}

// TestSplitTrimsAnOversizeDatagram is the framing half of G-FRM-3: cPackData is
// a fixed 512-byte field, so nothing past it can be framed, but the
// sub-packets inside those 512 bytes are still well-framed and still good data.
// Counting and logging the oversize is the caller's half (P2-04).
func TestSplitTrimsAnOversizeDatagram(t *testing.T) {
	// Two sub-packets inside the first 512 bytes, then a third that straddles
	// the 512-byte boundary and must not be framed. The third is compressed so
	// the rule that stops it is R8 -- the declared length overrunning the
	// trimmed cPackData -- rather than R6, which would fire first for an
	// uncompressed one that cannot even reach its own MessageLength.
	first := uncompressedSub(t, 240, 0, 0x11)  // 2 + 248 = 250
	second := uncompressedSub(t, 240, 0, 0x22) // 2 + 248 = 250, ending at 500
	third := compressedSub(240, 240, 0x33)     // starts at 500, declares 240

	d := envelope(3, concat(first, second, third))
	if len(d) <= 516 {
		t.Fatalf("fixture is %d bytes; this test needs an oversize datagram", len(d))
	}

	var c capture
	res := Split(d, &c)

	if res.SubPackets != 2 {
		t.Errorf("SubPackets = %d, want 2: the sub-packets that fit inside 512 are still processed",
			res.SubPackets)
	}
	if res.Rule != RuleLengthOverrun {
		t.Errorf("Rule = %v, want R8 for the sub-packet that straddles the 512-byte boundary", res.Rule)
	}
	for i, call := range c.calls {
		if len(call.payload) != 248 {
			t.Errorf("call %d: payload %d bytes, want 248", i, len(call.payload))
		}
	}
}

// TestMsgLenOffsetIsWhereTheBibleSaysItIs ties ADR-L06's derived constant to
// the literal the fixtures plant at. The two are written independently on
// purpose: if the derivation drifted, every uncompressed sub-packet would be
// framed from two bytes of Filler2, and a fixture that derived its offset the
// same way would drift with it and keep passing.
func TestMsgLenOffsetIsWhereTheBibleSaysItIs(t *testing.T) {
	if msgLenOffset != msgLenLiteralOffset {
		t.Fatalf("Split reads MessageLength at payload offset %d; bible section 2.8 puts it at %d "+
			"(prefix 8 + header offset 38)", msgLenOffset, msgLenLiteralOffset)
	}
	if messageLengthHeaderOffset != 38 {
		t.Errorf("MessageLength's header offset is %d, want 38 in BOTH layouts", messageLengthHeaderOffset)
	}
}

// TestSplitAllocatesNothing is framing's share of G-ALLOC-1. The handler is an
// interface rather than a func for exactly this reason: a method value would be
// a closure escaping to the heap once per datagram.
func TestSplitAllocatesNothing(t *testing.T) {
	d := envelope(3, concat(
		compressedSub(47, 47, 0xC1),
		uncompressedSub(t, 40, 0, 0x22),
		compressedSub(100, 100, 0xC3),
	))
	h := &nopHandler{}

	if n := testing.AllocsPerRun(1000, func() {
		if res := Split(d, h); res.Abandoned() {
			t.Fatalf("framing abandoned: %v", res.Rule)
		}
	}); n != 0 {
		t.Fatalf("Split allocates %v times per datagram, want 0", n)
	}
}

// TestRuleStringsAreDistinct keeps the diagnosis legible: nine rules sharing
// one counter means the rule name is what an operator reads.
func TestRuleStringsAreDistinct(t *testing.T) {
	seen := map[string]Rule{}
	for r := RuleNone; r <= RuleZeroAdvance; r++ {
		s := r.String()
		if s == "" || s == "Rule(?)" {
			t.Errorf("Rule %d has no description", r)
		}
		if prev, dup := seen[s]; dup {
			t.Errorf("Rule %d and %d share the description %q", prev, r, s)
		}
		seen[s] = r
	}
}

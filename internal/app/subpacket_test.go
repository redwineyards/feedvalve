package app

import (
	"encoding/binary"
	"strings"
	"testing"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// The sub-packet boundary: Vol 1 section 5.5's recover boundary, the dispatch
// around it, and the receiver halves of three gates P2-01 and P2-02 could only
// close from one side.
//
// The registry is supplied as a FAKE, which is what decode.Registry being an
// interface buys: a decoder that panics cannot be registered into the real
// catalogue from here, because registration is one-file-per-code inside decode
// and deliberately not exported.

// fakeRegistry is a decode.Registry holding one entry.
type fakeRegistry struct {
	code  model.TransCode
	entry *decode.RegistryEntry
}

func (f *fakeRegistry) Lookup(code model.TransCode) *decode.RegistryEntry {
	if f.entry != nil && code == f.code {
		return f.entry
	}
	return nil
}

func (f *fakeRegistry) Slot(code model.TransCode) int {
	if f.entry != nil && code == f.code {
		return 0
	}
	return -1
}

func (f *fakeRegistry) Codes() []model.TransCode {
	if f.entry == nil {
		return nil
	}
	return []model.TransCode{f.code}
}

func (f *fakeRegistry) Len() int {
	if f.entry == nil {
		return 0
	}
	return 1
}

// scriptedDecoder does whatever the test tells it to.
type scriptedDecoder struct {
	code     model.TransCode
	bodyLen  int
	maxUnits int
	onDecode func(model.MsgHeader, []byte, []model.UpdateUnit) (int, error)
}

func (d *scriptedDecoder) Code() model.TransCode { return d.code }
func (d *scriptedDecoder) BodyLen() int          { return d.bodyLen }
func (d *scriptedDecoder) MaxUnits() int         { return d.maxUnits }

func (d *scriptedDecoder) Decode(h model.MsgHeader, p []byte, out []model.UpdateUnit) (int, error) {
	if d.onDecode != nil {
		return d.onDecode(h, p, out)
	}
	return 0, nil
}

// subPacketFixture is a receiver wired to a fake registry, with the log
// captured. No socket and no goroutines: the boundary is a function, and
// testing it as one is what makes the panic case safe to run at all.
type subPacketFixture struct {
	r   *receiver
	buf *syncBuf
	in  *instruments
}

func newSubPacketFixture(t *testing.T, reg decode.Registry, tune func(*config.Config)) *subPacketFixture {
	t.Helper()

	cfg := config.Defaults()

	// The fixture's configuration declares whatever the fake registry
	// resolves, because obs sizes its {code} dimension from the CONFIGURED
	// inventory and the per-transcode counters are indexed by position in it.
	// A fixture that skipped this would resolve a counter at an index obs
	// never allocated -- which is exactly what Build now asserts against.
	for _, code := range reg.Codes() {
		cfg.Transcodes = append(cfg.Transcodes, config.TranscodeConfig{
			Code:  int(code),
			Class: "rate-limited",
		})
	}
	if tune != nil {
		tune(cfg)
	}
	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}

	buf := &syncBuf{}
	obsReg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(obsReg, cfg)
	obsReg.Freeze()

	g := &Graph{
		deps: Deps{
			Cfg: cfg, Reg: obsReg, Dests: dests, Stderr: buf, Inst: inst,
			Log:   obs.NewAppLogger(buf, obs.ParseLevel("info"), 10000, obsReg),
			Debug: obs.NewDebugLogger(buf, nil, false),
		},
		// Buffered to 1, exactly as Build makes it, so Graph.Fatal can report a
		// stall-budget expiry without a lifecycle goroutine to receive it. A
		// nil channel here would block the fan-out forever instead of failing.
		fatal: make(chan fatalEvent, 1),
	}
	g.setRuntime(&Runtime{Cfg: cfg, Reg: reg, Dests: dests})

	// Q1, which these tests fan out into. Allocated through the same method
	// Build uses, and left WITHOUT a consumer on purpose: at the default
	// capacity of 4096 no test here emits enough units to fill one, so every
	// send takes the fast path and the units stay readable afterwards. A test
	// that wants the blocked path shrinks the capacity instead.
	g.allocShardQueues(cfg.Runtime.Shards, cfg.Runtime.ShardQueueCapacity)

	rcv := newReceiver(g)
	rcv.rt = g.loadRuntime()
	return &subPacketFixture{r: rcv, buf: buf, in: &g.deps.Inst}
}

// bcastPayload builds an uncompressed broadcast payload with a BCAST_HEADER:
// the 8-byte prefix, the transaction code at payload 18, MessageLength at 46,
// and a body of the declared size.
//
// Offsets written as literals, from bible section 2.8, rather than computed the
// way the parser computes them.
func bcastPayload(code uint16, msgLen int, total int) []byte {
	p := make([]byte, total)
	p[0] = 8 // the netId repeated in byte 0
	binary.BigEndian.PutUint16(p[18:20], code)
	binary.BigEndian.PutUint16(p[46:48], uint16(msgLen))
	return p
}

// TestG_FRM_8_ADecoderPanicIsContained is gate G-FRM-8.
//
// A decoder rigged to panic must cost ONE MESSAGE, not the process: the recover
// boundary is HLD section 9.5's layer 4, "the last line, not the first". Two
// further properties the gate does not spell out but the design depends on: the
// panic is counted as panics_recovered{decode}, and the boundary returns ZERO
// units, so a decoder that panicked halfway through a fan-out contributes
// nothing rather than a partially populated prefix downstream could not tell
// from a complete one.
func TestG_FRM_8_ADecoderPanicIsContained(t *testing.T) {
	const code = 7208
	panicking := &scriptedDecoder{
		code: code, bodyLen: 470, maxUnits: 2,
		onDecode: func(_ model.MsgHeader, _ []byte, out []model.UpdateUnit) (int, error) {
			// Writes one unit, THEN panics: the half-populated case, which is
			// the one that matters. A decoder that panicked before writing
			// anything would pass a weaker test.
			out[0] = model.UpdateUnit{}
			panic("rigged by the gate test")
		},
	}
	f := newSubPacketFixture(t, &fakeRegistry{
		code:  code,
		entry: &decode.RegistryEntry{Dec: panicking, Code: code, Class: model.ClassRateLimited},
	}, nil)

	payload := bcastPayload(code, 470, 478)

	// The call must RETURN, not take the test binary with it.
	n := f.r.decodeSubPacket(payload, false, 0, 1)

	if n != 0 {
		t.Errorf("the boundary returned %d units after a panic; a half-written fan-out must "+
			"contribute nothing", n)
	}
	if got := f.in.panicsRecovered.Load(); got != 1 {
		t.Errorf("panics_recovered{decode} = %d, want 1", got)
	}
	if log := f.buf.String(); !strings.Contains(log, "recovered from a panic") {
		t.Errorf("the recovery was not logged:\n%s", log)
	}

	// And the NEXT sub-packet still decodes, which is what "costs one message"
	// means operationally.
	var decoded int
	panicking.onDecode = func(model.MsgHeader, []byte, []model.UpdateUnit) (int, error) {
		decoded++
		return 1, nil
	}
	if n := f.r.decodeSubPacket(payload, false, 1, 2); n != 1 || decoded != 1 {
		t.Errorf("after a contained panic the next sub-packet produced n=%d decoded=%d, want 1 and 1",
			n, decoded)
	}
}

// TestUnresolvedCodesAreSeparated is ADR-011's distinction: "we can decode this
// but nobody told us what to do with it" must be distinguishable from "we have
// never seen this code". The first is a configuration mistake an operator fixes
// in a minute; the second needs the exchange.
//
// BOTH BRANCHES ARE REACHABLE AS OF P2-06, which is what makes this test mean
// something: until the first decoder was registered, every code was unknown and
// the distinction was theoretical. 7208 is now catalogued, so an empty registry
// makes it UNCONFIGURED; 7320 is catalogued by nobody, so it stays unknown.
func TestUnresolvedCodesAreSeparated(t *testing.T) {
	f := newSubPacketFixture(t, &fakeRegistry{}, nil)

	t.Run("catalogued but not configured", func(t *testing.T) {
		if n := f.r.decodeSubPacket(bcastPayload(7208, 470, 478), false, 0, 1); n != 0 {
			t.Errorf("an unconfigured code produced %d units", n)
		}
		c, _ := f.in.unconfiguredTranscode(7208)
		if got := c.Load(); got != 1 {
			t.Errorf("unconfigured_transcode{7208} = %d, want 1", got)
		}
		// And NOT as unknown: conflating the two would hide a one-minute
		// configuration fix inside a protocol-change investigation.
		u, _ := f.in.unknownTranscode(7208)
		if got := u.Load(); got != 0 {
			t.Errorf("unknown_transcode{7208} = %d; 7208 has a decoder in this build", got)
		}
		if log := f.buf.String(); !strings.Contains(log, "not configured") {
			t.Errorf("the unconfigured WARN is missing:\n%s", log)
		}
	})

	t.Run("catalogued by nobody", func(t *testing.T) {
		// 7320 is out of the Phase 1 inventory on purpose (Vol 1 section 9.1):
		// it would emit up to 35 units with nowhere to put them.
		if n := f.r.decodeSubPacket(bcastPayload(7320, 470, 478), false, 0, 1); n != 0 {
			t.Errorf("an unknown code produced %d units", n)
		}
		c, _ := f.in.unknownTranscode(7320)
		if got := c.Load(); got != 1 {
			t.Errorf("unknown_transcode{7320} = %d, want 1", got)
		}
		u, _ := f.in.unconfiguredTranscode(7320)
		if got := u.Load(); got != 0 {
			t.Errorf("unconfigured_transcode{7320} = %d; nothing in this build decodes it", got)
		}
	})
}

// TestHeaderOnlyCodesEmitNothing is Vol 1 section 9.6: 6541 and 6503 are
// recognised but not decoded. No units, no registry lookup, and -- the part
// that matters -- no gap detection, because a MESSAGE_HEADER has no BCSeqNo and
// offset 14 holds the top half of a nanosecond timestamp.
func TestHeaderOnlyCodesEmitNothing(t *testing.T) {
	f := newSubPacketFixture(t, &fakeRegistry{}, nil)

	// 6541 at exactly 48 bytes: the heartbeat's resolution shape.
	heartbeat := make([]byte, 48)
	heartbeat[0] = 8
	binary.BigEndian.PutUint16(heartbeat[8:10], 6541)
	binary.BigEndian.PutUint16(heartbeat[46:48], 40)

	if n := f.r.decodeSubPacket(heartbeat, false, 0, 1); n != 0 {
		t.Errorf("a heartbeat produced %d units", n)
	}
	if got := f.in.heartbeatSeen.Load(); got != 1 {
		t.Errorf("heartbeat_seen = %d, want 1", got)
	}
	if got := f.in.messageHeaderSeen[obs.HeaderHeartbeat].Load(); got != 1 {
		t.Errorf("message_header_seen{6541} = %d, want 1", got)
	}
	// It must NOT have been counted as an unknown transcode: it is recognised,
	// just not decoded.
	c, _ := f.in.unknownTranscode(6541)
	if got := c.Load(); got != 0 {
		t.Errorf("unknown_transcode{6541} = %d; a heartbeat is recognised, not unknown", got)
	}
}

// TestShortPayloadIsRejectedAtTheHeaderStage is Vol 1 section 6.1's single
// length precondition, counted at the stage it fired.
func TestShortPayloadIsRejectedAtTheHeaderStage(t *testing.T) {
	f := newSubPacketFixture(t, &fakeRegistry{}, nil)

	for _, n := range []int{0, 1, 47} {
		if got := f.r.decodeSubPacket(make([]byte, n), false, 0, 1); got != 0 {
			t.Errorf("a %d-byte payload produced %d units", n, got)
		}
	}
	if got := f.in.shortPayload[obs.PayloadHeader].Load(); got != 3 {
		t.Errorf("short_payload{header} = %d, want 3", got)
	}
	if got := f.in.shortPayload[obs.PayloadBody].Load(); got != 0 {
		t.Errorf("short_payload{body} = %d; the header check fired, not the body one", got)
	}
}

// TestG_FRM_7_ErrorResponseDispatchesNothing is the receiver half of G-FRM-7,
// which P2-02 could only close at the parse level.
//
// bible section 2.10: a non-zero ErrorCode means the payload is
// MS_ERROR_RESPONSE, 182 bytes, regardless of the transaction code. The defect
// is dispatching on that code anyway and decoding an error response as whatever
// structure it implies.
func TestG_FRM_7_ErrorResponseDispatchesNothing(t *testing.T) {
	const code = 7208
	var decoded int
	f := newSubPacketFixture(t, &fakeRegistry{
		code: code,
		entry: &decode.RegistryEntry{Code: code, Dec: &scriptedDecoder{
			code: code, bodyLen: 470, maxUnits: 2,
			onDecode: func(model.MsgHeader, []byte, []model.UpdateUnit) (int, error) {
				decoded++
				return 1, nil
			},
		}},
	}, nil)

	payload := bcastPayload(code, 470, 478)
	binary.BigEndian.PutUint16(payload[20:22], 41) // ErrorCode, offset 12 in both layouts

	if n := f.r.decodeSubPacket(payload, false, 0, 1); n != 0 {
		t.Errorf("an error response produced %d units", n)
	}
	if decoded != 0 {
		t.Error("the decoder ran on a payload that is a 182-byte MS_ERROR_RESPONSE")
	}
	if got := f.in.errorResponse.Load(); got != 1 {
		t.Errorf("error_response = %d, want 1", got)
	}
	// ASM-V3's resolution: a bounded hex dump, so the header shape an error
	// response carries on the broadcast circuit can be settled from production
	// the first time it ever happens.
	if log := f.buf.String(); !strings.Contains(log, "ASM-V3") || !strings.Contains(log, "00000000") {
		t.Errorf("the error response was logged without its hex dump:\n%s", log)
	}
}

// TestBodyLenMismatchRejectsBeforeTheDecoder is Vol 1 section 6.7: the declared
// MessageLength against the registry's BodyLen, which is the runtime half of
// the compile-time layout gate. The gate proves our arithmetic matches the
// bible; this proves the wire does.
func TestBodyLenMismatchRejectsBeforeTheDecoder(t *testing.T) {
	const code = 7208
	var decoded int
	f := newSubPacketFixture(t, &fakeRegistry{
		code: code,
		entry: &decode.RegistryEntry{Code: code, Dec: &scriptedDecoder{
			code: code, bodyLen: 470, maxUnits: 2,
			onDecode: func(model.MsgHeader, []byte, []model.UpdateUnit) (int, error) {
				decoded++
				return 1, nil
			},
		}},
	}, nil)

	// MessageLength 400 against a declared BodyLen of 470: a protocol version
	// change shows up here, by code, instead of as silently skipped messages.
	if n := f.r.decodeSubPacket(bcastPayload(code, 400, 478), false, 0, 1); n != 0 {
		t.Errorf("a body-length mismatch produced %d units", n)
	}
	if decoded != 0 {
		t.Error("the decoder ran despite MessageLength disagreeing with its declared BodyLen")
	}
	if got := f.in.bodyLenMismatch[0].Load(); got != 1 {
		t.Errorf("body_len_mismatch{7208} = %d, want 1", got)
	}

	// The matching length decodes.
	if n := f.r.decodeSubPacket(bcastPayload(code, 470, 478), false, 0, 1); n != 1 || decoded != 1 {
		t.Errorf("the declared length was rejected: n=%d decoded=%d", n, decoded)
	}
}

// TestNetIDMismatchWarnsAndProcesses is the receiver half of G-FRM-5: a payload
// whose byte 0 is 4 (equity) against an envelope netId of 8 (commodity) must
// WARN, count, and be PROCESSED anyway. Turning the frame-alignment oracle into
// a filter is the defect.
//
// The sub-packet INDEX is the whole value of the line: on sub-packet 2 or later
// a mismatch almost always means the previous sub-packet's length was computed
// wrongly, which is a different problem with a different fix.
func TestNetIDMismatchWarnsAndProcesses(t *testing.T) {
	const code = 7208
	var decoded int
	f := newSubPacketFixture(t, &fakeRegistry{
		code: code,
		entry: &decode.RegistryEntry{Code: code, Dec: &scriptedDecoder{
			code: code, bodyLen: 470, maxUnits: 2,
			onDecode: func(model.MsgHeader, []byte, []model.UpdateUnit) (int, error) {
				decoded++
				return 1, nil
			},
		}},
	}, nil)

	payload := bcastPayload(code, 470, 478)
	payload[0] = 4 // equity, against the configured commodity 8

	if n := f.r.decodeSubPacket(payload, false, 1, 3); n != 1 {
		t.Errorf("a netId mismatch returned %d units; it must never drop", n)
	}
	if decoded != 1 {
		t.Error("a netId mismatch stopped the decoder; the oracle is not a filter")
	}
	if got := f.in.netidMismatch.Load(); got != 1 {
		t.Errorf("netid_mismatch = %d, want 1", got)
	}
	log := f.buf.String()
	if !strings.Contains(log, "sub-packet=2/3") {
		t.Errorf("the WARN does not name the sub-packet index:\n%s", log)
	}
	if !strings.Contains(log, "frame misalignment") {
		t.Errorf("a mismatch on sub-packet 2+ must carry the misalignment hint:\n%s", log)
	}
}

// TestNetIDCheckIsDisabledByZero is Vol 0 section 7.2: expectedNetId == 0
// disables the comparison.
func TestNetIDCheckIsDisabledByZero(t *testing.T) {
	f := newSubPacketFixture(t, &fakeRegistry{}, func(c *config.Config) {
		c.UDP.ExpectedNetID = 0
	})

	payload := bcastPayload(7208, 470, 478)
	payload[0] = 99
	f.r.decodeSubPacket(payload, false, 0, 1)

	if got := f.in.netidMismatch.Load(); got != 0 {
		t.Errorf("netid_mismatch = %d with the check disabled", got)
	}
}

// datagramOf wraps sub-packets in an envelope: cNetId, iNoPackets, then each
// sub-packet's 2-byte iCompLen followed by its bytes.
func datagramOf(t *testing.T, subs ...[]byte) *model.Datagram {
	t.Helper()

	var d model.Datagram
	d.Buf[0] = 8 // cNetId: commodity derivatives
	binary.BigEndian.PutUint16(d.Buf[2:4], uint16(len(subs)))

	off := 4
	for _, s := range subs {
		copy(d.Buf[off:], s)
		off += len(s)
	}
	if off > len(d.Buf) {
		t.Fatalf("fixture is %d bytes, over the ring slot's %d", off, len(d.Buf))
	}
	d.N = int32(off)
	return &d
}

// compressedSub is a sub-packet declaring iCompLen = len(stream).
func compressedSub(stream []byte) []byte {
	s := make([]byte, 2+len(stream))
	binary.BigEndian.PutUint16(s[0:2], uint16(len(stream)))
	copy(s[2:], stream)
	return s
}

// uncompressedSub is a sub-packet with iCompLen = 0 and the payload after it.
func uncompressedSub(payload []byte) []byte {
	s := make([]byte, 2+len(payload))
	copy(s[2:], payload)
	return s
}

// TestG_FRM_4_ATruncatedStreamCostsOneSubPacket closes the half of gate G-FRM-4
// that P2-01 structurally could not reach: "the NEXT sub-packet in the same
// datagram still decodes".
//
// It holds because the cursor advanced by iCompLen, which was read from the
// wire BEFORE the decompressor ran (Vol 1 section 5.6, HLD F3) -- so a
// decompression failure cannot misframe what follows it. This drives the whole
// datagram through framing.Split, because that property lives in the
// relationship between the loop and the handler and not in either alone.
func TestG_FRM_4_ATruncatedStreamCostsOneSubPacket(t *testing.T) {
	const code = 7208
	var decoded int
	f := newSubPacketFixture(t, &fakeRegistry{
		code: code,
		entry: &decode.RegistryEntry{Code: code, Dec: &scriptedDecoder{
			code: code, bodyLen: 470, maxUnits: 2,
			onDecode: func(model.MsgHeader, []byte, []model.UpdateUnit) (int, error) {
				decoded++
				return 2, nil
			},
		}},
	}, nil)

	// A stream DecompressSafe refuses: a literal-run opcode promising far more
	// input than follows it. Any rejected stream serves -- the assertion is
	// about what the failure costs, not about which sentinel it produces,
	// which internal/lzo already pins against frozen fixtures.
	bad := []byte{0x20, 0xFF, 0xFF, 0xFF}

	d := datagramOf(t,
		compressedSub(bad),
		uncompressedSub(bcastPayload(code, 470, 478)),
	)

	if err := f.r.decodeDatagram(d); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}

	// The compressed one failed...
	var failed uint64
	for _, c := range f.in.decompressFailed {
		failed += c.Load()
	}
	if failed != 1 {
		t.Errorf("decompress_failed = %d across every reason, want 1", failed)
	}
	// ...and the one after it decoded anyway. This is the assertion.
	if decoded != 1 {
		t.Errorf("the sub-packet after a failed decompression decoded %d times, want 1 -- "+
			"a decompression failure must cost ONE sub-packet, not the rest of the datagram",
			decoded)
	}
	if got := f.in.subpacketsTotal.Load(); got != 2 {
		t.Errorf("subpackets_total = %d, want 2", got)
	}
	if got := f.in.subpacketsCompressed.Load(); got != 1 {
		t.Errorf("subpackets_compressed = %d, want 1", got)
	}
	if got := f.in.framingAbandoned.Load(); got != 0 {
		t.Errorf("framing_abandoned = %d; a decompression failure must not abandon the datagram",
			got)
	}
}

// TestDecodeDatagramChecksTheEnvelopeNetID is the once-per-datagram half of the
// netId oracle (Vol 1 section 6.2): cNetId is CHAR[2] holding a small number,
// so a match in either byte is accepted, and a mismatch is counted and logged
// without dropping anything.
func TestDecodeDatagramChecksTheEnvelopeNetID(t *testing.T) {
	f := newSubPacketFixture(t, &fakeRegistry{}, nil)

	d := datagramOf(t, uncompressedSub(bcastPayload(7208, 470, 478)))
	d.Buf[0], d.Buf[1] = 4, 4 // equity in both bytes, against the configured 8

	if err := f.r.decodeDatagram(d); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}
	if got := f.in.netidMismatch.Load(); got == 0 {
		t.Error("an envelope netId mismatch was not counted")
	}
	if got := f.in.subpacketsTotal.Load(); got != 1 {
		t.Errorf("subpackets_total = %d; the envelope check must not drop the datagram", got)
	}
	if log := f.buf.String(); !strings.Contains(log, "envelope net_id mismatch") {
		t.Errorf("the envelope mismatch was not logged:\n%s", log)
	}
}

// TestDecodeDatagramReportsFramingAbandonment: framing_abandoned cannot
// distinguish nine rules, so the ERROR line has to name which one fired and
// where (Vol 1 section 4.2).
func TestDecodeDatagramReportsFramingAbandonment(t *testing.T) {
	f := newSubPacketFixture(t, &fakeRegistry{}, nil)

	// An uncompressed sub-packet declaring MessageLength 39: rule R7, the one
	// with the largest blast radius, which costs the rest of the datagram.
	bad := bcastPayload(7208, 39, 478)
	d := datagramOf(t, uncompressedSub(bad), uncompressedSub(bcastPayload(7208, 470, 478)))

	if err := f.r.decodeDatagram(d); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}
	if got := f.in.framingAbandoned.Load(); got != 1 {
		t.Errorf("framing_abandoned = %d, want 1", got)
	}
	log := f.buf.String()
	for _, want := range []string{"framing abandoned", "R7", "offset"} {
		if !strings.Contains(log, want) {
			t.Errorf("the ERROR line omits %q:\n%s", want, log)
		}
	}
}

// TestSubPacketPathAllocatesNothing is the sub-packet path's share of
// G-ALLOC-1, and the one P2-09 widens to the whole receive iteration.
//
// It covers the untraced path, which is the normal one: the reader is reset
// rather than constructed, the header is a struct field, and no value is boxed
// for a trace sink nobody attached.
func TestSubPacketPathAllocatesNothing(t *testing.T) {
	const code = 7208
	f := newSubPacketFixture(t, &fakeRegistry{
		code: code,
		entry: &decode.RegistryEntry{Code: code, Dec: &scriptedDecoder{
			code: code, bodyLen: 470, maxUnits: 2,
			onDecode: func(model.MsgHeader, []byte, []model.UpdateUnit) (int, error) {
				return 2, nil
			},
		}},
	}, nil)

	payload := bcastPayload(code, 470, 478)
	if n := f.r.decodeSubPacket(payload, false, 0, 1); n != 2 {
		t.Fatalf("setup: n=%d, want 2", n)
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		f.r.SubPacket(payload, false, 0, 1)
	}); allocs != 0 {
		t.Fatalf("the sub-packet path allocates %v times, want 0", allocs)
	}
}

// TestGapSlotSpacesCannotCollide pins the composition decode cannot check for
// itself: the detector's index space is the configured inventory followed by
// obs's dynamic table, and app is the package that can see both halves.
//
// Without the offset, configured transcode 3 and dynamic-table slot 3 would
// share one sequence space -- two unrelated counters interleaved into a storm
// of phantom gaps, under the one scope whose whole purpose is to separate them.
func TestGapSlotSpacesCannotCollide(t *testing.T) {
	if decode.ConfiguredSlots < 1 {
		t.Fatalf("ConfiguredSlots = %d", decode.ConfiguredSlots)
	}

	// obs's dynamic table is 64 entries (Vol 0 section 8.1). decode declares
	// its own copy of that number because it may not import obs, so the two
	// are pinned equal HERE.
	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	reg := obs.New(obsOptions(config.Defaults(), dests))
	seen := map[int]bool{}
	for code := 9000; code < 9000+64; code++ {
		slot, ok := reg.UnknownTranscodeSlot(code)
		if !ok {
			t.Fatalf("the dynamic table refused code %d before 64 were claimed", code)
		}
		if slot < 0 || slot >= 64 {
			t.Fatalf("code %d claimed slot %d, outside the 64 decode reserves for the "+
				"dynamic table", code, slot)
		}
		if seen[slot] {
			t.Fatalf("slot %d was claimed twice", slot)
		}
		seen[slot] = true
	}
	if len(seen) != 64 {
		t.Errorf("the dynamic table yielded %d distinct slots, want 64 -- decode reserves "+
			"exactly that many past ConfiguredSlots", len(seen))
	}

	// A 65th code overflows, which the detector routes to its aggregate
	// bucket rather than indexing out of range.
	if _, ok := reg.UnknownTranscodeSlot(9999); ok {
		t.Error("the dynamic table accepted a 65th code")
	}
}

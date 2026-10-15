package app

import (
	"errors"
	"time"

	"feedvalve/internal/decode"
	"feedvalve/internal/framing"
	"feedvalve/internal/lzo"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/wire"
)

// fanoutUnits sizes the fan-out buffer: Vol 0 section 9.1's 40, which is at
// least the MaxUnits of every catalogued code (35 for 7320/7210, 25 for 7220).
// decode's own one-sided layout gate asserts the relationship from its side.
const fanoutUnits = 40

// receiver owns every buffer the sub-packet path touches and is the only thing
// that may name them (Vol 0 section 9.1, Figure 1.2).
//
// NAMED FOR THE CONTRACT, which calls this function's owner the Receiver
// (section 5.5's handleSubPacket signature), though in the built topology it
// runs on the DECODER goroutine: the receiver goroutine reads and publishes to
// QD, and this drains it. One goroutine owns these buffers either way, which is
// the property that matters -- no synchronisation anywhere below.
type receiver struct {
	g *Graph

	// decompBuf is Vol 1 section 5.2's single decompression buffer: allocated
	// once, reused per sub-packet, 2048 bytes (section 5.3 and ASM-14).
	//
	// Its contents are valid for EXACTLY ONE SUB-PACKET. A decoder that
	// retained a slice of it would hand a shard a window onto bytes the next
	// compressed sub-packet in the same datagram overwrites microseconds
	// later -- cross-contamination between unrelated tokens that looks like a
	// decode bug and reproduces in nothing.
	decompBuf [lzo.MaxDecompressedLen]byte

	// fanoutBuf is where a decoder writes its units. Routed AFTER the recover
	// boundary returns (ADR-L10), never from inside it.
	fanoutBuf [fanoutUnits]model.UpdateUnit

	// reader and hdr are reset per message rather than constructed, which is
	// the whole of ADR-L07's allocation argument.
	reader wire.FieldReader
	hdr    model.MsgHeader

	// rt is the snapshot for the datagram being processed, loaded once at the
	// top of decodeDatagram and held for all of its sub-packets.
	rt *Runtime

	// gap is the BCSeqNo continuity detector, owned by this goroutine alone:
	// plain memory, no atomics, no locks (Vol 0 section 9.1).
	gap *decode.GapDetector

	// activeTokens is req section 12.4's per-second distinct-token set, owned
	// by this goroutine under the same rule as gap (Vol 1 section 11.3).
	activeTokens *tokenSet

	// activeSecond is the second activeTokens is currently accumulating,
	// derived from RecvMono. See decodeDatagram for why the boundary is crossed
	// here rather than by a ticker.
	activeSecond int64

	// The per-datagram fan-out state, reset at the top of decodeDatagram.
	//
	// t0 is the origin of the FAN-OUT budget and it is taken HERE, at dequeue,
	// not carried over from the receiver's read. Vol 0 section 9.5.6 splits one
	// budget into two owned by two goroutines, and section 9.2 says the
	// fan-out's is "started once per dequeued datagram": reusing the receiver's
	// t0 would charge this budget for however long the datagram sat in QD, so a
	// momentary queue would make every subsequent fan-out start already
	// expired.
	//
	// ONE budget per datagram, shared by every unit of every sub-packet it
	// produces (section 10.5 property 1). A per-unit deadline would let a
	// single datagram stall for 35 x 50 ms and satisfy its own rule the whole
	// way down.
	t0              time.Time
	recvMono        int64
	datagramBytes   int
	unitsInDatagram int

	// stallTimer is section 10.5 property 4: ONE reusable timer, allocated
	// once, owned by this goroutine. Allocating a timer per blocked send would
	// allocate on the one path that is already in trouble.
	stallTimer *time.Timer

	// fatalErr is sticky. framing.SubPacketHandler.SubPacket returns nothing --
	// framing's single job is splitting the envelope, and giving the callback a
	// return value would change the interface and its layout gate for a
	// condition that stops the process anyway -- so a stall-budget expiry is
	// recorded here, short-circuits the remaining sub-packets of this datagram,
	// and is returned by decodeDatagram once Split has unwound.
	fatalErr error

	in *instruments
}

// newReceiver allocates the sub-packet path's buffers once, before anything
// runs. A first-datagram-triggered allocation of these would land on packet one
// at market open, which is the one moment the budget has no slack.
func newReceiver(g *Graph) *receiver {
	cfg := g.deps.Cfg

	// Created already stopped and drained, so the first Reset arms a clean
	// timer. NewTimer is the only constructor that gives a *time.Timer with a
	// channel; the immediate Stop is what keeps it from firing before anything
	// has blocked.
	t := time.NewTimer(time.Hour)
	t.Stop()

	return &receiver{
		g:          g,
		in:         g.inst(),
		stallTimer: t,
		// 4 MiB at the default expectedRows, allocated HERE rather than on the
		// first datagram: a first-datagram allocation of this would land on
		// packet one at market open, the one moment with no slack.
		activeTokens: newTokenSet(cfg.Runtime.ExpectedRows),
		gap: decode.NewGapDetector(
			decode.ParseGapScope(cfg.Ordering.GapScope),
			cfg.Ordering.GapAuditPackets,
			g.gapReports(),
		),
	}
}

// gapSlot is the index the gap detector uses for per-transcode scope.
//
// It is THE SAME index the counter layer already computed for this code (Vol 1
// section 7.7), which is what keeps that scope off a map on the receive path:
// Registry.Slot for a configured code, and obs's dynamic table for one that is
// catalogued or unknown. A full dynamic table yields -1, which the detector
// routes to its aggregate bucket.
//
// Only consulted under per-transcode scope and during an audit; global scope
// ignores it, so the lookup is paid for only where it is used.
func (r *receiver) gapSlot(code model.TransCode) int {
	if s := r.rt.Reg.Slot(code); s >= 0 {
		return s
	}
	slot, ok := r.g.unknownSlot(int(code))
	if !ok {
		return -1
	}
	// Offset past the configured inventory's index space, so a configured
	// transcode and a dynamic-table code can never collide on one slot.
	return decode.ConfiguredSlots + slot
}

// decodeDatagram is the decoder goroutine's per-datagram entry point: it is
// what Deps.Decode becomes once there is something to decode.
//
// Returns an error only for a condition that must stop the process. A framing
// rejection is not one: it costs the rest of one datagram and is counted.
func (r *receiver) decodeDatagram(d *model.Datagram) error {
	// The fan-out budget's origin and the per-datagram fan-out state, reset
	// together at the top so nothing of the previous datagram survives into
	// this one's budget, counters or fatal line.
	r.t0 = time.Now()
	r.recvMono = d.RecvMono
	r.datagramBytes = int(d.N)
	r.unitsInDatagram = 0
	r.fatalErr = nil

	// The active-token set's one-second boundary (Vol 1 section 11.3).
	//
	// Crossed HERE, on this goroutine, and deliberately not by a ticker. The
	// set is plain memory with no atomics and no locks because one goroutine
	// owns it; a second goroutine rolling the generation would need
	// synchronisation on the structure the receive path touches per unit, which
	// is the cost the whole design avoids. Derived from RecvMono, which was
	// read once at the socket, so this reads NO CLOCK and costs one integer
	// compare per datagram.
	//
	// CONSEQUENCE, stated because it is a real one: with no traffic the
	// boundary never rolls and the instrument holds its last value. That is
	// correct for a _max over the stats interval -- no traffic means no active
	// tokens, and the alternative is a goroutine and a lock to publish a zero.
	if sec := d.RecvMono / 1_000_000_000; sec != r.activeSecond {
		r.activeSecond = sec
		r.in.activeTokens1sMax.Observe(uint64(r.activeTokens.Roll()))
	}

	// Step 1 of Vol 1 section 3.4, in the place this topology puts it: ONE
	// snapshot load, held for every sub-packet below (see loadRuntime).
	r.rt = r.g.loadRuntime()
	if r.rt == nil {
		// Build publishes the snapshot before any goroutine starts, so this
		// is unreachable; it is a nil check rather than a panic because the
		// decoder goroutine must not die of a startup-ordering mistake.
		return nil
	}

	// The envelope's cNetId, checked once per datagram against the same
	// expectation as each sub-packet's payload[0] (Vol 1 section 6.2).
	if n := d.Bytes(); len(n) >= 2 {
		if !wire.EnvelopeNetIDMatches(n[0:2], r.rt.Cfg.UDP.ExpectedNetID) {
			r.in.netidMismatch.Add(1)
			// Allow first: the observed bytes go in as a SLICE, which boxes
			// into the variadic and allocates -- once per datagram, on a feed
			// pointed at the wrong segment, where every datagram trips this
			// (Vol 1 section 11.5).
			if r.g.deps.Log.Allow(obs.ClassNetidMismatch) {
				r.g.deps.Log.Warnf(obs.ClassNetidMismatch,
					"envelope net_id mismatch expected=%d observed=%x -- wrong segment, or cNetId "+
						"is encoded differently than assumed", r.rt.Cfg.UDP.ExpectedNetID, n[0:2])
			}
		}
	}

	res := framing.Split(d.Bytes(), r)
	if res.Abandoned() {
		r.in.framingAbandoned.Add(1)
		// Index and offset, because framing_abandoned cannot distinguish nine
		// rules and the operator needs to know which one fired and where
		// (Vol 1 section 4.2).
		if r.g.deps.Log.Allow(obs.ClassFramingAbandoned) {
			r.g.deps.Log.Errorf(obs.ClassFramingAbandoned,
				"framing abandoned after %d of %d sub-packets: %v at offset %d",
				res.SubPackets, res.NoPackets, res.Rule, res.Offset)
		}
	}

	// The only error this function returns, and it stops the drain. A framing
	// rejection does not: it costs the rest of one datagram and is counted.
	return r.fatalErr
}

// SubPacket implements framing.SubPacketHandler. It is the sub-packet boundary
// of Vol 1 section 5.5.
//
// THE RECOVER IS IN decodeSubPacket, NOT HERE, and the split is ADR-L10. The
// routing of fanoutBuf can return ErrStallBudgetExpired, and it must sit
// OUTSIDE the recover boundary, because stall-budget expiry is the one
// condition the design says must stop the process and a recover() between it
// and the lifecycle would eat it. The process would then keep running with a
// saturated queue, silently, which is the exact outcome the requirement
// forbids.
func (r *receiver) SubPacket(payload []byte, compressed bool, idx, count int) {
	if r.fatalErr != nil {
		// An earlier sub-packet of this datagram exhausted the budget. Split
		// cannot be told to stop -- its handler returns nothing -- so the
		// remaining sub-packets are skipped here rather than decoded into
		// units that have nowhere to go.
		return
	}

	r.in.subpacketsTotal.Add(1)
	if compressed {
		r.in.subpacketsCompressed.Add(1)
	} else {
		r.in.subpacketsUncompressed.Add(1)
	}

	n := r.decodeSubPacket(payload, compressed, idx, count)

	// Outside the boundary, so the fatal path is a plain return that no
	// recover can see. P2-07's gap detector goes inside, between the header
	// parse and the registry lookup.
	r.fatalErr = r.route(r.fanoutBuf[:n], idx, count)
}

// decodeSubPacket is the protected region: decompress, resolve, dispatch.
//
// It recovers from any panic below it, so one malformed sub-packet or one
// decoder defect costs ONE MESSAGE rather than the process (HLD section 9.5
// layer 4). On recovery it returns 0, so a decoder that panicked halfway
// through a fan-out contributes NOTHING -- never a partially populated prefix
// of fanoutBuf, which downstream could not tell from a complete one.
//
// A separate function called from the loop rather than a defer inside it: Go's
// open-coded defers do not apply to a function containing a defer in a loop, so
// this placement is what keeps the cost at one deferred call per sub-packet
// (section 5.5, and the structure poc arrived at).
func (r *receiver) decodeSubPacket(payload []byte, compressed bool, idx, count int) (n int) {
	defer func() {
		if rec := recover(); rec != nil {
			n = 0
			r.in.panicsRecovered.Add(1)
			if r.g.deps.Log.Allow(obs.ClassPanicRecovered) {
				r.g.deps.Log.Errorf(obs.ClassPanicRecovered,
					"recovered from a panic decoding sub-packet %d/%d (%d bytes, compressed=%v): %v",
					idx+1, count, len(payload), compressed, rec)
			}
		}
	}()

	if compressed {
		m, err := lzo.DecompressSafe(r.decompBuf[:], payload)
		if err != nil {
			r.in.decompressFailed[decompressReason(err)].Add(1)
			// ONE SUB-PACKET, nothing else. The cursor advanced by iCompLen,
			// read from the wire before the decompressor ran, so every later
			// sub-packet in this datagram is still framed correctly (F3).
			return 0
		}
		r.in.decompressedLenMax.Observe(uint64(m))
		payload = r.decompBuf[:m]
	}

	// The netId oracle (Vol 1 section 6.2). Logged and counted, NEVER a drop:
	// the encoding assumption is itself unverified, and dropping on a check we
	// are guessing at would blind the feed on a guess. The INDEX is the whole
	// value of the line -- a mismatch on sub-packet 2 or later almost always
	// means the PREVIOUS sub-packet's length was computed wrongly.
	if len(payload) > 0 && !wire.NetIDMatches(payload[0], r.rt.Cfg.UDP.ExpectedNetID) {
		r.in.netidMismatch.Add(1)
		// Allow before the hint is chosen and the five arguments are boxed.
		// This is F5, a PER-SUB-PACKET warning on a misframed feed, so the
		// suppressed case is the overwhelmingly common one -- and a datagram
		// whose first sub-packet misframed produces one of these for every
		// sub-packet after it (Vol 1 section 11.5).
		if r.g.deps.Log.Allow(obs.ClassNetidMismatch) {
			hint := "wrong segment, or the netId prefix is encoded differently than assumed"
			if idx > 0 {
				hint = "likely frame misalignment: the previous sub-packet's length was probably " +
					"computed wrongly"
			}
			r.g.deps.Log.Warnf(obs.ClassNetidMismatch,
				"broadcast data net_id mismatch sub-packet=%d/%d expected=%d observed=%d -- %s",
				idx+1, count, r.rt.Cfg.UDP.ExpectedNetID, payload[0], hint)
		}
	}

	// Resolved ONCE for this sub-packet and used twice -- for the header read
	// here and for the body decoder below. Deciding it twice would let the two
	// halves of one message's trace disagree about whether it is being traced.
	sink := r.trace(payload, len(payload))

	switch rej := decode.ReadHeader(&r.reader, payload, len(payload), sink, &r.hdr); rej {
	case model.RejectNone:
	case model.RejectShortPayload:
		r.in.shortPayload[obs.PayloadHeader].Add(1)
		return 0
	case model.RejectErrorCode:
		// bible section 2.10: the payload is MS_ERROR_RESPONSE, 182 bytes,
		// regardless of the transaction code. No decoder runs.
		r.in.errorResponse.Add(1)
		// Allow FIRST: obs.HexDump builds a multi-line string through a
		// strings.Builder and ~80 Fprintf calls, and it was being built for
		// every error-response sub-packet whether or not the line survived the
		// bucket. At wire rate on a feed that is producing error responses --
		// exactly when the budget has no slack -- that is the single most
		// expensive thing on this path (Vol 1 section 11.5).
		if r.g.deps.Log.Allow(obs.ClassErrorResponse) {
			r.g.deps.Log.Warnf(obs.ClassErrorResponse,
				"error response: ErrorCode=%d in a %d-byte payload. Which header an error response "+
					"carries on the broadcast circuit is unstated (ASM-V3); first 64 bytes:\n%s",
				r.hdr.ErrorCode, len(payload), obs.HexDump(payload[:min(64, len(payload))]))
		}
		return 0
	default:
		r.in.shortPayload[obs.PayloadHeader].Add(1)
		return 0
	}

	// THE GAP DETECTOR, and this position is not interchangeable (Vol 1
	// section 6.5).
	//
	// AFTER resolution, because BCSeqNo exists only in a BCAST_HEADER: at a
	// MESSAGE_HEADER's offset 14 sits the top half of a nanosecond Timestamp,
	// which read as a sequence number fabricates a gap of hundreds of millions
	// on every nine-second heartbeat.
	//
	// BEFORE the registry, because an unconfigured transcode still carries a
	// perfectly good sequence number and occupies a real place in the sequence
	// space. Skipping those would make global scope under-count by exactly the
	// traffic we cannot decode, turning every unconfigured code into a
	// permanent phantom gap (section 7.6).
	r.gap.Observe(r.hdr, r.gapSlot(r.hdr.Code))

	if r.hdr.Kind == model.HeaderMessage {
		// Recognised, not decoded (Vol 1 section 9.6): no units, no registry
		// lookup, and deliberately no gap detection.
		r.in.messageHeaderSeen[headerCodeSlot(r.hdr.Code)].Add(1)
		if r.hdr.Code == codeHeartbeat {
			r.in.heartbeatSeen.Add(1)
		}
		return 0
	}

	entry := r.rt.Reg.Lookup(r.hdr.Code)
	if entry == nil {
		r.countUnresolved(r.hdr.Code)
		return 0
	}
	slot := r.rt.Reg.Slot(r.hdr.Code)
	r.in.packetsByTranscode[slot].Add(1)

	// The compressed branch's cross-check (Vol 1 section 5.7). bible section
	// 2.8: MessageLength covers the message from the header onward and
	// excludes the 8-byte prefix, so a correct compressed packet satisfies
	// decompressedLen == 8 + MessageLength. The two directions are NOT
	// symmetric.
	if compressed {
		want := int(wire.BroadcastDataPrefixLen) + int(r.hdr.MsgLen)
		if m := len(payload); m != want {
			r.in.decompressedLenMismatch[slot].Add(1)
			if r.g.deps.Log.Allow(obs.ClassSessionAnomaly) {
				r.g.deps.Log.Warnf(obs.ClassSessionAnomaly,
					"decompressed length %d against 8+MessageLength %d for code %d",
					m, want, r.hdr.Code)
			}
			if m < want {
				// The declared body is not all there; parsing it would read
				// past the decompressed data (HLD section 9.5 layer 3).
				r.in.shortPayload[obs.PayloadBody].Add(1)
				return 0
			}
			// m > want: the body is complete and there are extra trailing
			// bytes. Counted and decoded anyway -- the bible does not say
			// trailing bytes are impossible, and discarding a complete,
			// parseable message on an unverified assumption trades real data
			// for tidiness on a circuit with no retransmission.
		}
	}

	// Vol 1 section 6.7: the declared length against the registry's BodyLen,
	// and the payload against both, before the first field read.
	if bl := entry.Dec.BodyLen(); bl != 0 {
		if int(r.hdr.MsgLen) != bl {
			r.in.bodyLenMismatch[slot].Add(1)
			return 0
		}
		if len(payload) < wire.BroadcastDataPrefixLen+bl {
			r.in.shortPayload[obs.PayloadBody].Add(1)
			return 0
		}
	}

	// The body's field trace (Vol 1 section 8.6). Set UNCONDITIONALLY, including
	// to nil: a decoder holds its sink across calls, so skipping the nil case
	// would leave the last traced message's sink attached and start tracing
	// traffic nobody asked to trace -- at one boxed value per field.
	//
	// Until this line existed, SetTrace had no caller anywhere and
	// debug.traceTranscodes produced the 40-byte header and not one body field,
	// which is also what made runbook question Q6a ("are the bytes wrong, or
	// are the right bytes in the wrong field?") unanswerable by the tool it
	// names. Entry.Trace is resolved at registry build, so this costs one
	// interface store and no type assertion.
	if entry.Trace != nil {
		entry.Trace.SetTrace(sink)
	}

	units := entry.Dec.MaxUnits()
	if units > len(r.fanoutBuf) {
		units = len(r.fanoutBuf)
	}
	n, err := entry.Dec.Decode(r.hdr, payload, r.fanoutBuf[:units])
	if err != nil {
		// A decoder returns package-level sentinels only, so the error itself
		// allocates nothing. Which sentinel it was goes in the log, not in a
		// counter label: the counters that matter here already fired above.
		// Boxing it into the variadic does allocate, hence the Allow.
		if r.g.deps.Log.Allow(obs.ClassSessionAnomaly) {
			r.g.deps.Log.Errorf(obs.ClassSessionAnomaly, "decoding code %d: %v", r.hdr.Code, err)
		}
		return 0
	}
	if n > units {
		// A decoder that wrote more than it declared would have written past
		// fanoutBuf already; this is the assertion that says so rather than
		// letting the overflow travel downstream.
		if r.g.deps.Log.Allow(obs.ClassSessionAnomaly) {
			r.g.deps.Log.Errorf(obs.ClassSessionAnomaly,
				"code %d reported %d units with MaxUnits %d", r.hdr.Code, n, units)
		}
		return 0
	}
	return n
}

// countUnresolved separates ADR-011's two cases.
//
// "We can decode this but nobody told us what to do with it" is a
// configuration mistake an operator fixes in a minute; "we have never seen
// this code" is a protocol change that needs the exchange. One counter for both
// would hide the first inside the second.
//
// A DEFECT IN THE LEDGER, found here and reported rather than worked around:
// unconfigured_transcode is declared with dimTranscode (obs/ledger.go:175),
// which is indexed by the CONFIGURED inventory -- and a code in the catalogue
// but absent from the configuration has no slot in that inventory by
// definition. The branch is therefore structural today and unreachable, because
// the catalogue is empty until P2-06; it must be answered when the first
// decoder is registered.
func (r *receiver) countUnresolved(code model.TransCode) {
	if decode.CanDecode(code) {
		// We CAN decode this and nobody told us where to put it: a
		// configuration mistake an operator fixes in a minute, as against the
		// protocol change below. P2-05 could only log this, because the
		// counter's declared label was indexed by the configured inventory and
		// an unconfigured code has no slot in it; P2-06 moved the instrument
		// onto the shared dynamic table, so it counts properly now.
		c, ok := r.in.unconfiguredTranscode(int(code))
		c.Add(1)
		if r.g.deps.Log.Allow(obs.ClassSessionAnomaly) {
			r.g.deps.Log.Warnf(obs.ClassSessionAnomaly,
				"transcode %d has a decoder in this build but is not configured, so nothing says "+
					"which destination its fields belong to (ADR-011)%s", code, fullTable(ok))
		}
		return
	}
	c, ok := r.in.unknownTranscode(int(code))
	c.Add(1)
	if !ok && r.g.deps.Log.Allow(obs.ClassSessionAnomaly) {
		r.g.deps.Log.Warnf(obs.ClassSessionAnomaly,
			"transcode %d is not in this build's catalogue, and the unknown-code table is "+
				"full, so it is counted under unknown_transcode_other", code)
	}
}

// fullTable names the one case where a dynamic counter cannot be labelled: the
// 64-slot table is full, so the count landed in the shared overflow bucket.
// Said in the line rather than dropped, because a saturated table otherwise
// looks exactly like an absence of events.
func fullTable(ok bool) string {
	if ok {
		return ""
	}
	return " (the dynamic code table is full; counted under the overflow bucket)"
}

// trace returns the field-trace sink for the sub-packet in payload, or nil.
//
// NIL IS THE NORMAL ANSWER and it is an allocation decision: the value a sink
// receives is an any, so the caller boxes it, and boxing allocates for anything
// but a very small integer. Deciding here -- before the reader is handed the
// sink -- is what keeps a traced build and an untraced one the same cost on
// every field.
//
// THE CODE COMES FROM THE PAYLOAD, NOT FROM r.hdr, and that is a fix rather
// than a preference. This used to read r.hdr.Code while being evaluated as an
// argument to ReadHeader -- which zeroes r.hdr on entry. So the code deciding
// whether to trace was the PREVIOUS sub-packet's, and configuring
// debug.traceTranscodes for one code traced whatever happened to follow it.
// Silent in both directions: the operator diagnosing 7208 sees fields from
// another message and believes them.
//
// Any() first, so the normal path is one length check. Resolve runs a second
// time (ReadHeader calls it too) only when an operator has actually asked for a
// trace, and skipping Enabled's scan as well makes the untraced path cheaper
// than it was before.
func (r *receiver) trace(payload []byte, payloadLen int) wire.TraceSink {
	if r.g.deps.Debug == nil || !r.g.deps.Debug.Any() {
		return nil
	}
	if _, code, _ := decode.Resolve(payload, payloadLen); !r.g.deps.Debug.Enabled(int(code)) {
		return nil
	}
	return r.g.deps.Debug
}

// decompressReason maps an lzo sentinel onto its decompress_failed{reason}
// slot.
//
// The switch lives here rather than in lzo because lzo is stdlib-only -- it may
// not import obs, which is what keeps the binary free of cgo and liblzo2 -- and
// obs may not import lzo. app is the package allowed to see both.
func decompressReason(err error) obs.DecompressReason {
	switch {
	case errors.Is(err, lzo.ErrInputOverrun):
		return obs.ReasonInputOverrun
	case errors.Is(err, lzo.ErrOutputOverrun):
		return obs.ReasonOutputOverrun
	case errors.Is(err, lzo.ErrLookbehindOverrun):
		return obs.ReasonLookbehindOverrun
	case errors.Is(err, lzo.ErrInputNotConsumed):
		return obs.ReasonInputNotConsumed
	}
	// Unreachable: DecompressSafe returns one of the four or nil. Counted as a
	// truncated stream rather than dropped, because a failure with no slot is
	// worse than a failure in the wrong slot.
	return obs.ReasonInputOverrun
}

// headerCodeSlot maps the two MESSAGE_HEADER codes onto their
// message_header_seen{code} slots. Vol 0 section 8.2 pins the label to exactly
// these two values.
func headerCodeSlot(code model.TransCode) obs.MessageHeaderCode {
	if code == codeAssetRateInt {
		return obs.HeaderAssetRate
	}
	return obs.HeaderHeartbeat
}

// The two codes that carry a MESSAGE_HEADER (bible section 4.12, section 4.15).
// Declared here as well as in decode because app needs them for the counters
// and may not reach decode's unexported constants.
const (
	codeHeartbeat    model.TransCode = 6541
	codeAssetRateInt model.TransCode = 6503
)

package app

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/wire"
)

// Gate G-ALLOC-1: zero allocations across the WHOLE receive iteration.
//
// Vol 1 section 11.1 makes the rule absolute rather than a preference, and the
// mechanism is specific: the Go collector charges mark work to the goroutine
// that allocates, so a receive goroutine that allocates periodically performs
// mark-assist in the middle of the loop against a live heap Vol 0 section 3.7
// puts at ~189 MB. The stall's duration is a function of the HEAP, not of
// anything in the packet -- unbounded in principle and impossible to attribute
// when it happens. NFR4 protects exactly that path.
//
// Eleven packages already assert their own share of this gate -- framing, lzo,
// wire, the decoders, the gap detector, the fan-out. What none of them covers
// is the whole iteration on one goroutine, and HLD section 10.4 records the
// correction that scoping it to decode-and-fan-out alone left the gap detector
// and the token set uncovered. This file is that scope.
//
// The two halves of section 11.4, which cannot honestly be one test:
//
//	G-alloc-pipeline  a pre-built datagram, no socket. Every CI run.
//	G-alloc-socket    the same through a real *net.UDPConn, to cover
//	                  ReadFromUDPAddrPort (ASM-13). Here for a smoke signal
//	                  and on the LPAR for the answer.
//
// WHAT THE GATE IS HUNTING, from section 11.4's table: a fmt.Sprintf field name
// evaluated with tracing off; a newFieldReader returning an escaping pointer; a
// ReadFrom allocating one *net.UDPAddr per datagram; the active-token set
// rebuilt rather than cleared; an obs.Reg.Counter(id) lookup on
// the path; a fmt.Errorf in a decoder; and a log call whose arguments are built
// before the rate-limit check.

const allocGateCode = 7208

// allocGateFixture is a receiver wired to the REAL registry.
//
// Deliberately not newSubPacketFixture's fakeRegistry+scriptedDecoder: a
// scripted decoder that returns a unit count without reading the payload would
// let this gate pass while the real decoder allocated on every field. The real
// 7208 decoder is reachable because decode.BuildRegistry is exported, and
// pointing it at the real MD_TOKEN_QUOTE contribution is what makes the measured
// region the one production runs.
type allocGateFixture struct {
	r   *receiver
	g   *Graph
	buf *syncBuf
	in  *instruments
}

func newAllocGateFixture(t *testing.T, tune func(*config.Config)) *allocGateFixture {
	t.Helper()

	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	quote := dests.ByName("MD_TOKEN_QUOTE")
	if quote == nil {
		t.Fatal("MD_TOKEN_QUOTE is not in the default mapping")
	}
	cols := make([]string, 0, len(quote.Columns))
	for i := range quote.Columns {
		cols = append(cols, quote.Columns[i].Name())
	}

	cfg := config.Defaults()
	cfg.Transcodes = append(cfg.Transcodes, config.TranscodeConfig{
		Code:  allocGateCode,
		Class: "rate-limited",
		Contributions: []config.ContributionConfig{{
			Destination: "MD_TOKEN_QUOTE",
			Columns:     cols,
		}},
	})
	if tune != nil {
		tune(cfg)
	}

	buf := &syncBuf{}
	obsReg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(obsReg, cfg, dests)

	g := &Graph{
		deps: Deps{
			Cfg: cfg, Reg: obsReg, Dests: dests, Stderr: buf, Inst: inst,
			Log:   obs.NewAppLogger(buf, obs.ParseLevel("info"), cfg.Stats.EventRatePerMin, obsReg),
			Debug: obs.NewDebugLogger(buf, nil, false),
		},
		fatal: make(chan fatalEvent, 1),
	}

	// The same construction order Build uses: the counter sink exists before
	// the registry because BuildRegistry needs it to construct the decoders,
	// and the registry is attached afterwards.
	cnt := newDecodeCounters(&g.deps.Inst)
	reg, err := decode.BuildRegistry(transcodeSpecs(cfg), dests, cnt)
	if err != nil {
		t.Fatalf("BuildRegistry: %v", err)
	}
	cnt.setRegistry(reg)

	// FROZEN, as production is from startup step 9 onward. A gate that measured
	// an unfrozen registry would miss exactly the defect Vol 0 section 8.4
	// forbids -- an instrument resolved per event rather than once.
	obsReg.Freeze()

	g.setRuntime(&Runtime{Cfg: cfg, Reg: reg, Dests: dests})
	g.allocShardQueues(cfg.Runtime.Shards, cfg.Runtime.ShardQueueCapacity)

	// Q1 DRAINED, unlike newSubPacketFixture's deliberately unconsumed queues:
	// this runs the fan-out thousands of times, so an undrained 4,096-slot
	// channel would fill and turn the gate into a stall-budget test.
	g.startDiscardConsumers()
	t.Cleanup(g.stopShardConsumers)

	rcv := newReceiver(g)
	rcv.rt = g.loadRuntime()
	return &allocGateFixture{r: rcv, g: g, buf: buf, in: &g.deps.Inst}
}

// startDiscardConsumers drains Q1 with one goroutine per shard that receives
// and discards -- Phase 2's throwaway consumer, now test infrastructure
// (phase2-tasks.md: "the throwaway consumer is test infrastructure").
//
// Not the real shards, deliberately: testing.AllocsPerRun counts every
// goroutine's allocations, so a live shard's index inserts and arena growth on
// fresh tokens would be charged to the receive path this gate measures. Each
// closes shardDrainDone[k] exactly as a shard does, so stopShardConsumers
// joins either kind.
func (g *Graph) startDiscardConsumers() {
	g.shardConsumersStarted = true
	g.mark(&g.acquired, "shard-consumers")
	for k := range g.shardCh {
		ch, done := g.shardCh[k], g.shardDrainDone[k]
		go func() {
			defer close(done)
			for range ch {
			}
		}()
	}
}

// Absolute offsets inside a 7208 payload, transcribed from bible Part V rather
// than computed from the decoder's own constants -- the G-FIX-1 rule, and the
// reason the 213-byte stride did not survive a second time. 8 prefix + 40
// header = 48 is NoOfRecords; record 0 starts at 50 and the stride is 214.
const (
	mbpAbsNoOfRecords = 48
	mbpAbsRecord0     = 50
	mbpAbsStride      = 214
	mbpAbsPayloadLen  = 478 // 8 prefix + 470 declared
	mbpRecTokenOff    = 0
	mbpRecBookTypeOff = 4
)

// mbp7208Payload builds a real 7208 carrying noOfRecords records.
//
// Distinct tokens per record, so a wrong stride cannot produce two units that
// look right, and a book type of 1 so nothing downstream treats the record as
// a shape it should skip.
func mbp7208Payload(noOfRecords int) []byte {
	p := bcastPayload(allocGateCode, 470, mbpAbsPayloadLen)
	binary.BigEndian.PutUint16(p[mbpAbsNoOfRecords:mbpAbsNoOfRecords+2], uint16(noOfRecords))
	for rec := range noOfRecords {
		base := mbpAbsRecord0 + rec*mbpAbsStride
		binary.BigEndian.PutUint32(p[base+mbpRecTokenOff:], uint32(90001+rec))
		binary.BigEndian.PutUint16(p[base+mbpRecBookTypeOff:], 1)
	}
	return p
}

// TestG_ALLOC_1_TheWholeReceiveIterationAllocatesNothing is gate G-ALLOC-1's
// pipeline half (G-alloc-pipeline).
//
// SETUP: the real 7208 decoder behind the real registry, a frozen obs registry,
// Q1 drained by the discard consumers, tracing OFF -- which is the interesting
// case, because with tracing on allocation is expected and fine.
//
// STIMULUS: a pre-built datagram through receiver.decodeDatagram, 1,000 times,
// with RecvMono advanced one full second per call so the active-token set's
// generation boundary is crossed INSIDE the measured region rather than beside
// it.
//
// ASSERTION: testing.AllocsPerRun reports 0.
//
// WOULD HAVE CAUGHT: every row of section 11.4's regression table. The two it
// catches today, which is why this test is written before the fixes rather than
// after them, are the active-token set (which does not exist, so nothing
// samples req section 12.4) and the log call sites that build their arguments
// before the rate-limit check.
func TestG_ALLOC_1_TheWholeReceiveIterationAllocatesNothing(t *testing.T) {
	// The clean path on its own first. One uncompressed real 7208, no failure
	// branch, NO LOG LINE AT ALL -- so a non-zero result here is a defect in
	// the steady-state path itself and not in a rejection path, which is a
	// distinction worth having before reading the second sub-test.
	t.Run("steady state", func(t *testing.T) {
		f := newAllocGateFixture(t, nil)
		d := datagramOf(t, uncompressedSub(mbp7208Payload(2)))

		// Non-vacuity: the message must actually DECODE. A rejected message
		// fans out zero units, and a gate measuring a rejection would report
		// zero allocations for all the wrong reasons.
		d.RecvMono = 1
		if err := f.r.decodeDatagram(d); err != nil {
			t.Fatalf("decodeDatagram: %v", err)
		}
		if got := f.in.unitsEmitted.Load(); got != 2 {
			t.Fatalf("units_emitted = %d after one 7208 with NoOfRecords=2, want 2 -- "+
				"the fixture is not decoding, so this gate would measure nothing:\n%s",
				got, f.buf.String())
		}
		if log := f.buf.String(); log != "" {
			t.Fatalf("the steady-state path logged, so this sub-test is no longer "+
				"measuring the clean path:\n%s", log)
		}

		mono := int64(0)
		if n := testing.AllocsPerRun(1000, func() {
			// One full second per iteration, so every call crosses the
			// active-token set's boundary: the generation roll is measured,
			// not merely reachable.
			mono += 1_000_000_000
			d.RecvMono = mono
			_ = f.r.decodeDatagram(d)
		}); n != 0 {
			t.Errorf("the receive iteration allocated %v times per datagram, want 0.\n"+
				"Gate G-ALLOC-1 (Vol 1 section 11.4). The collector charges mark work to "+
				"the goroutine that allocates, so this is a stall on the receive path whose "+
				"duration is a function of the heap rather than of the packet.\n"+
				"Run with -gcflags=-m or a memory profile to attribute it; section 11.4's "+
				"table lists the ways this path has already been broken.", n)
		}
	})

	// Then the decompression path and the SUPPRESSED log path together, which
	// is section 11.5's case: the common case is suppression, so the arguments
	// must not be formatted to be thrown away.
	t.Run("decompress and the suppressed log path", func(t *testing.T) {
		f := newAllocGateFixture(t, func(c *config.Config) {
			// One event per class per minute. The warm-up call spends the
			// token; every measured call is suppressed, which is the state a
			// misframed feed spends essentially all of its time in -- and the
			// state section 11.5 says must cost nothing.
			c.Stats.EventRatePerMin = 1
		})

		// A frozen compressed sub-packet, plus the real 7208 uncompressed. Both
		// in ONE datagram, which section 11.4 requires -- and they fit, which
		// is not automatic: packed data is capped at 512 bytes (cPackData[512]),
		// the 7208 takes 480 of them with its length prefix, so the compressed
		// stream has to be a short one.
		//
		// The frozen stream decompresses to 512 zero bytes, so byte 0 is not
		// the segment netId and the netId oracle fires once per iteration. That
		// is deliberate and it is this sub-test's second half: F5 is exactly
		// the per-sub-packet WARN the token bucket exists to bound, and the
		// bytes cannot be made to carry netId 8 anyway -- the fixtures are
		// keyed by the SHA-256 of their input and there is no compressor in
		// this module.
		stream := frozenLZOStream(t, lzoInputZeros512)
		d := datagramOf(t,
			compressedSub(stream),
			uncompressedSub(mbp7208Payload(2)),
		)

		d.RecvMono = 1
		if err := f.r.decodeDatagram(d); err != nil {
			t.Fatalf("decodeDatagram: %v", err)
		}
		// Non-vacuity, three ways: the compressed sub-packet really was
		// decompressed, the uncompressed one really was decoded, and the WARN
		// this sub-test needs really did fire.
		if got := f.in.subpacketsCompressed.Load(); got != 1 {
			t.Fatalf("subpackets_compressed = %d, want 1 -- the compressed sub-packet "+
				"did not reach the decompressor:\n%s", got, f.buf.String())
		}
		var failed uint64
		for _, c := range f.in.decompressFailed {
			failed += c.Load()
		}
		if failed != 0 {
			t.Fatalf("decompress_failed = %d; the frozen stream must decompress cleanly "+
				"or this sub-test is measuring a rejection:\n%s", failed, f.buf.String())
		}
		if got := f.in.unitsEmitted.Load(); got != 2 {
			t.Fatalf("units_emitted = %d, want 2:\n%s", got, f.buf.String())
		}
		if got := f.in.netidMismatch.Load(); got != 1 {
			t.Fatalf("netid_mismatch = %d, want 1 -- without it this sub-test is not "+
				"measuring a log path at all", got)
		}

		// The warm-up call has spent the class's only token, so everything
		// measured below is suppressed. Asserted rather than assumed: if the
		// line were still being EMITTED, the formatting and the write would
		// allocate legitimately and the gate would be measuring the wrong
		// thing.
		before := len(f.buf.String())

		mono := int64(0)
		n := testing.AllocsPerRun(1000, func() {
			mono += 1_000_000_000
			d.RecvMono = mono
			_ = f.r.decodeDatagram(d)
		})

		if after := len(f.buf.String()); after != before {
			t.Fatalf("the log grew by %d bytes during the measured run, so the lines were "+
				"EMITTED rather than suppressed and this is not section 11.5's case",
				after-before)
		}
		if n != 0 {
			t.Errorf("decompress plus the suppressed log path allocated %v times per "+
				"datagram, want 0.\n"+
				"Vol 1 section 11.5 is a MUST: the bucket is tested BEFORE the log "+
				"arguments are constructed, not inside the logger. A suppressed line that "+
				"still allocates gives back exactly the cost the bucket was installed to "+
				"remove -- under precisely the condition where it matters most.", n)
		}
	})
}

// TestG_ALLOC_1_TheSocketPathAllocatesNothing is gate G-ALLOC-1's socket half
// (G-alloc-socket), and it exists to answer ASM-13: "ReadFromUDPAddrPort is
// allocation-free on the AIX build".
//
// SETUP: a real *net.UDPConn on loopback, a sender aimed at it, and the same
// receiver and real 7208 the pipeline half uses.
//
// STIMULUS: datagrams pre-queued into the socket buffer, then read and decoded
// one per run -- read, envelope split, decompress, resolve, gap-detect, decode,
// fan out, token sample, Q1 route.
//
// ASSERTION: testing.AllocsPerRun reports 0.
//
// WOULD HAVE CAUGHT: a revert to ReadFrom, which allocates one *net.UDPAddr per
// datagram -- 2,400/s, tolerable in itself, but it breaks this gate and the
// gate would then have to be scoped to exclude the socket (section 11.4).
//
// Driven SYNCHRONOUSLY from the test goroutine rather than through Graph.Run,
// which is deliberate: AllocsPerRun reads process-wide malloc counters, so
// measuring while the lifecycle's own goroutines ran would attribute their
// allocations to this gate and make it flaky in both directions. The socket is
// what this half adds over the pipeline half, and it is fully exercised here.
//
// NOT skipped under -short. scripts/build-aix.sh ships every internal/* test
// binary and scripts/lpar-smoke.sh runs them with -test.short, so staying
// unskipped is exactly what carries the AIX half of ASM-13 to the LPAR without
// a script change.
func TestG_ALLOC_1_TheSocketPathAllocatesNothing(t *testing.T) {
	// Smaller than the pipeline half's 1,000. Every datagram has to be resident
	// in the socket buffer before measurement starts -- see below -- and 200 x
	// 482 bytes is ~96 KB against the 640 KB udp_recvspace, which leaves room
	// rather than inviting a loopback drop into the one place a drop would look
	// like a gate failure.
	const runs = 200

	f := newAllocGateFixture(t, nil)

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("binding a loopback listener: %v", err)
	}
	defer conn.Close()
	if err := conn.SetReadBuffer(640 * 1024); err != nil {
		t.Logf("SetReadBuffer refused (%v); continuing at the default size", err)
	}

	sender, err := net.Dial("udp", conn.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialling the listener: %v", err)
	}
	defer sender.Close()

	// One real envelope: a 7208 with NoOfRecords == 2, the same message the
	// pipeline half decodes.
	envelope := datagramOf(t, uncompressedSub(mbp7208Payload(2))).Bytes()

	// Pre-queue every datagram, so no run waits on the sender and the
	// measurement contains reads and nothing else. A deadline rather than a
	// blocking read, so a dropped datagram fails this test instead of hanging
	// the suite -- and it is set ONCE, outside the measured region.
	for range runs + 1 {
		if _, err := sender.Write(envelope); err != nil {
			t.Fatalf("pre-queueing a datagram: %v", err)
		}
	}
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	buf := make([]byte, readBufferLen)
	var d model.Datagram
	var readErr error
	mono := int64(0)

	n := testing.AllocsPerRun(runs, func() {
		// THE LINE THIS GATE EXISTS FOR (lifecycle.go's read loop).
		got, _, err := conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			readErr = err
			return
		}
		if got > wire.MaxDatagramLen {
			got = wire.MaxDatagramLen
		}
		if got > len(d.Buf) {
			got = len(d.Buf)
		}
		copy(d.Buf[:got], buf[:got])
		d.N = int32(got)

		mono += 1_000_000_000
		d.RecvMono = mono
		_ = f.r.decodeDatagram(&d)
	})

	if readErr != nil {
		t.Fatalf("reading a pre-queued datagram: %v -- a loopback drop, not an allocation "+
			"result; re-run, and lower runs if it persists", readErr)
	}
	// Non-vacuity: the datagrams really did decode, two units each.
	if got := f.in.unitsEmitted.Load(); got < uint64(runs) {
		t.Fatalf("units_emitted = %d after %d datagrams of 2 units each; the socket path is "+
			"not decoding, so this gate would measure nothing:\n%s",
			got, runs, f.buf.String())
	}

	if n != 0 {
		t.Errorf("the socket receive iteration allocated %v times per datagram, want 0.\n"+
			"Gate G-ALLOC-1 / G-alloc-socket (Vol 1 section 11.4, ASM-13). The usual cause "+
			"is ReadFrom in place of ReadFromUDPAddrPort, which allocates one *net.UDPAddr "+
			"per datagram.", n)
	}
}

// TestErrorResponseLogAllocatesNothingWhenSuppressed pins the single most
// expensive log site on the receive path.
//
// The error-response WARN carries obs.HexDump(payload[:64]) -- a
// strings.Builder plus ~80 Fprintf calls -- and before P2-09 it was built for
// every error-response sub-packet whether or not the line survived the bucket.
// A feed producing error responses produces them at wire rate, which is exactly
// when the budget has no slack, so this is Vol 1 section 11.5's argument in its
// strongest form.
//
// Separate from the gate above because the gate's fixture decodes a valid
// message and never reaches this branch: an error response is rejected before
// any decoder runs (bible section 2.10). A site the gate cannot reach needs its
// own assertion, which is the whole reason the source scan in
// scan_allow_test.go exists alongside the gate.
func TestErrorResponseLogAllocatesNothingWhenSuppressed(t *testing.T) {
	f := newAllocGateFixture(t, func(c *config.Config) {
		c.Stats.EventRatePerMin = 1 // the warm-up call spends it
	})

	// ErrorCode at payload offset 20, non-zero, which ReadHeader checks before
	// it trusts the transaction code.
	payload := mbp7208Payload(2)
	binary.BigEndian.PutUint16(payload[20:22], 7)

	if n := f.r.decodeSubPacket(payload, false, 0, 1); n != 0 {
		t.Fatalf("decodeSubPacket returned %d units for an error response, want 0", n)
	}
	if got := f.in.errorResponse.Load(); got != 1 {
		t.Fatalf("error_response = %d, want 1 -- this test is not reaching the branch it "+
			"is about", got)
	}
	if !strings.Contains(f.buf.String(), "error response") {
		t.Fatalf("the warm-up call did not emit, so the bucket is not drained and the "+
			"measurement below would be of the EMITTED path:\n%s", f.buf.String())
	}

	before := len(f.buf.String())
	n := testing.AllocsPerRun(1000, func() {
		_ = f.r.decodeSubPacket(payload, false, 0, 1)
	})
	if after := len(f.buf.String()); after != before {
		t.Fatalf("the log grew by %d bytes during the measured run, so the lines were "+
			"emitted rather than suppressed", after-before)
	}
	if n != 0 {
		t.Errorf("the suppressed error-response path allocated %v times, want 0 -- the "+
			"hex dump is being built for a line that is then thrown away "+
			"(Vol 1 section 11.5)", n)
	}
}

// The field-trace plumbing: the fixture the differential harness drives, and
// the two assertions that pin what P2-10 had to fix before it could diff
// anything at all.
//
// Both fixes are in internal/app/subpacket.go and both were silent:
//
//  1. SetTrace had NO CALLER anywhere in the repo and was not on the Decoder
//     interface, so debug.traceTranscodes produced the 40-byte header and not
//     one body field. Runbook question Q6a ("are the bytes wrong, or are the
//     right bytes in the wrong field?", Vol 4 section 6.3) names the field
//     trace as the tool that answers it, and the trace could not answer it.
//
//  2. receiver.trace read r.hdr.Code while being evaluated as an ARGUMENT to
//     ReadHeader, which zeroes r.hdr on entry -- so the code that decided
//     whether to trace was the previous sub-packet's.
//
// Neither fails loudly. The second is the worse of the two: an operator
// diagnosing a layout bug on one transcode is shown fields belonging to a
// different message, at the right offsets, with plausible values.
package app

import (
	"strings"
	"testing"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// diffFixture is a receiver wired to the REAL registry over the REAL
// destination mapping, with the field trace ON and captured.
//
// Deliberately not newSubPacketFixture's fakeRegistry+scriptedDecoder: the
// whole point here is the bytes the real decoders read, so a scripted decoder
// would make every assertion in this file vacuous. It is also not
// newAllocGateFixture, which is pinned to 7208 alone with tracing off -- that
// gate's job is to measure the untraced path, and widening it would change what
// it measures.
type diffFixture struct {
	r     *receiver
	g     *Graph
	trace *syncBuf // the DebugLogger's own stream, nothing else in it
	log   *syncBuf
}

// newDiffFixture configures codes, traces traced, and returns the receiver.
//
// The two code lists are separate on purpose. V-DBG1
// (internal/config/validate.go) requires every entry in debug.traceTranscodes
// to be a CONFIGURED transcode -- a typo otherwise traces nothing and reads as
// the bug having gone away -- so a fixture that conflated them could not
// express the case the gating regression below needs: two codes configured,
// one traced.
func newDiffFixture(t *testing.T, configured []int, traced []int) *diffFixture {
	t.Helper()

	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}

	cfg := config.Defaults()
	for _, code := range configured {
		tc := config.TranscodeConfig{Code: code, Class: "rate-limited"}
		// Contributions come from the destination mapping's own view of which
		// codes feed it, rather than from a table here that could disagree
		// with it.
		for _, d := range dests.All() {
			if !d.Contributes(model.TransCode(code)) {
				continue
			}
			cols := make([]string, 0, len(d.Columns))
			for i := range d.Columns {
				cols = append(cols, d.Columns[i].Name())
			}
			tc.Contributions = append(tc.Contributions,
				config.ContributionConfig{Destination: d.Name, Columns: cols})
		}
		cfg.Transcodes = append(cfg.Transcodes, tc)
	}
	cfg.Debug.TraceTranscodes = append(cfg.Debug.TraceTranscodes, traced...)

	logBuf := &syncBuf{}
	traceBuf := &syncBuf{}
	obsReg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(obsReg, cfg, dests)

	g := &Graph{
		deps: Deps{
			Cfg: cfg, Reg: obsReg, Dests: dests, Stderr: logBuf, Inst: inst,
			Log: obs.NewAppLogger(logBuf, obs.ParseLevel("info"), cfg.Stats.EventRatePerMin, obsReg),
			// Its OWN writer, unlike production's shared stderr: the harness
			// parses this stream, and interleaved app lines would make a
			// parse failure look like a missing field.
			Debug: obs.NewDebugLogger(traceBuf, cfg.Debug.TraceTranscodes, false),
		},
		fatal: make(chan fatalEvent, 1),
	}

	cnt := newDecodeCounters(&g.deps.Inst)
	reg, err := decode.BuildRegistry(transcodeSpecs(cfg), dests, cnt)
	if err != nil {
		t.Fatalf("BuildRegistry: %v", err)
	}
	cnt.setRegistry(reg)

	// Frozen, as production is from startup step 9 onward.
	obsReg.Freeze()

	g.setRuntime(&Runtime{Cfg: cfg, Reg: reg, Dests: dests})
	g.allocShardQueues(cfg.Runtime.Shards, cfg.Runtime.ShardQueueCapacity)
	g.startDiscardConsumers()
	t.Cleanup(g.stopShardConsumers)

	rcv := newReceiver(g)
	rcv.rt = g.loadRuntime()
	return &diffFixture{r: rcv, g: g, trace: traceBuf, log: logBuf}
}

// traceLines returns the field-trace lines emitted so far, timestamp stripped.
//
// Only lines carrying a field are kept. That predicate is the same one the poc
// side uses (see pocFieldLines), which is what lets one parser read both
// streams.
func (f *diffFixture) traceLines(t *testing.T) []string {
	t.Helper()
	return fieldLinesFrom(f.trace.String())
}

// TestBodyFieldsAreTraced is the first half of section 1: a configured,
// traced transcode must produce RECORD fields, not just a header.
//
// Setup: 7208 configured and traced. Stimulus: one datagram carrying a real
// 7208 with two records. Assertion: the trace contains a record-0 field, a
// record-1 field, and the pad at record offset 17.
//
// Would have caught: SetTrace having no caller. Before this card the trace
// stopped at MessageLength (payload 46-48) and an operator could not see a
// single record field -- which is most of what a field trace is for.
func TestBodyFieldsAreTraced(t *testing.T) {
	f := newDiffFixture(t, []int{7208}, []int{7208})

	d := datagramOf(t, uncompressedSub(mbp7208Payload(2)))
	f.r.decodeDatagram(d)

	lines := f.traceLines(t)
	if len(lines) == 0 {
		t.Fatal("no field trace at all for a configured, traced transcode")
	}

	joined := strings.Join(lines, "\n")
	for _, want := range []string{
		"field=Token", // record 0's first field
		"field=pad",   // the pad at record offset 17, the pack(2) proof
		"field=LowPrice",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the trace has no %q line.\n"+
				"SetTrace had no caller in the repo before P2-10, so the body traced "+
				"nothing and debug.traceTranscodes produced a header and stopped.\n"+
				"got %d lines:\n%s", want, len(lines), joined)
		}
	}

	// Record 1 must appear too, and at the 214-byte stride: record 0's Token is
	// at payload 50, record 1's at 50+214 = 264. Written as literals, from the
	// bible, not from the decoder's constants.
	if !strings.Contains(joined, "offset=50-54") {
		t.Error("no field at payload offset 50-54; record 0's Token belongs there")
	}
	if !strings.Contains(joined, "offset=264-268") {
		t.Error("no field at payload offset 264-268; record 1's Token belongs there, " +
			"which is record 0 plus the 214-byte stride. A 213-byte stride would put " +
			"it at 263-267")
	}
}

// TestTraceIsGatedOnThisSubPacketsCode is the second half: the code that
// decides whether to trace must be the one in the payload being read.
//
// Setup: two codes configured, only 7208 traced. Stimulus: one datagram with a
// 7208 sub-packet followed by a 7305 sub-packet. Assertion: every traced field
// lies inside the 7208 sub-packet, and the 7305 contributes nothing.
//
// Would have caught the exact defect: receiver.trace read r.hdr.Code, which
// ReadHeader zeroes on entry, so the decision used the PREVIOUS sub-packet's
// code. Before the fix this test sees 7305's fields -- the message after the
// traced one -- and none of 7208's.
func TestTraceIsGatedOnThisSubPacketsCode(t *testing.T) {
	f := newDiffFixture(t, []int{7208, 7305}, []int{7208})

	d := datagramOf(t,
		uncompressedSub(mbp7208Payload(1)),
		uncompressedSub(sec7305DiffPayload(t, 7305)),
	)
	f.r.decodeDatagram(d)

	lines := f.traceLines(t)
	if len(lines) == 0 {
		t.Fatal("no field trace for the traced code; it was the first sub-packet in the datagram")
	}

	joined := strings.Join(lines, "\n")

	// 7305's body carries IssuedCapital, a DOUBLE no 7208 record has. Its
	// presence means the trace followed the wrong sub-packet.
	if strings.Contains(joined, "field=IssuedCapital") {
		t.Errorf("the trace carries 7305's IssuedCapital while only 7208 is traced.\n"+
			"receiver.trace is deciding on the wrong sub-packet's transaction code.\n%s", joined)
	}
	// And 7208's own body must be there, which is the other half of the same
	// defect: gating on a stale zero code traces nothing on the first
	// sub-packet.
	if !strings.Contains(joined, "field=Token") {
		t.Errorf("the traced code 7208 produced no Token field.\n%s", joined)
	}
}

// TestNothingIsTracedWhenNoCodeIsConfigured is the normal production state,
// asserted so a future change to the gating cannot turn tracing on by default.
//
// A trace is per-field and verbose; leaving it on at wire rate is both a
// performance and a disk problem, and Vol 4 section 6.2 lists a per-field trace
// among the things never logged unless debug.traceTranscodes names the code.
func TestNothingIsTracedWhenNoCodeIsConfigured(t *testing.T) {
	f := newDiffFixture(t, []int{7208}, nil)

	f.r.decodeDatagram(datagramOf(t, uncompressedSub(mbp7208Payload(2))))

	if lines := f.traceLines(t); len(lines) != 0 {
		t.Errorf("traced %d fields with debug.traceTranscodes empty:\n%s",
			len(lines), strings.Join(lines, "\n"))
	}
}

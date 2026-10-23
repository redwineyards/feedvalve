package app

import (
	"strings"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/health"
	"feedvalve/internal/model"
)

// statusRegistry resolves the five market-status codes to one scripted decoder
// that emits the unit vctMsgs would: general, with destination 2's row key set
// to the market type the test plants in the payload's first body byte.
type statusRegistry struct{ entries map[model.TransCode]*decode.RegistryEntry }

func (s *statusRegistry) Lookup(c model.TransCode) *decode.RegistryEntry { return s.entries[c] }
func (s *statusRegistry) Slot(c model.TransCode) int {
	for i, code := range s.Codes() {
		if code == c {
			return i
		}
	}
	return -1
}
func (s *statusRegistry) Codes() []model.TransCode {
	return []model.TransCode{6511, 6521, 6522, 6531, 6571}
}
func (s *statusRegistry) Len() int { return 5 }

// newSessionFeedFixture is a receiver whose registry decodes the five status
// codes. The market type of a datagram is the byte at payload offset 48.
func newSessionFeedFixture(t *testing.T) *subPacketFixture {
	t.Helper()
	reg := &statusRegistry{entries: map[model.TransCode]*decode.RegistryEntry{}}
	for _, c := range reg.Codes() {
		reg.entries[c] = &decode.RegistryEntry{Code: c, Dec: &scriptedDecoder{
			code: c, bodyLen: fanoutBodyLen, maxUnits: 1,
			onDecode: func(h model.MsgHeader, p []byte, out []model.UpdateUnit) (int, error) {
				out[0] = model.UpdateUnit{Code: h.Code, LogTime: 100, Flags: model.FlagGeneral}
				out[0].Contribs[2] = model.Contribution{Row: model.SingleKey(uint32(p[48])), Mask: 1}
				return 1, nil
			},
		}}
	}
	f := newSubPacketFixture(t, reg, func(c *config.Config) {
		c.Runtime.Shards = 1
		c.Runtime.GeneralShard = 0
	})
	f.r.g.sessionCh = make(chan model.SessionEvent, sessionChCap)
	return f
}

func statusDatagram(t *testing.T, code uint16, marketType byte) *model.Datagram {
	p := bcastPayload(code, fanoutBodyLen, fanoutPayloadLen)
	p[48] = marketType
	return datagramOf(t, uncompressedSub(p))
}

// TestG_SESS_4_AFullSessionChannelNeverBlocksAndNeverCostsData is gate G-SESS-4.
//
// SETUP: channel H pre-filled to its capacity of 64, with no consumer.
// STIMULUS: deliver another market-status message through the real
// decodeDatagram, under a deadline.
// ASSERTION: the call returns (the send did not block), session_events_dropped
// is 1, the channel still holds exactly 64, and the UpdateUnit REACHED its
// shard queue, so the dropped health event cost no data.
// WOULD HAVE CAUGHT: a blocking send on the receive path, which would put a
// logging concern inside the 50 ms stall budget; and a producer placed before
// the fan-out whose failure path returned early and dropped the unit.
// NON-VACUITY: with room in the channel the same datagram is queued, not
// dropped.
func TestG_SESS_4_AFullSessionChannelNeverBlocksAndNeverCostsData(t *testing.T) {
	f := newSessionFeedFixture(t)
	g := f.r.g

	for range sessionChCap {
		g.sessionCh <- model.SessionEvent{MarketType: 1, Code: 6511}
	}

	done := make(chan error, 1)
	go func() { done <- f.r.decodeDatagram(statusDatagram(t, 6521, 1)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("decodeDatagram: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("decodeDatagram blocked on a full session channel")
	}

	if got := f.in.sessionEventsDropped.Load(); got != 1 {
		t.Errorf("session_events_dropped = %d, want 1", got)
	}
	if n := len(g.sessionCh); n != sessionChCap {
		t.Errorf("channel holds %d, want %d", n, sessionChCap)
	}
	u := drain(t, g.shardCh[0], 1)[0]
	if u.Code != 6521 {
		t.Errorf("the shard received code %d, want the 6521 unit whose health event was dropped", u.Code)
	}

	// Non-vacuity.
	f2 := newSessionFeedFixture(t)
	if err := f2.r.decodeDatagram(statusDatagram(t, 6521, 1)); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}
	if f2.in.sessionEventsDropped.Load() != 0 || len(f2.r.g.sessionCh) != 1 {
		t.Errorf("with room the event must be queued: dropped=%d queued=%d",
			f2.in.sessionEventsDropped.Load(), len(f2.r.g.sessionCh))
	}
}

// TestTheReopenArrivesInOrderThroughTheChannelAndIsDrainedWhole is G-SESS-2 at
// app level: 6531, 6571, 6511 inside one datagram burst, same LogTime, are
// produced by the decoder, drained FIFO by one health tick, and leave the
// market Open with three transitions and no anomaly.
func TestTheReopenArrivesInOrderThroughTheChannelAndIsDrainedWhole(t *testing.T) {
	f := newSessionFeedFixture(t)
	g := f.r.g

	// Closed first, so the re-open is a Closed to PreOpen start.
	for _, c := range []uint16{6521, 6531, 6571, 6511} {
		if err := f.r.decodeDatagram(statusDatagram(t, c, 2)); err != nil {
			t.Fatalf("decodeDatagram(%d): %v", c, err)
		}
	}
	if n := len(g.sessionCh); n != 4 {
		t.Fatalf("%d events queued, want 4", n)
	}

	g.drainSessions()

	if len(g.sessionCh) != 0 {
		t.Errorf("%d events left after a drain; it must drain completely", len(g.sessionCh))
	}
	if st := health.SessionState(f.in.sessionState[1].Load()); st != health.SessionOpen {
		t.Errorf("session_state{2} = %v, want open", st)
	}
	if n := f.in.sessionTransitions[1].Load(); n != 4 {
		t.Errorf("session_transitions{2} = %d, want 4", n)
	}
	// Unknown to Closed is the restart edge; the re-open edges are all drawn.
	if n := f.in.sessionAnomalies[1].Load(); n != 0 {
		t.Errorf("session_anomalies{2} = %d, want 0", n)
	}
	for _, other := range []int{0, 2, 3} {
		if f.in.sessionTransitions[other].Load() != 0 {
			t.Errorf("market type %d moved on market type 2's events", other+1)
		}
	}
}

// TestAnAnomalousStatusIsCountedLoggedAndStillApplied is G-SESS-3 at app level:
// Open then 6522 directly (skipping 6521) is applied, counted once as an
// anomaly and logged under the session class.
func TestAnAnomalousStatusIsCountedLoggedAndStillApplied(t *testing.T) {
	f := newSessionFeedFixture(t)
	g := f.r.g
	for _, c := range []uint16{6511, 6511, 6522} {
		if err := f.r.decodeDatagram(statusDatagram(t, c, 1)); err != nil {
			t.Fatal(err)
		}
	}
	g.drainSessions()

	if st := health.SessionState(f.in.sessionState[0].Load()); st != health.SessionPostClose {
		t.Errorf("session_state{1} = %v, want post-close: the machine must not refuse to advance", st)
	}
	if n := f.in.sessionAnomalies[0].Load(); n != 1 {
		t.Errorf("session_anomalies{1} = %d, want 1 (the repeat is not an anomaly)", n)
	}
	if n := f.in.sessionTransitions[0].Load(); n != 3 {
		t.Errorf("session_transitions{1} = %d, want 3", n)
	}
	if !strings.Contains(f.buf.String(), "WARN session anomaly market_type=1 open -> post-close") {
		t.Errorf("missing the anomaly line:\n%s", f.buf.String())
	}
}

// TestAnUnsupportedMarketTypeIsLoggedAndCountsNothing pins D15.
func TestAnUnsupportedMarketTypeIsLoggedAndCountsNothing(t *testing.T) {
	f := newSessionFeedFixture(t)
	g := f.r.g
	g.sessionCh <- model.SessionEvent{MarketType: 9, Code: 6511}
	g.sessionCh <- model.SessionEvent{MarketType: 0, Code: 6511}
	g.drainSessions()
	for i := range numMarketTypes {
		if f.in.sessionTransitions[i].Load() != 0 || f.in.sessionAnomalies[i].Load() != 0 {
			t.Errorf("market type %d counted something for an unsupported event", i+1)
		}
	}
	if !strings.Contains(f.buf.String(), "unsupported market_type=9") {
		t.Errorf("missing the ignored-event line:\n%s", f.buf.String())
	}
}

// TestPublishSessionAllocatesNothing: channel H's producer sits on the receive
// path, so it joins G-ALLOC-1's zero budget in both the delivered and dropped
// arms.
func TestPublishSessionAllocatesNothing(t *testing.T) {
	f := newSessionFeedFixture(t)
	r := f.r
	units := make([]model.UpdateUnit, 1)
	units[0] = model.UpdateUnit{Code: 6511}
	units[0].Contribs[2] = model.Contribution{Row: model.SingleKey(1), Mask: 1}

	if n := testing.AllocsPerRun(100, func() {
		select { // keep the channel from filling so both arms are exercised below
		case <-r.g.sessionCh:
		default:
		}
		r.publishSession(units)
	}); n != 0 {
		t.Errorf("delivered arm allocates %v per call", n)
	}
	for len(r.g.sessionCh) < sessionChCap {
		r.g.sessionCh <- model.SessionEvent{}
	}
	if n := testing.AllocsPerRun(100, func() { r.publishSession(units) }); n != 0 {
		t.Errorf("dropped arm allocates %v per call", n)
	}
}

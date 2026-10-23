package app

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/health"
	"feedvalve/internal/model"
)

const sec = int64(time.Second)

// storeDatagram does what the receiver does per datagram (step 5 of the
// receive loop): one store of the monotonic receipt time into the gauge the
// circuit reads.
func storeDatagram(g *Graph, mono int64) { g.inst().lastDatagramMonoNs.Store(mono) }

// inWindowNoon and outOfWindow are wall readings either side of the default
// 09:00-23:55 Asia/Kolkata window.
var (
	inWindowNoon = time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)  // 17:30 IST
	outOfWindow  = time.Date(2026, 1, 5, 20, 0, 0, 0, time.UTC) // 01:30 IST
)

// TestG_LIVE_2_AnActiveMarketWithNoPulseNeverGoesQuiet is gate G-LIVE-2.
//
// SETUP: a graph with the default thresholds (warn 12 s) and a feed of
// 7208 datagrams, one a second, and NO 6541 heartbeat at all -- which is what
// an active market looks like, because the exchange sends no pulse while data
// flows (bible 4.12).
// STIMULUS: ten warn-periods (120 s) of one receiver store and one circuit tick
// per second.
// ASSERTION: the circuit is Live throughout, circuit_quiet_events and
// circuit_down_events are zero, and nothing was logged at WARN or above.
// WOULD HAVE CAUGHT: a machine driven by heartbeat_seen instead of by any
// datagram (ADR-013), which alarms continuously during normal trading.
// NON-VACUITY: the same loop with the datagrams stopped goes Quiet and counts
// it, so the Live above is the stream's doing and not an inert machine.
func TestG_LIVE_2_AnActiveMarketWithNoPulseNeverGoesQuiet(t *testing.T) {
	g, buf := testGraph(t, "127.0.0.1:0", nil)
	in := g.inst()
	warn := int64(g.loadRuntime().Cfg.Liveness.HeartbeatWarnSec)

	for s := int64(1); s <= 10*warn; s++ {
		storeDatagram(g, s*sec)
		g.circuitTick(s*sec+sec/2, inWindowNoon)
		if st := health.CircuitState(in.circuitState.Load()); st != health.CircuitLive {
			t.Fatalf("t=%ds: circuit_state = %v, want live with a datagram every second", s, st)
		}
	}
	if q, d := in.circuitQuietEvents.Load(), in.circuitDownEvents.Load(); q != 0 || d != 0 {
		t.Errorf("quiet=%d down=%d events, want 0 and 0", q, d)
	}
	for _, bad := range []string{" WARN ", " ERROR "} {
		if strings.Contains(buf.String(), bad) {
			t.Errorf("a healthy feed logged %q:\n%s", bad, buf.String())
		}
	}

	// Non-vacuity: stop the stream.
	last := 10 * warn * sec
	g.circuitTick(last+warn*sec, inWindowNoon)
	if got := health.CircuitState(in.circuitState.Load()); got != health.CircuitQuiet {
		t.Fatalf("after %d s of silence circuit_state = %v, want quiet: the loop above proved nothing", warn, got)
	}
	if in.circuitQuietEvents.Load() != 1 {
		t.Errorf("circuit_quiet_events = %d, want 1", in.circuitQuietEvents.Load())
	}
}

// TestG_LIVE_2_RealDatagramsFeedTheCircuitAndTheHeartbeatCounterDoesNot is the
// receiver-side leg of gate G-LIVE-2.
//
// SETUP: a running graph (real listener, real receiver loop, real stats
// goroutine) and a UDP sender. Before any datagram, last_datagram_mono_ns is 0
// and the circuit is not Live.
// STIMULUS: five 7208 envelopes over UDP, no 6541 at all.
// ASSERTION: last_datagram_mono_ns advanced from 0, the stats goroutine's own
// circuit tick reaches Live, and heartbeat_seen is still 0 -- so it was the
// datagrams, not a pulse, that made it Live.
// WOULD HAVE CAUGHT: the receiver ceasing to store the gauge, and a circuit fed
// from heartbeat_seen (ADR-013, D6).
// NON-VACUITY: the pre-send state above, Live only after the datagrams.
func TestG_LIVE_2_RealDatagramsFeedTheCircuitAndTheHeartbeatCounterDoesNot(t *testing.T) {
	g, _ := testGraphWith(t, "127.0.0.1:0", nil, func(*model.Datagram) error { return nil })
	in := g.inst()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "the listener to be bound", func() bool { return g.LocalAddr() != nil })

	if in.lastDatagramMonoNs.Load() != 0 ||
		health.CircuitState(in.circuitState.Load()) == health.CircuitLive {
		t.Fatalf("before any datagram the gauge is %d and the circuit %v: the test proves nothing",
			in.lastDatagramMonoNs.Load(), health.CircuitState(in.circuitState.Load()))
	}

	conn, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialling the listener: %v", err)
	}
	defer conn.Close()
	p := bcastPayload(7208, fanoutBodyLen, fanoutPayloadLen)
	wire := datagramOf(t, uncompressedSub(p)).Buf[:4+2+len(p)]
	for range 5 {
		if _, err := conn.Write(wire); err != nil {
			t.Fatalf("writing a 7208 datagram: %v", err)
		}
	}

	waitUntil(t, "last_datagram_mono_ns to advance", func() bool { return in.lastDatagramMonoNs.Load() > 0 })
	waitUntil(t, "the stats goroutine to find the circuit live", func() bool {
		return health.CircuitState(in.circuitState.Load()) == health.CircuitLive
	})
	if n := in.heartbeatSeen.Load(); n != 0 {
		t.Errorf("heartbeat_seen = %d, want 0: the circuit went Live without a pulse in the stream", n)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
}

// TestCircuitTransitionsAreLoggedAndCountedUnderTheirClasses walks the whole of
// Figure 4.6 and asserts each edge's gauge, counter and line (section 3.4).
func TestCircuitTransitionsAreLoggedAndCountedUnderTheirClasses(t *testing.T) {
	g, buf := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		c.Liveness.HeartbeatWarnSec = 10
		c.Liveness.HeartbeatDownSec = 30
		c.Liveness.StartupGraceSec = 60
	})
	in := g.inst()
	state := func() health.CircuitState { return health.CircuitState(in.circuitState.Load()) }
	tick := func(at int64) { g.circuitTick(at*sec, inWindowNoon) }

	tick(1)
	if state() != health.CircuitStarting {
		t.Fatalf("no datagram yet: %v", state())
	}
	storeDatagram(g, 2*sec)
	tick(3)
	if state() != health.CircuitLive || !strings.Contains(buf.String(), "circuit live first_datagram_after_ms=2000") {
		t.Fatalf("Starting to Live: state=%v log:\n%s", state(), buf.String())
	}
	tick(13) // 11 s of silence, warn is 10
	if state() != health.CircuitQuiet || in.circuitQuietEvents.Load() != 1 ||
		!strings.Contains(buf.String(), "WARN circuit quiet silent_s=11") {
		t.Fatalf("Live to Quiet: state=%v quiet=%d log:\n%s", state(), in.circuitQuietEvents.Load(), buf.String())
	}
	tick(33) // 31 s, down is 30
	if state() != health.CircuitDown || in.circuitDownEvents.Load() != 1 ||
		!strings.Contains(buf.String(), "ERROR circuit down silent_s=31 threshold_s=30") {
		t.Fatalf("Quiet to Down: state=%v down=%d log:\n%s", state(), in.circuitDownEvents.Load(), buf.String())
	}
	storeDatagram(g, 40*sec)
	tick(41)
	if state() != health.CircuitLive || in.circuitOutageMaxNs.Load() != uint64(38*sec) ||
		!strings.Contains(buf.String(), "circuit recovered outage_s=38 -- expect a BCSeqNo gap") {
		t.Fatalf("Down to Live: state=%v outage_max=%d log:\n%s", state(), in.circuitOutageMaxNs.Load(), buf.String())
	}
}

// TestStartupGraceExpiryIsDownAndRecoveryAfterItClaimsNoGap covers D17:
// Starting to Down names the listen address, and the recovery after it does not
// tell the operator to expect a BCSeqNo gap.
func TestStartupGraceExpiryIsDownAndRecoveryAfterItClaimsNoGap(t *testing.T) {
	g, buf := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		c.Liveness.StartupGraceSec = 5
	})
	g.circuitTick(6*sec, inWindowNoon)
	if health.CircuitState(g.inst().circuitState.Load()) != health.CircuitDown ||
		g.inst().circuitDownEvents.Load() != 1 {
		t.Fatalf("grace expiry: state/counter wrong")
	}
	if !strings.Contains(buf.String(), "ERROR circuit down: no datagram within startup grace grace_s=5 listen=127.0.0.1:0") {
		t.Errorf("missing the startup-grace line:\n%s", buf.String())
	}
	storeDatagram(g, 8*sec)
	g.circuitTick(9*sec, inWindowNoon)
	if strings.Contains(buf.String(), "BCSeqNo") {
		t.Errorf("recovery from startup silence mentions a sequence gap:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "circuit recovered after startup silence") {
		t.Errorf("missing the recovery line:\n%s", buf.String())
	}
}

// TestTheTradingWindowLowersSeverityAndNeverTheStateOrCounters is section 3.5
// rules 1 and 3 at app level: outside the window Down logs at WARN and Quiet at
// INFO, while the gauge and counters are identical to the in-window run.
func TestTheTradingWindowLowersSeverityAndNeverTheStateOrCounters(t *testing.T) {
	run := func(wall time.Time) (string, uint64, uint64, health.CircuitState) {
		g, buf := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
			c.Liveness.HeartbeatWarnSec = 10
			c.Liveness.HeartbeatDownSec = 30
		})
		storeDatagram(g, sec)
		g.circuitTick(2*sec, wall)
		g.circuitTick(12*sec, wall)
		g.circuitTick(40*sec, wall)
		in := g.inst()
		return buf.String(), in.circuitQuietEvents.Load(), in.circuitDownEvents.Load(),
			health.CircuitState(in.circuitState.Load())
	}
	inLog, inQ, inD, inS := run(inWindowNoon)
	outLog, outQ, outD, outS := run(outOfWindow)

	if inQ != outQ || inD != outD || inS != outS || inS != health.CircuitDown {
		t.Errorf("window changed state or counters: in=(%d,%d,%v) out=(%d,%d,%v)", inQ, inD, inS, outQ, outD, outS)
	}
	if !strings.Contains(inLog, "WARN circuit quiet") || !strings.Contains(inLog, "ERROR circuit down") {
		t.Errorf("in-window severities wrong:\n%s", inLog)
	}
	if !strings.Contains(outLog, "INFO circuit quiet") || !strings.Contains(outLog, "WARN circuit down") ||
		strings.Contains(outLog, "ERROR") {
		t.Errorf("outside-window severities wrong:\n%s", outLog)
	}
}

// TestAnUnusableTradingWindowFailsOpenAndWarnsOnce is G-LIVE-3 at app level:
// an unresolvable zone (unreachable through a validated load, D3) leaves the
// louder severity and warns once, not once per tick.
func TestAnUnusableTradingWindowFailsOpenAndWarnsOnce(t *testing.T) {
	g, buf := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		c.Liveness.HeartbeatWarnSec = 10
		c.Liveness.HeartbeatDownSec = 30
		c.Liveness.TradingWindow.TZ = "Not/AZone"
	})
	storeDatagram(g, sec)
	g.circuitTick(2*sec, outOfWindow)
	g.circuitTick(12*sec, outOfWindow)
	g.circuitTick(40*sec, outOfWindow)
	out := buf.String()
	if n := strings.Count(out, "tradingWindow unusable"); n != 1 {
		t.Errorf("the unusable-window warning appeared %d times, want 1:\n%s", n, out)
	}
	if !strings.Contains(out, "WARN circuit quiet") || !strings.Contains(out, "ERROR circuit down") {
		t.Errorf("a failed-open window must keep the louder severities:\n%s", out)
	}
}

// TestCircuitIntervalIsOneSecondClampedToHalfTheWarnThreshold pins section 3.3
// and that it is read from the configuration passed in, so a reload re-arms it.
func TestCircuitIntervalIsOneSecondClampedToHalfTheWarnThreshold(t *testing.T) {
	for _, c := range []struct {
		warn int
		want time.Duration
	}{
		{12, time.Second},
		{2, time.Second},
		{1, 500 * time.Millisecond},
	} {
		if got := circuitInterval(&config.LivenessConfig{HeartbeatWarnSec: c.warn}); got != c.want {
			t.Errorf("warn=%ds: interval %v, want %v", c.warn, got, c.want)
		}
	}
}

// TestTheStatsGoroutineDrivesTheCircuit wires the real goroutine: with a 1 s
// startup grace and no datagram, circuit_state reaches Down on its own timer.
func TestTheStatsGoroutineDrivesTheCircuit(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		c.Liveness.StartupGraceSec = 1
		c.Liveness.HeartbeatWarnSec = 1
		c.Liveness.HeartbeatDownSec = 2
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "circuit_state=down from the stats goroutine", func() bool {
		return g.inst().circuitState.Load() == int64(health.CircuitDown)
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if g.inst().circuitDownEvents.Load() != 1 {
		t.Errorf("circuit_down_events = %d, want 1", g.inst().circuitDownEvents.Load())
	}
}

package health

import "feedvalve/internal/model"

// CircuitState is the published circuit_state gauge value (Vol 4 section 3.3).
type CircuitState uint8

const (
	CircuitStarting CircuitState = 0
	CircuitLive     CircuitState = 1
	CircuitQuiet    CircuitState = 2
	CircuitDown     CircuitState = 3
)

func (s CircuitState) String() string {
	switch s {
	case CircuitStarting:
		return "starting"
	case CircuitLive:
		return "live"
	case CircuitQuiet:
		return "quiet"
	case CircuitDown:
		return "down"
	}
	return "invalid"
}

// LivenessConfig is the part of the liveness configuration the machine reads.
// It is health's own type, not config's: app projects one into the other, so
// health never imports config. All three values are seconds and are read fresh
// at every Tick, so a reload takes effect within one tick (G-LIVE-1).
type LivenessConfig struct {
	WarnSec  int // silence that moves Live to Quiet
	DownSec  int // silence that moves Quiet to Down
	GraceSec int // silence since process start that moves Starting to Down
}

// TransitionKind names the six edges of Figure 4.6 that change state.
type TransitionKind uint8

const (
	TransitionNone TransitionKind = iota
	StartingToLive
	StartingToDown
	LiveToQuiet
	QuietToLive
	QuietToDown
	DownToLive
)

// Transition is what one Tick did, with the inputs of the log line (Vol 4
// section 3.4). The machine logs and counts nothing itself.
type Transition struct {
	Kind TransitionKind
	From CircuitState
	To   CircuitState

	// SilentSec is the silence that triggered a move to Quiet or Down.
	SilentSec int64
	// ThresholdSec is the threshold that silence was compared against.
	ThresholdSec int64
	// FirstDatagramMs is the monotonic time of the first datagram, in ms since
	// process start (StartingToLive only).
	FirstDatagramMs int64
	// OutageSec is how long the silence lasted, measured between the last
	// datagram before it and the one that ended it (QuietToLive, DownToLive).
	OutageSec int64
	// OutageNs is OutageSec at full resolution, for circuit_outage_max_ns
	// (DownToLive only: D17, observed on recovery).
	OutageNs int64
	// FromStartup is true for a DownToLive whose outage began at process start,
	// so the caller must not say "expect a BCSeqNo gap": no datagram was seen
	// before it (D17).
	FromStartup bool
	// LastTranscode is the last transaction code seen before the silence.
	LastTranscode model.TransCode
}

// Circuit is the liveness machine of Figure 4.6. It has no clock, logger or
// counter: it is fed monotonic nanoseconds and returns what happened. It is
// owned by one goroutine and is not safe for concurrent use.
type Circuit struct {
	state    CircuitState
	seen     bool
	first    int64 // monotonic ns of the first datagram
	last     int64 // monotonic ns of the newest datagram
	lastCode model.TransCode

	silentFrom int64 // last datagram before the current Quiet/Down episode; 0 = startup
}

// State is the gauge value.
func (c *Circuit) State() CircuitState { return c.state }

// Observe records a datagram's monotonic receive time. A value <= 0 means "no
// datagram yet" (the gauge's initial value) and is ignored. Times that go
// backwards are ignored too, so the newest datagram always wins.
func (c *Circuit) Observe(recvMono int64) {
	if recvMono <= 0 {
		return
	}
	if !c.seen {
		c.seen = true
		c.first = recvMono
	}
	if recvMono > c.last {
		c.last = recvMono
	}
}

// SetLastTranscode records the code of the newest datagram, for the Quiet log
// line. It is optional: without it the transition reports code 0.
func (c *Circuit) SetLastTranscode(code model.TransCode) { c.lastCode = code }

const nsPerSec = int64(1_000_000_000)

// Tick advances the machine by at most one edge and returns it. Thresholds come
// from cfg at every call. A silence long enough to cross both thresholds in one
// call yields Live to Quiet first and Quiet to Down on the next tick.
func (c *Circuit) Tick(nowMono int64, cfg LivenessConfig) Transition {
	switch c.state {
	case CircuitStarting:
		if c.seen {
			c.state = CircuitLive
			return Transition{Kind: StartingToLive, From: CircuitStarting, To: CircuitLive,
				FirstDatagramMs: c.first / 1_000_000}
		}
		if nowMono >= int64(cfg.GraceSec)*nsPerSec {
			c.state = CircuitDown
			c.silentFrom = 0
			return Transition{Kind: StartingToDown, From: CircuitStarting, To: CircuitDown,
				SilentSec: nowMono / nsPerSec, ThresholdSec: int64(cfg.GraceSec)}
		}
	case CircuitLive:
		silent := nowMono - c.last
		if silent >= int64(cfg.WarnSec)*nsPerSec {
			c.state = CircuitQuiet
			c.silentFrom = c.last
			return Transition{Kind: LiveToQuiet, From: CircuitLive, To: CircuitQuiet,
				SilentSec: silent / nsPerSec, ThresholdSec: int64(cfg.WarnSec),
				LastTranscode: c.lastCode}
		}
	case CircuitQuiet:
		if c.last > c.silentFrom {
			c.state = CircuitLive
			out := c.last - c.silentFrom
			return Transition{Kind: QuietToLive, From: CircuitQuiet, To: CircuitLive,
				OutageSec: out / nsPerSec, LastTranscode: c.lastCode}
		}
		silent := nowMono - c.last
		if silent >= int64(cfg.DownSec)*nsPerSec {
			c.state = CircuitDown
			return Transition{Kind: QuietToDown, From: CircuitQuiet, To: CircuitDown,
				SilentSec: silent / nsPerSec, ThresholdSec: int64(cfg.DownSec),
				LastTranscode: c.lastCode}
		}
	case CircuitDown:
		if c.last > c.silentFrom {
			c.state = CircuitLive
			out := c.last - c.silentFrom
			return Transition{Kind: DownToLive, From: CircuitDown, To: CircuitLive,
				OutageSec: out / nsPerSec, OutageNs: out,
				FromStartup: c.silentFrom == 0, LastTranscode: c.lastCode}
		}
	}
	return Transition{}
}

// Severity is how loudly a transition should be logged. health has its own
// type so it need not import obs; app maps it onto log levels.
type Severity uint8

const (
	SeverityInfo Severity = iota
	SeverityWarn
	SeverityError
)

// SeverityOf is the trading-window gate of Vol 4 section 3.5. It lowers
// Quiet and Down by one level outside the window and touches nothing else:
// the state, the gauge and the counters are the caller's and never depend on
// it.
func SeverityOf(k TransitionKind, inWindow bool) Severity {
	switch k {
	case StartingToDown, QuietToDown:
		if inWindow {
			return SeverityError
		}
		return SeverityWarn
	case LiveToQuiet:
		if inWindow {
			return SeverityWarn
		}
		return SeverityInfo
	}
	return SeverityInfo
}

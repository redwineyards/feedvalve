package app

import (
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/health"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// circuitMonitor is the stats goroutine's half of Vol 4 section 3: the pure
// health.Circuit plus the trading window it gates log severity with. Owned by
// the stats goroutine alone; nothing else touches it.
type circuitMonitor struct {
	c health.Circuit

	// win is rebuilt only when the configured window changes, so a steady
	// state costs a struct comparison per tick and not a zone lookup.
	win    *health.Window
	winCfg config.TradingWindow
	winSet bool
}

// livenessOf projects config onto health's own type, as mirrorParams does for
// mirror: health may not import config.
func livenessOf(l *config.LivenessConfig) health.LivenessConfig {
	return health.LivenessConfig{
		WarnSec:  l.HeartbeatWarnSec,
		DownSec:  l.HeartbeatDownSec,
		GraceSec: l.StartupGraceSec,
	}
}

// circuitInterval is the evaluation tick of section 3.3: 1 s, clamped to at
// most heartbeatWarnSec/2, read from the LIVE configuration at each fire so a
// reload re-arms it. Not a configuration key.
func circuitInterval(l *config.LivenessConfig) time.Duration {
	d := time.Second
	if half := time.Duration(l.HeartbeatWarnSec) * time.Second / 2; half > 0 && half < d {
		d = half
	}
	return d
}

// window returns the Window for tw, rebuilding it when the configuration moved.
// A Window that could not be built is fail-open and is reported once per
// change, not once per tick.
func (m *circuitMonitor) window(tw config.TradingWindow, log *obs.AppLogger) *health.Window {
	if m.winSet && m.winCfg == tw {
		return m.win
	}
	w, err := health.NewWindow(health.TradingWindow{Start: tw.Start, End: tw.End, TZ: tw.TZ})
	if err != nil && log.Allow(obs.ClassCircuitQuiet) {
		log.Warnf(obs.ClassCircuitQuiet, "liveness.tradingWindow unusable, every hour counts as "+
			"in-window and circuit alarms keep their louder severity: %v", err)
	}
	m.win, m.winCfg, m.winSet = w, tw, true
	return w
}

// circuitTick is one evaluation: feed the machine the receiver's
// last_datagram_mono_ns, advance it, and log and count what it did. nowMono is
// monotonic nanoseconds since processStart -- the receiver's own clock -- and
// wall is used only to place the moment inside or outside the trading window.
// Both are parameters so a test drives it with no clock.
//
// The machine's state, the gauge and the counters never depend on the window;
// only the severity of the log line does (section 3.5 rule 1).
func (g *Graph) circuitTick(nowMono int64, wall time.Time) {
	in := g.inst()
	cfg := g.loadRuntime().Cfg
	m := &g.circuit

	m.c.Observe(in.lastDatagramMonoNs.Load())
	tr := m.c.Tick(nowMono, livenessOf(&cfg.Liveness))
	in.circuitState.Store(int64(m.c.State()))
	if tr.Kind == health.TransitionNone {
		return
	}

	inWindow := m.window(cfg.Liveness.TradingWindow, g.deps.Log).Contains(wall)
	log := g.deps.Log
	switch tr.Kind {
	case health.StartingToLive:
		log.Infof("circuit live first_datagram_after_ms=%d", tr.FirstDatagramMs)
	case health.StartingToDown:
		in.circuitDownEvents.Add(1)
		g.logCircuitDown(inWindow,
			"circuit down: no datagram within startup grace grace_s=%d listen=%s",
			tr.ThresholdSec, cfg.UDP.Listen)
	case health.LiveToQuiet:
		in.circuitQuietEvents.Add(1)
		if health.SeverityOf(tr.Kind, inWindow) == health.SeverityWarn {
			if log.Allow(obs.ClassCircuitQuiet) {
				log.Warnf(obs.ClassCircuitQuiet, "circuit quiet silent_s=%d", tr.SilentSec)
			}
		} else {
			log.Infof("circuit quiet silent_s=%d", tr.SilentSec)
		}
	case health.QuietToLive:
		log.Infof("circuit recovered quiet_s=%d", tr.OutageSec)
	case health.QuietToDown:
		in.circuitDownEvents.Add(1)
		g.logCircuitDown(inWindow, "circuit down silent_s=%d threshold_s=%d",
			tr.SilentSec, tr.ThresholdSec)
	case health.DownToLive:
		in.circuitOutageMaxNs.Observe(uint64(tr.OutageNs))
		if tr.FromStartup {
			// No datagram preceded the outage, so there is no sequence to have
			// a gap in (D17).
			log.Infof("circuit recovered after startup silence")
		} else {
			log.Infof("circuit recovered outage_s=%d -- expect a BCSeqNo gap", tr.OutageSec)
		}
	}
}

// logCircuitDown logs a Down transition at ERROR in the trading window and WARN
// outside it, under ClassCircuitDown either way.
func (g *Graph) logCircuitDown(inWindow bool, format string, a ...any) {
	log := g.deps.Log
	if !log.Allow(obs.ClassCircuitDown) {
		return
	}
	if inWindow {
		log.Errorf(obs.ClassCircuitDown, format, a...)
	} else {
		log.Warnf(obs.ClassCircuitDown, format, a...)
	}
}

// sessionChCap is channel H's capacity (Vol 4 section 4.5): five times the
// worst plausible burst, a simultaneous re-open of all four market types
// (4 x 3 = 12 events). Fixed at startup; not a configuration key.
const sessionChCap = 64

// publishSession is channel H's producer, called by the decoder after a
// sub-packet's units have been routed and OUTSIDE the recover boundary. The
// send never blocks: a full channel counts session_events_dropped and moves on,
// because the unit it describes has already been delivered to its shard and
// losing the health event costs a log line and a gauge update, never data. It
// is deliberately outside the stall budget (G-SESS-4).
func (r *receiver) publishSession(units []model.UpdateUnit) {
	for i := range units {
		ev, ok := decode.SessionEventOf(&units[i])
		if !ok {
			continue
		}
		select {
		case r.g.sessionCh <- ev:
		default:
			r.in.sessionEventsDropped.Add(1)
		}
	}
}

// healthTick is one pass of the stats goroutine's health timer: the circuit,
// then everything channel H has accumulated.
func (g *Graph) healthTick(nowMono int64, wall time.Time) {
	g.circuitTick(nowMono, wall)
	g.drainSessions()
}

// drainSessions applies every queued market-status event, FIFO, to the session
// machine and publishes the result (Vol 4 section 4.3). It drains completely:
// the re-open's three transitions arrive inside one second and must each be
// seen. Applying in arrival order is the point; LogTime is not consulted.
func (g *Graph) drainSessions() {
	in := g.inst()
	log := g.deps.Log
	for {
		select {
		case ev := <-g.sessionCh:
			res := g.sessions.Apply(ev)
			if res.Ignored {
				// D15: log-only, rate-limited, no counter.
				if log.Allow(obs.ClassSessionAnomaly) {
					log.Warnf(obs.ClassSessionAnomaly,
						"market status for unsupported market_type=%d code=%d ignored", ev.MarketType, ev.Code)
				}
				continue
			}
			i := ev.MarketType - 1
			in.sessionState[i].Store(int64(res.To))
			in.sessionTransitions[i].Add(1)
			switch {
			case res.Repeat:
				// The exchange may resend; a duplicate is not information.
			case res.Anomaly:
				in.sessionAnomalies[i].Add(1)
				if log.Allow(obs.ClassSessionAnomaly) {
					log.Warnf(obs.ClassSessionAnomaly,
						"session anomaly market_type=%d %s -> %s on code %d (not an expected edge; applied)",
						ev.MarketType, res.From, res.To, ev.Code)
				}
			default:
				log.Infof("session market_type=%d %s -> %s code=%d", ev.MarketType, res.From, res.To, ev.Code)
			}
		default:
			return
		}
	}
}

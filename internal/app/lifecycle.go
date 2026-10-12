package app

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"feedvalve/internal/model"
)

// maxDatagramLen bounds one read. 64 KB and not 516, deliberately: ReadFrom
// silently discards whatever does not fit and reports no error, so an
// exactly-sized buffer would make an oversized datagram indistinguishable from
// a full one. Carried over from poc, where the same comment sits on the same
// buffer.
const maxDatagramLen = 65536

// Run binds the socket, starts the goroutines, blocks until the context is
// cancelled or a terminal condition is delivered, then drains and returns.
//
// The returned error is nil for a requested shutdown -- INCLUDING one whose
// drain deadline expired and abandoned rows, which is a counted, logged
// degradation of a stop we asked for and not a failure (ADR-L37) -- and non-nil
// only for a terminal runtime condition, which main maps to exit code 4.
//
// THE DRAIN IS IN THE BODY, NOT A DEFER, and Vol 4 section 1.1 is explicit
// about why: "a defer cannot be sequenced against another defer's resource, and
// the writers need the Oracle pool open while they finish their last batch --
// while closing the pool is itself a defer registered at open time". Putting the
// drain in the body makes that ordering explicit instead of depending on LIFO
// unwind order.
func (g *Graph) Run(ctx context.Context) error {
	// Registered FIRST so it runs LAST: the shutdown summary must be the final
	// line in the log, after every resource has reported its own close
	// (section 1.5 step 13).
	defer g.logSummary()

	// teardown is idempotent and deferred as well as called, so the error
	// paths below -- a failed bind above all -- stop whatever is already
	// running. That is the runtime half of C-1 and what gate G-LIFE-3 asserts:
	// a bind failure must return an error with the database closed, not exit.
	defer g.teardown()

	// Step 14a: the decoder, BEFORE the producer that feeds it. Consumers
	// before producers is the same rule that puts the writers before the
	// shards and the shards before the receiver -- starting a producer first
	// burns queue slack for nothing.
	g.startDecoder()

	// Step 15: the stats and health goroutine. Before the socket, so the
	// liveness clock is already running when the first datagram could arrive.
	g.startStats()

	// Step 16: the listener, LAST of the resources, so nothing arrives before
	// there is somewhere to put it.
	if err := g.bind(); err != nil {
		return err
	}

	// Step 17: the receiver, then the SIGHUP handler, then block.
	g.startReader(ctx)
	stopHUP := g.installReloadHandler()
	defer stopHUP()

	ev, terminal := g.wait(ctx)
	g.logShutdownInitiated(ev, terminal)

	// Steps 3 to 10: the drain.
	g.teardown()

	if terminal {
		return &ExitError{
			Code: ExitTerminal,
			Err:  errors.New("terminal condition " + ev.reason.String() + ": " + ev.detail),
		}
	}
	return nil
}

// bind performs startup step 16: listen, set the receive buffer, and log the
// EFFECTIVE size.
//
// The effective size is logged because the kernel silently caps at
// udp_recvspace: the requested value is a wish, and only the effective one
// bounds Q0's 0.53 s of slack. An operator who reads the requested number and
// believes it has the wrong model of how much slack the process has.
func (g *Graph) bind() error {
	cfg := g.deps.Cfg

	conn, err := net.ListenPacket("udp", cfg.UDP.Listen)
	if err != nil {
		return exitf(ExitStartup, "binding %s: %w", cfg.UDP.Listen, err)
	}
	g.setConn(conn)
	g.mark(&g.acquired, "udp-listener")

	effective := cfg.UDP.ReadBufferBytes
	if uc, ok := conn.(*net.UDPConn); ok {
		if err := uc.SetReadBuffer(cfg.UDP.ReadBufferBytes); err != nil {
			// Not fatal: a kernel that refuses the size still delivers
			// datagrams, just with less slack. Refusing to start over it would
			// trade a measurable risk for a certain outage.
			g.deps.Log.Warnf(obsClassStartup, "SetReadBuffer(%d) refused: %v",
				cfg.UDP.ReadBufferBytes, err)
		}
		if n, err := readBufferSize(uc); err == nil {
			effective = n
		}
	}
	g.inst().readbufEffective.Store(int64(effective))
	g.deps.Log.Infof("socket bound listen=%s requested_rcvbuf=%d effective_rcvbuf=%d",
		cfg.UDP.Listen, cfg.UDP.ReadBufferBytes, effective)
	return nil
}

// startReader runs the receive loop. In P1-09 its whole job is read, copy,
// publish -- the shape Vol 0 section 9.5.1 describes once QD exists. The
// envelope split, the source-address check, the oversize rule and the sequence
// gap detector are Vol 1's and arrive in Phase 2; what is here is the part the
// LIFECYCLE owns, which is how the loop starts and how it stops.
func (g *Graph) startReader(ctx context.Context) {
	g.readerStarted = true
	go func() {
		defer close(g.readerDone)

		// Allocated once, outside the loop. Allocating per datagram is the
		// first thing that would show up as jitter at market open.
		buf := make([]byte, maxDatagramLen)
		var d model.Datagram
		consecutive := 0

		// Captured once. The listener does not change for the life of the
		// loop -- rebinding is startup-only (C-2) -- so re-reading it per
		// datagram would take a lock 2,400 times a second to learn the same
		// answer.
		conn := g.getConn()

		for {
			n, _, err := conn.ReadFrom(buf)
			if err != nil {
				// A read error during shutdown IS shutdown, not a fault. poc
				// logged it as a failure and the resulting ERROR line at every
				// clean stop trained operators to ignore the level.
				if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
					return
				}
				g.inst().udpReadErrors.Add(1)
				consecutive++
				if consecutive >= maxConsecutiveReadErrors {
					g.deps.Log.Errorf(obsClassStartup,
						"%d consecutive UDP read errors; giving up: %v", consecutive, err)
					return
				}
				continue
			}
			consecutive = 0

			// RecvMono is taken ONCE, immediately after the read, and is the
			// source of the ordering guard's receipt clock and of the fan-out
			// budget's start. Monotonic, never a wall-clock reading.
			d.RecvMono = int64(monotonicNow())
			if n > len(d.Buf) {
				n = len(d.Buf)
			}
			d.N = int32(n)
			copy(d.Buf[:n], buf[:n])

			g.inst().datagramsReceived.Add(1)
			if !g.qdProducer.TryPublish(&d) {
				g.inst().qdSendBlocked.Add(1)
			}
			g.inst().qdDepthMax.Observe(uint64(g.qdProducer.Len()))
		}
	}()
}

// maxConsecutiveReadErrors bounds the spin poc's log-and-continue loop would
// otherwise become. Vol 1 ASM-V5: 64 is small enough to bound a spin at full
// CPU and large enough that no plausible transient trips it. udp_read_errors
// should be exactly zero over a session; any non-zero value is an incident.
const maxConsecutiveReadErrors = 64

// startDecoder drains QD. In P1-09 the callback does nothing with the datagram:
// framing and decode are Phase 2, and what the lifecycle owns is that the
// consumer exists, that it is the ONLY consumer, and that it drains to empty on
// close rather than abandoning (Vol 0 section 9.5.7).
func (g *Graph) startDecoder() {
	g.decoderStarted = true
	g.mark(&g.acquired, "decoder")
	go func() {
		defer close(g.decoderDone)
		decode := g.deps.Decode
		if decode == nil {
			// Drain and discard. The datagram still has to be TAKEN, or
			// shutdown would abandon a ring with items in it.
			decode = func(*model.Datagram) error { return nil }
		}
		err := g.qdConsumer.Drain(decode)
		if err != nil {
			g.deps.Log.Errorf(obsClassStartup, "decoder stopped: %v", err)
		}
	}()
}

// startStats runs the emission goroutine of Vol 4 section 6.1: one grouped
// emission every stats.intervalSec.
//
// obs owns no goroutine and no ticker -- Emit is a method the stats goroutine
// calls -- so this is where the interval lives. The gauges obs calls "sampled
// at emission" are stored by their owner immediately before Emit, because Emit
// never calls back.
func (g *Graph) startStats() {
	g.statsStarted = true
	g.mark(&g.acquired, "stats-goroutine")

	interval := g.deps.Cfg.StatsInterval()
	go func() {
		defer close(g.statsDone)
		t := time.NewTicker(interval)
		defer t.Stop()

		last := time.Now()
		for {
			select {
			case <-g.statsStop:
				return
			case now := <-t.C:
				// A wall clock that jumped by much more than the interval is a
				// step, and it makes every timestamp in the log discontinuous.
				// Explaining that in ten seconds instead of an hour is the
				// whole value of the counter.
				if skew := now.Sub(last) - interval; skew > interval || skew < -interval {
					g.inst().clockStepDetected.Add(1)
				}
				last = now

				g.sampleGauges()
				if err := g.deps.Reg.Emit(g.deps.Stderr); err != nil {
					g.deps.Log.Errorf(obsClassStartup, "emitting the stats line: %v", err)
				}
			}
		}
	}()
}

// sampleGauges stores the values Vol 0 section 8.2 describes as sampled at
// emission and whose owner is the lifecycle.
func (g *Graph) sampleGauges() {
	in := g.inst()
	in.configGeneration.Store(int64(g.deps.Store.Gen()))
	in.writersConfigured.Store(int64(g.deps.Cfg.Writers.Count))
	in.qdWakeParked.Add(0) // owned by the decoder; see Parks below
	// The ring's own counters are plain numbers, not instruments: the ledger
	// assigns qd_wake_parked to the decoder, so the decoder's handle reports
	// it rather than the ring writing it directly.
	if parks := g.qdConsumer.Parks(); parks > g.lastParks {
		in.qdWakeParked.Add(parks - g.lastParks)
		g.lastParks = parks
	}
}

// installReloadHandler wires SIGHUP. It is a no-op until Phase 5 -- ADR-010's
// re-read, validate and atomic swap is section 2's work -- but the handler is
// installed now, and AFTER the graph exists, because a SIGHUP arriving
// mid-construction would race a half-built registry against a swap. Before this
// point SIGHUP's default disposition terminates the process, which during a
// sub-second startup is acceptable and is stated rather than discovered.
func (g *Graph) installReloadHandler() func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)

	stop := make(chan struct{})
	var once sync.Once
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-ch:
				g.deps.Log.Infof("SIGHUP received; configuration reload arrives in " +
					"Phase 5 (ADR-010). The running configuration is unchanged.")
			}
		}
	}()
	return func() {
		once.Do(func() {
			signal.Stop(ch)
			close(stop)
		})
	}
}

// wait blocks until a requested shutdown or a terminal condition. One entry
// point for both, because section 1.3 makes Draining the SAME sequence either
// way -- only the exit code differs. A Q1 overflow still leaves three healthy
// shards with dirty rows worth writing.
func (g *Graph) wait(ctx context.Context) (fatalEvent, bool) {
	select {
	case <-ctx.Done():
		return fatalEvent{}, false
	case ev := <-g.fatal:
		return ev, true
	}
}

func (g *Graph) logShutdownInitiated(ev fatalEvent, terminal bool) {
	g.drainStarted = time.Now()
	reason := "signal"
	if terminal {
		reason = ev.reason.String()
	}
	// The counter snapshot goes with it. HLD section 10.2: a terminal message
	// must carry the counters that led to it, because R12 -- "operators do not
	// read the 60 s line" -- is mitigated only if the last line is
	// self-contained.
	g.sampleGauges()
	g.deps.Log.Infof("shutdown initiated reason=%s%s", reason, detailSuffix(ev.detail))
	if err := g.deps.Reg.Emit(g.deps.Stderr); err != nil {
		g.deps.Log.Errorf(obsClassStartup, "emitting the shutdown snapshot: %v", err)
	}
}

func detailSuffix(detail string) string {
	if detail == "" {
		return ""
	}
	return " detail=" + detail
}

// teardown is shutdown steps 3 to 10. Idempotent, because it is both called in
// Run's body on the normal path and deferred for the error paths.
func (g *Graph) teardown() {
	g.teardownOnce.Do(func() {
		// Step 3: close the listener. This is the ONLY way to unblock an
		// in-flight ReadFrom -- no SetReadDeadline, no polling. A poll loop
		// would wake a sleeping goroutine ten times a second forever to serve
		// a shutdown that happens once.
		if c := g.getConn(); c != nil {
			_ = c.Close() // a second Close returns ErrClosed; discarded
			g.mark(&g.released, "udp-listener")
		}

		// Steps 4 and 5: the receiver observes the read error, sees
		// ctx.Err() != nil, and returns without logging a failure. Bounded by
		// its own stall budget plus one datagram.
		if g.readerStarted {
			<-g.readerDone
		}

		// The QD drain stage, between "stop reading" and "stop decoding"
		// (section 9.5.7). QD must be DRAINED, not abandoned, or the no-loss
		// property has a hole at shutdown precisely where it is easiest to
		// overlook -- so Close marks the ring and the decoder keeps delivering
		// until it is empty.
		if g.qdProducer != nil {
			g.qdProducer.Close()
			g.mark(&g.released, "qd-ring")
		}
		if g.decoderStarted {
			<-g.decoderDone
			g.mark(&g.released, "decoder")
		}

		// Steps 6 to 10 -- close shardCh[k], the shard drain under one shared
		// deadline, close batchQ, the writers' last batch -- arrive with
		// Phase 3 and Phase 4. The ordering is what P1-09 fixes; the stages
		// slot in here.

		if g.statsStarted {
			close(g.statsStop)
			<-g.statsDone
			g.mark(&g.released, "stats-goroutine")
		}
	})
}

// logSummary is shutdown step 13, registered first and therefore run last.
func (g *Graph) logSummary() {
	var took time.Duration
	if !g.drainStarted.IsZero() {
		took = time.Since(g.drainStarted)
	}
	g.deps.Log.Infof("shutdown complete drain_ms=%d acquired=%v released=%v",
		took.Milliseconds(), g.Acquired(), g.Released())
}

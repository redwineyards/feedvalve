package app

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/wire"
)

// readBufferLen bounds one read. 64 KB and not wire.MaxDatagramLen's 516,
// deliberately: ReadFrom silently discards whatever does not fit and reports no
// error, so an exactly-sized buffer would make an oversized datagram
// indistinguishable from a full one -- and an oversized datagram is an
// envelope-assumption break (F6) that must be reported, not hidden.
//
// NAMED FOR THE BUFFER AND NOT FOR THE DATAGRAM. This was maxDatagramLen until
// P1-10, which is the name wire now gives the envelope cap of 516 -- two very
// different quantities under one word, in a package that will import wire in
// Phase 2. poc had already made the same distinction with two names
// (maxDatagramLen 516, udpReadBufferLen 65536); this follows it.
const readBufferLen = 65536

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
// net.ListenUDP and not net.ListenPacket (Vol 1 section 3.1). The interface
// does not carry SetReadBuffer, so udp.readBufferBytes could not be applied
// through it, and it does not carry ReadFromUDPAddrPort either -- which is the
// allocation argument, not a style one. poc binds with ListenPacket
// (poc/gobrdclnt.go:111) and pays for it once per datagram.
func (g *Graph) bind() error {
	cfg := g.deps.Cfg

	addr, err := net.ResolveUDPAddr("udp", cfg.UDP.Listen)
	if err != nil {
		return exitf(ExitStartup, "resolving %s: %w", cfg.UDP.Listen, err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return exitf(ExitStartup, "binding %s: %w", cfg.UDP.Listen, err)
	}
	g.setConn(conn)
	g.mark(&g.acquired, "udp-listener")

	if err := conn.SetReadBuffer(cfg.UDP.ReadBufferBytes); err != nil {
		// Not fatal: a kernel that refuses the size still delivers datagrams,
		// just with less slack. Refusing to start over it would trade a
		// measurable risk for a certain outage.
		g.deps.Log.Warnf(obsClassStartup, "SetReadBuffer(%d) refused: %v",
			cfg.UDP.ReadBufferBytes, err)
	}
	effective := cfg.UDP.ReadBufferBytes
	if n, err := readBufferSize(conn); err == nil {
		effective = n
	}

	// The allow-list is parsed ONCE, here, because parsing per datagram would
	// allocate 2,400 times a second to learn the same answer (section 3.5).
	// It cannot fail on a validated config -- V-SRC1 checks it with the same
	// parser -- so a failure here means an unvalidated Config reached Run, and
	// refusing to start is the right answer: the alternative is a source
	// filter nobody realises is misconfigured.
	prefixes, err := cfg.UDP.SourcePrefixes()
	if err != nil {
		return exitf(ExitStartup, "parsing udp.sourceAllowList: %w", err)
	}
	g.srcPrefixes = prefixes

	g.inst().readbufEffective.Store(int64(effective))
	// Requested and effective SIDE BY SIDE. If they differ, the kernel capped
	// us at udp_recvspace and Q0's 0.53 s of slack -- the arithmetic NFR4's
	// budget is derived from -- is wrong by that ratio. An operator who sees
	// only the requested number has no way to know (ASM-V1).
	g.deps.Log.Infof("socket bound listen=%s requested_rcvbuf=%d effective_rcvbuf=%d "+
		"source_allow_list=%d source_enforce=%v",
		cfg.UDP.Listen, cfg.UDP.ReadBufferBytes, effective, len(prefixes), cfg.UDP.SourceEnforce)
	return nil
}

// startReader runs the receive loop: Vol 1 section 3.4's steps, in its order.
//
// Step 1, the atomic.Pointer[Runtime] snapshot (ADR-L11), is deliberately NOT
// here. It carries the decode.Registry, which arrives in P2-05; adding the load
// now would be a snapshot of something nothing reads.
//
// The envelope split and the decoders are reached through Deps.Decode on the
// DECODER goroutine, not from here. What the receiver owns is: read, classify
// the error, time it, check the source, check the size, publish.
func (g *Graph) startReader(ctx context.Context) {
	g.readerStarted = true
	go func() {
		defer close(g.readerDone)

		// Allocated once, outside the loop. Allocating per datagram is the
		// first thing that would show up as jitter at market open.
		buf := make([]byte, readBufferLen)
		var d model.Datagram
		var errs readErrorPolicy

		// Captured once. The listener does not change for the life of the
		// loop -- rebinding is startup-only (C-2) -- so re-reading it per
		// datagram would take a lock 2,400 times a second to learn the same
		// answer.
		conn := g.getConn()
		cfg := g.deps.Cfg
		in := g.inst()

		for {
			// Step 2. The only blocking call in the loop that is not bounded
			// by the stall budget, and it is SUPPOSED to block: this is how
			// the goroutine parks when the market is quiet.
			n, src, err := conn.ReadFromUDPAddrPort(buf)
			if err != nil {
				// Step 3, section 3.7.
				switch errs.classify(err, ctx) {
				case readShutdown:
					return
				case readFatal:
					in.udpReadErrors.Add(1)
					g.deps.Log.Errorf(obsClassStartup,
						"%d consecutive UDP read errors; giving up: %v", errs.consecutive, err)
					// RETURNED, not reported through Graph's terminal channel,
					// because Vol 1 section 3.4 specifies readLoop as
					// "returns ... a non-nil error on a fatal condition" and
					// Figure 1.3 draws it as "return err". The terminal
					// channel exists for goroutines the lifecycle cannot
					// join and which must report while still running -- the
					// shards and the writers. This one is joined by teardown,
					// so its error travels the way any function's does.
					g.readerErr = err
					return
				default:
					in.udpReadErrors.Add(1)
					g.deps.Log.Errorf(obsClassStartup, "UDP read error (%d consecutive): %v",
						errs.consecutive, err)
					continue
				}
			}
			errs.ok()

			// Step 4. Exactly one clock read per datagram, and it is the
			// origin of the per-iteration stall budget. RecvMono is also the
			// ordering guard's receipt clock, so it is taken ONCE, here,
			// immediately after the read -- never a wall-clock reading.
			d.RecvMono = int64(monotonicNow())

			// Step 5.
			in.datagramsReceived.Add(1)
			in.lastDatagramMonoNs.Store(d.RecvMono)

			// Step 6, section 3.5. An empty allow-list skips the check
			// entirely rather than matching against nothing.
			if len(g.srcPrefixes) > 0 && !sourceAllowed(g.srcPrefixes, src) {
				in.datagramsUnexpectedSrc.Add(1)
				g.deps.Log.Warnf(obs.ClassUnexpectedSource,
					"datagram from unexpected source %s (allow-list has %d entries, enforce=%v)",
					src, len(g.srcPrefixes), cfg.UDP.SourceEnforce)
				if cfg.UDP.SourceEnforce {
					// THE ONE PLACE FeedValve DISCARDS DATA ON PURPOSE
					// (section 3.5). Off by default and operator-armed,
					// because a wrong allow-list silently blinds a feed that
					// has no retransmission to recover from (bible section 1).
					continue
				}
			}

			// Step 7, section 3.8. Counted and logged HERE, before the copy
			// below truncates n, because this is the last point at which the
			// true length exists. Nothing framable is lost by that truncation:
			// cPackData is a fixed 512-byte field, so framing computes
			// packedLen = min(n-4, 512) and never reads past payload offset
			// 516. What is lost is a full hex dump of a datagram over 1 KiB --
			// and HLD F6's operator action is "escalate to the exchange", for
			// which the true length in this line is what matters.
			if n > wire.MaxDatagramLen {
				in.datagramsOversize.Add(1)
				g.deps.Log.Warnf(obs.ClassOversizeDatagram,
					"oversize datagram from %s: %d bytes, over the %d-byte envelope "+
						"(bible 3.2). Framing it to %d; the envelope assumption has changed -- "+
						"escalate to the exchange before trusting any decode",
					src, n, wire.MaxDatagramLen, wire.MaxPackedDataLen)
			}

			// Step 8. Truncation is a second-order consequence of the ring's
			// fixed slot, not the oversize policy, which is why it is silent
			// now that the line above is not.
			if n > len(d.Buf) {
				n = len(d.Buf)
			}
			d.N = int32(n)
			copy(d.Buf[:n], buf[:n])

			if !g.qdProducer.TryPublish(&d) {
				in.qdSendBlocked.Add(1)
			}
			in.qdDepthMax.Observe(uint64(g.qdProducer.Len()))
		}
	}()
}

// sourceAllowed reports whether src matches any configured prefix.
//
// A linear scan, because the expected list length is one or two (ASM-12: one
// exchange host) and netip.Prefix/netip.Addr are pointer-free values, so this
// is a handful of comparisons and no allocation. Unmap so a v4-mapped-v6
// source address compares against a v4 prefix the way an operator expects.
func sourceAllowed(prefixes []netip.Prefix, src netip.AddrPort) bool {
	addr := src.Addr().Unmap()
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// readOutcome is what the loop does about a read error.
type readOutcome uint8

const (
	// readContinue counts the error, logs it, and reads again.
	readContinue readOutcome = iota
	// readShutdown means the socket was closed under us on purpose.
	readShutdown
	// readFatal means maxConsecutiveReadErrors in a row.
	readFatal
)

// readErrorPolicy is Vol 1 section 3.7's rule, as a value so it can be tested
// without a socket.
//
// An interface over the connection would make the loop injectable and would
// also be an interface with exactly one production implementation and no other
// caller, which YAGNI overrules. The part worth testing is the POLICY, and the
// policy is four lines of arithmetic over a counter.
type readErrorPolicy struct {
	consecutive int
}

// classify records an error and says what to do about it.
//
// The ctx test comes FIRST and it is load-bearing: a read error while the
// context is cancelled means "shutting down", not a failure. poc logged it as a
// failure, and the resulting ERROR line at every clean stop trained operators
// to ignore the level -- which is a worse outcome than a missing line.
func (p *readErrorPolicy) classify(err error, ctx context.Context) readOutcome {
	if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
		return readShutdown
	}
	p.consecutive++
	if p.consecutive >= maxConsecutiveReadErrors {
		return readFatal
	}
	return readContinue
}

// ok resets the count. A successful read means whatever was wrong is over;
// without this the bound would be "64 errors ever" rather than "64 in a row",
// and a session with 64 unrelated transients spread across a day would stop the
// process at market close for no reason.
func (p *readErrorPolicy) ok() { p.consecutive = 0 }

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
//
// The third case is the receiver stopping on its own, which before P2-04 could
// not be observed at all: the reader goroutine returned after
// maxConsecutiveReadErrors and nothing woke this select, so the process sat
// here with no receiver, no terminal condition and no shutdown -- a silent
// zombie holding an Oracle pool open. Vol 1 section 3.7 makes persistent read
// failure terminal, and terminal means the drain runs.
func (g *Graph) wait(ctx context.Context) (fatalEvent, bool) {
	// A local copy so it can be nilled out: a closed channel is always ready,
	// so selecting on g.readerDone again after observing it would spin. A nil
	// channel in a select blocks forever, which is exactly "stop asking".
	readerDone := g.readerDone
	if !g.readerStarted {
		readerDone = nil
	}

	for {
		select {
		case <-ctx.Done():
			return fatalEvent{}, false
		case ev := <-g.fatal:
			return ev, true
		case <-readerDone:
			// Safe to read without a lock: the reader writes readerErr before
			// closing readerDone, and observing the close here is the
			// happens-before.
			if g.readerErr != nil {
				return fatalEvent{
					reason: FatalUDPReadFailure,
					detail: g.readerErr.Error(),
				}, true
			}
			// The reader stopped without an error, which means the socket was
			// closed under it -- a shutdown already under way. Keep waiting
			// for whatever asked for it.
			readerDone = nil
		}
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

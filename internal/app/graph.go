package app

import (
	"database/sql"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/framing"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// FatalReason enumerates the five terminal conditions of Vol 4 section 1.2
// exit code 4, so the shutdown line and the exit path cannot disagree about
// which one fired.
//
// They are an enumeration rather than an error string because the shutdown
// summary, the exit code and the log line all have to name the same condition,
// and a formatted string would let them drift.
type FatalReason uint8

const (
	// FatalQ1Overflow is F9 -- a shard queue send that exhausted its budget.
	// Vol 0 section 9.2, HLD section 6.6.
	FatalQ1Overflow FatalReason = iota
	// FatalDBFailFast is F13 -- the writer pool entered FailFast.
	FatalDBFailFast
	// FatalWriterPoolStop is F20 -- the two-writer statement rule stopped the
	// pool. HLD section 9.2.
	FatalWriterPoolStop
	// FatalBatchQSendFail is F21 -- a batchQ send that cannot fail by
	// construction, failed anyway. HLD section 7.3.
	FatalBatchQSendFail
	// FatalShardPanic is F26 -- a panic with an arena mid-mutation, which is
	// the one panic that cannot be contained and resumed.
	FatalShardPanic
	// FatalUDPReadFailure is Vol 1's V-7: maxConsecutiveReadErrors reads failed
	// in a row with the context still alive.
	//
	// poc logs and continues unconditionally, which on an unrecoverable socket
	// is a tight spin logging at full CPU -- worse on a 4-CPU capped partition
	// than stopping. Vol 1 section 3.7 makes it terminal instead, and terminal
	// here means the drain runs: the shards still owe their dirty rows to the
	// database, which is exactly what an os.Exit would skip.
	FatalUDPReadFailure

	numFatalReasons
)

var fatalReasonNames = [numFatalReasons]string{
	FatalQ1Overflow:     "F9-q1-overflow",
	FatalDBFailFast:     "F13-db-fail-fast",
	FatalWriterPoolStop: "F20-writer-pool-stop",
	FatalBatchQSendFail: "F21-batchq-send-failed",
	FatalShardPanic:     "F26-shard-panic",
	FatalUDPReadFailure: "V7-udp-read-failure",
}

func (r FatalReason) String() string {
	if r < numFatalReasons {
		return fatalReasonNames[r]
	}
	return "unknown-terminal-condition"
}

// fatalEvent is one terminal condition, with the detail that goes on the log
// line.
type fatalEvent struct {
	reason FatalReason
	detail string
}

// Deps is everything Build needs that the startup steps before it produced.
//
// Vol 4 section 1.7 sketches Build as taking four positional parameters; this
// is the same set plus the two loggers and the destination table, which the
// sketch omits because it predates P1-07. A struct rather than seven positional
// arguments, so a future addition cannot silently reorder them.
type Deps struct {
	Cfg   *config.Config
	Store *config.Store
	// DB is nil when the configuration declares no db object. It is a
	// *sql.DB and not an oradb-specific handle: section 1.7's sketch writes
	// *oradb.DB, but oradb exposes Open(ctx, DBSettings) (*sql.DB, error) --
	// the package is the connection POLICY (pool sizes, timeouts, DSN
	// hygiene), not a wrapper type.
	DB    *sql.DB
	Reg   obs.Registry
	Log   *obs.AppLogger
	Debug *obs.DebugLogger
	Dests *destinations.Set

	// Stderr is where the emission goes. Vol 4 section 6.7: "the entire
	// observability surface is stderr" -- there is no file, no socket and no
	// endpoint. Injected rather than taken from os.Stderr so the whole
	// lifecycle is testable in-process, which is what makes the startup and
	// drain gates possible at all.
	Stderr io.Writer

	// Inst are the instrument handles resolved at startup step 9, before the
	// registry was frozen.
	Inst instruments

	// Decode is called by the decoder goroutine for every datagram drained
	// from QD, in arrival order, on that one goroutine.
	//
	// This is the seam Phase 2 fills: framing's envelope split, the decoder
	// registry, and the fan-out onto shardCh[k] under the fan-out budget all
	// hang off it. nil means "drain and discard", which is what P1-09 ships --
	// the lifecycle owns that the consumer exists, is the only one, and drains
	// to empty on close; it does not own what the bytes mean.
	//
	// Returning a non-nil error stops the drain and fails Run, which is the
	// right shape for a decode path whose errors are either per-packet (and so
	// handled inside, by Reject) or terminal.
	Decode func(*model.Datagram) error
}

// Graph is the constructed process graph.
//
// No field is exported, because nothing outside app may hold a reference to the
// socket, the ring or a goroutine's done channel. What the tests need instead
// is the acquisition and release ORDER, which is recorded rather than exposed --
// see stages.
type Graph struct {
	deps Deps

	// QD, allocated at startup step 12 and drained at shutdown step 5a.
	qdProducer *framing.Producer
	qdConsumer *framing.Consumer

	// conn is the UDP listener, bound by Run as the LAST resource.
	//
	// Guarded by mu, because bind writes it on the lifecycle goroutine while
	// the receiver, the teardown and LocalAddr read it from others. The race
	// detector found this; nothing about it is visible in behaviour, which is
	// the whole argument for having fixed the toolchain so -race could run.
	//
	// THE CONCRETE TYPE, not net.PacketConn, and both reasons are load-bearing
	// (Vol 1 section 3.1): SetReadBuffer is not on the interface, and neither
	// is ReadFromUDPAddrPort. PacketConn.ReadFrom returns a net.Addr, which for
	// UDP is a heap-allocated *net.UDPAddr carrying a []byte IP -- per
	// datagram, 2,400 times a second, on the one goroutine ADR-006 keeps
	// allocation-free.
	conn *net.UDPConn

	// runtime is the published configuration-and-registry snapshot (ADR-L11),
	// loaded once per datagram by the decoder goroutine. Written by Build
	// before anything starts, and from Phase 5 by the SIGHUP path.
	runtime atomic.Pointer[Runtime]

	// readerErr is the receiver's terminal error, if it had one: written by
	// the reader goroutine immediately before it closes readerDone, read by
	// wait and by nothing else. The channel close is the happens-before, which
	// is why this needs no lock (Vol 1 section 3.4: readLoop "returns a
	// non-nil error on a fatal condition").
	readerErr error

	// srcPrefixes is udp.sourceAllowList, parsed once by bind and read only by
	// the receiver goroutine afterwards (Vol 1 section 3.5).
	//
	// Needs no lock, and the reason is the ordering rather than the type: bind
	// writes it on the lifecycle goroutine BEFORE startReader launches the
	// goroutine that reads it, and the go statement is the happens-before.
	// Nothing writes it again -- udp.listen and its allow-list are
	// startup-only (C-2, C-3).
	srcPrefixes []netip.Prefix

	// fatal carries the first terminal condition. Capacity 1 and guarded by
	// fatalOnce, so a goroutine reporting a terminal condition never blocks
	// even if the lifecycle has already begun draining for another reason.
	fatal     chan fatalEvent
	fatalOnce sync.Once

	readerDone  chan struct{}
	decoderDone chan struct{}
	statsDone   chan struct{}
	statsStop   chan struct{}

	// Whether each goroutine actually started, so teardown waits only on
	// channels something will close. A bind failure leaves the reader
	// unstarted, and waiting on its done channel would hang the error path --
	// which is the shape gate G-LIFE-3 exercises.
	readerStarted  bool
	decoderStarted bool
	statsStarted   bool

	teardownOnce sync.Once

	// lastParks converts the ring's cumulative park count into the delta
	// qd_wake_parked wants. The ring exposes a plain number because the
	// ledger assigns that instrument to the decoder, not to framing.
	lastParks uint64

	// stages records every resource acquisition and release, in order.
	//
	// This exists because the P1-09 card asks for "every deferred cleanup
	// observed to run (assert it, don't eyeball it)", and because gate
	// G-LIFE-1 is an assertion about ORDER -- that the socket is bound after
	// the pre-allocation and after the goroutines start. Neither is checkable
	// from the outside without a record, and a test that inspected real defers
	// would have to be the process.
	//
	// The record is in addition to real defers, never instead of them: a panic
	// must still unwind the resources, which only a defer does.
	mu       sync.Mutex
	acquired []string
	released []string

	drainStarted time.Time
}

// Build performs the startup steps that allocate but do not yet run: Vol 4
// section 1.4 step 10 (immutable derived state) and step 12 (pre-allocation).
//
// It deliberately starts NO goroutine and binds NO socket. The sketch in
// section 1.7 has Build starting the writer and shard goroutines, which do not
// exist until Phase 3; keeping Build allocation-only means a failure between
// Build and Run has nothing running to stop, which is what makes the unwind in
// gate G-LIFE-3 simple enough to be obviously correct. When Phase 3 adds those
// pools the same argument will apply to them: start them in Run, where the
// teardown that must stop them also lives.
//
// Step 11 -- OpenDB and its deferred Close -- happens in run before this,
// because the one-shot -dbcheck mode needs the connection without a graph.
func Build(d Deps) (*Graph, error) {
	if d.Cfg == nil {
		return nil, exitf(ExitStartup, "internal: Build with no configuration")
	}
	if d.Reg == nil {
		return nil, exitf(ExitStartup, "internal: Build with no instrument registry; "+
			"startup step 9 must resolve every instrument before the graph is built")
	}
	if d.Log == nil {
		return nil, exitf(ExitStartup, "internal: Build with no logger")
	}

	g := &Graph{
		deps:        d,
		fatal:       make(chan fatalEvent, 1),
		readerDone:  make(chan struct{}),
		decoderDone: make(chan struct{}),
		statsDone:   make(chan struct{}),
		statsStop:   make(chan struct{}),
	}

	// Step 10: the immutable derived state. The destination table, the ColMask
	// bit assignment, the image offsets and the contributor slots were all
	// resolved by loadAll and validated together with the configuration; this
	// records the dependency rather than redoing it. The decoder registry and
	// the policy map join it in Phase 2.
	if d.Dests == nil || d.Dests.Len() == 0 {
		return nil, exitf(ExitStartup, "internal: Build with no destination table")
	}

	// The decoder registry: level 2 of Vol 1 section 8.1, resolved against the
	// catalogue and the destination table. This is where rules V-T1 and V-T6
	// are evaluated -- the two config.Unchecked() named as needing the
	// registry since P1-06 -- so a configuration naming a transcode this build
	// cannot decode fails HERE, at startup, rather than by storing nothing for
	// it in production.
	// The decoders' counter sink, constructed BEFORE the registry because
	// BuildRegistry needs it to construct the decoders, and given the registry
	// immediately after -- which is the only order that works and the reason
	// setRegistry exists.
	d.Inst.log = d.Log
	cnt := newDecodeCounters(&d.Inst)
	reg, err := decode.BuildRegistry(transcodeSpecs(d.Cfg), d.Dests, cnt)
	if err != nil {
		return nil, exitf(ExitStartup, "building the decoder registry: %w", registryError(err))
	}

	// Sized from the CONFIGURED inventory, which is what obs sized its {code}
	// dimension from (obsOptions) -- not from reg.Len(), even though a
	// successful build makes them equal. They must be the same number or a
	// per-transcode counter would be resolved at an index obs never allocated,
	// and the two are derived from the same configuration by different paths,
	// so the agreement is asserted rather than assumed.
	if reg.Len() != len(d.Cfg.Transcodes) {
		return nil, exitf(ExitStartup, "internal: the registry resolved %d of %d configured "+
			"transcodes; every per-transcode counter is indexed by that position",
			reg.Len(), len(d.Cfg.Transcodes))
	}
	d.Inst.resolveTranscodeInstruments(d.Reg, len(d.Cfg.Transcodes))
	cnt.setRegistry(reg)
	g.deps = d

	// Published BEFORE any goroutine exists, which is what makes the decoder's
	// load unconditional rather than a nil check it has to repeat (ADR-L11).
	g.setRuntime(&Runtime{Cfg: d.Cfg, Reg: reg, Dests: d.Dests})
	g.mark(&g.acquired, "derived-state")

	// Step 12: pre-allocation, BEFORE any socket exists. A
	// first-datagram-triggered allocation of tens of megabytes would blow the
	// stall budget on packet one at market open, and an out-of-memory
	// condition must be a startup failure rather than a 10:00:00 failure.
	//
	// QD is the part of step 12 that exists today: one allocation of
	// capacity x 1040 bytes, pointer-free. The arenas, the shard indexes and
	// the batch free lists arrive with Phase 3.
	p, c, err := framing.NewRing(d.Cfg.Runtime.DatagramQueueCapacity)
	if err != nil {
		return nil, exitf(ExitStartup, "allocating the datagram ring at capacity %d: %w",
			d.Cfg.Runtime.DatagramQueueCapacity, err)
	}
	g.qdProducer, g.qdConsumer = p, c
	g.mark(&g.acquired, "qd-ring")

	return g, nil
}

// Fatal is how any goroutine reports a terminal condition.
//
// One-shot: the first caller wins, later callers are no-ops, and the reason of
// the first is what reaches the shutdown line and the exit code. That matters
// because one condition commonly causes another -- a database fail-fast stops
// the writer pool -- and the operator needs the condition that started it.
//
// It NEVER calls os.Exit. os.Exit skips every defer, which would leave the
// Oracle pool open and the drain unrun; this is the regression CLAUDE.md is
// written about. Fatal delivers to a channel and lets Run drain and return.
func (g *Graph) Fatal(reason FatalReason, detail string) {
	g.fatalOnce.Do(func() {
		g.fatal <- fatalEvent{reason: reason, detail: detail}
	})
}

// mark records a stage transition. The caller passes the slice to append to so
// acquisition and release share one lock and one ordering.
func (g *Graph) mark(into *[]string, name string) {
	g.mu.Lock()
	*into = append(*into, name)
	g.mu.Unlock()
}

// Acquired returns the resources the graph took, in order. For the ordering
// gate; not part of the running process's behaviour.
func (g *Graph) Acquired() []string { return g.snapshot(&g.acquired) }

// Released returns the resources the graph gave back, in order.
func (g *Graph) Released() []string { return g.snapshot(&g.released) }

func (g *Graph) snapshot(from *[]string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), (*from)...)
}

// inst returns the lifecycle's instrument handles, resolved before Freeze and
// carried in Deps.
func (g *Graph) inst() *instruments { return &g.deps.Inst }

// LocalAddr is the address the listener actually bound. It differs from the
// configured one whenever the configuration asks for port 0, which is how the
// tests get a free port without racing another process for a fixed one.
func (g *Graph) LocalAddr() net.Addr {
	c := g.getConn()
	if c == nil {
		return nil
	}
	return c.LocalAddr()
}

// setConn and getConn keep every access to the listener under one lock. The
// listener is written once, by bind, and read by the receiver, the teardown and
// LocalAddr -- three goroutines, so "written once" is not on its own enough.
func (g *Graph) setConn(c *net.UDPConn) {
	g.mu.Lock()
	g.conn = c
	g.mu.Unlock()
}

func (g *Graph) getConn() *net.UDPConn {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.conn
}

// gapReports is the gap detector's sink. decodeCounters implements both it and
// decode.Counters, because app holds every handle either needs.
func (g *Graph) gapReports() decode.Reports {
	g.deps.Inst.log = g.deps.Log
	return newDecodeCounters(&g.deps.Inst)
}

// unknownSlot is obs's dynamic code table, reached through the one accessor
// that returns an INDEX rather than a counter (Vol 1 section 7.7).
func (g *Graph) unknownSlot(code int) (int, bool) {
	type slotResolver interface {
		UnknownTranscodeSlot(code int) (int, bool)
	}
	if r, ok := g.deps.Reg.(slotResolver); ok {
		return r.UnknownTranscodeSlot(code)
	}
	return -1, false
}

// Len is the number of datagrams currently queued in QD. For diagnostics and
// for the drain assertion; the ring's own Len is exact only from the producer
// or the consumer goroutine.
func (g *Graph) Len() int {
	if g.qdConsumer == nil {
		return 0
	}
	return g.qdConsumer.Len()
}

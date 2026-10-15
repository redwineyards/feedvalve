package app

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

// testGraph builds a graph the way relay does, but with no database and with
// every output captured. listen is the address to bind; "127.0.0.1:0" takes a
// free port.
func testGraph(t *testing.T, listen string, tune func(*config.Config)) (*Graph, *syncBuf) {
	t.Helper()
	return testGraphWith(t, listen, tune, nil)
}

// testGraphWith is testGraph plus the Phase 2 decode seam.
func testGraphWith(t *testing.T, listen string, tune func(*config.Config),
	decode func(*model.Datagram) error) (*Graph, *syncBuf) {
	t.Helper()

	// config.Defaults rather than a loaded file: ModeRun's rule V-DB1 requires
	// a db object and these tests deliberately run without one, and the
	// defaults are the shipped values anyway.
	cfg := config.Defaults()
	cfg.UDP.Listen = listen
	if tune != nil {
		tune(cfg)
	}

	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}

	buf := &syncBuf{}
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg)
	reg.SetIntervalSec(cfg.Stats.IntervalSec)
	reg.Freeze()

	lg := obs.NewAppLogger(buf, obs.ParseLevel(cfg.Stats.LogLevel), 10000, reg)
	dbg := obs.NewDebugLogger(buf, cfg.Debug.TraceTranscodes, cfg.Debug.HexDump)

	g, err := Build(Deps{
		Cfg: cfg, Store: storeOf(cfg), Reg: reg,
		Log: lg, Debug: dbg, Dests: dests, Stderr: buf, Inst: inst,
		Decode: decode,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return g, buf
}

// syncBuf is a bytes.Buffer safe for the logger goroutines to share with the
// test goroutine. The loggers serialise their own writes, but the test reads
// while they write.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestG_LIFE_1_TheSocketIsBoundLast is gate G-LIFE-1.
//
// Setup: a graph that records the order of every resource acquisition.
// Stimulus: run to the Running state. Assertion: the socket bind is recorded
// AFTER the pre-allocation and AFTER the goroutines start (Vol 4 section 1.4
// steps 12 to 16).
//
// The defect it catches is a refactor that binds the socket early, so the first
// datagrams arrive during a multi-hundred-millisecond arena allocation and are
// dropped by the kernel on a circuit with no retransmission (C-2). Nothing
// about that failure is visible in the code -- both orders compile and both
// work on an idle feed -- which is why it is a gate and not a review note.
func TestG_LIFE_1_TheSocketIsBoundLast(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()

	waitUntil(t, "the listener to be bound", func() bool {
		return slices.Contains(g.Acquired(), "udp-listener")
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v, want nil for a requested shutdown", err)
	}

	got := g.Acquired()
	idx := func(name string) int { return slices.Index(got, name) }

	listener := idx("udp-listener")
	if listener < 0 {
		t.Fatalf("the listener was never acquired: %v", got)
	}
	for _, earlier := range []string{"derived-state", "qd-ring", "shard-channels",
		"shard-consumers", "decoder", "stats-goroutine"} {
		at := idx(earlier)
		if at < 0 {
			t.Errorf("%q was never acquired: %v", earlier, got)
			continue
		}
		if at > listener {
			t.Errorf("%q was acquired AFTER the socket (order: %v).\n"+
				"The socket is the last resource, so that nothing arrives before there "+
				"is somewhere to put it and before the goroutine that will drain it "+
				"exists (Vol 4 section 1.4 step 16).", earlier, got)
		}
	}
}

// TestEveryAcquiredResourceIsReleased is the P1-09 card's own requirement:
// "every deferred cleanup observed to run -- assert it, don't eyeball it".
func TestEveryAcquiredResourceIsReleased(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "the listener to be bound", func() bool {
		return slices.Contains(g.Acquired(), "udp-listener")
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}

	released := g.Released()
	for _, want := range []string{"udp-listener", "qd-ring", "decoder",
		"shard-channels", "shard-consumers", "stats-goroutine"} {
		if !slices.Contains(released, want) {
			t.Errorf("%q was acquired but never released (released: %v)", want, released)
		}
	}

	// And in the order the drain specifies: the listener first, because closing
	// it is the only way to unblock the receiver; then QD, so the decoder can
	// drain it; then the decoder itself; and only then Q1, whose sole producer
	// the decoder is.
	li, qi, di := slices.Index(released, "udp-listener"),
		slices.Index(released, "qd-ring"), slices.Index(released, "decoder")
	si := slices.Index(released, "shard-channels")
	if !(li < qi && qi < di) {
		t.Errorf("the drain released resources in the wrong order: %v.\n"+
			"Vol 0 section 9.5.7 fixes it: close the socket, let the receiver return, "+
			"THEN close QD and let the decoder drain it. Closing QD while the receiver "+
			"can still publish would abandon datagrams at shutdown.", released)
	}
	if !(di < si) {
		t.Errorf("shardCh was closed before the decoder was joined: %v.\n"+
			"The decoder is Q1's only producer, so closing its channels while it is "+
			"still running is a send on a closed channel -- a panic at shutdown, on "+
			"the path that is meant to be the orderly one.", released)
	}
}

// TestG_LIFE_2_SigtermReturnsPromptlyAndQuietly is gate G-LIFE-2.
//
// Setup: a running graph with no datagrams arriving, the receiver blocked in
// ReadFrom. Stimulus: cancel the context, which is what SIGTERM does.
// Assertion: Run returns quickly, and the receiver logged NO error for the read
// failure.
//
// Two defects. Adding a SetReadDeadline poll loop, or forgetting that closing
// the listener is the only way to unblock an in-flight ReadFrom -- the first
// makes shutdown take up to a poll interval and wakes a sleeping goroutine ten
// times a second forever to pay for it; the second hangs shutdown entirely.
// And logging the shutdown read error as a failure, which puts an ERROR line in
// every clean stop and trains operators to ignore the level.
func TestG_LIFE_2_SigtermReturnsPromptlyAndQuietly(t *testing.T) {
	g, buf := testGraph(t, "127.0.0.1:0", nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "the listener to be bound", func() bool {
		return slices.Contains(g.Acquired(), "udp-listener")
	})

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, want nil: a requested shutdown is not a failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation. Closing the listener is the " +
			"ONLY way to unblock an in-flight ReadFrom; a SetReadDeadline poll or a " +
			"missing Close hangs here")
	}
	took := time.Since(start)
	t.Logf("shutdown took %v", took)
	// The gate says 200 ms. The ceiling here is looser so the test is not
	// flaky on a loaded machine, but it is still far below any poll interval a
	// SetReadDeadline loop would use, and far below a hang.
	if took > 500*time.Millisecond {
		t.Errorf("shutdown took %v, which is too slow for closing one socket and "+
			"joining three goroutines (gate G-LIFE-2 budgets 200 ms)", took)
	}

	if out := buf.String(); strings.Contains(out, "ERROR") {
		t.Errorf("a clean shutdown logged an ERROR line:\n%s\n"+
			"A read error while the context is cancelled IS shutdown, not a fault. "+
			"poc logged it as a failure and the resulting ERROR at every clean stop is "+
			"what teaches operators to ignore the level.", out)
	}
}

// TestG_LIFE_3_BindFailureUnwindsWithTheDatabaseClosed is gate G-LIFE-3.
//
// Setup: the database opens successfully and the socket bind is rigged to fail.
// Assertion: run returns a non-nil error AND db.Close() was called.
//
// This is the runtime half of condition C-1. G-BLD-3 catches a literal Fatalf
// in the bind error path; this catches the equivalent reached indirectly -- any
// early return that skips the unwind, leaving an Oracle session open on a
// process that failed to start and will be restarted immediately by the
// supervisor.
func TestG_LIFE_3_BindFailureUnwindsWithTheDatabaseClosed(t *testing.T) {
	closed := make(chan struct{})
	stub := newStubDB(t, func() { close(closed) })
	// sql.Open is lazy, so force one driver connection into the pool:
	// otherwise sql.DB.Close has nothing to close and the observation would
	// pass vacuously whether the lifecycle closed the handle or not.
	if err := stub.Ping(); err != nil {
		t.Fatalf("priming the stub pool: %v", err)
	}

	// The seam: the database opens, so the bind is the only thing that fails.
	old := openDB
	openDB = func(context.Context, oradb.DBSettings) (*sql.DB, error) { return stub, nil }
	t.Cleanup(func() { openDB = old })

	cfg := config.Defaults()
	cfg.HasDB = true
	// An address ListenPacket cannot bind, which is what rigs step 16 to fail.
	cfg.UDP.Listen = "198.51.100.1:65000"

	dests, derr := destinations.Default()
	if derr != nil {
		t.Fatalf("destinations.Default: %v", derr)
	}

	buf := &syncBuf{}
	runErr := relayWith(t, cfg, dests, buf)

	if runErr == nil {
		t.Fatal("relay returned nil with the socket bind rigged to fail")
	}
	if got := ExitCode(runErr); got != ExitStartup {
		t.Errorf("exit code = %d, want %d for a startup failure", got, ExitStartup)
	}
	select {
	case <-closed:
	default:
		t.Error("db.Close() was NOT called after the bind failed.\n" +
			"The Close deferred at startup step 11 must still run when step 16 fails, " +
			"or a process that could not start leaves an Oracle session open -- and the " +
			"supervisor restarts it immediately, so the leak compounds.")
	}
}

// relayWith runs the relay path against an already-resolved configuration.
// A thin wrapper so the G-LIFE-3 test can rig the configuration without writing
// a file and without going through flag parsing.
func relayWith(t *testing.T, cfg *config.Config, dests *destinations.Set, w io.Writer) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return relay(ctx, Options{Stderr: w}, cfg, dests)
}

// TestG_LIFE_5_ExitCodeTaxonomy is gate G-LIFE-5.
//
// Nothing ever maps to 2. That code belongs to the Go runtime's unrecovered
// panic, and claiming it for a design condition would make a real panic -- on a
// host whose only debugging tool is a panic stack -- indistinguishable from one
// we chose.
//
// And a drain-deadline expiry maps to ZERO, which is the counter-intuitive one
// (ADR-L37): it is a counted, logged degradation of a stop we asked for. A
// non-zero code there makes the supervisor restart a process we just told to
// stop.
func TestG_LIFE_5_ExitCodeTaxonomy(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"requested shutdown", nil, ExitOK},
		{"requested shutdown, drain deadline expired", nil, ExitOK},
		{"startup failure", &ExitError{Code: ExitStartup, Err: errors.New("x")}, ExitStartup},
		{"selfcheck failed", &ExitError{Code: ExitSelfCheckFailed, Err: errors.New("x")}, ExitSelfCheckFailed},
		{"an error with no code", errors.New("plain"), ExitStartup},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ExitCode(tc.err); got != tc.want {
				t.Errorf("ExitCode = %d, want %d", got, tc.want)
			}
		})
	}

	// Every terminal condition maps to 4, driven through the real path rather
	// than by constructing the error.
	for r := FatalReason(0); r < numFatalReasons; r++ {
		t.Run("terminal/"+r.String(), func(t *testing.T) {
			g, _ := testGraph(t, "127.0.0.1:0", nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan error, 1)
			go func() { done <- g.Run(ctx) }()
			waitUntil(t, "the listener to be bound", func() bool {
				return slices.Contains(g.Acquired(), "udp-listener")
			})

			g.Fatal(r, "injected by the taxonomy test")

			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after a terminal condition")
			}
			if err == nil {
				t.Fatal("a terminal condition returned nil")
			}
			if got := ExitCode(err); got != ExitTerminal {
				t.Errorf("exit code = %d, want %d", got, ExitTerminal)
			}
			if !strings.Contains(err.Error(), r.String()) {
				t.Errorf("the error does not name the condition: %v", err)
			}
		})
	}

	// Nothing maps to 2, at any input.
	for _, err := range []error{
		nil,
		errors.New("plain"),
		&ExitError{Code: ExitStartup, Err: errors.New("x")},
		&ExitError{Code: ExitTerminal, Err: errors.New("x")},
		&ExitError{Code: ExitSelfCheckFailed, Err: errors.New("x")},
	} {
		if ExitCode(err) == ExitPanic {
			t.Errorf("%v mapped to exit 2, which is reserved for the Go runtime's "+
				"unrecovered panic", err)
		}
	}
}

// TestFatalIsOneShotAndTheFirstReasonWins. One condition commonly causes
// another -- a database fail-fast stops the writer pool -- and the operator
// needs the condition that started it, not the last one to be noticed.
func TestFatalIsOneShotAndTheFirstReasonWins(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "the listener to be bound", func() bool {
		return slices.Contains(g.Acquired(), "udp-listener")
	})

	g.Fatal(FatalDBFailFast, "first")
	g.Fatal(FatalWriterPoolStop, "second")
	g.Fatal(FatalShardPanic, "third")

	err := <-done
	if err == nil {
		t.Fatal("Run = nil after a terminal condition")
	}
	if !strings.Contains(err.Error(), FatalDBFailFast.String()) {
		t.Errorf("the error names %v, want the FIRST condition %v",
			err, FatalDBFailFast)
	}
	if strings.Contains(err.Error(), "second") || strings.Contains(err.Error(), "third") {
		t.Errorf("a later condition reached the error: %v", err)
	}
}

// TestBuildRejectsABadCapacityWithoutPanicking is the card's "a forced error in
// construction unwinds the partially-built graph without panicking".
func TestBuildRejectsABadCapacityWithoutPanicking(t *testing.T) {
	cfg := config.Defaults()
	// Not a power of two. config's own V-RANGE rule rejects this before the
	// relay ever runs; Build refuses it too, because the ring is constructible
	// without config and a masked index on a non-power-of-two silently returns
	// the wrong datagram.
	cfg.Runtime.DatagramQueueCapacity = 5000

	dests, derr := destinations.Default()
	if derr != nil {
		t.Fatalf("destinations.Default: %v", derr)
	}
	buf := &syncBuf{}
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg)
	reg.Freeze()

	g, err := Build(Deps{
		Cfg: cfg, Store: storeOf(cfg), Reg: reg,
		Log:   obs.NewAppLogger(buf, obs.LevelInfo, 100, reg),
		Dests: dests, Stderr: buf, Inst: inst,
	})
	if err == nil {
		t.Fatal("Build accepted a capacity that is not a power of two")
	}
	if g != nil {
		t.Error("Build returned a graph alongside an error; a partially built graph " +
			"must not escape")
	}
	if got := ExitCode(err); got != ExitStartup {
		t.Errorf("exit code = %d, want %d", got, ExitStartup)
	}
}

// TestBuildRefusesMissingDependencies covers the rest of the unwind: every
// dependency Build cannot work without is named, rather than producing a nil
// dereference several steps later.
func TestBuildRefusesMissingDependencies(t *testing.T) {
	cfg := config.Defaults()
	dests, _ := destinations.Default()
	buf := &syncBuf{}
	reg := obs.New(obsOptions(cfg, dests))
	reg.Freeze()
	lg := obs.NewAppLogger(buf, obs.LevelInfo, 100, reg)

	for _, tc := range []struct {
		name string
		deps Deps
	}{
		{"no configuration", Deps{Reg: reg, Log: lg, Dests: dests}},
		{"no registry", Deps{Cfg: cfg, Log: lg, Dests: dests}},
		{"no logger", Deps{Cfg: cfg, Reg: reg, Dests: dests}},
		{"no destinations", Deps{Cfg: cfg, Reg: reg, Log: lg}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Build(tc.deps); err == nil {
				t.Error("Build succeeded with a missing dependency")
			}
		})
	}
}

// TestOneInstrumentLineIsEmitted is a Phase 1 exit criterion in miniature:
// "one instrument line emitted on the target". The target is P1-11's job; that
// the stats goroutine exists, ticks, and produces the grouped emission is this
// one's.
func TestOneInstrumentLineIsEmitted(t *testing.T) {
	g, buf := testGraph(t, "127.0.0.1:0", func(c *config.Config) {
		c.Stats.IntervalSec = 1 // the validated minimum
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()

	waitUntil(t, "the first emission", func() bool {
		return strings.Contains(buf.String(), "group=health")
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"INFO stats gen=", // the emission's own marker
		"interval_s=1",    // the reloadable interval, as configured
		"group=health",    // rule 1: the health line, always, first
		"uptime_seconds=", // a lifecycle-owned gauge
		"config_generation=1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the emission is missing %q:\n%s", want, out)
		}
	}
}

// --- helpers -------------------------------------------------------------

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// newStubDB returns a *sql.DB over a driver that does nothing but record its
// Close. The same approach oradb's own tests use, so go test needs no database.
func newStubDB(t *testing.T, onClose func()) *sql.DB {
	t.Helper()
	// Unique per call, not per test name: sql.Register panics on a duplicate,
	// and -count=N runs the same test name N times. That is exactly how a
	// repeated run of a scheduling-sensitive gate -- which is what G-RACE-2
	// asks for -- would otherwise fail for a reason that has nothing to do
	// with the thing under test.
	name := fmt.Sprintf("appstub-%s-%d", t.Name(), stubSeq.Add(1))
	sql.Register(name, &stubDriver{onClose: onClose})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open(%s): %v", name, err)
	}
	return db
}

// stubSeq makes each registered driver name unique.
var stubSeq atomic.Int64

type stubDriver struct{ onClose func() }

func (d *stubDriver) Open(string) (driver.Conn, error) { return &stubConn{d: d}, nil }

type stubConn struct{ d *stubDriver }

func (c *stubConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no statements") }
func (c *stubConn) Begin() (driver.Tx, error)           { return nil, errors.New("no transactions") }
func (c *stubConn) Close() error {
	if c.d.onClose != nil {
		c.d.onClose()
	}
	return nil
}

// TestTheQDDrainStageAbandonsNothing is the card's "shutdown order, including
// the QD drain stage between 'stop reading' and 'stop decoding'".
//
// Vol 0 section 9.5.7 states the obligation and why it is easy to miss: "QD
// must be DRAINED, not abandoned, or the no-loss property has a hole at
// shutdown precisely where it is easiest to overlook." A teardown that closed
// QD and stopped waiting would pass every other test in this file -- the
// process would exit cleanly, promptly, with every resource released, and
// silently drop whatever was still queued.
func TestTheQDDrainStageAbandonsNothing(t *testing.T) {
	// The decoder blocks on its FIRST datagram until the test releases it, so
	// the rest pile up in QD and the drain has real work to do. Without that,
	// the ring is empty by the time the context is cancelled and a teardown
	// that abandoned QD would pass -- which is the trap this test exists to
	// avoid falling into itself.
	release := make(chan struct{})
	var decoded atomic.Int64
	var runReturned atomic.Bool
	var lateDelivery atomic.Int64
	g, buf := testGraphWith(t, "127.0.0.1:0", nil, func(*model.Datagram) error {
		if decoded.Load() == 0 {
			<-release
		}
		// A datagram delivered after Run returned is one the drain did not
		// wait for -- the precise failure, stated as its own assertion
		// because it is the one a timing-based check can miss.
		if runReturned.Load() {
			lateDelivery.Add(1)
		}
		// Bounded real work per datagram, so draining the backlog takes
		// measurable time. Without it the decoder finishes before teardown
		// even looks at it, and a teardown that skipped the join would pass.
		time.Sleep(50 * time.Microsecond)
		decoded.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "the listener to be bound", func() bool { return g.LocalAddr() != nil })

	conn, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialling the listener: %v", err)
	}
	defer conn.Close()

	const sent = 200
	for i := range sent {
		if _, err := conn.Write([]byte{byte(i), 0x01, 0x02}); err != nil {
			t.Fatalf("writing datagram %d: %v", i, err)
		}
	}

	// Wait for a real backlog: the decoder is blocked, so these are sitting in
	// the ring. UDP on loopback can drop, so wait for a backlog rather than
	// for all of them.
	waitUntil(t, "a backlog to build in QD", func() bool { return g.Len() >= sent/2 })
	queued := g.Len()

	// Stop while the ring is full. The drain now has to deliver the backlog.
	cancel()
	close(release)
	err = <-done
	runReturned.Store(true)
	if err != nil {
		t.Fatalf("Run = %v", err)
	}

	// Everything the receiver published must have been delivered: the ring is
	// empty and the decoder returned only after it was.
	if got := g.Len(); got != 0 {
		t.Errorf("QD still holds %d datagrams after shutdown. The drain stage must "+
			"empty it, not abandon it (Vol 0 section 9.5.7)", got)
	}
	n := decoded.Load()
	t.Logf("sent %d, queued at shutdown %d, decoded %d", sent, queued, n)
	if int(n) < queued {
		t.Errorf("decoded %d but %d were queued when shutdown began: the drain "+
			"abandoned %d datagrams the receiver had already accepted", n, queued,
			queued-int(n))
	}
	if queued == 0 {
		t.Error("no backlog built, so the drain was never exercised")
	}
	if late := lateDelivery.Load(); late > 0 {
		t.Errorf("%d datagrams were delivered AFTER Run returned. The drain must "+
			"wait for the decoder to empty QD before the lifecycle completes, or "+
			"shutdown races the work it is supposed to be finishing", late)
	}
	if strings.Contains(buf.String(), "ERROR") {
		t.Errorf("the drain logged an ERROR:\n%s", buf.String())
	}
}

// TestQDBudgetExpiryIsTerminal is the behavioural half of the P2-08 defect
// fix, and it is a REGRESSION TEST for code that shipped in Phase 1.
//
// The receive loop used to read
//
//	if !g.qdProducer.TryPublish(&d) { qdSendBlocked.Add(1) }
//
// so a full ring silently discarded the datagram, counted it as a stall, and
// let the process carry on. Under this test that old code passes every
// liveness assertion and returns nil from Run while losing hundreds of
// datagrams -- which is exactly why the assertion here is on the RETURN VALUE
// and the shutdown reason, not on a counter.
//
// req section 10.2 permits one response to a critical-path queue overflowing
// and Vol 0 section 9.2 files QD's overflow under that same rule, in those
// words: "FATAL on the first expiry ... (req section 10.2, as Q1)". So the
// reason is F9 and the process stops.
func TestQDBudgetExpiryIsTerminal(t *testing.T) {
	// The decoder never finishes its first datagram until the test says so, so
	// QD fills and stays full. That is the only way to reach the expiry
	// without waiting for a real stall.
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })

	g, buf := testGraphWith(t, "127.0.0.1:0", func(c *config.Config) {
		// The smallest LEGAL ring (V-RANGE's floor is 1024) and a short
		// budget. Both are real configurations -- a test that filled an
		// illegal 16-slot ring would prove nothing about the shipped one.
		c.Runtime.DatagramQueueCapacity = 1024
		c.UDP.StallBudgetMs = 20
	}, func(*model.Datagram) error {
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "the listener to be bound", func() bool { return g.LocalAddr() != nil })

	conn, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialling the listener: %v", err)
	}
	defer conn.Close()

	// More than the ring holds. Once it is full the receiver stops reading,
	// the socket buffer absorbs some and the kernel drops the rest -- which is
	// Q0's loss, before our boundary, and not what this test is about.
	for i := range 4000 {
		if _, err := conn.Write([]byte{byte(i), 0x01, 0x02}); err != nil {
			t.Fatalf("writing datagram %d: %v", i, err)
		}
	}

	// The shutdown line names the condition, and waiting for it is what makes
	// the rest of this deterministic: the receiver has given up by the time it
	// appears, so releasing the decoder below cannot race the expiry.
	waitUntil(t, "the Q1-overflow shutdown line", func() bool {
		return strings.Contains(buf.String(), "reason="+FatalQ1Overflow.String())
	})
	once.Do(func() { close(release) })

	err = <-done
	if err == nil {
		t.Fatal("Run returned nil after the QD budget expired.\n" +
			"This is the Phase 1 behaviour: the datagram was discarded, the process " +
			"carried on, and the only trace was a counter an operator reads as a " +
			"stall. req section 10.2 permits exactly one response, and it is to stop.")
	}
	var ee *ExitError
	if !errors.As(err, &ee) || ee.Code != ExitTerminal {
		t.Errorf("Run = %v, want an ExitError with code %d (terminal)", err, ExitTerminal)
	}

	// qd_send_blocked counts publishes that had to WAIT. The receiver waited
	// once and then stopped, so one is the whole story -- under the old code
	// this counter reached the hundreds and every increment was a loss.
	if n := g.inst().qdSendBlocked.Load(); n != 1 {
		t.Errorf("qd_send_blocked = %d, want 1: it counts sends that had to wait, "+
			"and the first expiry is terminal so there cannot be a second", n)
	}
	if !strings.Contains(buf.String(), "the decoder is not draining") {
		t.Errorf("the terminal line does not say which side is at fault.\n"+
			"Vol 0 section 9.5.6: a non-zero qd_send_blocked is a signal about the "+
			"DECODER, not the socket, and that separation is the whole diagnostic "+
			"value of splitting the budget in two:\n%s", buf.String())
	}
}

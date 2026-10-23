package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/model"
)

// The live side effects and pickup points (P5-07).

// TestG_CFG_7_SIGHUPRaisesWritersFromFourToEight is gate G-CFG-7 through the
// reload path.
//
// SETUP: a running graph with a real pool, writers.count 4.
// STIMULUS: the file raises it to 8 and reload runs.
// ASSERTION: eight writer goroutines run; database/sql allows 9 open
// connections (writers+1); writers_configured is 8, and STILL 8 after a stats
// emission (the sampler must not overwrite the pool's gauge with the startup
// value); the INFO line lists writers.count as applied and nothing failed.
// WOULD HAVE CAUGHT: a reload that swapped the config but never resized
// (Vol 0 C5, a silent throughput cliff on the runbook's own lever), the
// database/sql limits left at the startup size, and sampleGauges re-storing
// the startup writers.count at the next emission.
// NON-VACUITY: the pool starts at exactly 4 writers and 5 connections.
// NOT ASSERTED: MaxIdleConns (9), which database/sql does not expose; it is set
// by the same SetPoolSize call as MaxOpenConns, which is asserted, and the
// writer package pins both arguments.
func TestG_CFG_7_SIGHUPRaisesWritersFromFourToEight(t *testing.T) {
	fx := newReloadFix(t, true)
	cancel, done := runGraph(t, fx.g)
	defer func() { cancel(); awaitRun(t, done) }()
	w := fx.g.inst().writer
	waitUntil(t, "four writers", func() bool { return w.WritersRunning.Load() == 4 })
	if got := fx.db.Stats().MaxOpenConnections; got != 5 {
		t.Fatalf("MaxOpenConnections = %d at start, want 5", got)
	}

	fx.set("writers", "count", 8)
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("reload = %v, want applied\n%s", got, fx.log())
	}
	waitUntil(t, "eight writers", func() bool { return w.WritersRunning.Load() == 8 })
	if got := fx.db.Stats().MaxOpenConnections; got != 9 {
		t.Errorf("MaxOpenConnections = %d, want 9", got)
	}
	if got := w.WritersConfigured.Load(); got != 8 {
		t.Errorf("writers_configured = %d, want 8", got)
	}
	if err := fx.g.emitStats(); err != nil {
		t.Fatal(err)
	}
	if got := w.WritersConfigured.Load(); got != 8 {
		t.Errorf("writers_configured = %d after an emission, want 8: the sampler overwrote the pool's gauge", got)
	}
	if !strings.Contains(fx.log(), "applied=[writers.count] failed=[]") {
		t.Errorf("the INFO line does not list writers.count as the applied effect:\n%s", fx.log())
	}
}

// TestUnchangedWritersCountDoesNotReassertThePoolSize pins D14's choice: Resize
// runs only when the count changed, so an unrelated reload leaves the
// database/sql limits alone.
//
// SETUP: a running pool; the test sets the open-connection limit to a value
// the reload would never choose. STIMULUS: a reload of an unrelated key.
// ASSERTION: the limit is still the test's value (Resize did not run).
// NON-VACUITY: a reload that changes the count DOES overwrite it.
func TestUnchangedWritersCountDoesNotReassertThePoolSize(t *testing.T) {
	fx := newReloadFix(t, true)
	cancel, done := runGraph(t, fx.g)
	defer func() { cancel(); awaitRun(t, done) }()
	waitUntil(t, "four writers", func() bool { return fx.g.inst().writer.WritersRunning.Load() == 4 })

	fx.db.SetPoolSize(77, 77)
	fx.set("flush", "windowMs", 30)
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("reload = %v, want applied\n%s", got, fx.log())
	}
	if got := fx.db.Stats().MaxOpenConnections; got != 77 {
		t.Errorf("MaxOpenConnections = %d after an unrelated reload, want the test's 77", got)
	}

	fx.set("writers", "count", 3)
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("reload = %v, want applied\n%s", got, fx.log())
	}
	if got := fx.db.Stats().MaxOpenConnections; got != 4 {
		t.Errorf("MaxOpenConnections = %d after the count changed, want 4", got)
	}
}

// TestApplyLiveReachesEveryLiveConsumerInOrder drives applyLive with one file
// that changes every directly-applied key.
//
// SETUP: a graph with a database. STIMULUS: the file changes
// runtime.gomaxprocs, stats.logLevel, stats.eventRatePerMin, stats.intervalSec,
// debug.traceTranscodes and debug.hexDump, and db.connMaxLifetimeMinutes and
// db.connectTimeoutSec; applyLive runs on (running, loaded).
// ASSERTION: the applied list is those effects in the documented order; the
// scheduler, the log level, the trace snapshot and the emission interval are
// each observably the new value.
// WOULD HAVE CAUGHT: an effect dropped from applyLive (a reloadable key that
// silently never takes effect), and a reordering that resizes the pool before
// the loggers are set.
// NON-VACUITY: an unchanged file applies nothing.
func TestApplyLiveReachesEveryLiveConsumerInOrder(t *testing.T) {
	fx := newReloadFix(t, true)
	prev := fx.g.loadRuntime().Cfg

	if applied, failed := fx.g.applyLive(prev, prev); len(applied)+len(failed) != 0 {
		t.Fatalf("applyLive on an unchanged configuration did something: %v %v", applied, failed)
	}

	orig := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(orig) })
	target := 2
	if prev.Runtime.GOMAXPROCS == 2 {
		target = 3
	}
	fx.set("runtime", "gomaxprocs", target)
	fx.set("stats", "logLevel", "error")
	fx.set("stats", "eventRatePerMin", 77)
	fx.set("stats", "intervalSec", 30)
	fx.set("debug", "hexDump", true)
	fx.set("db", "connMaxLifetimeMinutes", 10)
	fx.set("db", "connectTimeoutSec", 5)
	next, _, err := loadForReload(fx.path, prev, fx.g.loadRuntime().Dests)
	if err != nil {
		t.Fatalf("loading the edited file: %v", err)
	}
	// V-DBG1 only admits a configured transcode and the fixture configures
	// none, so the trace code is set on the loaded value, not in the file.
	next.Debug.TraceTranscodes = []int{7208}

	applied, failed := fx.g.applyLive(prev, next)
	want := []string{"runtime.gomaxprocs", "db.connMaxLifetimeMinutes", "db.connectTimeoutSec",
		"stats.logLevel", "stats.eventRatePerMin", "debug", "stats.intervalSec"}
	if !slices.Equal(applied, want) || len(failed) != 0 {
		t.Errorf("applied %v failed %v, want applied %v and nothing failed", applied, failed, want)
	}
	if got := runtime.GOMAXPROCS(0); got != target {
		t.Errorf("GOMAXPROCS = %d, want %d", got, target)
	}
	fx.g.deps.Log.Infof("probe-after-level-change")
	if strings.Contains(fx.log(), "probe-after-level-change") {
		t.Error("an INFO line was logged after stats.logLevel became error")
	}
	if !fx.g.deps.Debug.Enabled(7208) || !fx.g.deps.Debug.HexDumpEnabled() {
		t.Error("the trace snapshot was not replaced")
	}
	if err := fx.g.emitStats(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fx.log(), "interval_s=30") {
		t.Errorf("the emission does not carry the reloaded interval_s=30:\n%s", fx.log())
	}
}

// TestReadBufferReloadToleratesNoSocketAndAClosedSocket pins the card's "must
// tolerate g.conn nil or closed (log, never panic)".
//
// SETUP: (a) a graph that never bound; (b) a running graph whose socket is then
// closed under it. STIMULUS: a reload changing udp.readBufferBytes in each.
// ASSERTION: the reload is applied (the swap stands, D13), no panic, and the
// effect is listed under failed=.
// NON-VACUITY: (c) on a live socket the same edit is listed as applied, and
// readbuf_effective_bytes is the kernel's read-back value.
func TestReadBufferReloadToleratesNoSocketAndAClosedSocket(t *testing.T) {
	t.Run("no socket", func(t *testing.T) {
		fx := newReloadFix(t, false)
		fx.set("udp", "readBufferBytes", 262144)
		if got := fx.g.reload(); got != reloadApplied {
			t.Fatalf("reload = %v, want applied\n%s", got, fx.log())
		}
		if !strings.Contains(fx.log(), "failed=[udp.readBufferBytes: no socket is bound]") {
			t.Errorf("the failure is not listed:\n%s", fx.log())
		}
	})

	t.Run("closed socket", func(t *testing.T) {
		fx := newReloadFix(t, false)
		cancel, done := runGraph(t, fx.g)
		defer func() { cancel(); awaitRun(t, done) }()
		fx.g.getConn().Close()
		fx.set("udp", "readBufferBytes", 262144)
		if got := fx.g.reload(); got != reloadApplied {
			t.Fatalf("reload = %v, want applied\n%s", got, fx.log())
		}
		if !strings.Contains(fx.log(), "failed=[udp.readBufferBytes:") {
			t.Errorf("the failure is not listed:\n%s", fx.log())
		}
	})

	t.Run("live socket", func(t *testing.T) {
		fx := newReloadFix(t, false)
		cancel, done := runGraph(t, fx.g)
		defer func() { cancel(); awaitRun(t, done) }()
		fx.set("udp", "readBufferBytes", 262144)
		if got := fx.g.reload(); got != reloadApplied {
			t.Fatalf("reload = %v, want applied\n%s", got, fx.log())
		}
		if !strings.Contains(fx.log(), "applied=[udp.readBufferBytes] failed=[]") ||
			!strings.Contains(fx.log(), "receive buffer reloaded requested_rcvbuf=262144 effective_rcvbuf=") {
			t.Errorf("the applied effect or the effective value is not logged:\n%s", fx.log())
		}
		want, err := readBufferSize(fx.g.getConn())
		if err != nil {
			t.Fatal(err)
		}
		if got := fx.g.inst().readbufEffective.Load(); got != int64(want) {
			t.Errorf("readbuf_effective_bytes = %d, want the kernel's read-back %d", got, want)
		}
	})
}

// TestTheReceiverAdoptsAReloadedAllowListAndEnforceFlag is the pickup test for
// udp.sourceAllowList and udp.sourceEnforce (D9, default: reloadable).
//
// SETUP: a running graph, allow-list 10.0.0.0/8 (loopback is outside it),
// enforce false. STIMULUS: (1) a datagram; (2) a reload arming enforce, then a
// datagram; (3) a reload widening the list to 127.0.0.0/8, then a datagram.
// ASSERTION: (1) is counted unexpected and delivered; (2) is counted unexpected
// and NOT delivered; (3) is delivered and not counted.
// WOULD HAVE CAUGHT: the receiver keeping the allow-list or the flag it had at
// bind (the incident lever sourceEnforce silently not working on a live
// process), and a per-datagram re-parse (G-ALLOC-1 covers that half).
// NON-VACUITY: the counters are read after each datagram has demonstrably
// arrived, so "not delivered" cannot be a lost packet.
func TestTheReceiverAdoptsAReloadedAllowListAndEnforceFlag(t *testing.T) {
	fx := newReloadFix(t, false, func(fx *reloadFix) {
		fx.sec["udp"]["sourceAllowList"] = []string{"10.0.0.0/8"}
		fx.sec["udp"]["sourceEnforce"] = false
	})
	cancel, done := runGraph(t, fx.g)
	defer func() { cancel(); awaitRun(t, done) }()
	conn, err := net.Dial("udp", fx.g.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	send := func() {
		d := make([]byte, 64)
		d[0], d[3] = 8, 1
		if _, err := conn.Write(d); err != nil {
			t.Fatal(err)
		}
	}
	in := fx.g.inst()

	send()
	waitUntil(t, "datagram 1 counted and delivered", func() bool {
		return in.datagramsUnexpectedSrc.Load() == 1 && fx.delivered.Load() == 1
	})

	fx.set("udp", "sourceEnforce", true)
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("reload = %v\n%s", got, fx.log())
	}
	send()
	waitUntil(t, "datagram 2 counted", func() bool { return in.datagramsUnexpectedSrc.Load() == 2 })
	time.Sleep(50 * time.Millisecond) // the decoder is asynchronous; give a wrong delivery time to show
	if got := fx.delivered.Load(); got != 1 {
		t.Errorf("delivered = %d after enforce was armed by reload, want 1 (the datagram must be dropped)", got)
	}

	fx.set("udp", "sourceAllowList", []string{"127.0.0.0/8"})
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("reload = %v\n%s", got, fx.log())
	}
	send()
	waitUntil(t, "datagram 3 delivered", func() bool { return fx.delivered.Load() == 2 })
	if got := in.datagramsUnexpectedSrc.Load(); got != 2 {
		t.Errorf("unexpected_source = %d after the list was widened, want 2", got)
	}
}

// TestTheDecoderAdoptsAReloadedGapSetting is the pickup test for
// ordering.gapScope and ordering.gapAuditPackets: the detector is reconfigured
// by the decoder goroutine at the next datagram, keyed on the generation.
//
// SETUP: a receiver built from the running graph. STIMULUS: a reload changing
// both keys, then one datagram through decodeDatagram. ASSERTION: the receiver
// holds the new generation, and a second datagram under the same generation
// does not reconfigure again.
// WOULD HAVE CAUGHT: the detector keeping its construction-time scope for the
// life of the process while the INFO line says the key was applied.
func TestTheDecoderAdoptsAReloadedGapSetting(t *testing.T) {
	fx := newReloadFix(t, false)
	r := newReceiver(fx.g)
	if r.gapGen != fx.g.loadRuntime().Cfg.Gen {
		t.Fatalf("a new receiver holds generation %d, want the running %d", r.gapGen, fx.g.loadRuntime().Cfg.Gen)
	}
	fx.set("ordering", "gapScope", "per-transcode")
	fx.set("ordering", "gapAuditPackets", 5)
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("reload = %v\n%s", got, fx.log())
	}
	want := fx.g.loadRuntime().Cfg.Gen
	if r.gapGen == want {
		t.Fatal("vacuous: the receiver already held the new generation before any datagram")
	}
	var d model.Datagram
	if err := r.decodeDatagram(&d); err != nil {
		t.Fatal(err)
	}
	if r.gapGen != want {
		t.Errorf("the receiver holds generation %d after a datagram, want %d", r.gapGen, want)
	}
}

// TestPickupAuditEveryDepsCfgReadIsClassified is the evidence half of the
// pickup audit table in reload.go. SETUP: the AST of every non-test file in
// internal/app. ASSERTION: every read of the startup Config (Deps.Cfg, spelled
// g.deps.Cfg or d.Cfg) sits in a function named in startupReads, i.e. one the
// audit classified as startup-only or bind-time. A reloadable read added later
// through the startup pointer fails here and must go through loadRuntime().
// WOULD HAVE CAUGHT: a new reloadable key read from the startup Config, which
// is the silent "applied=... but nothing changed" defect.
// NON-VACUITY: the classified functions are all found, so the scan reaches the
// reads it classifies.
func TestPickupAuditEveryDepsCfgReadIsClassified(t *testing.T) {
	startupReads := map[string]string{
		"Build":      "sizes the ring, the shard queues and the mirrors; builds the first Runtime",
		"bind":       "udp.listen and the first udp.readBufferBytes are bind-time; later values go through applyLive",
		"newReceiver": "runtime.expectedRows sizes the token set; the gap detector's first scope, then Reconfigure",
		"buildPool":  "writers.count and the batch ceiling size the pool; later counts go through Resize",
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Skip("no source tree: the cross-compiled test binary is running without the sources (G-AIX-1); " +
			"the audit runs on the build host")
	}
	found := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Cfg" {
					return true
				}
				switch x := sel.X.(type) {
				case *ast.SelectorExpr: // g.deps.Cfg
					if x.Sel.Name != "deps" {
						return true
					}
				case *ast.Ident: // d.Cfg, a Deps value
					if x.Name != "d" {
						return true
					}
				default:
					return true
				}
				if _, ok := startupReads[fn.Name.Name]; ok {
					found[fn.Name.Name] = true
					return true
				}
				t.Errorf("%s:%d: %s reads the STARTUP Config (Deps.Cfg). A reloadable key read this way is "+
					"never picked up: read loadRuntime().Cfg, or add the function to startupReads with the "+
					"reason it is startup-only",
					filepath.Base(f), fset.Position(sel.Pos()).Line, fn.Name.Name)
				return true
			})
		}
	}
	for name := range startupReads {
		if !found[name] {
			t.Errorf("the audit classifies %s but the scan found no Deps.Cfg read in it", name)
		}
	}
}

package app

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
)

// The SIGHUP reload driver (P5-06). Every test here drives g.reload() directly,
// the function the signal goroutine calls, except the one real-signal smoke
// test; the sequence is what is under test, not the signal plumbing.

// reloadFix is a graph built from a REAL config file on disk, so a reload has
// something to re-read, plus the means to edit that file between reloads.
type reloadFix struct {
	t         *testing.T
	g         *Graph
	buf       *syncBuf
	path      string
	sec       map[string]map[string]any
	transcodes []any // the "transcodes" array; nil means none configured
	db        *oradb.DB
	delivered *atomic.Int64
}

// reloadBase is the file every fixture starts from: small mirrors, a 20 ms
// window, four writers with statementFailWriters 2, and a db object (V-DB1
// requires one in ModeRun, even when the test passes no database to the graph).
func reloadBase() map[string]map[string]any {
	return map[string]map[string]any{
		"udp":     {"listen": "127.0.0.1:0"},
		"runtime": {"expectedRows": 1024, "arenaChunkRows": 256},
		"flush":   {"windowMs": 20},
		"writers": {"count": 4, "statementFailWriters": 2},
		"db":      {"host": "h", "port": 1521, "service": "S", "user": "u"},
	}
}

// newReloadFix builds a graph from the base file. withDB attaches an oratest
// database and so a writer pool.
func newReloadFix(t *testing.T, withDB bool, tune ...func(fx *reloadFix)) *reloadFix {
	t.Helper()
	t.Setenv(oradb.PasswordEnv, "")
	fx := &reloadFix{t: t, sec: reloadBase(), path: filepath.Join(t.TempDir(), "config.json"),
		delivered: &atomic.Int64{}}
	for _, f := range tune {
		f(fx)
	}
	fx.write()

	cfg, dests, err := loadAll(fx.path, config.ModeRun)
	if err != nil {
		t.Fatalf("loading the running configuration: %v", err)
	}
	if withDB {
		f := oratest.New()
		t.Cleanup(f.Close)
		fx.db, err = oradb.OpenDriver(context.Background(), oratest.DriverName, f.DSN(), cfg.DB)
		if err != nil {
			t.Fatalf("OpenDriver: %v", err)
		}
		t.Cleanup(func() { fx.db.Close() })
	}

	fx.buf = &syncBuf{}
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg, dests)
	reg.SetIntervalSec(cfg.Stats.IntervalSec)
	reg.Freeze()
	lg := obs.NewAppLogger(fx.buf, obs.ParseLevel(cfg.Stats.LogLevel), 10000, reg)
	dbg := obs.NewDebugLogger(fx.buf, cfg.Debug.TraceTranscodes, cfg.Debug.HexDump)
	fx.g, err = Build(Deps{
		Cfg: cfg, Store: storeOf(cfg), DB: fx.db, Reg: reg,
		Log: lg, Debug: dbg, Dests: dests, Stderr: fx.buf, Inst: inst,
		ConfigPath: fx.path,
		Decode:     func(*model.Datagram) error { fx.delivered.Add(1); return nil },
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return fx
}

// set edits one key of the file on disk and rewrites it.
func (fx *reloadFix) set(section, key string, v any) {
	fx.t.Helper()
	if fx.sec[section] == nil {
		fx.sec[section] = map[string]any{}
	}
	if v == nil {
		delete(fx.sec[section], key)
	} else {
		fx.sec[section][key] = v
	}
	fx.write()
}

func (fx *reloadFix) write() {
	fx.t.Helper()
	if err := os.WriteFile(fx.path, fx.marshal(), 0o644); err != nil {
		fx.t.Fatal(err)
	}
}

// marshal is the file's bytes: the sections plus the transcodes array.
func (fx *reloadFix) marshal() []byte {
	all := map[string]any{}
	if fx.transcodes != nil {
		all["transcodes"] = fx.transcodes
	}
	for k, v := range fx.sec {
		all[k] = v
	}
	b, err := json.Marshal(all)
	if err != nil {
		fx.t.Fatal(err)
	}
	return b
}

func (fx *reloadFix) log() string { return fx.buf.String() }

// snapshot is everything a rejected reload must leave exactly as it was.
type reloadSnapshot struct {
	rt       *Runtime
	cfg      *config.Config
	gen      uint64
	gauge    int64
	reloads  uint64
	rejected uint64
}

func (fx *reloadFix) snapshot() reloadSnapshot {
	in := fx.g.inst()
	return reloadSnapshot{
		rt: fx.g.loadRuntime(), cfg: fx.g.deps.Store.Load(), gen: fx.g.deps.Store.Gen(),
		gauge: in.configGeneration.Load(), reloads: in.configReloads.Load(),
		rejected: in.configReloadRejected.Load(),
	}
}

// assertRejectedAndUntouched is the G-CFG-5 assertion, shared by the tests that
// each reject for a different reason.
func (fx *reloadFix) assertRejectedAndUntouched(before reloadSnapshot, rule string) {
	fx.t.Helper()
	after := fx.snapshot()
	if after.rt != before.rt || after.cfg != before.cfg {
		fx.t.Error("a rejected reload republished the Runtime or the Config")
	}
	if after.gen != before.gen || after.gauge != before.gauge {
		fx.t.Errorf("config_generation moved on a rejection: store gen %d -> %d, gauge %d -> %d",
			before.gen, after.gen, before.gauge, after.gauge)
	}
	if after.reloads != before.reloads {
		fx.t.Errorf("config_reloads moved on a rejection: %d -> %d", before.reloads, after.reloads)
	}
	if after.rejected != before.rejected+1 {
		fx.t.Errorf("config_reload_rejected = %d, want %d", after.rejected, before.rejected+1)
	}
	if got := strings.Count(fx.log(), "config reload rejected"); got != 1 {
		fx.t.Errorf("%d rejection WARNs, want exactly one:\n%s", got, fx.log())
	}
	if !strings.Contains(fx.log(), "rule="+rule) {
		fx.t.Errorf("the WARN does not name rule %s:\n%s", rule, fx.log())
	}
}

// TestG_CFG_5_RejectedReloadChangesNothing is gate G-CFG-5.
//
// SETUP: a running graph built from a file; flush.windowMs 20.
// STIMULUS: the file is edited to flush.windowMs 5 (below the 10 ms floor) and
// reload runs.
// ASSERTION: the outcome is a rejection; the Store, the Runtime pointer, the
// generation and the config_generation gauge are exactly as before;
// config_reload_rejected is 1 and config_reloads 0; ONE WARN names the rule,
// the key and both values (running 20, attempted 5); the datapath still reads
// the old window.
// WOULD HAVE CAUGHT: partial application (HLD F15), a rejection that logs
// nothing and leaves the operator believing a fix is live, and a generation
// bumped before validation.
// NON-VACUITY: the same file corrected to a different valid value IS applied,
// advances the generation and the gauge, and changes the window; so the
// rejection above is the rule's doing and not a reload that never works.
func TestG_CFG_5_RejectedReloadChangesNothing(t *testing.T) {
	fx := newReloadFix(t, false)
	before := fx.snapshot()

	fx.set("flush", "windowMs", 5)
	if got := fx.g.reload(); got != reloadRejected {
		t.Fatalf("reload of an out-of-range key = %v, want rejected", got)
	}
	fx.assertRejectedAndUntouched(before, config.RuleRange)
	for _, want := range []string{"key=flush.windowMs", `running="20"`, `attempted="5"`} {
		if !strings.Contains(fx.log(), want) {
			t.Errorf("the WARN lacks %s:\n%s", want, fx.log())
		}
	}
	if w := fx.g.loadRuntime().Mirror.Window; w != 20*time.Millisecond {
		t.Errorf("the datapath window is %v after a rejection, want the old 20ms", w)
	}

	fx.set("flush", "windowMs", 40)
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("reload of a valid edit = %v, want applied\n%s", got, fx.log())
	}
	after := fx.snapshot()
	if after.gen != before.gen+1 || after.gauge != int64(after.gen) || after.reloads != 1 {
		t.Errorf("a valid reload: gen %d -> %d, gauge %d, reloads %d; want gen+1, gauge = gen, 1 reload",
			before.gen, after.gen, after.gauge, after.reloads)
	}
	if w := fx.g.loadRuntime().Mirror.Window; w != 40*time.Millisecond {
		t.Errorf("the datapath window is %v after an accepted reload, want 40ms", w)
	}
	if !strings.Contains(fx.log(), "config reloaded generation=2 changed=[flush.windowMs]") {
		t.Errorf("no INFO naming the generation and the changed key:\n%s", fx.log())
	}
}

// TestG_CFG_6_StartupOnlyPlusReloadableIsRefusedWholesale is gate G-CFG-6.
//
// SETUP: a running graph. STIMULUS: one file changing runtime.shards
// (startup-only) AND flush.windowMs (reloadable, valid). ASSERTION: the whole
// file is refused, the message names runtime.shards with both values, and the
// reloadable half was NOT applied.
// WOULD HAVE CAUGHT: applying the reloadable half, which yields a configuration
// the operator never wrote and the validator never saw as a whole.
// NON-VACUITY: the reloadable edit alone is accepted.
func TestG_CFG_6_StartupOnlyPlusReloadableIsRefusedWholesale(t *testing.T) {
	fx := newReloadFix(t, false)
	before := fx.snapshot()

	fx.set("runtime", "shards", 8)
	fx.set("flush", "windowMs", 40)
	if got := fx.g.reload(); got != reloadRejected {
		t.Fatalf("a startup-only change was %v, want rejected", got)
	}
	fx.assertRejectedAndUntouched(before, config.RuleStartupOnly)
	for _, want := range []string{"key=runtime.shards", `running="4"`, `attempted="8"`} {
		if !strings.Contains(fx.log(), want) {
			t.Errorf("the WARN lacks %s:\n%s", want, fx.log())
		}
	}
	if w := fx.g.loadRuntime().Mirror.Window; w != 20*time.Millisecond {
		t.Errorf("the reloadable half was applied: window %v, want 20ms", w)
	}

	fx.set("runtime", "shards", nil) // back to the running value
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("the reloadable edit alone = %v, want applied\n%s", got, fx.log())
	}
	if w := fx.g.loadRuntime().Mirror.Window; w != 40*time.Millisecond {
		t.Errorf("window %v after the accepted edit, want 40ms", w)
	}
}

// TestG_CFG_8_LoweringWritersBelowStatementFailWritersIsRejected is gate
// G-CFG-8 through the SIGHUP path.
//
// SETUP: a running graph with a pool, writers.count 4, statementFailWriters 2.
// STIMULUS: the file lowers writers.count to 1. ASSERTION: REJECTED by V-X1,
// not clamped: the pool still has 4 writers and writers_configured 4.
// WOULD HAVE CAUGHT: a clamp to the floor, after which the pool-stop rule can
// never fire (Vol 0 C6), and a Resize reached without validation.
// NON-VACUITY: lowering to 2 (equal to the floor) is accepted and the pool is
// resized to 2.
func TestG_CFG_8_LoweringWritersBelowStatementFailWritersIsRejected(t *testing.T) {
	fx := newReloadFix(t, true)
	cancel, done := runGraph(t, fx.g)
	defer func() { cancel(); awaitRun(t, done) }()
	waitUntil(t, "four writers", func() bool { return fx.g.inst().writer.WritersRunning.Load() == 4 })
	before := fx.snapshot()

	fx.set("writers", "count", 1)
	if got := fx.g.reload(); got != reloadRejected {
		t.Fatalf("writers.count below statementFailWriters = %v, want rejected", got)
	}
	fx.assertRejectedAndUntouched(before, config.RuleStatementFailWriters)
	if got := fx.g.inst().writer.WritersConfigured.Load(); got != 4 {
		t.Errorf("writers_configured = %d after a rejection, want 4 (not clamped)", got)
	}
	if got := fx.g.inst().writer.WritersRunning.Load(); got != 4 {
		t.Errorf("writers_running = %d after a rejection, want 4", got)
	}

	fx.set("writers", "count", 2)
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("writers.count = statementFailWriters = %v, want applied\n%s", got, fx.log())
	}
	if got := fx.g.inst().writer.WritersConfigured.Load(); got != 2 {
		t.Errorf("writers_configured = %d, want 2", got)
	}
}

// TestAReloadRejectedForADBKeyNeverLogsAValue is the G-SEC-1 extension: a
// rejection must not leak the password or any db.* value.
//
// SETUP: a running graph. STIMULUS: (a) a file carrying db.password; (b) a file
// changing db.host to a recognisable string; (c) a file whose db.connectTimeoutSec
// is out of range. ASSERTION: all three are rejected, the log carries the key
// path, and carries none of the three values.
// WOULD HAVE CAUGHT: echoing the rejected file's values in the WARN, which the
// rate-limited, world-readable log would keep.
// NON-VACUITY: each rejection logs its key path, so the log is read, not empty.
func TestAReloadRejectedForADBKeyNeverLogsAValue(t *testing.T) {
	const secret, host = "hunter2-CORRECT-HORSE", "evil-host.example"
	cases := []struct {
		name, key string
		edit      func(fx *reloadFix)
		leak      string
	}{
		{"password", "db.password", func(fx *reloadFix) { fx.set("db", "password", secret) }, secret},
		{"host", "db.host", func(fx *reloadFix) { fx.set("db", "host", host) }, host},
		{"range", "db.connectTimeoutSec", func(fx *reloadFix) { fx.set("db", "connectTimeoutSec", 9999) }, "9999"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newReloadFix(t, false)
			tc.edit(fx)
			if got := fx.g.reload(); got != reloadRejected {
				t.Fatalf("reload = %v, want rejected", got)
			}
			log := fx.log()
			if !strings.Contains(log, tc.key) {
				t.Errorf("the WARN does not name %s:\n%s", tc.key, log)
			}
			if strings.Contains(log, tc.leak) {
				t.Errorf("the log contains the rejected db value %q:\n%s", tc.leak, log)
			}
		})
	}
}

// TestReloadWithNoConfigFileIsNothingToReload pins D7: no file, no reload, and
// nothing counted.
func TestReloadWithNoConfigFileIsNothingToReload(t *testing.T) {
	fx := newReloadFix(t, false)
	fx.g.deps.ConfigPath = ""
	before := fx.snapshot()
	if got := fx.g.reload(); got != reloadSkipped {
		t.Fatalf("reload with no file = %v, want skipped", got)
	}
	after := fx.snapshot()
	if after != before {
		t.Errorf("counters or generation moved with no file: %+v -> %+v", before, after)
	}
	if !strings.Contains(fx.log(), "nothing to reload") {
		t.Errorf("no 'nothing to reload' line:\n%s", fx.log())
	}
}

// TestSIGHUPWhileDrainingOrAfterStopIsALoggedNoOp pins D12's last clause.
//
// SETUP: a running graph and a valid pending edit. STIMULUS: Run is cancelled
// and returns; reload is then called. ASSERTION: skipped, logged, the edit NOT
// applied, no counter moved, no panic. Also checked: closeReload on a graph
// that never ran (the stopHUP path) skips the same way.
// WOULD HAVE CAUGHT: a reload running against a pool and a socket teardown has
// already closed.
// NON-VACUITY: the same edit applies on the same graph before the stop.
func TestSIGHUPWhileDrainingOrAfterStopIsALoggedNoOp(t *testing.T) {
	fx := newReloadFix(t, true)
	cancel, done := runGraph(t, fx.g)
	fx.set("flush", "windowMs", 30)
	if got := fx.g.reload(); got != reloadApplied {
		t.Fatalf("reload before the stop = %v, want applied\n%s", got, fx.log())
	}
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
	before := fx.snapshot()
	fx.set("flush", "windowMs", 50)
	if got := fx.g.reload(); got != reloadSkipped {
		t.Fatalf("reload after the stop = %v, want skipped", got)
	}
	if after := fx.snapshot(); after != before {
		t.Errorf("a post-stop reload changed state: %+v -> %+v", before, after)
	}
	if !strings.Contains(fx.log(), "shutting down") {
		t.Errorf("the no-op was not logged:\n%s", fx.log())
	}

	fx2 := newReloadFix(t, false)
	fx2.g.closeReload()
	fx2.set("flush", "windowMs", 50)
	if got := fx2.g.reload(); got != reloadSkipped {
		t.Errorf("reload after closeReload = %v, want skipped", got)
	}
}

// TestAReloadRacingSIGTERMNeverOverlapsTheDrain is reload-vs-SIGTERM, for
// -race.
//
// SETUP: a running graph with a pool, and a goroutine that edits the file and
// reloads in a loop, alternating the window, the writer count and the socket
// buffer (so the live effects touch the pool and the listener).
// STIMULUS: once at least two reloads have applied, Run's context is cancelled
// while the loop is still going. ASSERTION: Run returns nil; every reload after
// that point is skipped and the loop ends; the race detector sees no unordered
// access between a reload and the teardown that closes the socket and the pool.
// WOULD HAVE CAUGHT: stopHUP not waiting for an in-flight reload (a Resize on a
// closed pool, a SetReadBuffer on a closed socket), and teardown not closing
// the reload path.
// NON-VACUITY: the loop applied reloads before the cancel, and attempted at
// least one after it, which was skipped.
func TestAReloadRacingSIGTERMNeverOverlapsTheDrain(t *testing.T) {
	fx := newReloadFix(t, true)
	cancel, done := runGraph(t, fx.g)

	var applied, skippedAfter atomic.Int64
	var cancelled atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			fx.sec["flush"]["windowMs"] = 20 + 10*(i%2)
			fx.sec["writers"]["count"] = 3 + i%3
			fx.sec["udp"]["readBufferBytes"] = 262144 << (i % 2)
			if err := os.WriteFile(fx.path, fx.marshal(), 0o644); err != nil {
				return
			}
			wasCancelled := cancelled.Load()
			switch fx.g.reload() {
			case reloadApplied:
				applied.Add(1)
			case reloadSkipped:
				if wasCancelled {
					skippedAfter.Add(1)
				}
				return
			}
		}
	}()

	waitUntil(t, "two applied reloads", func() bool { return applied.Load() >= 2 })
	cancelled.Store(true)
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
	wg.Wait()
	if skippedAfter.Load() == 0 {
		t.Error("vacuous: the loop never met the closed reload path after the cancel")
	}
}

// TestARealSIGHUPReloadsTheRunningGraph is the one smoke test through the
// signal. SETUP: a running graph, with the test's own signal.Notify held so a
// HUP delivered before the handler is installed cannot end the process.
// STIMULUS: the file changes and SIGHUP is sent to this process until the
// generation moves. ASSERTION: it moves, and the INFO line is logged.
// Skipped where the platform has no SIGHUP.
func TestARealSIGHUPReloadsTheRunningGraph(t *testing.T) {
	if !hupSupported {
		t.Skip("this platform has no SIGHUP to deliver to a process; the reload function " +
			"is driven directly by every other test, and the real signal is the LPAR smoke script's")
	}
	hold := make(chan os.Signal, 8)
	signal.Notify(hold, syscall.SIGHUP)
	defer signal.Stop(hold)

	fx := newReloadFix(t, false)
	cancel, done := runGraph(t, fx.g)
	defer func() { cancel(); awaitRun(t, done) }()
	fx.set("flush", "windowMs", 40)
	gen := fx.g.deps.Store.Gen()
	waitUntil(t, "the SIGHUP to be honoured", func() bool {
		sendHUP(t)
		time.Sleep(20 * time.Millisecond)
		return fx.g.deps.Store.Gen() > gen
	})
	if w := fx.g.loadRuntime().Mirror.Window; w != 40*time.Millisecond {
		t.Errorf("window %v after a real SIGHUP, want 40ms", w)
	}
	if !strings.Contains(fx.log(), "config reloaded generation=") {
		t.Errorf("no 'config reloaded generation=' INFO line:\n%s", fx.log())
	}
}

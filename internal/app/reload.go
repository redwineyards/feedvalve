package app

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/obs"
)

// SIGHUP reload: Vol 4 section 2, ADR-010. Load, validate wholesale, rebuild
// the derived state, publish ONCE, then apply the live side effects in order.
//
// The whole sequence lives in reload below and nowhere else, because its ORDER
// is the contract: a rejection must change nothing, the new Runtime must be in
// force before the pool re-reads its thresholds, and the pool's thresholds must
// be in force before the pool is resized (the new writers.statementFailWriters
// judges the new writers.count).

// PICKUP AUDIT (P5-07). Every reader of configuration under internal/app, by
// how it learns of a reload. TestPickupAuditEveryDepsCfgReadIsClassified fails
// if a new read of the STARTUP Config (Deps.Cfg) appears outside the
// startup-only functions; the reloadable keys all go through the Runtime.
//
//	udp.listen, runtime.{shards,generalShard,*Capacity,maxInFlightPerShard,
//	  expectedRows,arenaChunkRows,maxBatchSizeCeiling,destinationsFile},
//	  db.{host,port,service,user}      startup-only (V-R1); Build, bind
//	udp.readBufferBytes                applyLive: SetReadBuffer + read-back
//	udp.expectedNetId                  per datagram, rt.Cfg (subpacket.go)
//	udp.sourceAllowList, sourceEnforce per datagram, rt.SrcPrefixes / rt.Cfg
//	udp.stallBudgetMs                  per datagram, rt.Cfg (receiver, fanout)
//	runtime.gomaxprocs                 applyLive, only if changed
//	runtime.time1980UnixOffsetSec      writerParams, per batch (writer.Params);
//	                                   the runner refreshes cached binders
//	flush.*                            rt.Mirror, read per tick / epoch / batch
//	writers.count                      applyLive: Pool.Resize, only if changed
//	writers.* (the rest), db.queryTimeoutSec
//	                                   writerParams from rt.Cfg per batch;
//	                                   thresholds via Pool.ReloadHealth
//	db.connMaxLifetimeMinutes,
//	  db.connectTimeoutSec             applyLive: DB setters (next Acquire for a
//	                                   pinned session)
//	ordering.guard*                    rt.Mirror, read per evaluation / unit
//	ordering.guardEvalIntervalSec      stats goroutine re-arms the guard timer from
//	                                   rt.Mirror after each evaluation (stats.go
//	                                   guardInterval): takes effect within one OLD
//	                                   period, like stats.intervalSec (Vol 4 sec 2,
//	                                   documented rather than engineered around)
//	ordering.gapScope, gapAuditPackets decoder goroutine, GapDetector.Reconfigure
//	                                   at the next datagram after a new Gen
//	stats.logLevel, eventRatePerMin    applyLive: SetLevel, SetRatePerMin
//	stats.intervalSec                  applyLive: Reg.SetIntervalSec; the timer
//	                                   re-arms from rt.Cfg (stats.go)
//	liveness.*                         rt.Cfg at each circuit tick (stats.go)
//	debug.*                            applyLive: DebugLogger.Set
//	transcodes[].columns/class         registry rebuilt per reload (membership
//	                                   is startup-only, V-R3)

// reloadOutcome is what one SIGHUP did, for the tests and for the log.
type reloadOutcome uint8

const (
	// reloadApplied: the new configuration is published and its side effects
	// ran (some may be listed as failed; none rolls the swap back, D13).
	reloadApplied reloadOutcome = iota
	// reloadRejected: validation or the rebuild failed; nothing was touched.
	reloadRejected
	// reloadSkipped: no config file, or the lifecycle is no longer running.
	reloadSkipped
)

// installReloadHandler wires SIGHUP and returns the function that unwires it.
//
// Installed AFTER the graph exists, because a SIGHUP arriving mid-construction
// would race a half-built registry against a swap. Before this point SIGHUP's
// default disposition terminates the process, which during a sub-second startup
// is acceptable and is stated rather than discovered.
//
// The returned stop does not return until the signal goroutine has exited,
// which means until any in-flight reload has finished (D12), and then closes
// the reload path for good.
func (g *Graph) installReloadHandler() func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)

	stop := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-ch:
				g.deps.Log.Infof("SIGHUP received; reloading the configuration")
				g.reload()
			}
		}
	}()
	return func() {
		once.Do(func() {
			signal.Stop(ch)
			close(stop)
			<-done
			g.closeReload()
		})
	}
}

// closeReload makes every later reload a logged no-op. It takes reloadMu, so it
// waits for a reload that is already running: after it returns, no reload is in
// flight and none can start. Called by stopHUP and again by teardown (the net
// for error paths that never reached wait).
func (g *Graph) closeReload() {
	g.reloadMu.Lock()
	g.reloadClosed = true
	g.reloadMu.Unlock()
}

// reload is one SIGHUP. Serialised by reloadMu (D12). Safe to call directly,
// which is how the tests drive it.
func (g *Graph) reload() reloadOutcome {
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()

	if g.reloadClosed {
		g.deps.Log.Infof("SIGHUP ignored: the process is shutting down; nothing was reloaded")
		return reloadSkipped
	}
	// D7: with no config file the running configuration IS the defaults and
	// there is nothing to re-read. Not a rejection, so nothing is counted.
	if g.deps.ConfigPath == "" {
		g.deps.Log.Infof("SIGHUP ignored: no configuration file was given at startup; nothing to reload")
		return reloadSkipped
	}

	// Steps 1 and 2: load and validate against the RUNNING configuration and
	// field map (D2: ValidateAll in ModeRun, never a bare Store.Swap, which
	// skips the schema and destination rules).
	cur := g.loadRuntime()
	prev := cur.Cfg
	next, dests, err := loadForReload(g.deps.ConfigPath, prev, cur.Dests)
	if err != nil {
		return g.rejectReload(err)
	}

	// Step 3: everything that can fail, BEFORE anything is published, so a
	// failure here still leaves the previous configuration untouched. A fresh
	// decodeCounters per registry (D11): the old sink is never mutated, so a
	// datagram still decoding under the old snapshot keeps a consistent pair.
	cnt := newDecodeCounters(&g.deps.Inst)
	reg, err := decode.BuildRegistry(transcodeSpecs(next), dests, cnt)
	if err != nil {
		return g.rejectReload(registryError(err))
	}
	if reg.Len() != len(next.Transcodes) {
		return g.rejectReload(errors.New("internal: the registry resolved "+strconv.Itoa(reg.Len())+" of "+
			strconv.Itoa(len(next.Transcodes))+" configured transcodes"))
	}
	cnt.setRegistry(reg)
	rt, err := newRuntime(next, reg, dests)
	if err != nil {
		return g.rejectReload(errors.New("parsing udp.sourceAllowList: "+err.Error()))
	}

	// Step 4: ONE publication sequence (D1). Swap assigns the generation, so
	// the mirror parameters are derived from the Config only after it. Between
	// Swap and setRuntime Store.Load() is new while loadRuntime() is old; that
	// is benign because the datapath reads only loadRuntime().
	if _, err := g.deps.Store.Swap(next); err != nil {
		return g.rejectReload(err)
	}
	rt.Mirror = mirrorParams(next)
	g.setRuntime(rt)

	// Step 5: the pool re-reads its thresholds from the Runtime just
	// published, and does so BEFORE applyLive resizes it.
	if g.pool != nil {
		g.pool.ReloadHealth()
	}

	// Step 6.
	applied, failed := g.applyLive(prev, next)

	// Step 7. The gauge is stored now rather than at the next 60 s emission, so
	// "did my reload land?" is answerable at once.
	g.inst().configReloads.Add(1)
	g.inst().configGeneration.Store(int64(g.deps.Store.Gen()))
	// The text "config reloaded generation=" is grepped by scripts/lpar-smoke.sh
	// (step 4a) and quoted in docs/runbook.md; change all three together.
	g.deps.Log.Infof("config reloaded generation=%d changed=%v applied=%v failed=%v",
		next.Gen, changedKeys(prev, next), applied, failed)
	return reloadApplied
}

// rejectReload is F15: the operator's change did NOT take effect. One WARN
// naming the first rule, the key path and both values; for a db.* key the
// values are never printed, only the path.
func (g *Graph) rejectReload(err error) reloadOutcome {
	g.inst().configReloadRejected.Add(1)
	if g.deps.Log.Allow(obs.ClassReloadRejected) {
		g.warnRejected(err)
	}
	return reloadRejected
}

// warnRejected writes the one WARN. Only called under Allow.
func (g *Graph) warnRejected(err error) {
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		if g.deps.Log.Allow(obs.ClassReloadRejected) {
			g.deps.Log.Warnf(obs.ClassReloadRejected,
				"config reload rejected: %v; the running configuration is unchanged", err)
		}
		return
	}
	re := ve.RuleError()
	if strings.HasPrefix(re.KeyPath, "db.") {
		re.Running, re.Attempt = "", ""
	}
	// A rule about one key carries the path and both values. A cross-key rule
	// (V-X1 and its kind) has no single path, so its own message, which names
	// the keys and the values involved, stands in.
	detail := ""
	if re.KeyPath == "" {
		detail = " detail=" + strconv.Quote(re.Detail)
	}
	if g.deps.Log.Allow(obs.ClassReloadRejected) {
		g.deps.Log.Warnf(obs.ClassReloadRejected,
			"config reload rejected rule=%s key=%s running=%q attempted=%q%s problems=%d; "+
				"the running configuration is unchanged",
			re.Rule, re.KeyPath, re.Running, re.Attempt, detail, len(ve.Problems))
	}
}

// applyLive runs the reloadable side effects, in this order, each only if its
// own inputs changed. A failure is returned for the INFO line and never rolls
// the swap back (D13): the new configuration is already what the datapath is
// reading, so undoing it would be a second, unannounced change.
//
// The order is Vol 4 section 2.4's with D14 and D19: GOMAXPROCS, the socket,
// the database limits, the loggers and stats interval, and the pool size last,
// because the resize starts goroutines that read everything above.
func (g *Graph) applyLive(prev, next *config.Config) (applied, failed []string) {
	ok := func(name string) { applied = append(applied, name) }
	fail := func(name string, err error) { failed = append(failed, name+": "+err.Error()) }

	if prev.Runtime.GOMAXPROCS != next.Runtime.GOMAXPROCS {
		runtime.GOMAXPROCS(next.Runtime.GOMAXPROCS)
		ok("runtime.gomaxprocs")
	}

	if prev.UDP.ReadBufferBytes != next.UDP.ReadBufferBytes {
		g.applyReadBuffer(next.UDP.ReadBufferBytes, ok, fail)
	}

	if db := g.deps.DB; db != nil {
		if prev.DB.ConnMaxLifetimeMinutes != next.DB.ConnMaxLifetimeMinutes {
			db.SetConnMaxLifetime(time.Duration(next.DB.ConnMaxLifetimeMinutes)*time.Minute)
			ok("db.connMaxLifetimeMinutes")
		}
		if prev.DB.ConnectTimeoutSec != next.DB.ConnectTimeoutSec {
			db.SetConnectTimeout(time.Duration(next.DB.ConnectTimeoutSec)*time.Second)
			ok("db.connectTimeoutSec")
		}
	}

	if prev.Stats.LogLevel != next.Stats.LogLevel {
		g.deps.Log.SetLevel(obs.ParseLevel(next.Stats.LogLevel))
		ok("stats.logLevel")
	}
	if prev.Stats.EventRatePerMin != next.Stats.EventRatePerMin {
		g.deps.Log.SetRatePerMin(next.Stats.EventRatePerMin)
		ok("stats.eventRatePerMin")
	}
	if g.deps.Debug != nil && (!slices.Equal(prev.Debug.TraceTranscodes, next.Debug.TraceTranscodes) ||
		prev.Debug.HexDump != next.Debug.HexDump) {
		g.deps.Debug.Set(next.Debug.TraceTranscodes, next.Debug.HexDump)
		ok("debug")
	}
	if prev.Stats.IntervalSec != next.Stats.IntervalSec {
		// Reg.SetIntervalSec is off the Registry interface on purpose (it is
		// the lifecycle that applies a reload); the stats timers re-arm on
		// their own, within one old interval.
		if r, isReg := g.deps.Reg.(interface{ SetIntervalSec(int) }); isReg {
			r.SetIntervalSec(next.Stats.IntervalSec)
			ok("stats.intervalSec")
		}
	}

	if prev.Writers.Count != next.Writers.Count && g.pool != nil {
		// D14: Resize applies the database/sql limits itself, first. A pool
		// that has stopped or was never started errors here and is reported,
		// so the operator is not told "reloaded" with the old count unnoticed.
		if err := g.pool.Resize(context.Background(), next.Writers.Count); err != nil {
			fail("writers.count", err)
		} else {
			ok("writers.count")
		}
	}
	return applied, failed
}

// applyReadBuffer sets the receive buffer and reads back what the kernel gave,
// which is the only number that bounds Q0's slack. Tolerates a missing or
// closed socket: that is reported, never a panic.
func (g *Graph) applyReadBuffer(want int, ok func(string), fail func(string, error)) {
	conn := g.getConn()
	if conn == nil {
		fail("udp.readBufferBytes", errors.New("no socket is bound"))
		return
	}
	if err := conn.SetReadBuffer(want); err != nil {
		fail("udp.readBufferBytes", err)
		return
	}
	effective := want
	if n, err := readBufferSize(conn); err == nil {
		effective = n
	}
	g.inst().readbufEffective.Store(int64(effective))
	g.deps.Log.Infof("receive buffer reloaded requested_rcvbuf=%d effective_rcvbuf=%d", want, effective)
	ok("udp.readBufferBytes")
}

// changedKeys lists the configuration keys whose value differs, as dotted
// paths in the file's spelling. Names only, never a value, so a db.* key can be
// listed safely. The derived pool limits and the generation are not keys.
//
// Reflection over the Config here is the one place it is used in the process,
// and only on the SIGHUP path; the decode path stays reflection-free.
func changedKeys(prev, next *config.Config) []string {
	var out []string
	var walk func(path string, x, y reflect.Value)
	walk = func(path string, x, y reflect.Value) {
		if x.Kind() == reflect.Struct {
			t := x.Type()
			for i := 0; i < x.NumField(); i++ {
				name := t.Field(i).Name
				switch name {
				case "Gen", "HasDB", "MaxOpenConns", "MaxIdleConns":
					continue
				}
				key := keyName(name)
				if path != "" {
					key = path + "." + key
				}
				walk(key, x.Field(i), y.Field(i))
			}
			return
		}
		if x.Kind() == reflect.Slice && x.Len() == 0 && y.Len() == 0 {
			return
		}
		if !reflect.DeepEqual(x.Interface(), y.Interface()) {
			out = append(out, path)
		}
	}
	walk("", reflect.ValueOf(prev).Elem(), reflect.ValueOf(next).Elem())
	return out
}

// keyName turns a Go field name into the file's key: the leading capitals are
// lowered (UDP, DB, GOMAXPROCS, TZ, WindowMs), keeping the last of a run that
// precedes a lower-case letter (so "HTTPPort" would be "httpPort"), and a
// trailing "ID" is spelled "Id" as expectedNetId is.
func keyName(field string) string {
	r := []rune(field)
	n := 0
	for n < len(r) && unicode.IsUpper(r[n]) {
		n++
	}
	if n > 1 && n < len(r) {
		n--
	}
	if n == 0 {
		n = 1
	}
	s := strings.ToLower(string(r[:n])) + string(r[n:])
	if strings.HasSuffix(s, "ID") {
		s = s[:len(s)-2] + "Id"
	}
	return s
}

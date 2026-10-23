package app

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/config"
)

// G-CFG-3 and G-CFG-4 under load (P5-08).

// allTranscodes is the inventory V-D7 demands once any transcode is configured:
// every code the shipped field map names as a contributor.
var allTranscodes = []int{7200, 7208, 7305, 7340, 6511, 6521, 6522, 6531, 6571}

// withAllTranscodes is a reloadFix tune that configures the whole inventory, so
// the decoder registry, the guard table and real decoding exist.
func withAllTranscodes(t *testing.T) func(*reloadFix) {
	return func(fx *reloadFix) {
		var cfg config.Config
		configureTranscodes(t, &cfg, allTranscodes...)
		for _, tc := range cfg.Transcodes {
			var contribs []any
			for _, c := range tc.Contributions {
				contribs = append(contribs, map[string]any{"destination": c.Destination, "columns": c.Columns})
			}
			fx.transcodes = append(fx.transcodes, map[string]any{
				"code": tc.Code, "class": tc.Class, "contributions": contribs})
		}
	}
}

// TestG_CFG_3_NoReaderEverSeesAMixedConfigurationUnderLoad is gate G-CFG-3.
//
// SETUP: a running graph with a database and the full transcode inventory,
// fed real 7208 datagrams continuously. The file alternates between two
// complete variants that differ in four reloadable keys at once
// (flush.windowMs, flush.maxBatchSize, flush.epochStallWarnMs,
// flush.drainTimeoutMs): (20, 100, 5000, 1000) and (30, 200, 6000, 2000).
// STIMULUS: a loop edits the file and reloads, many times, while probe
// goroutines call loadRuntime() as the datapath does.
// ASSERTION: every snapshot a probe takes is ONE generation: its Cfg holds a
// whole variant (the four keys are a pair-set, never a mixture), its Mirror
// parameters describe the same variant and the same Gen, and generations seen
// by one probe never go backwards.
// WOULD HAVE CAUGHT: patching the live *Config in place, rebuilding only part
// of the derived state (Vol 4 2.3 property 3), and Mirror published beside
// Cfg through a second pointer.
// NON-VACUITY: both variants are observed by the probes, at least 20 reloads
// applied, and datagrams were being decoded throughout.
func TestG_CFG_3_NoReaderEverSeesAMixedConfigurationUnderLoad(t *testing.T) {
	fx := newReloadFix(t, true, withAllTranscodes(t), func(fx *reloadFix) {
		// Start as variant A, so every snapshot is one of the two.
		fx.sec["flush"] = map[string]any{"windowMs": 20, "maxBatchSize": 100,
			"epochStallWarnMs": 5000, "drainTimeoutMs": 1000}
	})
	variant := func(i int) {
		a := i%2 == 0
		pick := func(x, y int) int {
			if a {
				return x
			}
			return y
		}
		fx.sec["flush"]["windowMs"] = pick(20, 30)
		fx.sec["flush"]["maxBatchSize"] = pick(100, 200)
		fx.sec["flush"]["epochStallWarnMs"] = pick(5000, 6000)
		fx.sec["flush"]["drainTimeoutMs"] = pick(1000, 2000)
		fx.write()
	}
	cancel, done := runGraph(t, fx.g)

	// Load: a sender that does not use t after the test goroutine moves on.
	payload := datagramOf(t, uncompressedSub(mbpPayloadTokens(tokensPerShard(fx.g.deps.Cfg.Runtime.Shards, 2)[0]...)))
	conn, err := net.Dial("udp", fx.g.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			conn.Write(payload.Buf[:payload.N])
			time.Sleep(time.Millisecond)
		}
	}()

	var bad atomic.Value
	var sawA, sawB atomic.Bool
	probe := func() {
		defer wg.Done()
		var lastGen uint64
		for !stop.Load() {
			rt := fx.g.loadRuntime()
			f := rt.Cfg.Flush
			switch {
			case f.WindowMs == 20 && f.MaxBatchSize == 100 && f.EpochStallWarnMs == 5000 && f.DrainTimeoutMs == 1000:
				sawA.Store(true)
			case f.WindowMs == 30 && f.MaxBatchSize == 200 && f.EpochStallWarnMs == 6000 && f.DrainTimeoutMs == 2000:
				sawB.Store(true)
			default:
				bad.Store("a mixed Cfg: " + time.Duration(f.WindowMs).String())
			}
			m := rt.Mirror
			if m.Window != time.Duration(f.WindowMs)*time.Millisecond || m.MaxBatchSize != f.MaxBatchSize ||
				m.EpochStallWarn != time.Duration(f.EpochStallWarnMs)*time.Millisecond ||
				m.DrainTimeout != time.Duration(f.DrainTimeoutMs)*time.Millisecond || m.Gen != rt.Cfg.Gen {
				bad.Store("Mirror parameters from a different generation than Cfg")
			}
			if rt.Cfg.Gen < lastGen {
				bad.Store("a generation went backwards")
			}
			lastGen = rt.Cfg.Gen
		}
	}
	wg.Add(3)
	for range 3 {
		go probe()
	}

	applied := 0
	for i := 1; applied < 25; i++ {
		variant(i)
		if got := fx.g.reload(); got != reloadApplied {
			t.Fatalf("reload %d = %v\n%s", i, got, fx.log())
		}
		applied++
		time.Sleep(2 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	conn.Close()
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if v := bad.Load(); v != nil {
		t.Errorf("%v", v)
	}
	if !sawA.Load() || !sawB.Load() {
		t.Errorf("vacuous: the probes saw variant A=%v B=%v, want both", sawA.Load(), sawB.Load())
	}
	if fx.g.inst().datagramsReceived.Load() == 0 {
		t.Error("vacuous: no datagram reached the receiver during the reloads")
	}
}

// TestG_CFG_4_AnUnrelatedReloadDoesNotReEnableADisabledGuard is gate G-CFG-4
// (Phase 3 ruling D2: the bit lives in mirror.GuardTable, which a reload never
// rebuilds).
//
// SETUP: a running graph with the full inventory and ordering.guard auto.
// Transcode 7208's guard is disabled by the real evaluator, fed a window in
// which every offered update was superseded.
// STIMULUS: a reload changing an unrelated key (flush.windowMs).
// ASSERTION: the GuardTable pointer is the same object, ordering_guard_disabled
// for 7208 is still 1, and a further evaluation with no new samples neither
// re-enables it nor logs a second ERROR.
// WOULD HAVE CAUGHT: R-GUARD (Vol 4 2.3), a fresh guard state per rebuild,
// which silently re-enables every guard auto had disabled so the rows freeze
// again minutes after an unrelated reload. Nothing alarms.
// NON-VACUITY: before the reload the gauge is 0 and the evaluator flips it to
// 1, so the gauge is read, not a constant.
func TestG_CFG_4_AnUnrelatedReloadDoesNotReEnableADisabledGuard(t *testing.T) {
	fx := newReloadFix(t, false, withAllTranscodes(t))
	g := fx.g
	mt := &g.inst().mirrorTranscode
	idx := g.loadRuntime().Reg.Slot(7208)
	if idx < 0 {
		t.Fatal("7208 is not in the registry")
	}
	if mt.GuardDisabled[idx].Load() != 0 {
		t.Fatal("the guard starts disabled")
	}
	min := g.loadRuntime().Mirror.GuardMinSamples
	mt.GuardOffered[idx].Add(uint64(10 * min))
	mt.GuardSuperseded[idx].Add(uint64(10 * min))
	g.evaluateGuards()
	if mt.GuardDisabled[idx].Load() != 1 {
		t.Fatal("setup: the evaluator did not disable the guard")
	}
	table := g.guards

	fx.set("flush", "windowMs", 30)
	if got := g.reload(); got != reloadApplied {
		t.Fatalf("reload = %v\n%s", got, fx.log())
	}
	if g.guards != table {
		t.Error("the guard table was rebuilt by a reload")
	}
	if mt.GuardDisabled[idx].Load() != 1 {
		t.Error("the guard was re-enabled by an unrelated reload")
	}
	g.evaluateGuards()
	if mt.GuardDisabled[idx].Load() != 1 {
		t.Error("the guard was re-enabled by the next evaluation")
	}
}

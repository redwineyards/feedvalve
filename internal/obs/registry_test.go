package obs

import (
	"strings"
	"testing"
)

// testOptions is the shape of the shipped configuration at the Phase 1
// defaults: four shards, the three destinations of HLD section 8.2, and the two
// transaction codes Phase 1 decodes.
func testOptions() Options {
	return Options{
		Shards:       4,
		Destinations: []string{"md_token_quote", "md_security_master", "md_market_status"},
		Columns: [][]string{
			{"last_traded_price", "open_price", "high_price"},
			{"isin", "symbol"},
			{"message_text"},
		},
		Transcodes: []int{7208, 7305},
	}
}

func newFrozen(t *testing.T) *Reg {
	t.Helper()
	r := New(testOptions())
	r.Freeze()
	return r
}

// mustPanic runs fn and returns the panic text, failing if it did not panic.
func mustPanic(t *testing.T, what string, fn func()) string {
	t.Helper()
	var msg string
	func() {
		defer func() {
			if v := recover(); v != nil {
				msg, _ = v.(string)
			}
		}()
		fn()
		t.Fatalf("%s did not panic", what)
	}()
	return msg
}

// TestG_OBS_1_ResolutionAfterFreezePanics is gate G-OBS-1.
//
// Setup: a registry that panics if Counter/Max/Gauge is called after Run
// begins. Stimulus: a full pipeline run under load. Assertion: no panic.
//
// This is the unit half -- that the panic exists and fires. The defect it
// catches is "a per-event map lookup and a []Label allocation on the receive
// goroutine, exactly the cost ADR-006 exists to avoid" (Vol 4 section 7.1).
// Without the panic, a resolution call left on a hot path by mistake would work
// perfectly and merely be slow, which is the hardest kind of defect to find on
// a platform with no profiler.
func TestG_OBS_1_ResolutionAfterFreezePanics(t *testing.T) {
	r := newFrozen(t)

	for _, tc := range []struct {
		name string
		call func()
	}{
		{"Counter", func() { r.Counter(IDDatagramsReceived) }},
		{"CounterAt", func() { r.CounterAt(IDFlushOnTick, 0) }},
		{"CounterAt2", func() { r.CounterAt2(IDMessageTruncated, 0, 0) }},
		{"Max", func() { r.Max(IDDecompressedLenMax) }},
		{"MaxAt", func() { r.MaxAt(IDShardLoopMaxNs, 0) }},
		{"Gauge", func() { r.Gauge(IDCircuitState) }},
		{"GaugeAt", func() { r.GaugeAt(IDShardDepth, 0) }},
		{"GaugeAt2", func() { r.GaugeAt2(IDDirtyDepth, 0, 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := mustPanic(t, tc.name+" after Freeze", tc.call)
			if !strings.Contains(msg, "G-OBS-1") {
				t.Errorf("the panic does not name the gate it enforces: %q", msg)
			}
		})
	}
}

// TestResolutionBeforeFreezeIsFine is the other half: the accessors must work
// during startup step 9, or there is no way to resolve anything at all.
func TestResolutionBeforeFreezeIsFine(t *testing.T) {
	r := New(testOptions())
	if c := r.Counter(IDDatagramsReceived); c == nil {
		t.Fatal("Counter returned nil before Freeze")
	}
	if r.Frozen() {
		t.Error("Frozen reports true before Freeze was called")
	}
	r.Freeze()
	if !r.Frozen() {
		t.Error("Frozen reports false after Freeze")
	}
}

// TestEmitBeforeFreezePanics guards the other direction of the same boundary:
// Emit reads buffers that Freeze allocates, so calling it early would index a
// nil slice and report a confusing runtime error rather than the real mistake.
func TestEmitBeforeFreezePanics(t *testing.T) {
	r := New(testOptions())
	msg := mustPanic(t, "Emit before Freeze", func() { _ = r.Emit(discard{}) })
	if !strings.Contains(msg, "Freeze") {
		t.Errorf("the panic does not explain the ordering: %q", msg)
	}
}

// TestSameKeyResolvesToTheSamePointer is what lets every subsystem resolve
// independently at startup and still share one counter.
func TestSameKeyResolvesToTheSamePointer(t *testing.T) {
	r := New(testOptions())
	a := r.CounterAt(IDFlushOnTick, 2)
	b := r.CounterAt(IDFlushOnTick, 2)
	if a != b {
		t.Error("two resolutions of flush_on_tick{2} returned different pointers")
	}
	if c := r.CounterAt(IDFlushOnTick, 3); a == c {
		t.Error("flush_on_tick{2} and flush_on_tick{3} are the same pointer, so two " +
			"shards would increment one counter and the per-shard breakdown would be " +
			"meaningless")
	}
}

// TestEveryLabelledSlotIsDistinct walks the whole ledger and proves no two
// slots alias.
//
// Slab offsets are computed arithmetic over runtime arities and a padding
// stride, which is exactly the kind of code that is subtly wrong rather than
// obviously wrong: an off-by-one in the stride would make shard 3's counter
// land on shard 0's, and every individual value would still look plausible.
func TestEveryLabelledSlotIsDistinct(t *testing.T) {
	r := New(testOptions())
	seenC := map[*Counter]string{}
	seenM := map[*Max]string{}
	seenG := map[*Gauge]string{}

	for id := range numInstruments {
		row := &ledger[id]
		d := &r.dims[row.dim]
		for i := range d.size {
			off := r.base[id] + i*d.stride
			where := row.name + "[" + itoa(int(i)) + "]"
			switch row.shape {
			case shapeCounter:
				p := &r.counters[off]
				if prev, dup := seenC[p]; dup {
					t.Errorf("%s aliases %s", where, prev)
				}
				seenC[p] = where
			case shapeMax:
				p := &r.maxes[off]
				if prev, dup := seenM[p]; dup {
					t.Errorf("%s aliases %s", where, prev)
				}
				seenM[p] = where
			case shapeGauge:
				p := &r.gauges[off]
				if prev, dup := seenG[p]; dup {
					t.Errorf("%s aliases %s", where, prev)
				}
				seenG[p] = where
			}
		}
	}
}

// TestShapeMismatchPanicsWithTheLedgerShape catches the one class of error the
// single ID space permits that three typed ID spaces would not.
//
// It is a startup panic rather than a compile error, which is the cost of
// keeping one ledger array as the single source of truth. The panic has to say
// what the shape actually is, because the mistake looks like a typo and reads
// like a working line.
func TestShapeMismatchPanicsWithTheLedgerShape(t *testing.T) {
	r := New(testOptions())
	msg := mustPanic(t, "circuit_state read as a Counter", func() {
		r.Counter(IDCircuitState)
	})
	if !strings.Contains(msg, "circuit_state") || !strings.Contains(msg, "shape G") {
		t.Errorf("the panic does not name the instrument and its real shape: %q", msg)
	}
}

// TestLabelArityMismatchPanics is the reason there are separate At and At2
// accessors instead of one variadic. A {k,destination} instrument given one
// index would otherwise land on destination zero of that shard and look
// entirely plausible.
func TestLabelArityMismatchPanics(t *testing.T) {
	r := New(testOptions())

	msg := mustPanic(t, "dirty_depth with one index", func() { r.GaugeAt(IDDirtyDepth, 0) })
	if !strings.Contains(msg, "{k,destination}") {
		t.Errorf("the panic does not name the expected label spelling: %q", msg)
	}

	msg = mustPanic(t, "datagrams_received with an index", func() {
		r.CounterAt(IDDatagramsReceived, 0)
	})
	if !strings.Contains(msg, "datagrams_received") {
		t.Errorf("the panic does not name the instrument: %q", msg)
	}
}

func TestLabelOutOfRangePanics(t *testing.T) {
	r := New(testOptions())
	msg := mustPanic(t, "flush_on_tick{4} with 4 shards", func() {
		r.CounterAt(IDFlushOnTick, 4)
	})
	if !strings.Contains(msg, "0..3") {
		t.Errorf("the panic does not state the valid range: %q", msg)
	}
}

// TestPaddedDimensionsAreSpacedAcrossCacheLines pins the false-sharing
// decision, because it is invisible in behaviour and would otherwise be
// "tidied" away by someone reading the stride as redundant.
func TestPaddedDimensionsAreSpacedAcrossCacheLines(t *testing.T) {
	r := New(testOptions())
	for _, id := range []ID{IDFlushOnTick, IDRowsCreated} {
		d := &r.dims[ledger[id].dim]
		if d.stride != padStride {
			t.Errorf("%s is labelled %s, which several goroutines write concurrently, "+
				"but its stride is %d rather than padStride. Sixteen atomic.Uint64s "+
				"share one 128-byte ppc64 cache line, and Vol 4 section 6.7 says there "+
				"is no profiler on that platform to find the stall with",
				ledger[id].name, d.brace, d.stride)
		}
	}
	// And the unpadded ones stay unpadded: padding every dimension would waste
	// the slab for no benefit, since most labels have a single writer.
	if got := r.dims[dimPanicStage].stride; got != 1 {
		t.Errorf("{stage} stride = %d, want 1", got)
	}
}

// TestWriterLabelIsSizedToTheReloadableMaximum records a correction made during
// implementation.
//
// stmt_cache_entries{writer} was first sized to writers.count at startup,
// which is wrong: writers.count is reloadable -- it is absent from
// startupOnly() in internal/config/validate.go and section 9.3 calls raising it
// "the one lever" for a write path that cannot keep up. A reload that added
// writers would leave them with nowhere to publish, which is invisible rather
// than wrong and therefore worse.
func TestWriterLabelIsSizedToTheReloadableMaximum(t *testing.T) {
	r := New(testOptions())
	if got := r.dims[dimWriter].size; got != maxWriters {
		t.Errorf("{writer} has %d slots, want %d (the V-RANGE maximum for "+
			"writers.count). Sizing it to the startup count loses every writer a "+
			"reload adds", got, maxWriters)
	}
	// Resolvable at the top of the range, which is the case that would panic
	// if the sizing regressed.
	r.GaugeAt(IDStmtCacheEntries, maxWriters-1)
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

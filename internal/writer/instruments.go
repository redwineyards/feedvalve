package writer

import (
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// Instruments is the writer's set of metric handles, resolved once at startup
// before the registry is frozen (G-OBS-1) and then held directly, so the
// bind path never does a lookup. It is a plain struct (ruling D5): no
// interface, no obs-side type. A nil handle is skipped, which keeps tests that
// do not care about metrics free of a registry.
type Instruments struct {
	// BindNonfinite counts a NaN or infinite double bound as NULL, by
	// {destination, column bit}.
	BindNonfinite [model.MaxDestinations][model.MaxColumns]*obs.Counter
	// BindBadTime counts a non-positive time bound as NULL, by
	// {destination, column bit}.
	BindBadTime [model.MaxDestinations][model.MaxColumns]*obs.Counter

	cache cacheInstruments // P4-06; see ResolveCache

	// Run is what one batch execution reports through (runBatch); ResolvePool
	// fills it.
	Run *RunInstruments

	// Pool-level handles (P4-09), filled by ResolvePool.
	WritersConfigured *obs.Gauge   // writers_configured
	WritersRunning    *obs.Gauge   // writers_running
	WritersStopped    *obs.Counter // writers_stopped: a member retired itself
	HealthState       *obs.Gauge   // writer_health_state, enum 0-2
	WritesAttempted   *obs.Counter
	WritesSucceeded   *obs.Counter
	WritesFailed      *obs.Counter
	BatchesWritten    *obs.Counter
	ConnRecycled      [3]*obs.Counter // writer_conn_recycled{reason}
	Probe             [2]*obs.Counter // write_probe{result}
	WriteRetry        [4]*obs.Counter // write_retry{class}

	// OracleError counts oracle_error{code}. It is a func so the writer holds
	// no registry: ResolvePool takes the concrete method value.
	OracleError func(code int)
}

// ResolvePool resolves the batch-execution and pool handles for the first
// `destinations` destinations. Call it before reg.Freeze.
func (in *Instruments) ResolvePool(reg *obs.Reg, destinations int) {
	in.Run = NewRunInstruments(reg, destinations)
	in.WritersConfigured = reg.Gauge(obs.IDWritersConfigured)
	in.WritersRunning = reg.Gauge(obs.IDWritersRunning)
	in.WritersStopped = reg.Counter(obs.IDWritersStopped)
	in.HealthState = reg.Gauge(obs.IDWriterHealthState)
	in.WritesAttempted = reg.Counter(obs.IDWritesAttempted)
	in.WritesSucceeded = reg.Counter(obs.IDWritesSucceeded)
	in.WritesFailed = reg.Counter(obs.IDWritesFailed)
	in.BatchesWritten = reg.Counter(obs.IDBatchesWritten)
	for i := range in.ConnRecycled {
		in.ConnRecycled[i] = reg.CounterAt(obs.IDWriterConnRecycled, i)
	}
	for i := range in.Probe {
		in.Probe[i] = reg.CounterAt(obs.IDWriteProbe, i)
	}
	for i := range in.WriteRetry {
		in.WriteRetry[i] = reg.CounterAt(obs.IDWriteRetry, i)
	}
	oe := reg.OracleError
	in.OracleError = func(code int) { oe(code).Add(1) }
}

// NewInstruments resolves the handles for the first `destinations`
// destinations against reg. Call it before reg.Freeze.
func NewInstruments(reg *obs.Reg, destinations int) *Instruments {
	in := &Instruments{}
	for d := 0; d < destinations && d < model.MaxDestinations; d++ {
		for c := 0; c < model.MaxColumns; c++ {
			in.BindNonfinite[d][c] = reg.CounterAt2(obs.IDBindNonfinite, d, c)
			in.BindBadTime[d][c] = reg.CounterAt2(obs.IDBindBadTime, d, c)
		}
	}
	return in
}

// --- grouping and statement cache (P4-06) ---

// StmtCacheEntries is the per-writer gauge of cached statements. Resolve it
// with ResolveCache before the registry is frozen.
// StmtCacheUncached counts an execution that prepared, ran and closed a
// statement because the cache was full (W-6).
// BatchRowEmptyMask counts rows refused for an empty mask, by destination (W-4).
type cacheInstruments struct {
	StmtCacheEntries  []*obs.Gauge
	StmtCacheUncached *obs.Counter
	BatchRowEmptyMask [model.MaxDestinations]*obs.Counter
}

// ResolveCache resolves the grouping and cache handles for `writers` writers
// and `destinations` destinations. Call it before reg.Freeze.
func (in *Instruments) ResolveCache(reg *obs.Reg, writers, destinations int) {
	in.cache.StmtCacheEntries = make([]*obs.Gauge, writers)
	for w := range in.cache.StmtCacheEntries {
		in.cache.StmtCacheEntries[w] = reg.GaugeAt(obs.IDStmtCacheEntries, w)
	}
	in.cache.StmtCacheUncached = reg.Counter(obs.IDStmtCacheUncached)
	for d := 0; d < destinations && d < model.MaxDestinations; d++ {
		in.cache.BatchRowEmptyMask[d] = reg.CounterAt(obs.IDBatchRowEmptyMask, d)
	}
}

func (in *Instruments) entriesGauge(w int) *obs.Gauge {
	if in == nil || w < 0 || w >= len(in.cache.StmtCacheEntries) {
		return nil
	}
	return in.cache.StmtCacheEntries[w]
}

func add(c *obs.Counter) {
	if c != nil {
		c.Add(1)
	}
}

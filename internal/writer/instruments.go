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

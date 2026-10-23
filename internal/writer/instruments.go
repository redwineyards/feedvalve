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

func add(c *obs.Counter) {
	if c != nil {
		c.Add(1)
	}
}

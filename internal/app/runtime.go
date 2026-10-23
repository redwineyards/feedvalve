package app

import (
	"hash/maphash"
	"net/netip"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/mirror"
)

// Runtime is the configuration and everything derived from it, published as ONE
// pointer (ADR-L11).
//
// The receive path needs two things that change together on reload: the
// *config.Config and the *decode.Registry built from it. The registry cannot
// live inside Config, because config may not import decode -- that edge would
// cycle, which is why BuildRegistry lives in decode and takes a *config.Config.
//
// One pointer and not two, and the reason is a skew window rather than
// tidiness: with two atomic pointers swapped in sequence there is an instant in
// which a reader observes the NEW config with the OLD registry. In the worst
// case that is a transcode configured for a destination the registry has not
// resolved a slot for -- a contributor-slot mismatch, which is exactly why the
// transcode set is startup-only in the first place. A skew window reachable
// only under reload is the kind of defect that reproduces once a quarter.
//
// A mutex was the other alternative, and it is a lock on the receive path for a
// value that changes on SIGHUP.
type Runtime struct {
	Cfg   *config.Config
	Reg   decode.Registry
	Dests *destinations.Set

	// Mirror is Cfg projected onto the shard's reloadable parameters (phase 3
	// ruling D1). Inside this snapshot rather than behind a second
	// atomic.Pointer, for the skew argument above: a shard loading Params must
	// never see a generation the rest of the snapshot does not describe.
	Mirror *mirror.Params

	// SrcPrefixes is udp.sourceAllowList parsed ONCE per generation, here and
	// not per datagram (parsing allocates; Vol 1 section 3.5). The receiver
	// loads the snapshot once per datagram, so udp.sourceAllowList and
	// udp.sourceEnforce are reloadable (Vol 0 section 7.2) with no allocation
	// and no second pointer.
	SrcPrefixes []netip.Prefix
}

// newRuntime builds one snapshot from a validated configuration and the
// registry built from it. The only constructor, so the startup path and the
// SIGHUP path cannot derive the snapshot differently. It fails only on an
// unvalidated allow-list, which V-SRC1 rejects at load.
func newRuntime(cfg *config.Config, reg decode.Registry, dests *destinations.Set) (*Runtime, error) {
	prefixes, err := cfg.UDP.SourcePrefixes()
	if err != nil {
		return nil, err
	}
	return &Runtime{Cfg: cfg, Reg: reg, Dests: dests, Mirror: mirrorParams(cfg), SrcPrefixes: prefixes}, nil
}

// mirrorParams projects the configuration onto mirror.Params -- the ONE place
// it is derived, so the startup path and Phase 5's reload cannot disagree
// about a unit conversion. mirror cannot import config (ruling D1: config
// carries oradb.DBSettings, and G-DEP-1 is transitive), which is why this lives
// in app, beside transcodeSpecs and obsOptions.
func mirrorParams(cfg *config.Config) *mirror.Params {
	return &mirror.Params{
		Gen:            cfg.Gen,
		Window:         cfg.Window(),
		MaxBatchSize:   cfg.Flush.MaxBatchSize,
		EpochStallWarn: time.Duration(cfg.Flush.EpochStallWarnMs) * time.Millisecond,
		DrainTimeout:   cfg.DrainTimeout(),

		Guard:                 parseGuard(cfg.Ordering.Guard),
		GuardSuppressionRatio: cfg.Ordering.GuardSuppressionRatio,
		GuardMinSamples:       cfg.Ordering.GuardMinSamples,
		GuardEvalInterval:     time.Duration(cfg.Ordering.GuardEvalIntervalSec) * time.Second,
	}
}

// mirrorSizing is the startup-only half: what sizes the structures NewShard
// allocates, plus the one process-wide digest seed every shard shares.
func mirrorSizing(cfg *config.Config, seed maphash.Seed) mirror.Sizing {
	r := &cfg.Runtime
	return mirror.Sizing{
		Shards:              r.Shards,
		ExpectedRows:        r.ExpectedRows,
		ArenaChunkRows:      r.ArenaChunkRows,
		MaxInFlightPerShard: r.MaxInFlightPerShard,
		MaxBatchSizeCeiling: r.MaxBatchSizeCeiling,
		Seed:                seed,
	}
}

// parseGuard maps ordering.guard onto mirror.GuardMode. config's V-RANGE has
// already restricted it to on, off and auto; auto, the default, is the
// fallback for the branch no validated Config reaches.
func parseGuard(s string) mirror.GuardMode {
	switch s {
	case "on":
		return mirror.GuardOn
	case "off":
		return mirror.GuardOff
	}
	return mirror.GuardAuto
}

// loadRuntime returns the current snapshot.
//
// Called ONCE per datagram and held for the whole datagram (Vol 0 section 7.4).
// Holding one snapshot for the whole datagram is what stops the configuration
// changing halfway through a fan-out -- the stall budget in particular, which
// would otherwise be measured against one deadline for the first unit and
// another for the thirty-fifth.
//
// WHERE this load happens is a reconciliation worth recording. Vol 0 section
// 7.4 says "receiver -- top of each read-loop iteration, before
// ReadFromUDPAddrPort", and Vol 1 section 2.1 draws read, frame, decode and
// route as one goroutine. The topology that was actually built has QD between
// a receiver and a decoder goroutine (Vol 0 section 9.5, P1-08/P1-09, and
// framing/doc.go explains why QD sits where the contract never placed it). The
// invariant the contract is protecting is "one snapshot per datagram, held for
// its whole processing", and in this topology that is the top of the DECODER's
// work -- the receiver does nothing a snapshot could skew.
func (g *Graph) loadRuntime() *Runtime { return g.runtime.Load() }

// setRuntime publishes a snapshot. Called by Build at startup and, from
// Phase 5, by the SIGHUP path after it has validated and built a whole new one.
func (g *Graph) setRuntime(rt *Runtime) { g.runtime.Store(rt) }

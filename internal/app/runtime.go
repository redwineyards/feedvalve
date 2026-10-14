package app

import (
	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
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

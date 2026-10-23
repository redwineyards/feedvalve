// Package health owns two state machines: circuit liveness, which decides
// whether the feed is arriving, and the market session, which tracks the
// exchange's own open and close transitions.
//
// health MUST NOT import oradb, writer or mirror.
//
// Liveness is driven by the heartbeat the exchange sends on a published
// interval. The warn and down thresholds are configuration rather than
// constants, because that interval is the exchange's to change. Outside the
// trading window the severity is downgraded, never the data: the window gates
// how loudly a silence is reported, and nothing else.
//
// The session machine consumes events by value so it observes each transition
// rather than a sampled end state -- a same-day re-open can pass through several
// states inside one second, and a machine that sampled would see only the last.
//
// Specified by docs/lld/04-lifecycle-ops.md (sections 3 and 4).
package health

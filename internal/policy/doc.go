// Package policy maps a transaction code to its class -- rate-limited or
// non-rate-limited -- and validates the inventory.
//
// policy MUST NOT import oradb, writer, mirror or database/sql.
//
// The class decides how a decoded unit is treated downstream. A rate-limited
// transaction code arrives on the exchange's own schedule and coalescing is the
// point; a non-rate-limited one is event-driven and must not be held back. A
// non-rate-limited code observed running at a high rate is warned about, because
// that combination is what turns flush amplification into a capacity problem.
//
// The class table is configuration, loaded with the rest of the config in P1-05,
// and consumed from Phase 2 onward.
//
// Specified by docs/lld/01-receive-decode.md.
package policy

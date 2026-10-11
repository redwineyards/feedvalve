// Package obs owns the counter and gauge registry, the periodic emission line,
// and the two loggers: appLogger for operational output and errors, and
// debugLogger for field-level tracing.
//
// obs MUST NOT import any other internal package. That is what makes it
// importable from everywhere else without creating a cycle.
//
// Three instrument shapes, and no others. A counter is a monotonic atomic
// counter. A max is a compare-and-swap high-water mark, reset at emission. A
// gauge is published by the goroutine that owns the underlying state -- never
// read across a goroutine boundary from a shard-owned slice, which would be a
// data race on a platform where the race detector does not exist.
//
// Labels resolve to fixed arrays at startup. Every instrument must reach a
// struct field, never a map lookup, on any path that runs per datagram. The
// registry is keyed by an ID constant rather than by a name, which is a
// deliberate departure from section 8.4's sketch: a name-keyed accessor can
// only honour that MUST by convention, while an ID-keyed one honours it by
// construction and turns a mistyped instrument into a build error. What section
// 8.4 asks to be substitutable is the backend, not the naming scheme.
//
// Resolution ends at Freeze, after which every accessor panics (gate G-OBS-1).
// The one exception is UnknownTranscode, because an unknown transaction code is
// by definition not knowable at startup; it resolves through a CAS into a
// fixed-size table and allocates nothing.
//
// obs owns NO goroutine and holds no context. The sixty-second ticker belongs
// to app's stats goroutine, which calls Emit; there is nothing here to start or
// stop. Nor does Emit ever call back into its caller -- the gauges that Vol 0
// section 8.2 describes as sampled at emission, batchq_depth and
// writers_configured among them, are Stored by their owner immediately before
// Emit is called. A sampling callback would put a closure and an interface call
// on the one path that must not allocate.
//
// The health group is a PROJECTION, not an ownership group. Every instrument
// has exactly one home group, which names the line it is emitted on; the health
// line is a separate ordered list of the dozen values the runbook's first three
// questions read, emitted first and every interval. An instrument named there
// is therefore emitted twice in one emission, and that is the intent.
//
// There is no packet logger, and adding one back would be a mistake. The
// proof-of-concept's full-payload logger wrote several times the wire rate to a
// world-readable, append-only file; the HLD deletes it, and the output-size cap
// that existed to contain it goes with it. A secret that lands in that file is
// there permanently.
//
// Carried over, in part, from poc/logger.go.
//
// Specified by docs/lld/00-foundations.md section 8. Built in P1-07.
package obs

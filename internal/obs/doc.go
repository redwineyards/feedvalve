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
// struct field, never a map lookup, on any path that runs per datagram.
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

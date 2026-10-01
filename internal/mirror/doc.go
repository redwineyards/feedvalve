// Package mirror owns the shard goroutine: the row index, the entry arenas, the
// accumulating and cohort rings, the epoch state machine, and the batch free
// list.
//
// mirror MUST NOT import oradb, writer, database/sql or net.
//
// A shard goroutine that cannot name a database handle cannot block on a commit.
// The HLD depends on that property and states it only in prose; here it is a
// compile error instead. The net prohibition is the same argument from the other
// side: a shard must not be able to touch the socket either.
//
// The mirror is the queue. Backlog is bounded by the key universe rather than by
// the arrival rate, so overload produces larger batches and older data -- never
// loss.
//
// Two details that look like implementation freedom but are not. The dirty
// structure is a real circular buffer, not a slice: the failure path head-inserts
// deliberately, and a slice has no constant-time head insertion, so a database
// outage would otherwise turn into hundreds of megabytes of memmove per epoch and
// crash the process through its own retry path. And the column mask is cleared
// when the pump copies it into a batch, not when the batch completes, so a merge
// arriving mid-flight re-dirties the entry instead of having its columns erased
// by the completion.
//
// Specified by docs/lld/02-shard-mirror.md. Built in Phase 3.
package mirror

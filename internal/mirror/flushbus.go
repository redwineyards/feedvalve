package mirror

import (
	"sync/atomic"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// FlushBus is the only state shared between shard goroutines (Vol 2 section 3,
// ADR-L20). It holds nothing but atomics and channels, so no field of one shard
// is reachable from another. Constructed once by app, immutable thereafter.
//
// Two halves, and both are required:
//
//   - req[k], the LEVEL: "shard k owes an epoch". This is what the shard acts on;
//     its main loop re-reads it after every select case (section 2.3).
//   - wake[k], the EDGE: capacity 1, carrying no information. Its only job is to
//     make a parked shard's select return. A dropped edge is harmless, because a
//     shard with a token already pending is going to re-read the level anyway.
//
// wake[k] is never closed: a closed chan struct{} is permanently ready and would
// turn the wake arm into a spin loop (section 3.6). Shutdown is signalled by
// closing shardCh[k] and by ctx.
type FlushBus struct {
	req  []atomic.Bool
	wake []chan struct{}
}

// busInstruments are the bus's counters, resolved before the registry freezes so
// Trigger does no lookup on the shard path.
type busInstruments struct {
	coalesced   *obs.Counter   // flush_coalesced: a Swap found the bit already set
	wakeDropped []*obs.Counter // flush_wake_dropped{j}: wake[j]'s buffer was full
}

// NewFlushBus builds the bus for shards shard goroutines.
func NewFlushBus(shards int) *FlushBus {
	b := &FlushBus{
		req:  make([]atomic.Bool, shards),
		wake: make([]chan struct{}, shards),
	}
	for j := range b.wake {
		b.wake[j] = make(chan struct{}, 1)
	}
	return b
}

// Trigger broadcasts a flush request to every shard, the caller's own included.
// Called by a shard goroutine after it merged a non-rate-limited contribution
// that left a row dirty (section 4 step 9) -- never by the router, never before
// the merge.
//
// For every j: set the level with Swap, counting flush_coalesced if it was
// already set; then a NON-BLOCKING send on the edge, counting
// flush_wake_dropped{j} if the buffer was full. There is no special case for
// j == the caller: a non-blocking send on a buffered channel never parks the
// sender, so a shard signalling itself cannot deadlock (section 3.4).
//
// Wait-free; cannot block, allocate or fail.
func (b *FlushBus) Trigger(inst *busInstruments) {
	for j := range b.req {
		if b.req[j].Swap(true) {
			inst.coalesced.Add(1)
		}
		select {
		case b.wake[j] <- struct{}{}:
		default:
			inst.wakeDropped[j].Add(1)
		}
	}
}

// Taken clears shard k's request. Called only by shard k, and only once an
// epoch is definitely opening -- after the rings were found non-empty
// (section 6.3, step 2 before step 3).
func (b *FlushBus) Taken(k model.ShardID) { b.req[k].Store(false) }

// Pending reports shard k's level. Called by shard k once per loop pass.
func (b *FlushBus) Pending(k model.ShardID) bool { return b.req[k].Load() }

// Wake is shard k's edge, for its select. A receive means only "re-check the
// level"; the wake arm does nothing else (section 3.3).
func (b *FlushBus) Wake(k model.ShardID) <-chan struct{} { return b.wake[k] }

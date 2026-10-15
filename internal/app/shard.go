package app

import (
	"feedvalve/internal/model"
)

// ShardOf maps a unit onto one of n shards (Vol 1 section 10.4, HLD section
// 6.5).
//
// General units -- those with FlagGeneral -- go to generalShard per FR8,
// REGARDLESS of any Token field the message happens to carry. That is INV-SCOPE
// and it is not a special case to be tidied away: bible section 4.5's market
// status messages carry a Token field that names a contract, while the row they
// write is keyed by MarketType and is market-wide. Routing one of those by its
// Token would put the same MarketType row on four different shards, which
// breaks ASM-15's single-writer-per-row and produces intermittent lost updates
// with no error anywhere.
//
// For token-scoped units the token is passed through an integer finaliser
// before the modulo. The mix is NOT decoration: exchange tokens are allocated
// in contiguous blocks, so a plain token % n correlates shard assignment with
// contract family and puts a whole expiry series on one shard.
//
// The mix MUST be deterministic and stable for the life of the deployment: it
// decides which shard owns a row, so changing it re-keys every mirror. That is
// also why runtime.shards and runtime.generalShard are startup-only (Vol 0
// section 7.2, contradictions C3/C4).
func ShardOf(u *model.UpdateUnit, n int, generalShard int) model.ShardID {
	if u.General() {
		return model.ShardID(generalShard)
	}
	return model.ShardID(mix32(uint32(u.Token)) % uint32(n))
}

// mix32 is a 32->32 bit avalanche of the fmix32 shape: two xor-shifts and two
// odd multiplies, about four instructions, no allocation, no branch, no
// dependency.
//
// THE PROPERTIES ARE NORMATIVE, NOT THE CONSTANTS (Vol 1 section 10.4). Any
// stable finaliser satisfying these three would do:
//
//  1. deterministic across runs and architectures, so -selfcheck's ASM-15 check
//     and the running process agree about which shard owns a row;
//  2. avalanche in the LOW bits, because % n with a small n uses only those --
//     a mix that scrambled only the high bits would leave token % 4 exactly
//     where it started;
//  3. allocation-free and branch-free, because this runs once per unit on the
//     path G-ALLOC-1 measures at zero.
//
// Both multipliers are odd, which makes each multiply a bijection on uint32 and
// is what keeps the whole function one-to-one: two distinct tokens can collide
// on a shard, but never because the mix threw information away.
func mix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// allocShardQueues allocates Q1: one channel and one done channel per shard.
//
// A method rather than four lines inside Build, because the sub-packet tests
// construct a Graph directly -- they exercise the decode path without a
// lifecycle -- and a fan-out into a nil slice is an index panic rather than a
// useful failure. One allocator means the fixture and the real graph cannot
// disagree about the shape of the thing being fanned into.
func (g *Graph) allocShardQueues(shards, capacity int) {
	g.shardCh = make([]chan model.UpdateUnit, shards)
	g.shardDrainDone = make([]chan struct{}, shards)
	for k := range g.shardCh {
		g.shardCh[k] = make(chan model.UpdateUnit, capacity)
		g.shardDrainDone[k] = make(chan struct{})
	}
}

// startShardConsumers starts the thing that drains Q1.
//
// Phase 2 produces units and NOTHING consumes them -- the shards, the arenas
// and the merge are Phase 3. So each shard channel gets one goroutine that
// receives and discards, and Phase 3 replaces the loop body with the shard's
// own loop.
//
// It runs in the built graph rather than only in tests, and that is a decision
// with two reasons. Vol 0 section 9.5.7's shutdown ordering -- close the socket,
// drain QD, THEN close each shardCh[k] -- is exercised end to end now instead of
// first appearing in Phase 3; and without a consumer the Phase 2 binary against
// a real feed would queue 4,096 units and then take the fatal path, which would
// report a defect that is really just an unfinished phase.
//
// Started BEFORE the decoder, which is the same consumers-before-producers rule
// that already puts the decoder before the receiver: starting a producer first
// burns queue slack for nothing.
func (g *Graph) startShardConsumers() {
	g.shardConsumersStarted = true
	g.mark(&g.acquired, "shard-consumers")
	for k := range g.shardCh {
		ch, done := g.shardCh[k], g.shardDrainDone[k]
		go func() {
			defer close(done)
			// Receives and discards until the channel is closed. The loop
			// variable is omitted deliberately: binding each 360-byte unit to a
			// name only to drop it is a copy the compiler need not make.
			for range ch {
			}
		}()
	}
}

// stopShardConsumers is shutdown step 6: close every shardCh[k] and wait for
// its consumer to finish what is already queued.
//
// CLOSED, NOT ABANDONED, and the ordering is what makes that safe: this runs
// only after the decoder goroutine has returned, so the sole producer is gone
// and no send can race the close. Closing while the decoder still held the
// channel would panic on send -- which is why this is a separate function
// called from one place in teardown rather than a defer that could be
// registered anywhere.
func (g *Graph) stopShardConsumers() {
	if !g.shardConsumersStarted {
		return
	}
	for k := range g.shardCh {
		close(g.shardCh[k])
	}
	g.mark(&g.released, "shard-channels")

	for k := range g.shardDrainDone {
		<-g.shardDrainDone[k]
	}
	g.mark(&g.released, "shard-consumers")
}

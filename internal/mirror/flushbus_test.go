package mirror

import (
	"sync"
	"testing"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// newTestBus is a bus of the given width with its own fresh counters.
func newTestBus(shards int) (*FlushBus, *busInstruments) {
	inst := &busInstruments{coalesced: &obs.Counter{}, wakeDropped: make([]*obs.Counter, shards)}
	for j := range inst.wakeDropped {
		inst.wakeDropped[j] = &obs.Counter{}
	}
	return NewFlushBus(shards), inst
}

// tokens reports how many wake tokens shard k has buffered (0 or 1).
func tokens(b *FlushBus, k model.ShardID) int { return len(b.wake[k]) }

// TestFlushBusSelfTriggerNeverBlocks: Trigger sends on the caller's own wake
// channel while the caller is, by construction, not in select. Repeated on a
// full buffer, it must still return at once (section 3.4). An unbuffered or
// blocking send here is a permanent deadlock of the shard against itself --
// the single likeliest way to build this wrong -- so the test runs Trigger on
// a goroutine that receives from nothing and bounds it with a deadline.
func TestFlushBusSelfTriggerNeverBlocks(t *testing.T) {
	b, inst := newTestBus(1)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			b.Trigger(inst)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Trigger blocked when the only shard signalled itself")
	}
	// Non-vacuity: the 2nd and 3rd sends really did meet a full buffer.
	if got := inst.wakeDropped[0].Load(); got != 2 {
		t.Fatalf("flush_wake_dropped{0} = %d, want 2 -- the buffer was never full, "+
			"so the blocking case was not exercised", got)
	}
}

// TestFlushBusEdgeLevelTable is one sub-test per row of Vol 2 section 3.3's
// table: the edge and the level can disagree in both directions, and each
// disagreement has a specified, correct outcome.
func TestFlushBusEdgeLevelTable(t *testing.T) {
	t.Run("two triggers while busy: one token, both counters move", func(t *testing.T) {
		b, inst := newTestBus(1)
		b.Trigger(inst)
		b.Trigger(inst)
		if !b.Pending(0) || tokens(b, 0) != 1 {
			t.Fatalf("level=%v tokens=%d, want set and 1", b.Pending(0), tokens(b, 0))
		}
		if c, d := inst.coalesced.Load(), inst.wakeDropped[0].Load(); c != 1 || d != 1 {
			t.Fatalf("flush_coalesced=%d flush_wake_dropped=%d, want 1 and 1", c, d)
		}
	})

	t.Run("Taken before the token is drained: a stale token, level clear", func(t *testing.T) {
		b, inst := newTestBus(1)
		b.Trigger(inst)
		b.Taken(0) // an epoch opened for an unrelated tick
		if b.Pending(0) || tokens(b, 0) != 1 {
			t.Fatalf("level=%v tokens=%d, want clear and 1", b.Pending(0), tokens(b, 0))
		}
		<-b.Wake(0) // the one wasted pass
		if b.Pending(0) {
			t.Fatal("the stale token's pass found the level set; it must find it clear")
		}
	})

	t.Run("edge dropped while busy: the level alone carries the request", func(t *testing.T) {
		b, inst := newTestBus(1)
		b.Trigger(inst) // an earlier trigger whose token is still unread
		b.Taken(0)      // ... and whose epoch has already opened
		b.Trigger(inst) // the new request: its edge is dropped
		if got := inst.wakeDropped[0].Load(); got != 1 {
			t.Fatalf("flush_wake_dropped = %d, want 1: the edge was supposed to be lost", got)
		}
		if !b.Pending(0) {
			t.Fatal("the level is clear after a trigger whose edge was dropped -- the request is lost")
		}
		if got := inst.coalesced.Load(); got != 0 {
			t.Fatalf("flush_coalesced = %d, want 0: Taken cleared the bit before the second Swap", got)
		}
	})

	t.Run("parked shard: the edge unparks it and the level is set", func(t *testing.T) {
		b, inst := newTestBus(2)
		saw := make(chan bool)
		go func() {
			<-b.Wake(1) // parked with nothing to do
			saw <- b.Pending(1)
		}()
		b.Trigger(inst) // from shard 0
		select {
		case pending := <-saw:
			if !pending {
				t.Fatal("shard 1 woke and found its level clear")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a parked shard was never woken by Trigger")
		}
		if tokens(b, 1) != 0 {
			t.Fatal("the delivered token is still buffered")
		}
	})
}

// TestFlushBusTakenClearsOnlyItsOwnLevel: Taken(k) is shard k consuming its
// own request; it must not consume another shard's.
func TestFlushBusTakenClearsOnlyItsOwnLevel(t *testing.T) {
	b, inst := newTestBus(4)
	b.Trigger(inst)
	for k := range model.ShardID(4) {
		if !b.Pending(k) {
			t.Fatalf("Trigger did not set shard %d's level; it must reach every shard", k)
		}
	}
	b.Taken(2)
	for k := range model.ShardID(4) {
		if want := k != 2; b.Pending(k) != want {
			t.Fatalf("after Taken(2), Pending(%d) = %v, want %v", k, b.Pending(k), want)
		}
	}
}

// TestFlushBusConcurrentTriggerAndConsume runs S triggering goroutines against
// S shard loops doing what stage B does -- wake, Pending, Taken -- and checks
// two conservation laws that hold exactly however the operations interleave:
//
//	level: S*T swaps == flush_coalesced + rising edges, and every rising edge is
//	       either consumed by a Taken or still pending at the end. Only shard k
//	       clears req[k], and only right after seeing it set, so each Taken
//	       consumes exactly one rising edge.
//	edge:  per shard, T sends == flush_wake_dropped{j} + received + buffered.
//
// A lost request or a double count breaks one of them. Meant to be run with
// -race -count=20.
func TestFlushBusConcurrentTriggerAndConsume(t *testing.T) {
	const S, T = 4, 2000
	b, inst := newTestBus(S)
	stop := make(chan struct{})
	var taken, received [S]uint64

	var shards sync.WaitGroup
	for k := range model.ShardID(S) {
		shards.Add(1)
		go func() {
			defer shards.Done()
			for {
				select {
				case <-b.Wake(k):
					received[k]++
				case <-stop:
					return
				}
				if b.Pending(k) {
					b.Taken(k)
					taken[k]++
				}
			}
		}()
	}

	var triggers sync.WaitGroup
	for range S {
		triggers.Add(1)
		go func() {
			defer triggers.Done()
			for range T / S {
				b.Trigger(inst)
			}
		}()
	}
	triggers.Wait()
	close(stop)
	shards.Wait()

	var rising, pending uint64
	for k := range model.ShardID(S) {
		rising += taken[k]
		if b.Pending(k) {
			pending++
		}
		sent := uint64(T) // every Trigger sends to every shard
		if got := inst.wakeDropped[k].Load() + received[k] + uint64(tokens(b, k)); got != sent {
			t.Errorf("shard %d: dropped+received+buffered = %d, want %d", k, got, sent)
		}
	}
	if got, want := inst.coalesced.Load()+rising+pending, uint64(S*T); got != want {
		t.Errorf("coalesced(%d)+taken(%d)+pending(%d) = %d, want %d swaps",
			inst.coalesced.Load(), rising, pending, got, want)
	}
	if rising == 0 {
		t.Error("no shard ever consumed a request; the race was not exercised")
	}
}

// TestFlushBusTriggerAllocatesNothing: Trigger runs inside the merge on the
// shard goroutine and must be allocation-free.
func TestFlushBusTriggerAllocatesNothing(t *testing.T) {
	b, inst := newTestBus(4)
	if n := testing.AllocsPerRun(1000, func() { b.Trigger(inst) }); n != 0 {
		t.Fatalf("Trigger allocated %v times per run, want 0", n)
	}
	// Non-vacuity: the measured calls did the work -- coalescing and dropping.
	if inst.coalesced.Load() == 0 || inst.wakeDropped[3].Load() == 0 {
		t.Fatal("the measured Triggers neither coalesced nor dropped; nothing was measured")
	}
}

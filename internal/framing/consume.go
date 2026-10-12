package framing

import (
	"feedvalve/internal/model"
)

// spinBeforePark is how many times the consumer re-reads tail before parking.
//
// 64, and the number is chosen against what a spin can actually bridge. The
// only gap worth spinning through is the window where the producer sits between
// "write slot" and "store tail" -- a 1040-byte copy and an atomic store, order
// 50 to 150 ns on the target. One iteration is one sequentially-consistent
// atomic load, a few nanoseconds, so 64 of them is roughly 300 to 600 ns: about
// 4x the window, with margin.
//
// It CANNOT bridge the inter-datagram gap, which at ~2,400 datagrams/s is
// ~400 microseconds. That is what the park is for, and no plausible spin bound
// would reach it -- Vol 0 section 9.5.4 rejects busy-waiting precisely because
// a spin long enough to cover idle time burns 25 % of the partition's entitled
// capacity of 4.
//
// Worst case, an idle feed where the spin always fails: 64 x ~10 ns x 2,400/s
// is about 1.5 ms per second, 0.15 % of one core. That is the price of not
// paying a scheduler round trip on every burst-internal gap, and it is the
// number to argue with if anyone wants to change this.
//
// THERE IS NO runtime.Gosched IN THE SPIN, and that is deliberate. Gosched is a
// scheduler entry at 100 to 200 ns, so 64 of them would be 6 to 13 microseconds
// on EVERY empty observation -- ~3 % of a core, about 500x the cost of the park
// section 9.5.4 computed at 0.006 %. It is also strictly worse behaviour: it
// yields and returns, putting the consumer back on a runqueue to spin again,
// whereas the park leaves the runqueue and is woken by a direct handoff from
// the producer. The spin exists to catch a sub-microsecond handoff WITHOUT
// entering the scheduler; once you are willing to enter it, park.
const spinBeforePark = 64

// Gate: the spin bound must stay small. One-sided, in the form
// destinations/layout.go uses for ASM-16b, because the rule is "at most": a
// spin an order of magnitude longer is a different design and should fail the
// build rather than be reviewed.
const _ = uint(256 - spinBeforePark)

// Parks returns how many times the consumer has parked. The decoder feeds this
// into qd_wake_parked, which Vol 0 section 8.2a reads as: "Rising with load
// means the spin bound is mistuned; near-constant means the feed is idle, which
// is normal."
//
// It is also what the tests spin on instead of sleeping, which is the only way
// to exercise the park and wake edges deterministically.
func (c *Consumer) Parks() uint64 { return c.r.parks.Load() }

// Drain delivers every datagram to fn, in arrival order, until the ring is
// closed AND empty. It is the whole consumer side: the dequeue, the spin, the
// park and the drain obligation.
//
// The loop lives inside the primitive rather than at the call site on purpose.
// Three properties have to hold together for the wakeup to be correct, and all
// three are easy to break from outside:
//
//   - The head store must precede the final emptiness check before parking.
//   - Emptiness must be checked before closed, or shutdown abandons data.
//   - The slot must be copied out before head moves.
//
// A decoder written against a bare Dequeue would have to re-derive all three.
//
// EXACTLY ONE GOROUTINE MAY DRAIN. A second concurrent call returns
// ErrMultipleConsumers immediately; see ErrMultipleConsumers for why this is
// not a style preference. Sequential calls are fine -- the guard is released on
// return -- which is what lets a test drain, assert, and drain again.
//
// fn returning a non-nil error stops the drain and Drain returns that error,
// WITHOUT draining the remainder. That asymmetry is deliberate: drain-to-empty
// is an obligation on the clean shutdown path of section 9.5.7, whereas an
// error out of the decoder is a fatal path already heading for exit, where
// abandoning the remainder is correct.
//
// The *model.Datagram handed to fn is valid ONLY for the duration of the call.
// It points at consumer-local storage, not into the ring.
func (c *Consumer) Drain(fn func(*model.Datagram) error) error {
	r := c.r
	if !r.consuming.CompareAndSwap(false, true) {
		return ErrMultipleConsumers
	}
	defer r.consuming.Store(false)

	// One local slot, reused for the life of the drain. Allocated here rather
	// than per item so the steady state allocates nothing.
	var d model.Datagram

	for {
		h := r.head.Load() // consumer-owned; cannot be stale for us

		if h != r.tail.Load() {
			// COPY THE SLOT OUT BEFORE MOVING head.
			//
			// This copy is not redundant and removing it is the likeliest bug
			// in this file. If fn were handed &r.slots[i] and head moved
			// first, the producer would be free to overwrite that slot while
			// the decoder was still reading it -- silent corruption of a
			// datagram already accepted, with no error path and nothing to
			// alarm on. One 1040-byte copy, about 50 ns, buys the slot's
			// lifetime.
			d = r.slots[h&r.mask]
			r.head.Store(h + 1)

			if err := fn(&d); err != nil {
				return err
			}
			continue
		}

		// Empty. Spin briefly: the producer may be mid-publish.
		if r.spin(h) {
			continue
		}

		// Still empty, and head is stored, so a producer publishing now is
		// guaranteed to send us a token (see TryPublish). Only NOW consider
		// closure -- emptiness before closed is section 9.5.7's "drained, not
		// abandoned", and checking them the other way round would discard
		// whatever the receiver published just before it stopped.
		if r.closed.Load() {
			// One last look, because a publish may have landed between the
			// emptiness test above and the closed test here.
			if h != r.tail.Load() {
				continue
			}
			return nil
		}

		r.parks.Add(1)
		<-r.wake
		// Level-plus-edge: the token is a hint, never the state. The loop
		// re-reads tail from the top, so a spurious or stale token costs one
		// wasted pass and nothing else -- which is what makes the
		// unconditional send in TryPublish safe.
	}
}

// spin re-reads tail up to spinBeforePark times, reporting whether data
// arrived. h is the consumer's head, already stored.
func (r *ring) spin(h uint64) bool {
	for range spinBeforePark {
		if h != r.tail.Load() {
			return true
		}
	}
	return false
}

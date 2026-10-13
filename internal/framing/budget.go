package framing

import (
	"runtime"
	"time"

	"feedvalve/internal/model"
)

// since is time.Since behind a variable so budget expiry is testable without
// sleeping. Tests replace it; nothing else may.
//
// It is time.Since and not a subtraction of two wall-clock readings. Vol 4
// section 10.2 makes that a MUST: an NTP step makes every elapsed measurement
// wrong and can yield a NEGATIVE duration, which as a remaining-budget
// computation hands a timer a value near 2^64 and as a uint64 counter argument
// is catastrophic.
var since = time.Since

// PublishWithin copies d into the ring, waiting for space until the budget
// expires.
//
// This is the only thing in framing that knows about clocks or about model's
// error vocabulary, and it is separate from TryPublish for three reasons. It is
// a policy about what to do when the ring is full, not a property of the ring.
// Vol 0 section 9.5.6 defines TWO budgets owned by two different goroutines,
// and the other one -- fan-out -- covers Q1 sends the ring knows nothing about.
// And a clock read on the publish path would be a measurable regression on an
// operation the contract describes as "a 1 KB copy and an atomic store".
//
// t0 is the caller's, not ours, and that is the whole point: Vol 1 section 10.5
// property 1 requires ONE budget per read-loop iteration, shared across every
// unit a datagram fans out, because "a per-unit deadline would let a single
// datagram stall for 35 x 50 ms and satisfy its own rule the whole way down".
// The receiver takes t0 once per datagram and passes it here and to the fan-out.
//
// Returns ErrClosed if the ring is closed, model.ErrStallBudgetExpired if the
// budget runs out with the ring still full, and nil on success. Expiry is fatal
// to the process (req section 10.2 -- "FeedValve discards no unit anywhere"),
// but that decision belongs to the caller: this returns an error and never
// exits, because os.Exit would skip the defers that close the database.
func (p *Producer) PublishWithin(d *model.Datagram, t0 time.Time, budget time.Duration) error {
	// Fast path first, and it reads no clock. At the occupancy Vol 0 section
	// 9.5.6 expects -- "the receiver's budget should be almost unused" --
	// essentially every publish ends here.
	if p.TryPublish(d) {
		return nil
	}
	if p.r.closed.Load() {
		return ErrClosed
	}

	// Full. Unlike the consumer's park there is nothing to park ON: section
	// 9.5.3 gives the ring two indices and ONE channel, producer to consumer,
	// and adding a second channel for the reverse direction would be a new
	// mechanism for a case that is already terminal. A full 8192-slot ring is
	// 0.53 s of socket slack gone, which means the decoder is dead and this
	// budget is about to expire.
	//
	// So this is a dying path, optimised for reporting rather than throughput,
	// and runtime.Gosched is right HERE even though it is wrong in the
	// consumer's spin: there is no park available, the alternative is burning
	// the budget in a hot loop, and yielding gives the consumer -- the
	// goroutine we are waiting on -- a chance to run.
	for {
		if since(t0) >= budget {
			return model.ErrStallBudgetExpired
		}
		runtime.Gosched()
		if p.TryPublish(d) {
			return nil
		}
		if p.r.closed.Load() {
			return ErrClosed
		}
	}
}

package framing

import (
	"errors"
	"sync/atomic"
	"time"

	"feedvalve/internal/model"
)

var (
	// ErrCapacity is returned by NewQD for a capacity below one.
	ErrCapacity = errors.New("framing: QD capacity must be at least 1")

	// ErrClosed is returned by a publish after Close. Publishing after close is
	// a contract violation -- Vol 0 section 9.5.7 stops the receiver before QD
	// is closed -- so this exists to make the violation visible and harmless
	// rather than to be handled.
	ErrClosed = errors.New("framing: QD is closed")

	// ErrMultipleConsumers is returned by a second concurrent Drain.
	//
	// Vol 0 section 9.5.5: "QD has exactly one consumer. Not a pool, not now,
	// not later." A second consumer reorders datagrams relative to arrival,
	// and arrival order at a single goroutine is ADR-007's mechanism 1 -- the
	// thing that makes ordering structural rather than dependent on the
	// sequence guard, which is a defence and can auto-disable itself per
	// transcode.
	ErrMultipleConsumers = errors.New("framing: QD already has a consumer")
)

// qd is QD, the datagram queue between the receiver and the decoder: a buffered
// channel of model.Datagram values.
//
// A channel and not the lock-free ring Vol 0 section 9.5 first specified.
// Section 9.5.4 measures what lock-free bought at this feed's ~2,400
// datagrams/s -- "Lock-free buys no throughput here and it would be dishonest
// to claim otherwise" -- and names the alternative: "a buffered channel is a
// correct drop-in and the throughput loss is unmeasurable". The channel brings
// its own park, wake and close-then-drain semantics, which the ring had to
// build by hand. Model.Datagram is pointer-free, so the buffer -- 8192 x 1040 B
// at the default capacity -- is one allocation the collector never scans.
type qd struct {
	ch chan model.Datagram

	// closed lets a publish after Close return ErrClosed instead of panicking
	// on a send to a closed channel. Close is called only once the receiver has
	// stopped (teardown joins the reader first), so the flag and the close are
	// never raced by a publish.
	closed atomic.Bool

	// consuming is Drain's single-consumer guard (section 9.5.5).
	consuming atomic.Bool
}

// Producer is the publishing half of QD. Exactly one goroutine may hold it.
type Producer struct{ q *qd }

// Consumer is the draining half of QD. Exactly one goroutine may drain it.
type Consumer struct{ q *qd }

// NewQD allocates QD with room for capacity datagrams and returns its two
// halves. The shipped process takes capacity from runtime.datagramQueueCapacity.
func NewQD(capacity int) (*Producer, *Consumer, error) {
	if capacity < 1 {
		return nil, nil, ErrCapacity
	}
	q := &qd{ch: make(chan model.Datagram, capacity)}
	return &Producer{q: q}, &Consumer{q: q}, nil
}

// Len returns the number of datagrams currently queued. From a third goroutine
// it is a sample, which is all a depth high-water mark needs.
func (p *Producer) Len() int { return len(p.q.ch) }

// Len returns the number of datagrams currently queued. See Producer.Len.
func (c *Consumer) Len() int { return len(c.q.ch) }

// TryPublish copies d into QD without blocking. It returns false when QD is full
// or closed; PublishWithin is the policy that decides what full means.
//
// It reads no clock: at the expected occupancy essentially every publish takes
// this path, and a clock read here would measure a quantity that is zero
// (Vol 1 section 10.5 property 3).
func (p *Producer) TryPublish(d *model.Datagram) bool {
	if p.q.closed.Load() {
		return false
	}
	select {
	case p.q.ch <- *d:
		return true
	default:
		return false
	}
}

// PublishWithin copies d into QD, waiting for space until budget has elapsed
// since t0.
//
// t0 is the caller's: Vol 1 section 10.5 property 1 requires ONE budget per
// read-loop iteration, shared across every unit a datagram fans out, so the
// receiver takes t0 once per datagram and passes it here and to the fan-out.
//
// Returns nil on success, ErrClosed if QD is closed, and
// model.ErrStallBudgetExpired if QD stays full for the whole budget. Expiry is
// fatal to the process (req section 10.2 -- "FeedValve discards no unit
// anywhere"), but that decision is the caller's: this returns an error and
// never exits, because os.Exit would skip the defers that close the database.
//
// The timer is allocated only on the slow path, which a full QD makes a dying
// one: a full 8192-deep queue is 3.4 s of feed the decoder has not taken.
func (p *Producer) PublishWithin(d *model.Datagram, t0 time.Time, budget time.Duration) error {
	if p.TryPublish(d) {
		return nil
	}
	if p.q.closed.Load() {
		return ErrClosed
	}
	left := budget - time.Since(t0)
	if left <= 0 {
		return model.ErrStallBudgetExpired
	}
	t := time.NewTimer(left)
	defer t.Stop()
	select {
	case p.q.ch <- *d:
		return nil
	case <-t.C:
		return model.ErrStallBudgetExpired
	}
}

// Close stops publishing. Drain keeps delivering until QD is empty before it
// returns: Vol 0 section 9.5.7, "QD must be DRAINED, not abandoned". Idempotent.
//
// It must be called only once the producing goroutine has stopped publishing,
// which teardown guarantees by joining the receiver first.
func (p *Producer) Close() {
	if p.q.closed.CompareAndSwap(false, true) {
		close(p.q.ch)
	}
}

// Drain delivers every datagram to fn, in arrival order, until QD is closed AND
// empty.
//
// EXACTLY ONE GOROUTINE MAY DRAIN. A second concurrent call returns
// ErrMultipleConsumers immediately. Sequential calls are fine -- the guard is
// released on return.
//
// fn returning a non-nil error stops the drain and Drain returns that error,
// WITHOUT draining the remainder: drain-to-empty is an obligation on the clean
// shutdown path, whereas an error out of the decoder is a fatal path already
// heading for exit.
//
// The *model.Datagram handed to fn is valid ONLY for the duration of the call:
// it points at one consumer-local value, reused for every datagram so the
// steady state allocates nothing.
func (c *Consumer) Drain(fn func(*model.Datagram) error) error {
	q := c.q
	if !q.consuming.CompareAndSwap(false, true) {
		return ErrMultipleConsumers
	}
	defer q.consuming.Store(false)

	var d model.Datagram
	for {
		var ok bool
		if d, ok = <-q.ch; !ok {
			return nil
		}
		if err := fn(&d); err != nil {
			return err
		}
	}
}

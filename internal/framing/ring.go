package framing

import (
	"errors"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"feedvalve/internal/model"
)

// cacheLine is the ppc64 cache line: 128 bytes, NOT 64.
//
// Vol 0 section 9.5.3 is explicit that head and tail must sit on separate
// lines, "or the producer and consumer write the same line on every operation
// and false sharing costs more than the lock this structure exists to avoid".
// Getting this wrong costs throughput and nothing else -- there is no
// correctness symptom and no profiler on the target (Vol 4 section 6.7) -- so
// it is pinned at build time rather than left to review.
const cacheLine = 128

var (
	// ErrCapacityNotPowerOfTwo is returned by NewRing. The ring wraps by
	// masking, so a non-power-of-two does not merely waste space: it silently
	// corrupts the index and the ring starts returning the wrong datagram.
	// config rejects it too (runtime.datagramQueueCapacity, rule V-RANGE), but
	// the ring is constructible without config and must refuse it itself.
	ErrCapacityNotPowerOfTwo = errors.New("framing: QD capacity must be a power of two")

	// ErrClosed is returned by a publish after Close. Publishing after close is
	// a contract violation -- Vol 0 section 9.5.7 stops the receiver before the
	// ring is closed -- so this exists to make the violation visible and
	// harmless rather than to be handled.
	ErrClosed = errors.New("framing: QD is closed")

	// ErrMultipleConsumers is returned by a second concurrent Drain.
	//
	// Vol 0 section 9.5.5: "QD has exactly one consumer. Not a pool, not now,
	// not later." A second consumer reorders datagrams relative to arrival,
	// and arrival order at a single goroutine is ADR-007's mechanism 1 -- the
	// thing that makes ordering structural rather than dependent on the
	// sequence guard, which is a defence and can auto-disable itself per
	// transcode. Widening QD would silently demote the whole ordering story to
	// that defence.
	ErrMultipleConsumers = errors.New("framing: QD already has a consumer")
)

// ring is QD: the lock-free single-producer/single-consumer datagram ring of
// Vol 0 section 9.5.
//
// It is unexported and reached only through Producer and Consumer. That split
// is the enforcement mechanism for section 9.5.5, and for an invariant the
// contract does not state but needs just as badly -- see NewRing.
//
// THE WAKEUP DISCIPLINE, in one sentence, because it makes all three wake paths
// correct at once:
//
//	ANNOUNCE, THEN SIGNAL.
//
// TryPublish stores tail, then sends. Close stores closed, then sends. The
// consumer observes tail, then observes closed, then parks. Reverse any
// announcement and its signal and a wakeup is lost. Every wake path in this
// file is an instance of that sentence.
type ring struct {
	// head is the consumer's index. Consumer-owned: only Drain stores it.
	head atomic.Uint64
	_    [cacheLine - unsafe.Sizeof(atomic.Uint64{})]byte

	// tail is the producer's index. Producer-owned: only TryPublish stores it.
	tail atomic.Uint64
	_    [cacheLine - unsafe.Sizeof(atomic.Uint64{})]byte

	// Immutable after NewRing, so no padding is needed around them: a field
	// nobody writes cannot cause false sharing.
	slots []model.Datagram
	mask  uint64

	// wake is the capacity-1 park channel, and the capacity is load-bearing.
	//
	// It is the "level" half of level-plus-edge: a non-blocking send into an
	// empty buffer leaves a token for a consumer that has not blocked yet, so
	// the signal is not lost to a consumer still spinning. An unbuffered
	// channel here would drop that token to the default arm and the consumer
	// would sleep until the next publish -- a latency hole of roughly one
	// inter-datagram gap, which is 400 microseconds at the expected rate.
	//
	// It is NEVER closed. Close() performs the same non-blocking send the
	// producer does, because a send on a closed channel panics and a
	// non-blocking send on a never-closed channel is safe from any goroutine,
	// concurrently with any other send, forever. That removes the only unsafe
	// sequencing this structure would otherwise have.
	wake chan struct{}

	// closed is written once, by Close, and read by the consumer. Not a
	// channel close, see wake.
	closed atomic.Bool

	// consuming is Drain's single-consumer guard (section 9.5.5).
	consuming atomic.Bool

	// Cold-path counters, exposed so the receiver and decoder can feed
	// qd_wake_parked and the full-ring signal into obs. They are NOT obs
	// instruments: internal/obs/ledger.go assigns qd_depth_max and
	// qd_send_blocked to the receiver and qd_wake_parked to the decoder, and if
	// the ring wrote them that ownership column would be a lie.
	parks atomic.Uint64
	fulls atomic.Uint64
}

// Producer is the publishing half of QD. Exactly one goroutine may hold it.
type Producer struct{ r *ring }

// Consumer is the draining half of QD. Exactly one goroutine may hold it.
type Consumer struct{ r *ring }

// NewRing allocates QD and returns its two halves.
//
// capacity must be a power of two. It may be as small as 1 -- deliberately
// smaller than the 1024 floor config enforces on runtime.datagramQueueCapacity
// (rule V-RANGE), because the ring is a primitive that knows nothing about
// config and because capacity 1 is the sharpest available test of the park and
// wake edges: every publish fills the ring and every consume empties it. The
// shipped process gets its capacity from config and so gets the 1024 floor too.
//
// The slots are one allocation, made once, and pointer-free: model.Datagram
// contains no pointer at any depth (gate G-PTR-1), so the collector never
// traces the ring. At the configured default that is 8192 x 1040 B = 8.5 MB.
//
// TWO HANDLES, NOT ONE, and the second reason is not in the contract. Section
// 9.5.5 makes single-CONSUMER non-negotiable and says why. Single-PRODUCER is
// equally load-bearing and goes unmentioned: tail is a read-modify-write
// through a producer-local, so two producers would both write a slot and both
// store tail+1, and one datagram would be lost SILENTLY -- no error, no
// counter, and no race detector on the target platform to catch it. Handing out
// one Producer and one Consumer makes both invariants a property of code shape
// instead of a comment someone has to obey.
func NewRing(capacity int) (*Producer, *Consumer, error) {
	if capacity < 1 || bits.OnesCount(uint(capacity)) != 1 {
		return nil, nil, ErrCapacityNotPowerOfTwo
	}
	r := &ring{
		slots: make([]model.Datagram, capacity),
		mask:  uint64(capacity) - 1,
		wake:  make(chan struct{}, 1),
	}
	return &Producer{r: r}, &Consumer{r: r}, nil
}

// Cap returns the ring's capacity.
func (p *Producer) Cap() int { return len(p.r.slots) }

// Cap returns the ring's capacity.
func (c *Consumer) Cap() int { return len(c.r.slots) }

// Len returns the number of datagrams currently queued.
//
// Exact when called from the producer or the consumer goroutine, since each
// owns one of the two indices. From any third goroutine it is two independent
// atomic loads and therefore an estimate -- which is all a depth gauge needs,
// and is why qd_depth_max is a high-water mark rather than a sampled gauge
// (Vol 0 section 8.2a: "QD is empty almost always; a sampled gauge would read 0
// forever").
func (p *Producer) Len() int { return int(p.r.length()) }

// Len returns the number of datagrams currently queued. See Producer.Len.
func (c *Consumer) Len() int { return int(c.r.length()) }

func (r *ring) length() uint64 { return r.tail.Load() - r.head.Load() }

// Fulls returns how many publishes found the ring full. The receiver feeds this
// into qd_send_blocked, which per Vol 0 section 9.5.6 "is a signal about the
// DECODER, not about the socket": publishing is a 1 KB copy and an atomic
// store, so the receiver's budget should be almost unused.
func (p *Producer) Fulls() uint64 { return p.r.fulls.Load() }

// TryPublish copies d into the ring. It never blocks and never allocates.
//
// Returns false when the ring is full or closed; the caller decides what that
// means. PublishWithin is the wrapper that turns "full" into a bounded wait and
// finally into model.ErrStallBudgetExpired, and it lives apart from this method
// so that THIS path reads no clock at all -- Vol 1 section 10.5 property 3, for
// the same reason the shard send does not: at the expected occupancy
// essentially every publish takes the fast path, and a time.Since here would
// add tens of thousands of clock reads a second to measure a quantity that is
// zero.
//
// The element is taken by pointer to avoid a second 1040-byte copy at the call
// site; the single copy into the slot is unavoidable, and is what makes the ring
// own its data rather than alias the receiver's read buffer.
func (p *Producer) TryPublish(d *model.Datagram) bool {
	r := p.r
	if r.closed.Load() {
		return false
	}

	// tail is producer-owned, so this load cannot be stale for us. It is read
	// through the atomic rather than cached in a local because a cached tail is
	// one refactor away from a cached head, and a cached head would break the
	// full test.
	t := r.tail.Load()
	if t-r.head.Load() > r.mask { // i.e. == capacity
		r.fulls.Add(1)
		return false
	}

	r.slots[t&r.mask] = *d

	// ANNOUNCE: the slot is written before the index moves, so a consumer that
	// sees this tail sees a complete slot. atomic.Uint64's Store is
	// sequentially consistent, which is stronger than the release this needs --
	// correct and portable, with no unsafe and no assembly (section 9.5.3).
	r.tail.Store(t + 1)

	// THEN SIGNAL, unconditionally.
	//
	// Section 9.5.4 says "the producer does a non-blocking send on the wake
	// channel after publishing into an EMPTY ring". Read the natural way --
	// test emptiness before publishing -- that drops wakeups:
	//
	//   head=0, tail=1. The consumer reads slot 0, stores head=1, tests
	//   head == load(tail) -> 1 == 1 -> empty, and parks. The producer
	//   publishes item 2; its load(head) is ordered before the consumer's
	//   store and returns 0, so it computes tail_before - head = 1 != 0,
	//   concludes "not empty", and SKIPS the send. Now head=1, tail=2: an item
	//   is queued and the consumer is asleep with no token.
	//
	// A conditional send can be made correct by deciding after the tail store
	// and re-loading head -- send iff (tail+1) - load(head) == 1 -- and that is
	// provable. It is not what this does, because the proof needs three
	// premises nothing in the code states: the channel must be capacity 1, the
	// consumer's head store must precede its final emptiness check, and the
	// producer's head load must be fresh. The second dies under batched head
	// publication (drain N, store head once), a standard SPSC optimisation, and
	// its failure mode is a 400 microsecond latency hole rather than a crash --
	// so nothing would find it.
	//
	// Unconditional costs one non-blocking channel send per publish. Section
	// 9.5.4 prices a channel operation at ~25 ns and the feed at ~2,400
	// datagrams/s: 60 microseconds per second, 0.006 % of a core. That section
	// also says, twice, that this is the right trade -- "Lock-free buys no
	// throughput here and it would be dishonest to claim otherwise", and "a
	// buffered channel is a correct drop-in". And Vol 2 section 3 settles the
	// shape question: the flush wakeup section 9.5.4 claims to match sends
	// unconditionally, with no emptiness predicate and no special case, because
	// "a special case would be one more branch and one more thing to get
	// wrong".
	select {
	case r.wake <- struct{}{}:
	default:
	}
	return true
}

// Close stops publishing and wakes a parked consumer so it can drain.
//
// Vol 0 section 9.5.7: "close QD and let the decoder drain it ... QD must be
// DRAINED, not abandoned, or the no-loss property has a hole at shutdown
// precisely where it is easiest to overlook." So Close does not discard
// anything: it marks the ring closed, and Drain keeps delivering until the ring
// is empty before it returns.
//
// It is a method on Producer so that "the receiver stops first" is a property of
// code shape -- the receiver calls it as defer p.Close() -- rather than an
// ordering comment in another volume. Idempotent, and safe to call while the
// consumer is parked.
func (p *Producer) Close() {
	r := p.r
	if !r.closed.CompareAndSwap(false, true) {
		return // already closed; the first caller sent the token
	}
	// ANNOUNCE (closed, above), THEN SIGNAL -- the same non-blocking send
	// TryPublish uses, on the same never-closed channel.
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Closed reports whether Close has been called.
func (p *Producer) Closed() bool { return p.r.closed.Load() }

// Gate: head and tail must be exactly one cache line apart.
//
// In the two-sided form of Vol 0 section 2 rule 7 (model/const.go, model/layout.go):
// the line compiles only when the separation is exactly cacheLine, because
// otherwise one of the two terms is negative and converting a negative constant
// to uint is a compile-time error. A field inserted between them, or a shrunk
// pad, fails the build.
//
// Separation is the right property, and alignment is not needed: Go guarantees
// at most 8-byte alignment for a struct, so the ring's base address is not
// 128-aligned -- but two addresses exactly 128 bytes apart always fall in
// different 128-byte lines whatever the base. unsafe.Offsetof is a compile-time
// constant and does not make the build cgo-dependent (model/layout.go:5-21).
const (
	headTailGap = unsafe.Offsetof(ring{}.tail) - unsafe.Offsetof(ring{}.head)

	_ = uint(headTailGap-cacheLine) + uint(cacheLine-headTailGap)
	_ = uint(cacheLine-128) + uint(128-cacheLine)
)

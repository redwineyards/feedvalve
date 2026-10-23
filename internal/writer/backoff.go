package writer

import "time"

// backoffJitter is the +-20% spread of V3 section 9.1, so writers failing in
// lockstep do not reconnect in lockstep.
const backoffJitter = 0.2

// backoff computes the sleep between a failed batch and the next attempt:
// initial, doubling, capped at max, then jittered. It holds no state; the
// caller owns the consecutive-failure count. rnd returns a value in [0,1)
// (math/rand/v2's Float64 in production) and is injected for tests.
type backoff struct {
	initial, max time.Duration
	rnd          func() float64
}

// delay returns the sleep after the failures-th consecutive failure: initial
// for the first, doubling after, capped at max before jitter. Jitter is
// applied last, so the result lies in [0.8, 1.2] x the capped value. Zero or
// negative failures give no sleep.
func (b backoff) delay(failures int) time.Duration {
	if failures <= 0 {
		return 0
	}
	d := b.initial
	for i := 1; i < failures && d < b.max; i++ {
		d *= 2
	}
	if d > b.max {
		d = b.max
	}
	f := 1 + backoffJitter*(2*b.rnd()-1)
	return time.Duration(float64(d) * f)
}

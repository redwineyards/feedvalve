package app

import (
	"time"

	"feedvalve/internal/obs"
)

// obsClassStartup is the log event class for lifecycle conditions. The classes
// are obs's, because obs owns the per-class token buckets.
const obsClassStartup = obs.ClassStartupFailed

// processStart is the monotonic origin.
//
// Go exposes no raw monotonic clock, but a time.Time from time.Now carries a
// monotonic reading and time.Since uses it, so a duration measured from a fixed
// origin IS monotonic. That is exactly what model.Datagram.RecvMono needs: its
// absolute value means nothing and only differences are ever taken -- the
// ordering guard's receipt comparisons and the fan-out budget's start.
//
// Vol 4 section 10.2 makes this a MUST: never t1.UnixNano() - t0.UnixNano().
// Subtracting wall-clock readings across an NTP step can produce a negative
// duration, which as an unsigned counter argument is catastrophic and as a
// remaining-budget computation hands a timer a value near 2^64.
//
// The receive loop reads the clock ONCE per datagram and derives both values
// it needs from that one reading -- t0 for the enqueue budget and
// t0.Sub(processStart) for RecvMono (Vol 1 section 10.5 property 3). There was
// a monotonicNow() helper here until P2-08; it went when the budget arrived,
// because a second time.Now() for the second value would have been the clock
// read that section is written against.
var processStart = time.Now()

// nextRecvMono is the receipt clock the ordering guard needs: strictly
// increasing per receiver, which the guard's RecvMono tiebreak (Vol 2 section
// 5.3 step 8, obligation B-V1b) assumes. time.Now's monotonic reading only
// advances once per timer tick on some platforms (windows/amd64), so two
// datagrams read in one tick would otherwise get the same stamp and the guard
// would drop the second as a re-delivery. One reader goroutine owns last, so
// bumping a tie by a nanosecond costs nothing and leaves real differences
// untouched. Closes phase3-tasks seam 9.
func nextRecvMono(now, last int64) int64 {
	if now <= last {
		return last + 1
	}
	return now
}

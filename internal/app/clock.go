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

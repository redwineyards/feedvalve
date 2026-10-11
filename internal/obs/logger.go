package obs

import (
	"io"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

// Level is the appLogger threshold, from stats.logLevel.
//
// Three values, not four, because the configuration validates stats.logLevel to
// exactly info | warn | error (internal/config/validate.go). FATAL is a
// severity that is always emitted and never a threshold -- there is no
// configuration in which a terminal condition goes unlogged.
type Level uint8

const (
	LevelInfo Level = iota
	LevelWarn
	LevelError
)

// ParseLevel maps the configured string to a Level. The configuration has
// already rejected anything else, so an unknown value falls back to the most
// verbose rather than failing: a logger is not the place to discover a
// validation gap, and losing INFO would lose the effective-configuration dump
// that is the most useful artifact in a postmortem.
func ParseLevel(s string) Level {
	switch s {
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// bucket is one event class's token bucket.
//
// Per class, which is the whole design. Vol 4 section 6.2: a misframed feed
// produces one WARN per sub-packet, and at ~10,000 messages/s "an uncapped
// logger is both the loudest and the slowest thing in the process -- it
// converts a data problem into an availability problem, and it does so on the
// receive goroutine, which is the one NFR4 protects". A single global bucket
// would be worse than none: one noisy class would starve every other class of
// tokens, so the first symptom would hide the second.
//
// Lock-free, because the goroutine that most needs to log cheaply is the one
// that must never block.
type bucket struct {
	tokens     atomic.Int64
	lastRefill atomic.Int64 // monotonic nanoseconds since the logger's origin
	suppressed atomic.Uint64
}

// take consumes a token, reporting whether the event may be logged and how
// many of its class were suppressed since the last one that was.
//
// Refill is computed from elapsed MONOTONIC time, never from a difference of
// wall-clock readings -- Vol 4 section 10.2's MUST. A backwards NTP step would
// otherwise make elapsed negative and either stall the refill until the clock
// caught up or, as an unsigned quantity, grant a colossal number of tokens.
func (b *bucket) take(nowNs, ratePerMin int64) (ok bool, suppressed uint64) {
	const perMinute = int64(time.Minute)

	last := b.lastRefill.Load()
	if elapsed := nowNs - last; elapsed > 0 && ratePerMin > 0 {
		if gain := elapsed * ratePerMin / perMinute; gain > 0 {
			// Advance lastRefill only by the time actually converted into
			// tokens, so the remainder is not discarded -- otherwise a class
			// checked more often than once per token would never refill.
			consumed := gain * perMinute / ratePerMin
			if b.lastRefill.CompareAndSwap(last, last+consumed) {
				if b.tokens.Add(gain) > ratePerMin {
					b.tokens.Store(ratePerMin)
				}
			}
		}
	}

	if b.tokens.Add(-1) >= 0 {
		return true, b.suppressed.Swap(0)
	}
	// Hand the token back. Several goroutines failing at once drive the count
	// transiently negative and then restore it; the bucket self-corrects and
	// the only consequence is that a burst may suppress one extra event, which
	// is the safe direction.
	b.tokens.Add(1)
	b.suppressed.Add(1)
	return false, 0
}

// AppLogger is the operational logger: startup, errors, transitions, and the
// shutdown summary. Its destination is stderr and there is no other.
//
// Vol 4 section 6.7: there is no metrics agent, no scrape endpoint, no sidecar
// and no tracing collector on this platform, so "the entire observability
// surface is stderr". Rotation is the supervisor's job -- there is no
// cappedWriter and no size cap here, because there is no packet log to cap.
//
// There is NO packet logger and adding one back would be a mistake. poc's
// full-payload logger wrote several times the wire rate to a world-readable,
// append-only file; HLD section 10.2 deletes it and the -maxout cap goes with
// it. On this host operational logs are long-lived and append-only, so a secret
// that lands in one is there permanently.
//
// Built on *log.Logger for the mutex that keeps concurrent lines from
// interleaving, but with its flags set to ZERO and the timestamp written by
// hand. The stats emission deliberately does not go through log.Printf at all,
// because it allocates and gate G-OBS-2 forbids that on the emission path.
//
// The flags are zero because log.LstdFlags and the specified emission line
// disagree, and they have to agree. LstdFlags renders a date as 2026/10/11
// with slashes -- poc inherited that from the default -- while Vol 4 section
// 6.1's example emission line reads "2026-09-24 03:14:07.118412" with hyphens.
// Only one of them can be right, because the whole value of a shared format is
// that an operator can interleave a WARN with a trend by grep, and section 6.7
// leaves them no other way to correlate the two. Section 6.1 is where the
// contract actually writes a timestamp, and its form is sortable and
// unambiguous, so timeLayout wins and both paths use it. poc's slashes were the
// standard library's default rather than a decision.
type AppLogger struct {
	lg    *log.Logger
	level Level
	rate  atomic.Int64
	reg   *Reg

	// buckets points at whichever set this logger rate limits against: the
	// registry's, or the standalone set when there is no registry yet.
	buckets *[numClasses]bucket

	// start is the monotonic origin the buckets measure from.
	start time.Time
}

// NewAppLogger builds the logger of startup step 6.
//
// reg may be nil, which is what lets the logger exist before the registry
// does -- step 6 precedes step 9, and a failure between them is exactly when
// logging must not run away. The only consequence is that log_events_suppressed
// goes uncounted. ratePerMin is stats.eventRatePerMin.
//
// The buckets start FULL. A bucket that started empty would suppress the first
// event of every class, because the refill is computed from elapsed time and no
// time has elapsed yet -- so the very first netId mismatch, the one that says
// the frame is misaligned, would be the one that went unlogged.
func NewAppLogger(w io.Writer, level Level, ratePerMin int, reg *Reg) *AppLogger {
	l := &AppLogger{
		lg:    log.New(w, "", 0), // timestamps are written by write(); see the type comment
		level: level,
		reg:   reg,
		start: time.Now(),
	}
	l.rate.Store(int64(ratePerMin))

	if reg != nil {
		l.buckets = &reg.buckets
	} else {
		l.buckets = &standaloneBuckets
	}
	for i := range l.buckets {
		l.buckets[i].tokens.Store(int64(ratePerMin))
		l.buckets[i].lastRefill.Store(0)
		l.buckets[i].suppressed.Store(0)
	}
	return l
}

// SetRatePerMin applies a reloaded stats.eventRatePerMin.
func (l *AppLogger) SetRatePerMin(n int) { l.rate.Store(int64(n)) }

// SetLevel applies a reloaded stats.logLevel.
func (l *AppLogger) SetLevel(v Level) { l.level = v }

// Infof logs an unconditional operational line. INFO is not rate limited: Vol 4
// section 6.2 lists what it carries -- startup, the effective configuration,
// the bound socket, the Oracle version, accepted reloads, circuit and session
// transitions, shutdown -- and every one of those is per-process or happens at
// human rate. A bucket would only risk dropping the postmortem.
func (l *AppLogger) Infof(format string, a ...any) {
	if l.level > LevelInfo {
		return
	}
	l.write("INFO ", format, a...)
}

// Warnf logs a rate-limited warning.
//
// The bucket is taken BEFORE the line is formatted, so a suppressed event costs
// one atomic add and no formatting, no write and no syscall. Note that the
// caller's arguments are still evaluated and boxed into the variadic at the
// call site: a hot path that would format something expensive should compute it
// lazily, because this method cannot un-evaluate it. What the bucket bounds is
// the dominant cost -- the format, the write and the contention on stderr.
func (l *AppLogger) Warnf(class Class, format string, a ...any) {
	if l.level > LevelWarn {
		return
	}
	l.emit("WARN ", class, format, a...)
}

// Errorf logs a rate-limited error. Rate limited for the same reason as WARN:
// F3 and F4 are per-sub-packet conditions, so a corrupt feed produces them at
// wire rate.
func (l *AppLogger) Errorf(class Class, format string, a ...any) {
	l.emit("ERROR ", class, format, a...)
}

// Terminalf logs one of the five terminal conditions of Vol 4 section 1.2 exit
// code 4, at severity FATAL. It is never rate limited and it is always emitted.
//
// It is NOT called Fatalf, and the name is load-bearing. Gate G-BLD-3 is a
// go/ast source scan that fails the build on any log.Fatal*, logger.Fatal*,
// os.Exit or runtime.Goexit outside cmd/feedvalve/main.go, and it exists
// because poc had to be restructured when appLogger.Fatalf skipped the defer
// that closed the database. A method named Fatalf here would either trip that
// scan or force a carve-out that weakens it, and HLD section 10.2's phrase
// "FATAL-then-exit" reads as an invitation to exactly that mistake.
//
// This method WRITES and RETURNS. Terminating is app.Graph.Fatal's job: it
// delivers to fatalCh and lets run drain and return, so every defer fires.
func (l *AppLogger) Terminalf(format string, a ...any) {
	l.write("FATAL ", format, a...)
}

// emit takes the class's token and writes the line, appending the suppression
// suffix when events of that class were dropped since the last one.
func (l *AppLogger) emit(severity string, class Class, format string, a ...any) {
	if class >= numClasses {
		class = 0
	}
	b := &l.buckets[class]

	ok, suppressed := b.take(int64(time.Since(l.start)), l.rate.Load())
	if !ok {
		// Make the suppression itself observable. A large
		// log_events_suppressed{class} is a signal in its own right -- it says
		// the feed is producing one condition faster than anyone can read.
		if l.reg != nil {
			l.reg.counters[l.reg.base[IDLogEventsSuppressed]+uint32(class)].Add(1)
		}
		return
	}
	if suppressed > 0 {
		// The suffix goes on the NEXT emitted line of the class, which is what
		// makes the rate limit honest: a reader of that line learns both what
		// happened and that it happened more often than the line implies.
		l.write(severity, format+" suppressed="+strconv.FormatUint(suppressed, 10), a...)
		return
	}
	l.write(severity, format, a...)
}

// write prepends the shared timestamp layout and hands the line to log.Logger,
// whose flags are zero so this is the only place a timestamp is formatted.
func (l *AppLogger) write(severity, format string, a ...any) {
	l.lg.Printf(time.Now().Format(timeLayout)+" "+severity+format, a...)
}

// standaloneBuckets back an AppLogger built without a registry, so the rate
// limit still applies before the registry exists -- startup step 6 precedes
// step 9, and a failure between them is exactly when logging must not run away.
var standaloneBuckets [numClasses]bucket

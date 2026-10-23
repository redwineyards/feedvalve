package writer

import (
	"context"
	"errors"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

// Config is the startup half of the writer's configuration: what NewPool and
// Start need once. writer does not import config (config carries
// oradb.DBSettings, and the seam is the same one mirror uses, ruling D1);
// app projects config onto these plain values.
type Config struct {
	Writers int // writers.count at Start
	Ceiling int // runtime.maxBatchSizeCeiling, the grouper and binder scratch
}

// Params is the reloadable half. A published *Params is immutable: a reload
// builds a new one. A writer loads it once per batch and holds that pointer
// for the whole batch, retry included (V3 2.4 step 4: timeouts must not change
// mid-transaction). The four health thresholds are read once, by NewPool; the
// SIGHUP driver that would change them is Phase 5 (D17).
type Params struct {
	QueryTimeout    time.Duration // db.queryTimeoutSec, per batch (D13)
	RetryInitial    time.Duration // writers.retryInitialMs
	RetryMax        time.Duration // writers.retryMaxMs
	ConnIdleRelease time.Duration // writers.connIdleReleaseSec; 0 disables the idle release
	ConnHold        time.Duration // db.connMaxLifetimeMinutes; 0 disables the bounded hold
	StmtCacheSize   int           // writers.stmtCacheSize

	// Time1980UnixOffsetSec is runtime.time1980UnixOffsetSec (Vol 3 A8),
	// reloadable: a batch binds with the value its Params carried. 0 means
	// DefaultTime1980UnixOffsetSec (an offset of 0 would put the epoch at 1970,
	// which no reading of the spec supports).
	Time1980UnixOffsetSec int64

	FailFastWindow       time.Duration // writers.failFastSeconds
	FailFastWrites       int           // writers.failFastWrites
	StatementFailWriters int           // writers.statementFailWriters
	StatementFailWindow  time.Duration // writers.statementFailWindowSec
}

// time1980 is the offset a batch binds with.
func (p *Params) time1980() int64 {
	if p.Time1980UnixOffsetSec == 0 {
		return DefaultTime1980UnixOffsetSec
	}
	return p.Time1980UnixOffsetSec
}

// exitKind is why a writer's loop returned.
type exitKind uint8

const (
	exitNormal  exitKind = iota // batchQ closed, quit, Close or context
	exitRetired                 // the writer retired itself (ErrStatement or panic)
)

var (
	errShutdown = errors.New("shutdown")
	errFailFast = errors.New("writer pool in fail-fast")
)

// writer is one goroutine's state. Everything but quit is touched only by the
// owning goroutine, which is what the stmtCache, the binders and the Session
// all require.
type writer struct {
	p    *Pool
	id   int
	quit chan struct{} // closed by Resize to shrink; checked only between batches
	// quitting is Pool.mu's: set when quit is closed so Resize does not count
	// the writer as active.
	quitting bool

	owner loopOwner // G-ASSERT-1

	r     *runner
	cache *stmtCache
	par   *Params // the last loaded; refreshed once per batch

	sess     *oradb.Session
	holdC    <-chan time.Time
	stopHold func()

	failures int // consecutive failed batches, for the backoff
}

// acquire pins a session and arms the bounded hold (V3 2.3).
func (w *writer) acquire(ctx context.Context) error {
	s, err := w.p.db.Acquire(ctx)
	if err != nil {
		return err
	}
	w.sess = s
	w.holdC, w.stopHold = nil, nil
	if w.par.ConnHold > 0 {
		w.holdC, w.stopHold = w.p.after(w.par.ConnHold)
	}
	return nil
}

// release returns the session to the pool and counts why. It runs only
// between batches or on exit. reason < 0 counts nothing (the writer is
// leaving).
func (w *writer) release(reason int) {
	if w.sess == nil {
		return
	}
	_ = w.sess.Release()
	w.sess = nil
	if w.stopHold != nil {
		w.stopHold()
	}
	w.holdC, w.stopHold = nil, nil
	if reason >= 0 && reason < len(w.p.inst.ConnRecycled) {
		add(w.p.inst.ConnRecycled[reason])
	}
}

// attempt is one runBatch, acquiring a session first when none is held. An
// acquire failure is a batch failure like any other: the rows are Failed and
// the classified error is returned for the retry decision.
func (w *writer) attempt(ctx context.Context, b *model.Batch) attempt {
	if w.sess == nil {
		if err := w.acquire(ctx); err != nil {
			if ctx.Err() != nil {
				b.Err = errShutdown
				failPending(b, 0)
				return attempt{}
			}
			cerr, ok := w.p.db.Classify(err)
			if !ok {
				// Not an Oracle error and not a shutdown; the text may quote
				// the DSN, so none of it is kept.
				cerr = &oradb.Error{Class: oradb.ErrUnknown, Err: errors.New("acquire session failed")}
			}
			b.Err = cerr
			failPending(b, uint16(cerr.Code))
			return attempt{failure: cerr}
		}
	}
	return w.r.runBatch(ctx, w.sess, b)
}

// failPending turns every Pending row of b into Failed with code. Rows with a
// terminal outcome are left alone.
func failPending(b *model.Batch, code uint16) {
	n := min(max(int(b.N), 0), len(b.Rows))
	for i := range b.Rows[:n] {
		if r := &b.Rows[i]; r.Outcome == model.OutcomePending {
			r.Outcome, r.ErrCode = model.OutcomeFailed, code
		}
	}
}

// resetForRetry makes a batch whose first attempt failed look fresh to the
// second: runBatch's sweep turned its Pending rows Failed. Rejected rows (an
// empty mask) stay Rejected.
func resetForRetry(b *model.Batch) {
	n := min(max(int(b.N), 0), len(b.Rows))
	for i := range b.Rows[:n] {
		if r := &b.Rows[i]; r.Outcome == model.OutcomeFailed {
			r.Outcome, r.ErrCode = model.OutcomePending, 0
		}
	}
	b.Err = nil
}

func (p *Pool) warnf(c obs.Class, format string, a ...any) {
	if p.log != nil {
		p.log.Warnf(c, format, a...)
	}
}

func (p *Pool) errorf(c obs.Class, format string, a ...any) {
	if p.log != nil {
		p.log.Errorf(c, format, a...)
	}
}

func setGauge(g *obs.Gauge, v int64) {
	if g != nil {
		g.Store(v)
	}
}

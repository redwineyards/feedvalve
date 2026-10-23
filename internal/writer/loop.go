package writer

import (
	"context"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

// loop is one writer's goroutine body (V3 2.4). It returns why it left; the
// deferred release and closeAll run on every path, so no session or prepared
// statement outlives the writer.
//
// The ordering the design rests on, in code order:
//
//  1. A backoff sleep happens here, at the top, with no batch in hand (V3
//     9.2): a writer that slept while holding a batch would pin one of its
//     shard's in-flight slots for up to five seconds.
//  2. The session is acquired lazily, when a batch needs it.
//  3. Quit, Close and context are looked at only between batches, so a shrink
//     or a shutdown never abandons a transaction.
func (p *Pool) loop(ctx context.Context, w *writer) exitKind {
	w.owner.enter(w.id)
	defer w.owner.exit()

	w.par = p.params()
	w.cache = newStmtCache(p.db.Prepare, p.dests, w.par.StmtCacheSize, p.inst, w.id)
	w.r = newRunner(w.id, p.db, p.dests, w.cache, p.cfg.Ceiling, w.par.QueryTimeout)
	if p.cfg.Time1980UnixOffsetSec != 0 {
		w.r.time1980 = p.cfg.Time1980UnixOffsetSec
	}
	w.r.inst, w.r.run, w.r.log, w.r.oracleErr = p.inst, p.inst.Run, p.log, p.inst.OracleError
	defer func() {
		w.release(-1)
		w.cache.closeAll()
	}()

	for {
		// Not in FailFast: there the writer fails each batch at once (D2), and a
		// sleep here would hold the shards' in-flight slots for up to retryMax.
		if w.failures > 0 && p.health.State() != HealthFailFast {
			p.backoffAndProbe(ctx, w)
		}
		b, ok := p.next(ctx, w)
		if !ok {
			return exitNormal
		}
		w.par = p.params() // once per batch; the batch and its retry use this one
		if p.serve(ctx, w, b) == exitRetired {
			return exitRetired
		}
		if w.sess != nil && w.sess.Expired(p.now()) {
			w.release(int(obs.RecycleHold))
		}
	}
}

// next waits for a batch. It returns false when the writer must leave: batchQ
// closed (everything queued has been received by then), quit, context, or
// Close with nothing left queued. Idle and hold releases happen inside it, and
// only here, between batches.
func (p *Pool) next(ctx context.Context, w *writer) (*model.Batch, bool) {
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-w.quit:
			return nil, false
		default:
		}
		select {
		case <-p.closing:
			// Close: finish what is already queued, then leave.
			select {
			case b, ok := <-p.batchQ:
				return b, ok
			default:
				return nil, false
			}
		default:
		}

		var idleC <-chan time.Time // nil blocks forever: no session, nothing to release
		stopIdle := func() {}
		if w.sess != nil {
			idleC, stopIdle = p.after(w.par.ConnIdleRelease)
		}
		select {
		case b, ok := <-p.batchQ:
			stopIdle()
			return b, ok
		case <-idleC:
			w.release(int(obs.RecycleIdle))
		case <-w.holdC:
			stopIdle()
			w.release(int(obs.RecycleHold))
		case <-ctx.Done():
			stopIdle()
		case <-w.quit:
			stopIdle()
		case <-p.closing:
			stopIdle()
		}
	}
}

// backoffAndProbe sleeps the current backoff and, when the pool is Degraded,
// probes the database with one Ping so a quiet market cannot sit Degraded until
// the 30 s budget fires on a database that has already recovered (V3 9.3).
func (p *Pool) backoffAndProbe(ctx context.Context, w *writer) {
	bo := backoff{initial: w.par.RetryInitial, max: w.par.RetryMax, rnd: p.rnd}
	p.pause(ctx, w, bo.delay(w.failures))
	if ctx.Err() != nil || p.health.State() != HealthDegraded {
		return
	}
	if err := p.db.PingContext(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		add(p.inst.Probe[obs.ProbeFail])
		act := p.health.OnProbeFailure(w.id)
		p.publishHealth()
		if act == StopPool {
			p.stopPool(&StopError{Kind: StopFailFast, Cause: err})
		}
		return
	}
	add(p.inst.Probe[obs.ProbeOK])
	w.failures = 0
	p.health.OnProbeSuccess(w.id)
	p.publishHealth()
}

func (p *Pool) publishHealth() { setGauge(p.inst.HealthState, int64(p.health.State())) }

// serve runs one batch to completion: the retry loop around runBatch, the
// health and counter updates, and the blocking return to the owning shard.
//
// MUST: after the send on the completion channel nothing here touches b. The
// shard owns it from that instant and may already be rewriting it, so every
// value this function still needs afterwards -- the success flag, the failure,
// the statement key, the retry class -- is a local captured before the send.
func (p *Pool) serve(ctx context.Context, w *writer, b *model.Batch) exitKind {
	shard, epoch, rows := b.Shard, b.Epoch, b.N
	w.r.queryTimeout = w.par.QueryTimeout
	add(p.inst.WritesAttempted)

	var (
		res        attempt
		retried    bool
		retryClass obs.RetryClass
		failFast   bool
	)
	if p.health.State() == HealthFailFast {
		// D2: keep receiving and fail at once, so the shard's slots drain.
		failFast = true
		b.Err = errFailFast
		failPending(b, 0)
	} else {
		var retry batchRetry
		for {
			res = w.attempt(ctx, b)
			if res.panicked || res.failure == nil || res.phase2 { // D6d: none after phase 2
				break
			}
			if retry.decide(res.failure.Class) != retryOnce {
				break
			}
			retryClass, _ = retryClassOf(res.failure.Class)
			retried = true
			add(p.inst.WriteRetry[retryClass]) // every retry performed, whatever its outcome
			if res.failure.Class == oradb.ErrSession {
				w.release(int(obs.RecycleError))
			}
			// Known inflation: the grouper and binder run again over the same
			// rows, so batch_row_empty_mask and bind_bad_time count a row once
			// more on the retry. Diagnostic counters on a rare path.
			resetForRetry(b)
		}
	}

	ok := !failFast && !res.panicked && res.failure == nil && b.Err == nil
	failure, key, panicked := res.failure, res.key, res.panicked
	act := Continue
	switch {
	case ok:
		// D6c: a retried-then-successful batch touches no health state; only a
		// first-attempt commit may recover a Degraded pool.
		if !retried {
			wasDegraded := p.health.degraded.Load()
			p.health.OnSuccess(w.id)
			if wasDegraded {
				p.publishHealth()
			}
		}
		add(p.inst.WritesSucceeded)
		add(p.inst.BatchesWritten)
		if retried {
			p.warnf(obs.ClassWriteFailed, "writer: batch committed after one retry writer=%d shard=%d epoch=%d class=%s",
				w.id, shard, epoch, retryClass)
		}
	case failFast, panicked:
		add(p.inst.WritesFailed)
	case failure != nil:
		add(p.inst.WritesFailed)
		act = p.health.OnFailure(w.id, failure, key)
		p.publishHealth()
		p.errorf(obs.ClassWriteFailed, "writer: batch failed writer=%d shard=%d epoch=%d rows=%d class=%s ORA-%05d",
			w.id, shard, epoch, rows, failure.Class, failure.Code)
	}

	// The send. Blocking by design (Vol 0 9.2): the channel holds a shard's
	// whole in-flight allowance, so it cannot fill.
	if int(shard) >= len(p.completion) {
		p.errorf(obs.ClassWriteFailed, "writer: batch shard %d outside %d completion channels; batch dropped", shard, len(p.completion))
	} else {
		p.completion[shard] <- b
	}

	switch {
	case panicked:
		p.retire(w, "panic")
		return exitRetired
	case act == RetireWriter:
		p.retire(w, "statement "+key.String())
		return exitRetired
	case act == StopPool:
		if failure.Class == oradb.ErrStatement {
			p.stopPool(&StopError{Kind: StopStatement, Identity: key.String(), Cause: failure})
		} else {
			p.stopPool(&StopError{Kind: StopFailFast, Cause: failure})
		}
	}
	switch {
	case ok && !retried:
		w.failures = 0
	case failure != nil && failure.Class != oradb.ErrRowData:
		w.failures++
	}
	return exitNormal
}

// retire records a writer leaving of its own accord (ErrStatement or a panic).
func (p *Pool) retire(w *writer, why string) {
	add(p.inst.WritersStopped)
	p.errorf(obs.ClassWriterStopped, "writer: writer %d retired (%s)", w.id, why)
}

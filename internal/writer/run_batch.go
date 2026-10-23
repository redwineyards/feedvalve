package writer

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

// RunInstruments are the handles one batch execution reports through
// (V3 section 14.3), resolved before the registry is frozen. A nil handle is
// skipped. They are separate from Instruments, which belongs to the binder
// and cache cards; the pool card (P4-09) wires both from one registry.
type RunInstruments struct {
	// RowsWritten counts rows committed, by destination.
	RowsWritten [model.MaxDestinations]*obs.Counter
	// Panics is panics_recovered{stage=writer}.
	Panics *obs.Counter
	// Busy is the writers_busy gauge: incremented for the length of a batch.
	Busy *obs.Gauge
	// RowRejected counts rows phase 2 rejected for their own data, by
	// destination (D11: the writer's counter).
	RowRejected [model.MaxDestinations]*obs.Counter
	// RowsAffectedMismatch counts executions whose RowsAffected differed from
	// the rows bound, by destination. It changes no outcome (V3 6.5).
	RowsAffectedMismatch [model.MaxDestinations]*obs.Counter
	// FallbackPerrow is batch_fallback_perrow: entries into phase 2.
	FallbackPerrow *obs.Counter
}

// NewRunInstruments resolves the handles for the first `destinations`
// destinations against reg. Call it before reg.Freeze.
func NewRunInstruments(reg *obs.Reg, destinations int) *RunInstruments {
	in := &RunInstruments{
		Panics: reg.CounterAt(obs.IDPanicsRecovered, int(obs.StageWriter)),
		Busy:   reg.Gauge(obs.IDWritersBusy),

		FallbackPerrow: reg.Counter(obs.IDBatchFallbackPerrow),
	}
	for d := 0; d < destinations && d < model.MaxDestinations; d++ {
		in.RowsWritten[d] = reg.CounterAt(obs.IDRowsWritten, d)
		in.RowRejected[d] = reg.CounterAt(obs.IDRowRejected, d)
		in.RowsAffectedMismatch[d] = reg.CounterAt(obs.IDRowsAffectedMismatch, d)
	}
	return in
}

// attempt is what one runBatch tells its caller beyond the batch itself.
type attempt struct {
	// failure is the classified database error, nil on success, on shutdown
	// and on a panic. The retry loop (P4-09) branches on failure.Class.
	failure *oradb.Error
	// panicked: the writer must retire its session and itself (D10).
	panicked bool
	// phase2 is true when the attempt reached the scalar re-run; a non-row
	// failure after it is never retried (D6d).
	phase2 bool
	// key is the statement identity being bound or executed when failure
	// occurred; only ErrStatement uses it (the pool-stop ledger).
	key stmtKey
}

// runner is one writer's batch executor. It owns the per-writer scratch (the
// grouper, the binders, the statement cache) and is used by one goroutine.
type runner struct {
	id      int // writer id, for log lines
	db      *oradb.DB
	dests   *destinations.Set
	cache   *stmtCache
	group   *grouper
	binders map[stmtKey]*Binder
	ceiling int

	queryTimeout time.Duration // per batch (D13); a caller deadline wins
	time1980     int64

	inst *Instruments    // binder counters; nil is fine
	run  *RunInstruments // nil is fine
	log  *obs.AppLogger  // nil is fine

	// oracleErr counts oracle_error{code}; the caller passes a func taken
	// from the concrete registry. nil is fine.
	oracleErr func(code int)
	// retire is called when a panic means the session's state is unknown.
	retire func()

	now         func() time.Time // nil means time.Now
	afterCommit func()           // test seam, called once after a successful commit
	skipPhase1  bool             // test seam: go straight to phase 2 (V3 self-gate)
	where       stmtKey          // statement identity being bound or executed: the panic line and ErrStatement's identity
}

// newRunner builds the executor for writer id. ceiling is maxBatchSizeCeiling.
func newRunner(id int, db *oradb.DB, dests *destinations.Set, cache *stmtCache, ceiling int, queryTimeout time.Duration) *runner {
	return &runner{id: id, db: db, dests: dests, cache: cache, group: newGrouper(ceiling),
		binders: make(map[stmtKey]*Binder), ceiling: ceiling, queryTimeout: queryTimeout,
		time1980: DefaultTime1980UnixOffsetSec}
}

// binder returns the Binder for a statement identity, building it on first use.
func (r *runner) binder(k stmtKey) (*Binder, error) {
	if b, ok := r.binders[k]; ok {
		b.Time1980UnixOffsetSec = r.time1980 // a reload may have changed it since b was built
		return b, nil
	}
	d := r.dests.ByID(k.Dest)
	if d == nil {
		return nil, errors.New("writer: statement " + k.String() + ": no such destination")
	}
	b, err := NewBinder(d, k.Mask, r.ceiling)
	if err != nil {
		return nil, err
	}
	b.Inst, b.Time1980UnixOffsetSec = r.inst, r.time1980
	r.binders[k] = b
	return b, nil
}

// runBatch is ONE attempt at b as one transaction on s (V3 sections 6.3, 7.1,
// 7.4, 7.5); the retry loop is the caller's. On every path, including a
// panic, no row of b is left OutcomePending, and b.Err is non-nil whenever
// any row is Failed:
//
//   - success: Err nil, every row that was Pending is Committed, set only
//     after Commit returned nil;
//   - a database failure: Err is the classified error, Pending rows Failed
//     with ErrCode the bare ORA number (D9);
//   - context.Canceled: shutdown, not a failure. Err "shutdown", Failed rows,
//     nothing counted;
//   - a panic: recovered here, outside ExecTx, which has already rolled back
//     and re-panicked. Err "writer panic", the session retired, the panic
//     counted, Pending rows Failed. A row already Committed stays so.
//
// N == 0 is a no-op success: nothing is bound, no transaction is begun and an
// Err already on b is left as it was. N outside Rows sets Err and fails the
// rows that exist.
//
// The sweep only turns Pending into Failed; it never overwrites a terminal
// outcome. runBatch never touches Digest, Mask, Slot, Row or Image.
//
// KNOWN RISK, not covered by a test: a panic raised inside a database/sql
// driver call (as opposed to writer code) leaves the Tx close lock held, so
// the Rollback in oradb.Session.ExecTx's recover blocks and this function never
// gets to recover. It matters only if the driver itself panics mid-call; the
// fix belongs in ExecTx (bounded rollback) and is for the orchestrator.
func (r *runner) runBatch(ctx context.Context, s *oradb.Session, b *model.Batch) (res attempt) {
	if r.run != nil && r.run.Busy != nil {
		r.run.Busy.Add(1)
		defer r.run.Busy.Add(-1)
	}
	code := 0
	defer func() {
		if p := recover(); p != nil {
			res.panicked = true
			b.Err = errors.New("writer panic")
			if r.run != nil {
				add(r.run.Panics)
			}
			if r.log != nil {
				r.log.Errorf(obs.ClassPanicRecovered, "writer: panic recovered writer=%d shard=%d epoch=%d stmt=%s: %v",
					r.id, b.Shard, b.Epoch, r.where.String(), p)
			}
			if r.retire != nil {
				r.retire()
			}
		}
		if b.Err == nil {
			return
		}
		n := min(max(int(b.N), 0), len(b.Rows))
		for i := range b.Rows[:n] {
			if row := &b.Rows[i]; row.Outcome == model.OutcomePending {
				row.Outcome, row.ErrCode = model.OutcomeFailed, uint16(code)
			}
		}
	}()

	if b.N < 0 || int(b.N) > len(b.Rows) {
		b.Err = errors.New("writer: batch N outside Rows")
		return res
	}
	if b.N == 0 {
		return res
	}
	updatedAt := time.Now().UTC()
	if r.now != nil {
		updatedAt = r.now().UTC()
	}
	if _, has := ctx.Deadline(); !has && r.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.queryTimeout)
		defer cancel()
	}

	// Phase 1 is the array path. A row-data failure there has been rolled back
	// by ExecTx and sends the WHOLE batch to phase 2, once (V3 6.4); every
	// other class is the caller's to retry or give back.
	var err error
	toPhase2 := r.skipPhase1
	if !toPhase2 {
		err = s.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return r.phase1(ctx, tx, b, updatedAt)
		})
		if err != nil {
			if cerr, ok := r.db.Classify(err); ok && cerr.Class == oradb.ErrRowData {
				// oracle_error stays first-come (D16): the trigger's code.
				if r.oracleErr != nil && cerr.Code > 0 {
					r.oracleErr(cerr.Code)
				}
				toPhase2 = true
			}
		}
	}
	if toPhase2 {
		res.phase2 = true
		if r.run != nil {
			add(r.run.FallbackPerrow)
		}
		err = s.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return r.phase2(ctx, tx, b, updatedAt)
		})
	}
	if err != nil {
		cerr, ok := r.db.Classify(err)
		if !ok {
			b.Err = errors.New("shutdown")
			return res
		}
		b.Err, res.failure, res.key, code = cerr, cerr, r.where, cerr.Code
		if code > 0 && r.oracleErr != nil {
			r.oracleErr(code)
		}
		return res
	}

	// Commit returned nil: only now is a row Committed. Ranges are clamped so a
	// bad one cannot panic after the rows are already durable.
	var written [model.MaxDestinations]uint64
	for d := range written {
		start := min(max(int(b.DestStart[d]), 0), len(b.Rows))
		end := min(start+max(int(b.DestCount[d]), 0), len(b.Rows))
		for i := start; i < end; i++ {
			if row := &b.Rows[i]; row.Outcome == model.OutcomePending {
				row.Outcome = model.OutcomeCommitted
				written[d]++
			}
		}
	}
	// A row no destination range reached is a bug: fail it rather than leave
	// it Pending (B-V3).
	for i := range b.Rows[:b.N] {
		if row := &b.Rows[i]; row.Outcome == model.OutcomePending {
			row.Outcome = model.OutcomeFailed
			b.Err = errors.New("writer: row outside every destination range")
		}
	}
	if r.run != nil {
		for d, n := range written {
			if n > 0 && r.run.RowsWritten[d] != nil {
				r.run.RowsWritten[d].Add(n)
			}
		}
	}
	if r.afterCommit != nil {
		r.afterCommit()
	}
	return res
}

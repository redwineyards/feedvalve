package writer

import (
	"context"
	"database/sql"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

// phase2 re-runs the whole batch inside tx with scalar binds, one exec per
// row, in phase 1's order (V3 6.4, ADR-W01). Each error is unambiguously that
// row's: an ErrRowData error marks the row Rejected with the bare ORA number
// (D9), counts row_rejected{d} and carries on. Any other error, including
// shutdown, is returned so ExecTx rolls back and runBatch abandons the batch;
// rows already Rejected stay Rejected (A15). Rows that execute cleanly stay
// Pending until runBatch sees the commit.
func (r *runner) phase2(ctx context.Context, tx *sql.Tx, b *model.Batch, updatedAt time.Time) error {
	for d := model.DestID(0); d < model.MaxDestinations; d++ {
		if b.DestCount[d] == 0 {
			continue
		}
		// Phase 1 may have stopped before reaching d, so its empty masks may
		// not be marked yet; ones it already marked are skipped, not recounted.
		r.rejectEmptyMasks(b, d)
		// Empty masks are Rejected by now, so group (with no instruments)
		// only skips them.
		idx, err := r.group.group(b, d, nil, nil)
		if err != nil {
			return err
		}
		for len(idx) > 0 {
			n := runLen(b.Rows, idx)
			if err := r.execRows(ctx, tx, b, stmtKey{Dest: d, Mask: b.Rows[idx[0]].Mask}, idx[:n], updatedAt); err != nil {
				return err
			}
			idx = idx[n:]
		}
	}
	return nil
}

// rejectEmptyMasks is the grouper's empty-mask rule (W-4) for a batch that
// reaches the grouper only in phase 2.
func (r *runner) rejectEmptyMasks(b *model.Batch, d model.DestID) {
	start, n := int(b.DestStart[d]), int(b.DestCount[d])
	if start < 0 || n < 0 || start+n > len(b.Rows) {
		return // group reports the bad span
	}
	for i := start; i < start+n; i++ {
		row := &b.Rows[i]
		if row.Mask != 0 || row.Outcome != model.OutcomePending {
			continue
		}
		row.Outcome = model.OutcomeRejected
		if r.inst != nil {
			add(r.inst.cache.BatchRowEmptyMask[d])
		}
		if r.log != nil {
			r.log.Warnf(obs.ClassWriteFailed, "writer: batch row empty mask dest=%d slot=%d rejected", d, row.Slot)
		}
	}
}

// execRows runs one statement identity's rows one exec each.
func (r *runner) execRows(ctx context.Context, tx *sql.Tx, b *model.Batch, k stmtKey, idx []int32, updatedAt time.Time) error {
	r.where = k
	bd, err := r.binder(k)
	if err != nil {
		return err
	}
	st, cached, err := r.cache.get(ctx, k)
	if err != nil {
		return err
	}
	defer r.cache.done(st, cached)
	stmt := tx.StmtContext(ctx, st)
	for _, i := range idx {
		row := &b.Rows[i]
		args, err := bd.BindRow(row, updatedAt)
		if err != nil {
			return err
		}
		res, err := stmt.ExecContext(ctx, args...)
		if err == nil {
			r.checkAffected(res, k, 1)
			continue
		}
		cerr, ok := r.db.Classify(err)
		if !ok || cerr.Class != oradb.ErrRowData {
			return err
		}
		// Every ErrRowData code is a 5-digit ORA number below 65536, so the
		// uint16 narrowing is lossless (pinned by the policy table).
		row.Outcome, row.ErrCode = model.OutcomeRejected, uint16(cerr.Code)
		if r.run != nil {
			add(r.run.RowRejected[k.Dest])
		}
		if r.log != nil {
			r.log.Errorf(obs.ClassRowRejected, "writer: row rejected dest=%d slot=%d ORA-%05d", k.Dest, row.Slot, cerr.Code)
		}
	}
	return nil
}

package writer

import (
	"context"
	"database/sql"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// phase1 array-binds each (destination, mask) run of b and executes it inside
// tx: destinations 0, 1, 2 in order, masks ascending, rows ascending by key
// (V3 sections 4.2, 7.2). It sets no outcome except the empty-mask Rejected
// the grouper sets; Committed is runBatch's to set after Commit. The error is
// the driver's own, unclassified.
func (r *runner) phase1(ctx context.Context, tx *sql.Tx, b *model.Batch, updatedAt time.Time) error {
	for d := model.DestID(0); d < model.MaxDestinations; d++ {
		if b.DestCount[d] == 0 {
			continue
		}
		idx, err := r.group.group(b, d, r.inst, r.log)
		if err != nil {
			return err
		}
		for len(idx) > 0 {
			n := runLen(b.Rows, idx)
			if err := r.execRun(ctx, tx, b, stmtKey{Dest: d, Mask: b.Rows[idx[0]].Mask}, idx[:n], updatedAt); err != nil {
				return err
			}
			idx = idx[n:]
		}
	}
	return nil
}

// execRun runs one statement identity's rows as a single array exec.
func (r *runner) execRun(ctx context.Context, tx *sql.Tx, b *model.Batch, k stmtKey, idx []int32, updatedAt time.Time) error {
	r.where = k.String()
	bd, err := r.binder(k)
	if err != nil {
		return err
	}
	args, err := bd.BindIdx(b.Rows, idx, updatedAt)
	if err != nil {
		return err
	}
	st, cached, err := r.cache.get(ctx, k)
	if err != nil {
		return err
	}
	defer r.cache.done(st, cached)
	res, err := tx.StmtContext(ctx, st).ExecContext(ctx, args...)
	if err == nil {
		r.checkAffected(res, k, len(idx))
	}
	return err
}

// checkAffected reports RowsAffected != want as a WARN and a counter and does
// nothing else: the driver's array semantics are unproven, so a mismatch must
// never change an outcome (V3 6.5).
func (r *runner) checkAffected(res sql.Result, k stmtKey, want int) {
	if res == nil {
		return
	}
	n, err := res.RowsAffected()
	if err != nil || n == int64(want) {
		return
	}
	if r.run != nil && int(k.Dest) < len(r.run.RowsAffectedMismatch) {
		add(r.run.RowsAffectedMismatch[k.Dest])
	}
	if r.log != nil {
		r.log.Warnf(obs.ClassWriteFailed, "writer: rows affected mismatch stmt=%s affected=%d bound=%d", k.String(), n, want)
	}
}

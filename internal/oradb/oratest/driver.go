package oratest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sijms/go-ora/v2/network"
)

// DriverName is the name the driver is registered under with database/sql.
const DriverName = "oratest"

// Pseudo-SQL keys under which the script is consulted for operations that have
// no statement text. Real statements never start with '<'.
const (
	OpOpen     = "<open>"
	OpPing     = "<ping>"
	OpBegin    = "<begin>"
	OpCommit   = "<commit>"
	OpRollback = "<rollback>"
)

// ORA returns the error go-ora returns for an Oracle error number, e.g.
// ORA(1) is ORA-00001. oradb classifies on this type, so tests get the same
// answer the real driver would produce.
func ORA(code int) error { return network.NewOracleError(code) }

// Call describes one operation reaching the fake, for the script to key on.
type Call struct {
	// SQL is the statement text, or one of the Op* keys.
	SQL string
	// N is the 1-based number of this call for this SQL text, counting failed
	// ones. Retries by database/sql after ErrBadConn count too.
	N int
	// Total is the 1-based number of this call among all operations.
	Total int
	// Rows is the length of the array bound to the first array placeholder, or
	// 0 for a scalar or an operation with no arguments.
	Rows int
	// Conn is the serial number of the connection, starting at 1.
	Conn int
	// Args are the bound values; an array bind is one slice per placeholder.
	// They are a private copy, so a script may keep them without aliasing the
	// caller's reused bind slices. Nil if SetRecordArgs(false) and no script.
	Args []driver.Value
}

// Result is what the script wants to happen to a Call. The zero value is
// success.
type Result struct {
	// Err is returned from the operation.
	Err error
	// FailRow is the array row Err is about. It is only recorded (see
	// ExecRecord): a stub cannot reproduce go-ora's batch error reporting.
	FailRow int
	// Delay is added to the fake's latency, and honours ctx.
	Delay time.Duration
	// Drop kills the connection: it is evicted from the pool and the operation
	// returns Err, or driver.ErrBadConn if Err is nil.
	Drop bool
	// Affected overrides RowsAffected; nil means the array length (1 for a
	// scalar).
	Affected *int64
}

// Affect is a helper for Result.Affected.
func Affect(n int64) *int64 { return &n }

// Script decides the outcome of a call. It is invoked without the fake's lock
// held, so it may read the fake's counters.
type Script func(Call) Result

// ExecRecord is one statement execution as the fake saw it.
type ExecRecord struct {
	Call
	Err     error
	FailRow int
}

// Counts is a snapshot of the fake's counters.
type Counts struct {
	Opens, Closes, Pings       int
	Begins, Commits, Rollbacks int
	// Prepares counts driver-level Prepare calls per connection serial.
	Prepares map[int]int
	// Execs counts statement executions per SQL text.
	Execs map[string]int
}

// Fake is one scripted database. Create it with New, hand DSN() to
// sql.Open(DriverName, ...), and read Counts() afterwards.
type Fake struct {
	dsn string

	mu       sync.Mutex
	script   Script
	latency  time.Duration
	canned   map[string]canned
	counts   Counts
	perSQL   map[string]int
	total    int
	nextConn int
	execs    []ExecRecord
	noArgs   bool
}

type canned struct {
	cols []string
	rows [][]driver.Value
}

var (
	regMu  sync.Mutex
	fakes  = map[string]*Fake{}
	serial int
)

func init() { sql.Register(DriverName, drv{}) }

// New creates a fake and registers it under a fresh DSN.
func New() *Fake {
	regMu.Lock()
	defer regMu.Unlock()
	serial++
	f := &Fake{
		dsn:    fmt.Sprintf("oratest-%d", serial),
		canned: map[string]canned{},
		perSQL: map[string]int{},
		counts: Counts{Prepares: map[int]int{}, Execs: map[string]int{}},
	}
	fakes[f.dsn] = f
	return f
}

// DSN is the data source name that selects this fake. It carries no password.
func (f *Fake) DSN() string { return f.dsn }

// Close unregisters the fake. Connections already open keep working.
func (f *Fake) Close() {
	regMu.Lock()
	delete(fakes, f.dsn)
	regMu.Unlock()
}

// SetScript installs the script; nil means every call succeeds.
func (f *Fake) SetScript(s Script) { f.mu.Lock(); f.script = s; f.mu.Unlock() }

// SetLatency makes every operation take at least d, aborted by its context.
func (f *Fake) SetLatency(d time.Duration) { f.mu.Lock(); f.latency = d; f.mu.Unlock() }

// SetRows installs the canned result of a query with exactly this SQL text.
func (f *Fake) SetRows(sqlText string, cols []string, rows [][]driver.Value) {
	f.mu.Lock()
	f.canned[sqlText] = canned{cols: cols, rows: rows}
	f.mu.Unlock()
}

// SetRecordArgs controls whether Execs() keeps each execution's bound values
// (default true). Turn it off for long stress runs, where keeping every batch's
// arrays alive would dominate memory.
func (f *Fake) SetRecordArgs(on bool) { f.mu.Lock(); f.noArgs = !on; f.mu.Unlock() }

// Counts returns a deep copy of the counters.
func (f *Fake) Counts() Counts {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.counts
	c.Prepares = map[int]int{}
	for k, v := range f.counts.Prepares {
		c.Prepares[k] = v
	}
	c.Execs = map[string]int{}
	for k, v := range f.counts.Execs {
		c.Execs[k] = v
	}
	return c
}

// Execs returns every statement execution in order, including failed ones.
func (f *Fake) Execs() []ExecRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ExecRecord(nil), f.execs...)
}

// step numbers a call, consults the script, waits out the latency and returns
// the outcome. The wait honours ctx; a cancelled wait returns ctx.Err().
func (f *Fake) step(ctx context.Context, conn int, sqlText string, args []driver.Value) (Call, Result, error) {
	f.mu.Lock()
	f.total++
	f.perSQL[sqlText]++
	call := Call{SQL: sqlText, N: f.perSQL[sqlText], Total: f.total, Rows: arrayLen(args), Conn: conn}
	script, latency := f.script, f.latency
	if script != nil || !f.noArgs {
		// The caller reuses its bind slices; keep a private copy for the script
		// and the exec log so neither aliases the writer's memory.
		call.Args = copyArgs(args)
	}
	f.mu.Unlock()

	var res Result
	if script != nil {
		res = script(call)
	}
	switch sqlText {
	case OpOpen, OpCommit, OpRollback:
		// driver.Open, Tx.Commit and Tx.Rollback carry no context, so a wait
		// there could never be aborted.
		latency, res.Delay = 0, 0
	}
	if d := latency + res.Delay; d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return call, res, ctx.Err()
		}
	} else if err := ctx.Err(); err != nil {
		return call, res, err
	}
	return call, res, nil
}

// copyArgs deep-copies array arguments; scalars are immutable values.
func copyArgs(args []driver.Value) []driver.Value {
	if args == nil {
		return nil
	}
	out := make([]driver.Value, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case []int64:
			out[i] = append([]int64(nil), v...)
		case []sql.NullString:
			out[i] = append([]sql.NullString(nil), v...)
		case []sql.NullFloat64:
			out[i] = append([]sql.NullFloat64(nil), v...)
		case []sql.NullTime:
			out[i] = append([]sql.NullTime(nil), v...)
		default:
			out[i] = a
		}
	}
	return out
}

// arrayLen is the length of the first array argument, else 0.
func arrayLen(args []driver.Value) int {
	for _, a := range args {
		switch v := a.(type) {
		case []int64:
			return len(v)
		case []sql.NullString:
			return len(v)
		case []sql.NullFloat64:
			return len(v)
		case []sql.NullTime:
			return len(v)
		}
	}
	return 0
}

type drv struct{}

func (drv) Open(dsn string) (driver.Conn, error) {
	regMu.Lock()
	f := fakes[dsn]
	regMu.Unlock()
	if f == nil {
		return nil, fmt.Errorf("oratest: no fake registered for dsn %q", dsn)
	}
	f.mu.Lock()
	f.nextConn++
	id := f.nextConn
	f.mu.Unlock()

	_, res, err := f.step(context.Background(), id, OpOpen, nil)
	if err == nil {
		err = res.Err
	}
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.counts.Opens++
	f.mu.Unlock()
	return &conn{f: f, id: id}, nil
}

type conn struct {
	f    *Fake
	id   int
	dead bool
}

var (
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
)

// outcome folds a step into the error the operation returns, marking the
// connection dead on a scripted drop.
func (c *conn) outcome(res Result, err error) error {
	if err != nil {
		return err
	}
	if res.Drop {
		c.dead = true
		if res.Err == nil {
			return driver.ErrBadConn
		}
	}
	return res.Err
}

func (c *conn) IsValid() bool { return !c.dead }

func (c *conn) ResetSession(context.Context) error {
	if c.dead {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) Close() error {
	c.f.mu.Lock()
	c.f.counts.Closes++
	c.f.mu.Unlock()
	return nil
}

func (c *conn) Ping(ctx context.Context) error {
	_, res, err := c.f.step(ctx, c.id, OpPing, nil)
	if err = c.outcome(res, err); err != nil {
		return err
	}
	c.f.mu.Lock()
	c.f.counts.Pings++
	c.f.mu.Unlock()
	return nil
}

// CheckNamedValue accepts the four array shapes the writer binds, one slice
// per placeholder, and defers to the standard conversion for scalars.
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error { return checkValue(nv) }

func checkValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case []int64, []sql.NullString, []sql.NullFloat64, []sql.NullTime:
		return nil
	}
	v, err := driver.DefaultParameterConverter.ConvertValue(nv.Value)
	if err != nil {
		return err
	}
	nv.Value = v
	return nil
}

func (c *conn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}

func (c *conn) PrepareContext(_ context.Context, q string) (driver.Stmt, error) {
	if c.dead {
		return nil, driver.ErrBadConn
	}
	c.f.mu.Lock()
	c.f.counts.Prepares[c.id]++
	c.f.mu.Unlock()
	return &stmt{c: c, q: q}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if c.dead {
		return nil, driver.ErrBadConn
	}
	_, res, err := c.f.step(ctx, c.id, OpBegin, nil)
	if err = c.outcome(res, err); err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	c.f.counts.Begins++
	c.f.mu.Unlock()
	return &tx{c: c}, nil
}

func (c *conn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	return c.exec(ctx, q, args)
}

func (c *conn) exec(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if c.dead {
		return nil, driver.ErrBadConn
	}
	call, res, err := c.f.step(ctx, c.id, q, values(args))
	err = c.outcome(res, err)

	c.f.mu.Lock()
	c.f.counts.Execs[q]++
	c.f.execs = append(c.f.execs, ExecRecord{Call: call, Err: err, FailRow: res.FailRow})
	c.f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	n := int64(1)
	if call.Rows > 0 {
		n = int64(call.Rows)
	}
	if res.Affected != nil {
		n = *res.Affected
	}
	return driver.RowsAffected(n), nil
}

func (c *conn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if c.dead {
		return nil, driver.ErrBadConn
	}
	_, res, err := c.f.step(ctx, c.id, q, values(args))
	if err = c.outcome(res, err); err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	cn, ok := c.f.canned[q]
	c.f.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("oratest: no canned rows for query %q", q)
	}
	return &rows{cols: cn.cols, data: cn.rows}, nil
}

func values(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

type tx struct{ c *conn }

func (t *tx) Commit() error   { return t.end(OpCommit) }
func (t *tx) Rollback() error { return t.end(OpRollback) }

func (t *tx) end(op string) error {
	_, res, err := t.c.f.step(context.Background(), t.c.id, op, nil)
	err = t.c.outcome(res, err)
	if err != nil {
		return err // Commits and Rollbacks count successes only
	}
	t.c.f.mu.Lock()
	if op == OpCommit {
		t.c.f.counts.Commits++
	} else {
		t.c.f.counts.Rollbacks++
	}
	t.c.f.mu.Unlock()
	return nil
}

type stmt struct {
	c *conn
	q string
}

var (
	_ driver.StmtExecContext   = (*stmt)(nil)
	_ driver.NamedValueChecker = (*stmt)(nil)
)

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error { return checkValue(nv) }

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.exec(ctx, s.q, args)
}

// The non-context methods are required by driver.Stmt but database/sql prefers
// the context ones above.
func (s *stmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("oratest: use ExecContext")
}

func (s *stmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("oratest: prepared queries are not supported")
}

type rows struct {
	cols []string
	data [][]driver.Value
	i    int
}

func (r *rows) Columns() []string { return r.cols }
func (r *rows) Close() error      { return nil }
func (r *rows) Next(dest []driver.Value) error {
	if r.i >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.i])
	r.i++
	return nil
}

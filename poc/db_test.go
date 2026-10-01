package main

// Tests for the Oracle connection layer. These must pass with no database
// available, so everything below the integration test at the bottom runs
// against a stub database/sql driver registered here rather than against
// Oracle. The integration test skips unless FEEDVALVE_ORACLE_DSN is set.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sijms/go-ora/v2/network"
)

// --- stub driver -----------------------------------------------------------

// stubDriver records what was executed and can be told to fail at specific
// points, so transaction and timeout behavior can be tested without a server.
type stubDriver struct {
	mu sync.Mutex

	openErr   error
	beginErr  error
	execErr   error
	commitErr error
	rollErr   error

	rowsAffected int64
	execDelay    time.Duration

	statements []string
	args       [][]driver.Value
	commits    int
	rollbacks  int
	opens      int
}

func (d *stubDriver) record(query string) {
	d.recordArgs(query, nil)
}

func (d *stubDriver) recordArgs(query string, args []driver.NamedValue) {
	values := make([]driver.Value, len(args))
	for i, a := range args {
		values[i] = a.Value
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.statements = append(d.statements, query)
	d.args = append(d.args, values)
}

func (d *stubDriver) snapshot() (statements []string, commits, rollbacks, opens int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.statements...), d.commits, d.rollbacks, d.opens
}

func (d *stubDriver) Open(string) (driver.Conn, error) {
	d.mu.Lock()
	d.opens++
	err := d.openErr
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &stubConn{d: d}, nil
}

type stubConn struct{ d *stubDriver }

func (c *stubConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *stubConn) Close() error                        { return nil }

func (c *stubConn) Begin() (driver.Tx, error) {
	if c.d.beginErr != nil {
		return nil, c.d.beginErr
	}
	return &stubTx{d: c.d}, nil
}

// Ping makes database/sql's PingContext work without a real round trip.
func (c *stubConn) Ping(context.Context) error { return nil }

// execBehaviour reads the injectable fields under the lock, so a test can flip
// them (e.g. to simulate a database recovering) while the worker is running.
func (d *stubDriver) execBehaviour() (time.Duration, error, int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.execDelay, d.execErr, d.rowsAffected
}

func (c *stubConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.d.recordArgs(query, args)

	delay, execErr, rows := c.d.execBehaviour()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if execErr != nil {
		return nil, execErr
	}
	return stubResult{rows: rows}, nil
}

type stubResult struct{ rows int64 }

func (r stubResult) LastInsertId() (int64, error) { return 0, errors.New("not supported") }
func (r stubResult) RowsAffected() (int64, error) { return r.rows, nil }

type stubTx struct{ d *stubDriver }

func (t *stubTx) Commit() error {
	t.d.mu.Lock()
	defer t.d.mu.Unlock()
	t.d.commits++
	return t.d.commitErr
}

func (t *stubTx) Rollback() error {
	t.d.mu.Lock()
	defer t.d.mu.Unlock()
	t.d.rollbacks++
	return t.d.rollErr
}

var (
	stubDriverSeq   int
	stubDriverSeqMu sync.Mutex
)

// newStubDB registers a fresh stub driver under a unique name (database/sql
// panics on duplicate registration and offers no way to unregister) and opens
// a DB against it.
func newStubDB(t *testing.T, d *stubDriver, cfg DBConfig) *DB {
	t.Helper()

	stubDriverSeqMu.Lock()
	stubDriverSeq++
	name := fmt.Sprintf("stub-oracle-%d", stubDriverSeq)
	stubDriverSeqMu.Unlock()

	sql.Register(name, d)

	db, err := openDB(context.Background(), name, "stub-dsn", cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func testDBConfig() DBConfig {
	return DBConfig{Server: "db.example.internal", Port: 1521, Service: "TESTSVC", User: "feeduser", Password: "s3cr3t"}
}

// --- config -----------------------------------------------------------------

func TestDBConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*DBConfig)
		wantErr string
	}{
		{name: "complete"},
		{name: "no server", mutate: func(c *DBConfig) { c.Server = "" }, wantErr: "server"},
		{name: "no service", mutate: func(c *DBConfig) { c.Service = "" }, wantErr: "service"},
		{name: "no user", mutate: func(c *DBConfig) { c.User = "" }, wantErr: "user"},
		{name: "no password", mutate: func(c *DBConfig) { c.Password = "" }, wantErr: dbPasswordEnv},
		{name: "bad port", mutate: func(c *DBConfig) { c.Port = 70000 }, wantErr: "port out of range"},
		{name: "idle exceeds open", mutate: func(c *DBConfig) { c.MaxIdleConns = 9; c.MaxOpenConns = 2 }, wantErr: "exceeds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testDBConfig()
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			err := cfg.withDefaults().validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// TestDBConfigValidateNamesEveryMissingField keeps the "fix one, discover the
// next" loop from happening on a deployment host.
func TestDBConfigValidateNamesEveryMissingField(t *testing.T) {
	err := DBConfig{}.withDefaults().validate()
	if err == nil {
		t.Fatal("empty config validated")
	}
	for _, want := range []string{"server", "service", "user", dbPasswordEnv} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestDBConfigDefaults(t *testing.T) {
	cfg := DBConfig{Server: "h", Service: "s", User: "u", Password: "p"}.withDefaults()

	if cfg.Port != defaultOraclePort {
		t.Errorf("Port = %d, want %d", cfg.Port, defaultOraclePort)
	}
	if cfg.MaxOpenConns != defaultMaxOpenConns {
		t.Errorf("MaxOpenConns = %d, want %d", cfg.MaxOpenConns, defaultMaxOpenConns)
	}
	if cfg.ConnMaxLifetime != defaultConnMaxLifetime {
		t.Errorf("ConnMaxLifetime = %s, want %s", cfg.ConnMaxLifetime, defaultConnMaxLifetime)
	}
	if cfg.QueryTimeout != defaultQueryTimeout {
		t.Errorf("QueryTimeout = %s, want %s", cfg.QueryTimeout, defaultQueryTimeout)
	}

	// Explicit values must survive.
	custom := DBConfig{Server: "h", Service: "s", User: "u", Password: "p", Port: 1600, MaxOpenConns: 9}.withDefaults()
	if custom.Port != 1600 || custom.MaxOpenConns != 9 {
		t.Errorf("explicit values overwritten: port=%d maxopen=%d", custom.Port, custom.MaxOpenConns)
	}
}

// TestDBDescribeOmitsPassword is the guard against writing a production
// credential into the packet log. The password here is also a substring of
// nothing else, so a hit is unambiguous.
func TestDBDescribeOmitsPassword(t *testing.T) {
	cfg := testDBConfig()
	cfg.Password = "hunter2-UNIQUE-TOKEN"

	described := cfg.describe()
	if strings.Contains(described, cfg.Password) {
		t.Fatalf("describe() leaked the password: %s", described)
	}
	for _, want := range []string{cfg.Server, cfg.Service, cfg.User} {
		if !strings.Contains(described, want) {
			t.Errorf("describe() = %q, missing %q", described, want)
		}
	}
	// The DSN is expected to contain it -- that is exactly why it must never
	// be logged, and why describe() exists.
	if !strings.Contains(cfg.dsn(), cfg.Password) {
		t.Log("note: dsn() no longer embeds the password verbatim; redaction is still correct but this test's premise changed")
	}
}

// TestRedactDSNScrubsDriverErrors covers the second line of defence: a driver
// error that quotes the connection string back at us.
func TestRedactDSNScrubsDriverErrors(t *testing.T) {
	cfg := testDBConfig()
	cfg.Password = "hunter2-UNIQUE-TOKEN"
	dsn := cfg.dsn()

	leaky := fmt.Errorf("connect failed for %s", dsn)
	scrubbed := redactDSN(leaky, dsn, cfg)
	if strings.Contains(scrubbed.Error(), cfg.Password) {
		t.Errorf("redactDSN left the password in: %s", scrubbed)
	}

	// A bare password with no surrounding DSN must also go.
	bare := redactDSN(errors.New("auth rejected for hunter2-UNIQUE-TOKEN"), dsn, cfg)
	if strings.Contains(bare.Error(), cfg.Password) {
		t.Errorf("redactDSN left a bare password in: %s", bare)
	}

	// A clean error must be returned unchanged so errors.Is/As keep working.
	sentinel := errors.New("nothing sensitive")
	if got := redactDSN(sentinel, dsn, cfg); !errors.Is(got, sentinel) {
		t.Errorf("redactDSN rewrapped a clean error, breaking errors.Is")
	}
	if redactDSN(nil, dsn, cfg) != nil {
		t.Error("redactDSN(nil) should be nil")
	}
}

func TestDBConfigFromFile(t *testing.T) {
	t.Setenv(dbPasswordEnv, "from-env")

	path := writeTempConfig(t, `{
	  "udp": ":9100",
	  "db": {
	    "server": "oracle.internal", "port": 1600, "service": "CDTS", "user": "feedvalve",
	    "maxopenconns": 6, "maxidleconns": 3, "connmaxlifetime": "15m",
	    "connecttimeout": "5s", "querytimeout": "45s"
	  }
	}`)

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.DB == nil {
		t.Fatal("db object not parsed")
	}

	dbCfg, err := cfg.DB.toDBConfig()
	if err != nil {
		t.Fatalf("toDBConfig: %v", err)
	}

	want := DBConfig{
		Server: "oracle.internal", Port: 1600, Service: "CDTS", User: "feedvalve",
		Password: "from-env", MaxOpenConns: 6, MaxIdleConns: 3,
		ConnMaxLifetime: 15 * time.Minute, ConnectTimeout: 5 * time.Second, QueryTimeout: 45 * time.Second,
	}
	if dbCfg != want {
		t.Errorf("toDBConfig() =\n %+v\nwant\n %+v", dbCfg, want)
	}
}

// TestDBConfigAbsentUsesDefaults checks the pointer-field idiom still
// distinguishes "omitted" from "set to zero" for the nested object.
func TestDBConfigAbsentUsesDefaults(t *testing.T) {
	t.Setenv(dbPasswordEnv, "p")

	path := writeTempConfig(t, `{"db": {"server": "h", "service": "s", "user": "u"}}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	dbCfg, err := cfg.DB.toDBConfig()
	if err != nil {
		t.Fatalf("toDBConfig: %v", err)
	}
	if dbCfg.Port != 0 || dbCfg.QueryTimeout != 0 {
		t.Errorf("omitted keys should stay zero for withDefaults to fill, got port=%d querytimeout=%s", dbCfg.Port, dbCfg.QueryTimeout)
	}
	if got := dbCfg.withDefaults(); got.Port != defaultOraclePort || got.QueryTimeout != defaultQueryTimeout {
		t.Errorf("defaults not applied: %+v", got)
	}
}

func TestDBConfigRejectsBadDuration(t *testing.T) {
	t.Setenv(dbPasswordEnv, "p")

	for _, bad := range []string{`"querytimeout": "30 seconds"`, `"connmaxlifetime": "-5m"`, `"connecttimeout": ""`} {
		t.Run(bad, func(t *testing.T) {
			path := writeTempConfig(t, `{"db": {"server": "h", "service": "s", "user": "u", `+bad+`}}`)
			cfg, err := loadConfig(path)
			if err != nil {
				t.Fatalf("loadConfig: %v", err)
			}
			if _, err := cfg.DB.toDBConfig(); err == nil {
				t.Errorf("%s was accepted", bad)
			}
		})
	}
}

// TestNoDBObjectMeansNoDatabase confirms a config file without a "db" key
// leaves the relay's existing behavior untouched.
func TestNoDBObjectMeansNoDatabase(t *testing.T) {
	path := writeTempConfig(t, `{"udp": ":9000", "netid": 8}`)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.DB != nil {
		t.Errorf("db should be nil when the key is absent, got %+v", cfg.DB)
	}
	// The nil receiver must be safe -- main calls this only when non-nil, but
	// the method should not be a landmine.
	if got, err := cfg.DB.toDBConfig(); err != nil || got != (DBConfig{}) {
		t.Errorf("nil dbConfig.toDBConfig() = (%+v, %v), want zero value and nil", got, err)
	}
}

func writeTempConfig(t *testing.T, contents string) string {
	t.Helper()
	path := t.TempDir() + "/config.json"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

// --- connection / pool ------------------------------------------------------

func TestOpenDBAppliesPoolSettingsAndPings(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{}
	cfg := testDBConfig()
	cfg.MaxOpenConns = 3
	cfg.MaxIdleConns = 1
	db := newStubDB(t, d, cfg)

	stats := db.sql.Stats()
	if stats.MaxOpenConnections != 3 {
		t.Errorf("MaxOpenConnections = %d, want 3", stats.MaxOpenConnections)
	}
	if _, _, _, opens := d.snapshot(); opens == 0 {
		t.Error("OpenDB did not connect; sql.Open is lazy, so a bad host would only fail on the first write")
	}
}

func TestOpenDBFailsWhenUnreachable(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{openErr: errors.New("dial tcp: connection refused for stub-dsn")}
	cfg := testDBConfig()

	stubDriverSeqMu.Lock()
	stubDriverSeq++
	name := fmt.Sprintf("stub-oracle-fail-%d", stubDriverSeq)
	stubDriverSeqMu.Unlock()
	sql.Register(name, d)

	_, err := openDB(context.Background(), name, cfg.dsn(), cfg)
	if err == nil {
		t.Fatal("openDB succeeded against an unreachable server")
	}
	if strings.Contains(err.Error(), cfg.Password) {
		t.Errorf("connection error leaked the password: %v", err)
	}
	if !strings.Contains(err.Error(), cfg.Server) {
		t.Errorf("connection error %q should name the target", err)
	}
}

func TestOpenDBRejectsIncompleteConfig(t *testing.T) {
	_, err := OpenDB(context.Background(), DBConfig{Server: "h"})
	if err == nil {
		t.Fatal("OpenDB accepted a config with no user or password")
	}
	if !strings.Contains(err.Error(), dbPasswordEnv) {
		t.Errorf("error should point at the password env var, got: %v", err)
	}
}

// --- exec / transactions ----------------------------------------------------

func TestExecReturnsRowsAffected(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{rowsAffected: 7}
	db := newStubDB(t, d, testDBConfig())

	rows, err := db.Exec(context.Background(), "UPDATE security SET base_price = :1 WHERE token = :2", 2500, 98765)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if rows != 7 {
		t.Errorf("rows affected = %d, want 7", rows)
	}

	statements, _, _, _ := d.snapshot()
	if len(statements) != 1 || !strings.Contains(statements[0], "UPDATE security") {
		t.Errorf("unexpected statements: %q", statements)
	}
}

func TestExecPropagatesError(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{execErr: errors.New("ORA-00942: table or view does not exist")}
	db := newStubDB(t, d, testDBConfig())

	if _, err := db.Exec(context.Background(), "UPDATE nope SET x = :1", 1); err == nil {
		t.Fatal("Exec succeeded despite a driver error")
	}
}

func TestExecTxCommitsOnSuccess(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{rowsAffected: 1}
	db := newStubDB(t, d, testDBConfig())

	err := db.ExecTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "UPDATE security SET base_price = :1 WHERE token = :2", 2500, 1)
		return err
	})
	if err != nil {
		t.Fatalf("ExecTx: %v", err)
	}

	_, commits, rollbacks, _ := d.snapshot()
	if commits != 1 || rollbacks != 0 {
		t.Errorf("commits=%d rollbacks=%d, want 1 and 0", commits, rollbacks)
	}
}

func TestExecTxRollsBackOnError(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{}
	db := newStubDB(t, d, testDBConfig())
	sentinel := errors.New("callback failed")

	err := db.ExecTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "UPDATE security SET base_price = :1", 1); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("ExecTx error = %v, want the callback's error unwrapped", err)
	}

	_, commits, rollbacks, _ := d.snapshot()
	if commits != 0 || rollbacks != 1 {
		t.Errorf("commits=%d rollbacks=%d, want 0 and 1", commits, rollbacks)
	}
}

// TestExecTxRollsBackOnPanic is the one that matters operationally: a panic in
// a callback must not leave a transaction open holding row locks until the
// session dies.
func TestExecTxRollsBackOnPanic(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{}
	db := newStubDB(t, d, testDBConfig())

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("panic was swallowed; it must continue to unwind after the rollback")
			}
		}()
		_ = db.ExecTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
			panic("boom")
		})
	}()

	_, commits, rollbacks, _ := d.snapshot()
	if commits != 0 || rollbacks != 1 {
		t.Errorf("commits=%d rollbacks=%d, want 0 and 1", commits, rollbacks)
	}
}

func TestExecTxReportsCommitFailure(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{commitErr: errors.New("ORA-01555: snapshot too old")}
	db := newStubDB(t, d, testDBConfig())

	err := db.ExecTx(context.Background(), func(context.Context, *sql.Tx) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "commit") {
		t.Fatalf("ExecTx error = %v, want a commit failure", err)
	}
}

// TestExecHonoursQueryTimeout checks the configured timeout actually bounds a
// slow statement, so a hung database cannot pin a caller indefinitely.
func TestExecHonoursQueryTimeout(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{execDelay: 2 * time.Second}
	cfg := testDBConfig()
	cfg.QueryTimeout = 50 * time.Millisecond
	db := newStubDB(t, d, cfg)

	start := time.Now()
	_, err := db.Exec(context.Background(), "UPDATE slow SET x = :1", 1)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Exec ignored the query timeout")
	}
	if elapsed > time.Second {
		t.Errorf("Exec took %s; the %s timeout was not applied", elapsed, cfg.QueryTimeout)
	}
}

// TestExecRespectsCallerDeadline confirms a caller's own deadline is not
// replaced by the (longer) configured default.
func TestExecRespectsCallerDeadline(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{execDelay: 2 * time.Second}
	cfg := testDBConfig()
	cfg.QueryTimeout = time.Hour
	db := newStubDB(t, d, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := db.Exec(ctx, "UPDATE slow SET x = :1", 1); err == nil {
		t.Fatal("Exec ignored the caller's deadline")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Exec took %s; the caller's deadline was overridden by the config default", elapsed)
	}
}

// --- dummy row --------------------------------------------------------------

func TestUpdateDummyRow(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{rowsAffected: 1}
	db := newStubDB(t, d, testDBConfig())

	updateDummyRow(context.Background(), db.sql, DummyRow{
		ID: 1, TransactionCode: 7305, PacketIndex: 0, PacketCount: 1,
		Source: "10.0.0.5:9000", PayloadBytes: 302, UpdatedAt: time.Now(),
	})

	statements, commits, rollbacks, _ := d.snapshot()
	if commits != 1 || rollbacks != 0 {
		t.Errorf("commits=%d rollbacks=%d, want 1 and 0", commits, rollbacks)
	}
	if len(statements) != 1 {
		t.Fatalf("got %d statements, want 1: %q", len(statements), statements)
	}
	sql := statements[0]
	if !strings.Contains(sql, "UPDATE "+dummyTableName) {
		t.Errorf("statement does not update %s: %s", dummyTableName, sql)
	}
	// Oracle binds, not MySQL ones. A "?" here would fail at parse time with a
	// confusing ORA-00936 against a real server.
	if strings.Contains(sql, "?") {
		t.Errorf("statement uses ? placeholders instead of Oracle :n binds: %s", sql)
	}
	for i := 1; i <= 7; i++ {
		if !strings.Contains(sql, fmt.Sprintf(":%d", i)) {
			t.Errorf("statement is missing bind :%d: %s", i, sql)
		}
	}
}

// TestUpdateDummyRowForPacketNoDatabase is what keeps the whole decoder test
// suite working: processPacket calls into this on every packet, and almost no
// test configures a database.
func TestUpdateDummyRowForPacketNoDatabase(t *testing.T) {
	setPacketDB(nil)
	// Must not panic and must not log an error.
	appBuf := captureAppLogger(t)
	updateDummyRowForPacket(nil, 0, 1, 7305, 302)
	if appBuf.String() != "" {
		t.Errorf("no-database path logged something: %s", appBuf.String())
	}
}

// installPacketDB points the packet path at a stub database, for one test.
func installPacketDB(t *testing.T, d *stubDriver) *DB {
	t.Helper()
	db := newStubDB(t, d, testDBConfig())
	setPacketDB(db)
	t.Cleanup(func() { setPacketDB(nil) })
	return db
}

func TestUpdateDummyRowForPacketWritesPacketDetails(t *testing.T) {
	captureAppLogger(t)

	d := &stubDriver{rowsAffected: 1}
	installPacketDB(t, d)

	updateDummyRowForPacket(&net.UDPAddr{IP: net.ParseIP("10.0.0.5"), Port: 9000}, 1, 3, 7208, 478)

	statements, commits, rollbacks, _ := d.snapshot()
	if len(statements) != 1 {
		t.Fatalf("got %d statements, want 1", len(statements))
	}
	if commits != 1 || rollbacks != 0 {
		t.Errorf("commits=%d rollbacks=%d, want 1 and 0", commits, rollbacks)
	}
}

// TestUpdateDummyRowForPacketSurvivesDatabaseErrors: the relay must keep
// decoding the feed when the database is unreachable.
func TestUpdateDummyRowForPacketSurvivesDatabaseErrors(t *testing.T) {
	appBuf := captureAppLogger(t)

	d := &stubDriver{execErr: errors.New("ORA-00942: table or view does not exist")}
	installPacketDB(t, d)

	updateDummyRowForPacket(nil, 0, 1, 7305, 302) // must not panic

	if !strings.Contains(appBuf.String(), "ERROR update dummy row") {
		t.Errorf("database failure was not reported: %s", appBuf.String())
	}
	if _, _, rollbacks, _ := d.snapshot(); rollbacks != 1 {
		t.Errorf("rollbacks = %d, want 1", rollbacks)
	}
}

// TestUpdateDummyRowForPacketWarnsOnMissingRow covers the (0, nil) case, which
// silently does nothing on a real server if the row was never seeded.
func TestUpdateDummyRowForPacketWarnsOnMissingRow(t *testing.T) {
	appBuf := captureAppLogger(t)

	d := &stubDriver{rowsAffected: 0}
	installPacketDB(t, d)

	updateDummyRowForPacket(nil, 0, 1, 7305, 302)

	if !strings.Contains(appBuf.String(), "affected 0 rows") {
		t.Errorf("missing-row case was not reported: %s", appBuf.String())
	}
}

// TestProcessPacketUpdatesDummyRowForEveryTransactionCode is the regression
// test for the wiring itself: the update must fire for 7208, for 7305, and for
// a transaction code that falls through to the default branch.
func TestProcessPacketUpdatesDummyRowForEveryTransactionCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload func(*testing.T) []byte
	}{
		{"7208", func(t *testing.T) []byte {
			return buildMSBroadcastOnlyMBPMessage(t, netIDCommodityDerivative, 12345)
		}},
		{"7305", func(t *testing.T) []byte {
			return buildMSSecurityUpdateInfoMessage(t, netIDCommodityDerivative, 999, "TESTSYM")
		}},
		{"unhandled code", func(t *testing.T) []byte {
			return buildHeaderOnlyPayload(t, netIDCommodityDerivative, 101) // 7202
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captureAppLogger(t)
			capturePacketLogger(t)

			d := &stubDriver{rowsAffected: 1}
			installPacketDB(t, d)

			payload := tc.payload(t)
			processPacket(nil, 0, 1, len(payload), payload)

			statements, _, _, _ := d.snapshot()
			if len(statements) != 1 {
				t.Fatalf("processPacket issued %d dummy-row updates, want 1: %q", len(statements), statements)
			}
			if !strings.Contains(statements[0], "UPDATE "+dummyTableName) {
				t.Errorf("unexpected statement: %s", statements[0])
			}
		})
	}
}

// TestProcessPacketSkipsDummyRowOnParseFailure documents the boundary: the
// update sits after the packet is logged, so a payload that never gets that
// far does not write anything.
func TestProcessPacketSkipsDummyRowOnParseFailure(t *testing.T) {
	captureAppLogger(t)
	capturePacketLogger(t)

	d := &stubDriver{rowsAffected: 1}
	installPacketDB(t, d)

	// Announces 7305 but is far too short for the 294-byte body.
	truncated := buildMSSecurityUpdateInfoMessage(t, netIDCommodityDerivative, 999, "SHORT")[:100]
	processPacket(nil, 0, 1, len(truncated), truncated)

	if statements, _, _, _ := d.snapshot(); len(statements) != 0 {
		t.Errorf("expected no dummy-row write for a failed parse, got %d statements: %q", len(statements), statements)
	}
}

// TestUpdateDummyRowSurvivesRepeatedFailures: the packet path must keep
// working across repeated database failures -- each one is just logged.
func TestUpdateDummyRowSurvivesRepeatedFailures(t *testing.T) {
	appBuf := captureAppLogger(t)

	d := &stubDriver{execErr: errors.New("ORA-12541: TNS:no listener")}
	installPacketDB(t, d)

	updateDummyRowForPacket(nil, 0, 1, 7305, 302)
	updateDummyRowForPacket(nil, 1, 1, 7305, 302)

	if logLines := strings.Count(appBuf.String(), "ORA-12541"); logLines != 2 {
		t.Errorf("logged %d failure lines, want 2 (one per call)", logLines)
	}

	// Recover the database and confirm the next update succeeds.
	d.mu.Lock()
	d.execErr = nil
	d.rowsAffected = 1
	d.mu.Unlock()

	updateDummyRowForPacket(nil, 2, 1, 7305, 302)
	statements, _, _, _ := d.snapshot()
	if len(statements) != 3 {
		t.Fatalf("got %d statements, want 3", len(statements))
	}
}

// --- error mapping ----------------------------------------------------------

func TestOracleErrCode(t *testing.T) {
	wrapped := fmt.Errorf("exec: %w", network.NewOracleError(1))
	code, ok := oracleErrCode(wrapped)
	if !ok || code != 1 {
		t.Errorf("oracleErrCode = (%d, %v), want (1, true) for a wrapped ORA-00001", code, ok)
	}

	if _, ok := oracleErrCode(errors.New("plain error")); ok {
		t.Error("oracleErrCode claimed a non-Oracle error had an ORA code")
	}
	if _, ok := oracleErrCode(nil); ok {
		t.Error("oracleErrCode(nil) should report false")
	}
}

// --- shutdown ---------------------------------------------------------------

// TestReceiveLoopStopsOnContextCancel is the test behind "the connection lives
// for the whole process and is closed when it dies": receiveLoop must return
// on cancellation, because everything downstream (db.Close, file close) hangs
// off it returning. ReadFrom cannot be interrupted any other way -- closing
// the listener is what unblocks it.
func TestReceiveLoopStopsOnContextCancel(t *testing.T) {
	captureAppLogger(t)
	capturePacketLogger(t)

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		receiveLoop(ctx, conn)
		close(returned)
	}()

	// Deliver one datagram so the loop is demonstrably running before we stop it.
	client, err := net.Dial("udp", conn.LocalAddr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	if _, err := client.Write([]byte{0, 8, 0, 0}); err != nil {
		t.Fatalf("write: %v", err)
	}

	cancel()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("receiveLoop did not return after cancellation; shutdown would hang and the database connection would never be closed")
	}
}

// TestReceiveLoopClosesConnectionOnShutdown checks the listener really is
// closed, not merely abandoned.
func TestReceiveLoopClosesConnectionOnShutdown(t *testing.T) {
	captureAppLogger(t)

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		receiveLoop(ctx, conn)
		close(returned)
	}()

	cancel()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("receiveLoop did not return")
	}

	// A second Close on an already-closed conn reports an error; that is the
	// signal that the loop closed it.
	if err := conn.Close(); err == nil {
		t.Error("listener was still open after shutdown")
	}
}

// TestRunReturnsOnContextCancel exercises the full run() path -- flag parsing,
// listener, receive loop, and the deferred cleanup that closes the database
// connection. Without a "db" object it opens no connection, but it is the same
// return path: cancelling the context must make run() return so that its
// defers (db.Close, outFile.Close, udpConn.Close) execute.
//
// This is the piece that cannot be checked by signalling the process on
// Windows, where a kill is a hard TerminateProcess rather than a deliverable
// signal.
func TestRunReturnsOnContextCancel(t *testing.T) {
	captureAppLogger(t)
	capturePacketLogger(t)

	// run() registers flags on the default CommandLine, so give it a clean one.
	origArgs, origFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = origArgs, origFlags })
	flag.CommandLine = flag.NewFlagSet("relay-test", flag.ContinueOnError)
	os.Args = []string{
		"relay-test",
		"-udp", "127.0.0.1:0",
		"-out", t.TempDir() + "/relay.log",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx) }()

	// Give run() a moment to reach the receive loop, then stop it.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned %v, want nil on a clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancellation; deferred cleanup (including db.Close) would never run")
	}
}

// TestSetPacketDBLifecycle mirrors the sequence run() performs around the
// receive loop: install the connection, then clear it and close it.
func TestSetPacketDBLifecycle(t *testing.T) {
	captureAppLogger(t)

	if packetDB != nil {
		t.Fatal("packet db was left set by an earlier test")
	}

	d := &stubDriver{rowsAffected: 1}
	db := newStubDB(t, d, testDBConfig())

	setPacketDB(db)
	if packetDB != db {
		t.Error("setPacketDB did not install the connection")
	}

	setPacketDB(nil)
	if packetDB != nil {
		t.Error("setPacketDB(nil) did not clear the connection")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After shutdown the packet path must go quiet rather than touch a stopped
	// writer or a closed connection.
	appBuf := captureAppLogger(t)
	updateDummyRowForPacket(nil, 0, 1, 7305, 302)
	if appBuf.String() != "" {
		t.Errorf("packet path touched the database after shutdown: %s", appBuf.String())
	}
}

// --- integration ------------------------------------------------------------

// TestDBIntegrationUpdate is the test that actually demonstrates writes
// against Oracle. It is skipped unless FEEDVALVE_ORACLE_DSN names a reachable
// database, so the default `go test ./...` needs no server.
//
//	FEEDVALVE_ORACLE_DSN='oracle://user:pass@host:1521/SERVICE' go test -run TestDBIntegration ./poc/
func TestDBIntegrationUpdate(t *testing.T) {
	dsn := os.Getenv("FEEDVALVE_ORACLE_DSN")
	if dsn == "" {
		t.Skip("FEEDVALVE_ORACLE_DSN not set; skipping Oracle integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := openDB(ctx, oracleDriverName, dsn, DBConfig{Server: "integration", Service: "integration", User: "integration", Password: "unused"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer db.Close()

	table := fmt.Sprintf("feedvalve_it_%d", time.Now().UnixNano()%100000)
	if _, err := db.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (token NUMBER(10) PRIMARY KEY, base_price NUMBER(10))", table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(context.Background(), "DROP TABLE "+table); err != nil {
			t.Errorf("drop table %s: %v", table, err)
		}
	})

	if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO %s (token, base_price) VALUES (:1, :2)", table), 98765, 2400); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.Exec(ctx, fmt.Sprintf("UPDATE %s SET base_price = :1 WHERE token = :2", table), 2500, 98765)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if rows != 1 {
		t.Errorf("update affected %d rows, want 1", rows)
	}

	var price int
	if err := db.sql.QueryRowContext(ctx, fmt.Sprintf("SELECT base_price FROM %s WHERE token = :1", table), 98765).Scan(&price); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if price != 2500 {
		t.Errorf("base_price = %d, want 2500", price)
	}

	// A transaction that fails must leave the row untouched.
	sentinel := errors.New("deliberate")
	err = db.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET base_price = :1 WHERE token = :2", table), 9999, 98765); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("ExecTx error = %v, want the sentinel", err)
	}
	if err := db.sql.QueryRowContext(ctx, fmt.Sprintf("SELECT base_price FROM %s WHERE token = :1", table), 98765).Scan(&price); err != nil {
		t.Fatalf("read back after rollback: %v", err)
	}
	if price != 2500 {
		t.Errorf("base_price = %d after a rolled-back transaction, want 2500", price)
	}

	if version, err := db.ServerVersion(ctx); err != nil {
		t.Errorf("ServerVersion: %v", err)
	} else {
		t.Logf("connected to Oracle %s", version)
	}
}

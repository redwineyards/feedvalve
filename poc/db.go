package main

// Oracle connectivity for the relay. This file is the connection layer only:
// it opens a pooled connection, runs DML, and reports errors usefully. Nothing
// here is wired into the packet path -- see the note on ExecTx below.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	// Imported for its side effect of registering the "oracle" driver, and for
	// BuildUrl. go-ora is a pure Go driver: no cgo, no Oracle Instant Client,
	// which is what keeps the AIX cross-compile (CGO_ENABLED=0 GOOS=aix) working.
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"
)

// oracleDriverName is the driver name go-ora registers with database/sql.
const oracleDriverName = "oracle"

// dbPasswordEnv is the only place the database password is read from.
// Deliberately not a flag (flags are visible in ps output) and not a config
// file key (config files get committed).
const dbPasswordEnv = "FEEDVALVE_DB_PASSWORD"

// Defaults chosen for a relay that writes steadily rather than in bursts.
const (
	// defaultMaxOpenConns is deliberately small. Oracle caps concurrent
	// sessions via the instance's sessions/processes init parameters, and an
	// unbounded pool from several relay instances can exhaust them.
	defaultMaxOpenConns = 4
	defaultMaxIdleConns = 2

	// defaultConnMaxLifetime bounds how long a pooled session is reused.
	// Oracle profiles (IDLE_TIME) and stateful firewalls drop idle sessions
	// without telling the client; the first reuse of a dead one surfaces as
	// ORA-03113/ORA-03114. Retiring connections before that happens turns an
	// error into a reconnect.
	defaultConnMaxLifetime = 30 * time.Minute

	defaultConnectTimeout = 10 * time.Second
	defaultQueryTimeout   = 30 * time.Second

	defaultOraclePort = 1521
)

// DBConfig describes how to reach the database. Everything except Password
// comes from the JSON config file (see config.go); Password comes only from
// dbPasswordEnv.
type DBConfig struct {
	Server   string
	Port     int
	Service  string
	User     string
	Password string

	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration

	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
}

// withDefaults fills in unset fields. Zero means "unset" for every field here,
// so a config file that omits a key gets the default rather than a broken
// value like a zero timeout.
func (c DBConfig) withDefaults() DBConfig {
	if c.Port == 0 {
		c.Port = defaultOraclePort
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = defaultMaxOpenConns
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = defaultMaxIdleConns
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = defaultQueryTimeout
	}
	return c
}

// validate reports what is missing, naming every missing field at once rather
// than one per run.
func (c DBConfig) validate() error {
	var missing []string
	if c.Server == "" {
		missing = append(missing, "server")
	}
	if c.Service == "" {
		missing = append(missing, "service")
	}
	if c.User == "" {
		missing = append(missing, "user")
	}
	if c.Password == "" {
		missing = append(missing, "password (set "+dbPasswordEnv+")")
	}
	if len(missing) > 0 {
		return fmt.Errorf("incomplete database config: missing %s", strings.Join(missing, ", "))
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("database port out of range: %d", c.Port)
	}
	if c.MaxIdleConns > c.MaxOpenConns {
		return fmt.Errorf("maxidleconns (%d) exceeds maxopenconns (%d)", c.MaxIdleConns, c.MaxOpenConns)
	}
	return nil
}

// dsn builds the go-ora connection URL. The result contains the password and
// must never be logged or wrapped into an error -- use describe() for that.
func (c DBConfig) dsn() string {
	return go_ora.BuildUrl(c.Server, c.Port, c.Service, c.User, c.Password, nil)
}

// describe renders the connection target for logs and error messages with no
// password in it. Everything it prints is drawn from the individual config
// fields rather than by redacting the assembled DSN, so there is no
// URL-escaping edge case that can let the password slip through.
func (c DBConfig) describe() string {
	return fmt.Sprintf("oracle://%s@%s:%d/%s", c.User, c.Server, c.Port, c.Service)
}

// DB is a pooled Oracle connection.
type DB struct {
	sql          *sql.DB
	queryTimeout time.Duration
	target       string // describe() output, safe to log
}

// OpenDB connects to Oracle and verifies the connection before returning, so a
// bad host or credential fails at startup rather than on the first write.
func OpenDB(ctx context.Context, cfg DBConfig) (*DB, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return openDB(ctx, oracleDriverName, cfg.dsn(), cfg)
}

// openDB is the driver-agnostic half of OpenDB, split out so tests can point
// it at a stub driver instead of a real database.
func openDB(ctx context.Context, driverName, dsn string, cfg DBConfig) (*DB, error) {
	cfg = cfg.withDefaults()

	handle, err := sql.Open(driverName, dsn)
	if err != nil {
		// sql.Open's error can echo the DSN back, so report our own text.
		return nil, fmt.Errorf("open database %s: %w", cfg.describe(), redactDSN(err, dsn, cfg))
	}

	handle.SetMaxOpenConns(cfg.MaxOpenConns)
	handle.SetMaxIdleConns(cfg.MaxIdleConns)
	handle.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	db := &DB{sql: handle, queryTimeout: cfg.QueryTimeout, target: cfg.describe()}

	// sql.Open is lazy -- it validates arguments but does not connect. Ping is
	// what actually establishes a session.
	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := handle.PingContext(pingCtx); err != nil {
		handle.Close()
		return nil, fmt.Errorf("connect to %s: %w", cfg.describe(), redactDSN(err, dsn, cfg))
	}

	appLogger.Printf("INFO database connected target=%s max_open_conns=%d max_idle_conns=%d conn_max_lifetime=%s query_timeout=%s",
		cfg.describe(), cfg.MaxOpenConns, cfg.MaxIdleConns, cfg.ConnMaxLifetime, cfg.QueryTimeout)
	return db, nil
}

// redactDSN defends against a driver error that quotes the connection string
// back at us: any occurrence of the DSN, or of the bare password, is replaced
// before the error can reach a log file. Belt and braces on top of never
// passing the DSN into a message ourselves.
func redactDSN(err error, dsn string, cfg DBConfig) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	replaced := text
	if dsn != "" {
		replaced = strings.ReplaceAll(replaced, dsn, cfg.describe())
	}
	if cfg.Password != "" {
		replaced = strings.ReplaceAll(replaced, cfg.Password, "[redacted]")
	}
	if replaced == text {
		return err
	}
	return errors.New(replaced)
}

// Close releases the pool.
func (d *DB) Close() error {
	if d == nil || d.sql == nil {
		return nil
	}
	return d.sql.Close()
}

// Ping verifies a live session, establishing one if the pool is empty.
func (d *DB) Ping(ctx context.Context) error {
	ctx, cancel := d.withTimeout(ctx)
	defer cancel()
	if err := d.sql.PingContext(ctx); err != nil {
		return fmt.Errorf("ping %s: %w", d.target, err)
	}
	return nil
}

// withTimeout applies the configured query timeout unless the caller already
// set a deadline of their own.
func (d *DB) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || d.queryTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d.queryTimeout)
}

// Exec runs a single INSERT/UPDATE/DELETE/MERGE and returns the affected row
// count.
//
// Bind placeholders are Oracle's, not MySQL's: use positional :1, :2, ... or
// named :token with sql.Named. A "?" is not a placeholder to Oracle -- it
// fails at parse time with a confusing ORA-00936 "missing expression" rather
// than anything that points at the real problem.
//
//	db.Exec(ctx, "UPDATE security SET base_price = :1 WHERE token = :2", price, token)
//	db.Exec(ctx, "UPDATE security SET base_price = :p WHERE token = :t",
//	    sql.Named("p", price), sql.Named("t", token))
//
// There is no ON CONFLICT in Oracle; upserts are
// MERGE INTO t USING dual ON (...) WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...
func (d *DB) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	ctx, cancel := d.withTimeout(ctx)
	defer cancel()

	result, err := d.sql.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		// Not fatal: the statement did run. Report the count as unknown.
		return 0, fmt.Errorf("exec succeeded but row count unavailable: %w", err)
	}
	return rows, nil
}

// ExecTx runs fn inside a transaction, committing if it returns nil and
// rolling back otherwise. A panic inside fn also rolls back before the panic
// continues to unwind, so a bug in a callback cannot leave a transaction open
// holding row locks.
func (d *DB) ExecTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
	ctx, cancel := d.withTimeout(ctx)
	defer cancel()

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				appLogger.Printf("ERROR rollback after panic failed target=%s err=%v", d.target, rbErr)
			}
			panic(r)
		}
	}()

	if err := fn(ctx, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			// Report both: the rollback failure explains why locks may still
			// be held, the original error explains why we rolled back at all.
			return fmt.Errorf("transaction failed (%w); rollback also failed: %v", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// ServerVersion returns the database banner, useful for confirming which
// instance a deployment actually reached.
func (d *DB) ServerVersion(ctx context.Context) (string, error) {
	ctx, cancel := d.withTimeout(ctx)
	defer cancel()

	var banner string
	// v$version is readable by default; product_component_version avoids
	// needing SELECT on v_$version in locked-down schemas.
	const query = "SELECT version FROM product_component_version WHERE ROWNUM = 1"
	if err := d.sql.QueryRowContext(ctx, query).Scan(&banner); err != nil {
		return "", fmt.Errorf("read server version: %w", err)
	}
	return banner, nil
}

// dummyTableName is the placeholder table UpdateDummyRow writes to. It exists
// to exercise the write path end to end before any real schema is designed;
// replace it (and DummyRow) once the actual security-master tables exist.
const dummyTableName = "feedvalve_dummy"

// DummyRow is one row of dummyTableName. ID selects which row to update; every
// other field is overwritten.
//
// The table this expects:
//
//	CREATE TABLE feedvalve_dummy (
//	  id               NUMBER(10)    PRIMARY KEY,
//	  transaction_code NUMBER(5),
//	  packet_index     NUMBER(10),
//	  packet_count     NUMBER(10),
//	  source           VARCHAR2(64),
//	  payload_bytes    NUMBER(10),
//	  updated_at       TIMESTAMP
//	);
//	INSERT INTO feedvalve_dummy (id) VALUES (1);
//	COMMIT;
type DummyRow struct {
	ID              int64
	TransactionCode int16
	PacketIndex     int
	PacketCount     int
	Source          string
	PayloadBytes    int
	UpdatedAt       time.Time
}

// updateDummyRowQuery is the statement updateDummyRow runs. row.ID selects
// which row to update; every other field is overwritten.
const updateDummyRowQuery = `UPDATE ` + dummyTableName + `
   SET transaction_code = :1,
       packet_index     = :2,
       packet_count     = :3,
       source           = :4,
       payload_bytes    = :5,
       updated_at       = :6
 WHERE id = :7`

// updateDummyRow writes row inside its own transaction -- begin, update,
// commit -- against a plain *sql.DB, with no dependency on the DB type.
//
// It never returns an error: the caller is the UDP receive path, and a
// database problem must not stop the relay decoding the feed. Every SQL
// error (begin, exec, commit) is logged here instead. A successful update
// that affects 0 rows almost always means the row was never seeded -- see
// the DDL on DummyRow -- and is logged as a WARN.
func updateDummyRow(ctx context.Context, db *sql.DB, row DummyRow) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		appLogger.Printf("ERROR begin transaction for dummy row update err=%v", err)
		return
	}

	result, err := tx.ExecContext(ctx, updateDummyRowQuery,
		row.TransactionCode,
		row.PacketIndex,
		row.PacketCount,
		row.Source,
		row.PayloadBytes,
		row.UpdatedAt,
		row.ID,
	)
	if err != nil {
		appLogger.Printf("ERROR update dummy row err=%v", err)
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			appLogger.Printf("ERROR rollback dummy row update err=%v", rbErr)
		}
		return
	}

	if err := tx.Commit(); err != nil {
		appLogger.Printf("ERROR commit dummy row update err=%v", err)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		appLogger.Printf("ERROR read rows affected for dummy row update err=%v", err)
		return
	}
	if rows == 0 {
		appLogger.Printf("WARN update dummy row affected 0 rows: id=%d not present in %s -- seed it, see the DDL on DummyRow",
			row.ID, dummyTableName)
	}
}

// oracleErrCode extracts the ORA- number from an error, if it came from the
// server. Lets callers distinguish an expected collision (ORA-00001, unique
// constraint) from an operational failure (ORA-12541, no listener) without
// string-matching error text.
func oracleErrCode(err error) (int, bool) {
	var oraErr *network.OracleError
	if errors.As(err, &oraErr) {
		return oraErr.ErrCode, true
	}
	return 0, false
}

// dbPasswordFromEnv reads the password from the environment. Separate function
// so tests can describe the contract without touching the process env.
func dbPasswordFromEnv() string {
	return os.Getenv(dbPasswordEnv)
}

// packetDB is the process-wide connection used by the packet path, following
// the same package-level pattern as expectedNetID and the loggers. It is nil
// when no database is configured, which is the normal case for the decoder
// tests -- updateDummyRowForPacket no-ops rather than failing. It is set once
// in run() before the receive loop starts, so no locking is needed.
var packetDB *DB

func setPacketDB(db *DB) {
	packetDB = db
}

// packetDummyRowID is the dummy row every packet updates. One fixed row means
// each packet overwrites the last -- enough to prove the write path works,
// which is all the dummy table is for.
const packetDummyRowID = 1

// updateDummyRowForPacket writes one packet's identifying details to the dummy
// row via updateDummyRow. Called from processPacket for every transaction
// code.
//
// This runs synchronously on the UDP receive goroutine, so a slow database
// directly delays the next ReadFrom -- deliberately simple, at the cost of
// datagrams the kernel may drop while a write is in flight. It no-ops when no
// database is configured.
func updateDummyRowForPacket(from net.Addr, packetIndex, packetCount int, transactionCode int16, payloadBytes int) {
	if packetDB == nil {
		return
	}

	source := ""
	if from != nil {
		source = from.String()
	}

	updateDummyRow(context.Background(), packetDB.sql, DummyRow{
		ID:              packetDummyRowID,
		TransactionCode: transactionCode,
		PacketIndex:     packetIndex,
		PacketCount:     packetCount,
		Source:          source,
		PayloadBytes:    payloadBytes,
		UpdatedAt:       time.Now(),
	})
}

// runDBCheck connects, verifies a round trip, reports the server version, and
// disconnects. Read-only: it proves connectivity and credentials from the
// deployment host without starting the relay or writing anything.
func runDBCheck(ctx context.Context, cfg DBConfig) error {
	db, err := OpenDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	var one int
	queryCtx, cancel := db.withTimeout(ctx)
	defer cancel()
	if err := db.sql.QueryRowContext(queryCtx, "SELECT 1 FROM dual").Scan(&one); err != nil {
		return fmt.Errorf("round-trip query: %w", err)
	}
	if one != 1 {
		return fmt.Errorf("round-trip query returned %d, want 1", one)
	}

	version, err := db.ServerVersion(ctx)
	if err != nil {
		// Not fatal -- the connection works, the schema just may not grant
		// access to the version view.
		appLogger.Printf("WARN database version unavailable err=%v", err)
		version = "unknown"
	}
	appLogger.Printf("INFO database check ok target=%s version=%s", db.target, version)
	return nil
}

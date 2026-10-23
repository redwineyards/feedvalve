package writer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
)

// leakFindings returns every needle that appears in the captured log text.
func leakFindings(logText string, needles map[string]string) []string {
	var found []string
	for what, n := range needles {
		if n != "" && strings.Contains(logText, n) {
			found = append(found, what)
		}
	}
	return found
}

// TestG_SEC_1_WriterFailureRunLeaksNothingToTheLog is gate G-SEC-1 for the
// writer.
//
// SETUP: a two-writer pool over an oratest fake, a real AppLogger writing to a
// buffer, a password set in the environment before the session layer is built
// (so its redactor knows it), and a distinctive row key and image bytes. The
// fake's driver errors quote the DSN and the password, as a real driver can.
// STIMULUS: batch 1 fails its array bind with ORA-01438 and rejects one row on
// the row-by-row pass; batches 2 and 3 fail every MERGE with ORA-00942 until
// the pool stops. Every Err the writer hands back is also collected.
// ASSERTION: the log is not empty and does contain the rejected-row line (so
// the scan has something to scan); no log line, no batch Err and no StopError
// text contains the password, the DSN, the statement text, the table names,
// or any bind value (the row key and the image bytes included; a rejected
// row is attributed by dest, slot and ORA code only).
// WOULD HAVE CAUGHT: logging the driver error unredacted, logging the
// statement text with a failure, or logging a row's key or image bytes in a
// rejection line.
func TestG_SEC_1_WriterFailureRunLeaksNothingToTheLog(t *testing.T) {
	const (
		secret  = "Zx9-s3cr3t-PW"
		sentKey = 918273645
		pattern = byte(0x5A)
	)
	t.Setenv(oradb.PasswordEnv, secret)
	f := oratest.New()
	t.Cleanup(f.Close)
	db, err := oradb.OpenDriver(context.Background(), oratest.DriverName, f.DSN(), oradb.DBSettings{
		Host: "h", Port: 1521, Service: "S", User: "u",
		ConnectTimeoutSec: 10, QueryTimeoutSec: 30, ConnMaxLifetimeMinutes: 30,
		MaxOpenConns: 4, MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	var mode atomic.Int32 // 0: row-data failures, 1: statement failures
	f.SetScript(func(c oratest.Call) oratest.Result {
		if !isMerge(c) {
			return oratest.Result{}
		}
		leak := func(code int) error {
			return fmt.Errorf("driver: connect %s password=%s sql=%s: %w", f.DSN(), secret, c.SQL, oratest.ORA(code))
		}
		if mode.Load() == 1 {
			return oratest.Result{Err: leak(942)}
		}
		if c.Rows > 1 {
			return oratest.Result{Err: leak(1438)}
		}
		if len(c.Args) > 0 && fmt.Sprint(c.Args[0]) == fmt.Sprint(sentKey) {
			return oratest.Result{Err: leak(1438)}
		}
		return oratest.Result{}
	})

	set := destSet(t)
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: 1})
	in := NewInstruments(reg, 3)
	in.ResolveCache(reg, 32, 3)
	in.ResolvePool(reg, 3)
	reg.Freeze()
	var logBuf bytes.Buffer
	lg := obs.NewAppLogger(&logBuf, obs.LevelInfo, 100000, reg)

	par := defaultParams()
	p, err := NewPool(db, set, Config{Writers: 2, Ceiling: rigCeiling}, func() *Params { return par }, in, lg)
	if err != nil {
		t.Fatal(err)
	}
	comp := make(chan *model.Batch, 4)
	batchQ := make(chan *model.Batch, 8)
	if err := p.Start(context.Background(), batchQ, []chan<- *model.Batch{comp}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		p.Close(ctx)
	})

	mk := func() *model.Batch {
		b := standard()
		for i := range b.Rows {
			for j := range b.Rows[i].Image {
				b.Rows[i].Image[j] = pattern
			}
		}
		for d := 0; d < 3; d++ {
			b.Rows[b.DestStart[d]].Row = sentKey
		}
		return b
	}
	recv := func() *model.Batch {
		select {
		case b := <-comp:
			return b
		case <-time.After(5 * time.Second):
			t.Fatal("no completion")
			return nil
		}
	}
	var errTexts []string
	collect := func(b *model.Batch) {
		if b.Err != nil {
			errTexts = append(errTexts, b.Err.Error())
		}
	}

	batchQ <- mk()
	collect(recv())

	mode.Store(1)
	batchQ <- mk()
	collect(recv())
	eventually(t, "a writer to retire", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.live) == 1
	})
	batchQ <- mk()
	collect(recv())
	eventually(t, "Stopped", func() bool { return stopped(p) })
	if se := p.StopReason(); se != nil {
		errTexts = append(errTexts, se.Error())
	}

	// A statement text the writer could have logged, and the bind values: the
	// sentinel key, and the 0x5A image bytes as %v of a byte slice would print
	// them (decimal, space separated) and as %x would (hex).
	d0 := &set.All()[0]
	text, _, err := destinations.StatementFor(d0, d0.FullMask())
	if err != nil {
		t.Fatal(err)
	}
	needles := map[string]string{
		"password":       secret,
		"dsn":            f.DSN(),
		"statement text": "MERGE INTO",
		"using clause":   "FROM dual",
		"full statement": text,
		"table name":     set.All()[0].Table,
		"row key":        fmt.Sprint(sentKey),
		"image as %v":    "90 90 90 90",
		"image as %x":    "5a5a5a5a",
	}
	logText := logBuf.String()
	if !strings.Contains(logText, "row rejected") {
		t.Fatalf("scan is vacuous: no rejected-row line in the captured log:\n%s", logText)
	}
	if bad := leakFindings(logText, needles); len(bad) != 0 {
		t.Errorf("log leaks %v:\n%s", bad, logText)
	}
	if bad := leakFindings(strings.Join(errTexts, "\n"), map[string]string{"password": secret, "dsn": f.DSN()}); len(bad) != 0 {
		t.Errorf("returned errors leak %v:\n%s", bad, strings.Join(errTexts, "\n"))
	}
	if len(errTexts) == 0 {
		t.Error("scan is vacuous: no batch Err was collected")
	}

	// Non-vacuity of the detector itself.
	var all []string
	for _, n := range needles {
		all = append(all, n)
	}
	if got := leakFindings(strings.Join(all, " | "), needles); len(got) != len(needles) {
		t.Errorf("detector found %d of %d needles in a line holding every one: %v", len(got), len(needles), got)
	}
	if got := leakFindings("x password="+secret+" y", needles); len(got) != 1 || got[0] != "password" {
		t.Errorf("detector found %v in a line that holds only the password", got)
	}
}

// TestDBIntegrationWriter is the OQ-W3 live spike (ruling D15). It is
// NON-GATING: it skips unless FEEDVALVE_ORACLE_DSN is set, and every
// observation is logged for the record rather than asserted, because the point
// is to learn what the real server and go-ora do. Only setup failures fail it.
//
//	FEEDVALVE_ORACLE_DSN='oracle://user:pass@host:1521/SVC' go test -run TestDBIntegrationWriter -v ./internal/writer/
//
// Items: t_commit; array RowsAffected; all-NULL NCHAR; TIMESTAMP round trip;
// ORA-01438 attribution on an array bind; transaction usable after a statement
// error.
func TestDBIntegrationWriter(t *testing.T) {
	dsn := os.Getenv("FEEDVALVE_ORACLE_DSN")
	if dsn == "" {
		t.Skip("FEEDVALVE_ORACLE_DSN not set; OQ-W3 live spike skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := oradb.OpenDriver(ctx, "oracle", dsn, oradb.DBSettings{
		Host: "spike", Port: 1521, Service: "spike", User: "spike",
		ConnectTimeoutSec: 15, QueryTimeoutSec: 60, ConnMaxLifetimeMinutes: 30,
		MaxOpenConns: 2, MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	s, err := db.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer s.Release()

	table := fmt.Sprintf("FV_SPIKE_%d", time.Now().UnixNano()%1000000000)
	if len(table) > 30 {
		table = table[:30]
	}
	err = s.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE "+table+" (k NUMBER(10) PRIMARY KEY, c NCHAR(4), ts TIMESTAMP, n NUMBER(3))")
		return err
	})
	if err != nil {
		t.Fatalf("create scratch table: %v", err)
	}
	defer s.ExecTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "DROP TABLE "+table)
		return err
	})

	merge := "MERGE INTO " + table + " t USING (SELECT :1 AS k, :2 AS c, :3 AS ts, :4 AS n FROM dual) s ON (t.k = s.k) " +
		"WHEN MATCHED THEN UPDATE SET t.c = s.c, t.ts = s.ts, t.n = s.n " +
		"WHEN NOT MATCHED THEN INSERT (k, c, ts, n) VALUES (s.k, s.c, s.ts, s.n)"
	ts := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)

	t.Run("t_commit and array RowsAffected", func(t *testing.T) {
		keys := []int64{1, 2, 3}
		cs := []sql.NullString{{}, {}, {}} // all NULL NCHAR
		tss := []sql.NullTime{{Time: ts, Valid: true}, {Time: ts, Valid: true}, {Time: ts, Valid: true}}
		ns := []sql.NullFloat64{{Float64: 1, Valid: true}, {Float64: 2, Valid: true}, {Float64: 3, Valid: true}}
		err := s.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			res, err := tx.ExecContext(ctx, merge, keys, cs, tss, ns)
			if err != nil {
				return err
			}
			n, rerr := res.RowsAffected()
			t.Logf("OQ-W3 array RowsAffected = %d (err %v), bound 3", n, rerr)
			return nil
		})
		if err != nil {
			t.Logf("array merge failed: %v", err)
		}
		start := time.Now()
		err = s.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, merge, []int64{4}, []sql.NullString{{}}, []sql.NullTime{{Time: ts, Valid: true}}, []sql.NullFloat64{{Float64: 4, Valid: true}})
			return err
		})
		t.Logf("OQ-W3 t_commit (one-row transaction, begin to commit) = %v err=%v", time.Since(start), err)
	})

	t.Run("NULL NCHAR and TIMESTAMP round trip", func(t *testing.T) {
		var c sql.NullString
		var got sql.NullTime
		err := s.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, "SELECT c, ts FROM "+table+" WHERE k = 1").Scan(&c, &got)
		})
		if err != nil {
			t.Logf("select failed: %v", err)
			return
		}
		t.Logf("OQ-W3 NCHAR NULL valid=%v; TIMESTAMP in=%v out=%v equal=%v", c.Valid, ts, got.Time, got.Time.Equal(ts))
	})

	t.Run("ORA-01438 attribution and transaction usable after a statement error", func(t *testing.T) {
		keys := []int64{10, 11, 12}
		cs := []sql.NullString{{}, {}, {}}
		tss := []sql.NullTime{{Time: ts, Valid: true}, {Time: ts, Valid: true}, {Time: ts, Valid: true}}
		ns := []sql.NullFloat64{{Float64: 1, Valid: true}, {Float64: 99999, Valid: true}, {Float64: 3, Valid: true}} // row 1 overflows NUMBER(3)
		err := s.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, merge, keys, cs, tss, ns)
			if err == nil {
				t.Logf("OQ-W3 array with an overflowing row: NO error")
				return nil
			}
			e, ok := db.Classify(err)
			t.Logf("OQ-W3 array failure: classified=%v class/code=%v; raw text: %v", ok, e, err)
			// The same transaction, after the statement error.
			_, err2 := tx.ExecContext(ctx, merge, []int64{20}, []sql.NullString{{}}, []sql.NullTime{{Time: ts, Valid: true}}, []sql.NullFloat64{{Float64: 5, Valid: true}})
			t.Logf("OQ-W3 transaction usable after a statement error: next exec err=%v", err2)
			return nil
		})
		t.Logf("transaction end err=%v", err)
	})
}

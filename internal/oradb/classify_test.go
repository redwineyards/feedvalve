package oradb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sijms/go-ora/v2/network"
)

func ora(code int) error { return network.NewOracleError(code) }

// TestClassifyTable pins V3 §8.2 against a hand-written table. The expected
// classes are typed out here, not read from codeClass, so a wrong entry in the
// implementation cannot agree with itself.
var classifyCases = []struct {
	code  int
	class ErrClass
}{
		{1, ErrRowData}, {1400, ErrRowData}, {1438, ErrRowData}, {1722, ErrRowData},
		{2290, ErrRowData}, {2291, ErrRowData}, {12899, ErrRowData},
		{28, ErrSession}, {1012, ErrSession}, {1033, ErrSession}, {1034, ErrSession},
		{1089, ErrSession}, {2396, ErrSession}, {2399, ErrSession}, {3113, ErrSession},
		{3114, ErrSession}, {3135, ErrSession}, {12170, ErrSession}, {12514, ErrSession},
		{12537, ErrSession}, {12541, ErrSession},
		{1653, ErrSpace}, {1654, ErrSpace}, {1688, ErrSpace}, {1691, ErrSpace},
		{60, ErrDeadlock}, {54, ErrDeadlock},
		{904, ErrStatement}, {936, ErrStatement}, {942, ErrStatement}, {917, ErrStatement},
		{920, ErrStatement}, {933, ErrStatement}, {947, ErrStatement}, {1031, ErrStatement},
		{24381, ErrUnknown}, // D6a
	{99999, ErrUnknown},
}

func TestClassifyTable(t *testing.T) {
	var r Redactor
	for _, c := range classifyCases {
		for name, err := range map[string]error{
			"bare":    ora(c.code),
			"wrapped": fmt.Errorf("exec: %w", fmt.Errorf("batch: %w", ora(c.code))),
		} {
			e, ok := r.Classify(err)
			if !ok || e.Class != c.class || e.Code != c.code {
				t.Errorf("ORA-%d %s: got (%v, ok=%v), want class %v", c.code, name, e, ok, c.class)
			}
		}
	}
}

// Every code in the production map must appear in the hand-written table as a
// non-Unknown row, so a stray extra entry cannot slip in unlisted.
func TestCodeClassHasNoUnlistedCodes(t *testing.T) {
	n := 0
	for _, c := range classifyCases {
		if c.class != ErrUnknown {
			n++
		}
	}
	if n != len(codeClass) {
		t.Errorf("table has %d classified codes, codeClass has %d", n, len(codeClass))
	}
}

// Sentinel priority: Canceled beats everything, Deadline/BadConn beat the ORA
// table. errors.Join carries both so the order of checks is what decides.
func TestClassifySentinelPriority(t *testing.T) {
	var r Redactor
	if e, ok := r.Classify(errors.Join(ora(942), context.Canceled)); ok || e != nil {
		t.Errorf("Canceled+ORA-942: got (%v, %v), want not-ok", e, ok)
	}
	for name, err := range map[string]error{
		"deadline+942": errors.Join(ora(942), context.DeadlineExceeded),
		"badconn+1":    errors.Join(ora(1), driver.ErrBadConn),
	} {
		e, ok := r.Classify(err)
		if !ok || e.Class != ErrSession {
			t.Errorf("%s: got (%v, %v), want ErrSession", name, e, ok)
		}
	}
	// The ORA code is preserved on the sentinel path.
	if e, _ := r.Classify(errors.Join(ora(942), context.DeadlineExceeded)); e.Code != 942 {
		t.Errorf("Code = %d, want 942", e.Code)
	}
	if e, _ := r.Classify(context.DeadlineExceeded); e.Code != 0 {
		t.Errorf("bare deadline Code = %d, want 0", e.Code)
	}
}

func TestClassifySentinels(t *testing.T) {
	var r Redactor
	for name, err := range map[string]error{
		"canceled":         context.Canceled,
		"canceled wrapped": fmt.Errorf("exec: %w", context.Canceled),
		"nil":              nil,
	} {
		if e, ok := r.Classify(err); ok || e != nil {
			t.Errorf("%s: got (%v, %v), want not-ok", name, e, ok)
		}
	}
	for name, err := range map[string]error{
		"deadline":      context.DeadlineExceeded,
		"deadline wrap": fmt.Errorf("exec: %w", context.DeadlineExceeded),
		"badconn":       driver.ErrBadConn,
		"badconn wrap":  fmt.Errorf("exec: %w", driver.ErrBadConn),
	} {
		if e, ok := r.Classify(err); !ok || e.Class != ErrSession {
			t.Errorf("%s: got (%v, %v), want ErrSession", name, e, ok)
		}
	}
}

// A message that merely says ORA-00942 is not an Oracle error: classifying by
// text is forbidden because the driver renders code 60 as "ORA-60".
func TestClassifyNeverMatchesText(t *testing.T) {
	var r Redactor
	for _, err := range []error{errors.New("ORA-00942"), errors.New("ORA-03113: end-of-file")} {
		e, ok := r.Classify(err)
		if !ok || e.Class != ErrUnknown || e.Code != 0 {
			t.Errorf("%q: got (%v, %v), want Unknown with no code", err, e, ok)
		}
	}
}

// leakDriver quotes the DSN it was opened with in the error it fails with, as
// a real driver can. failAt picks the phase: "connect" fails Open's ping,
// "query" connects and then fails Check's round trip.
//
// last records the raw error text it emitted, so the test can prove the
// unredacted error really did contain the password.
type leakDriver struct {
	failAt string
	last   string
}

func (d *leakDriver) Open(name string) (driver.Conn, error) {
	if d.failAt == "connect" {
		d.last = fmt.Sprintf("dial failed for %s", name)
		return nil, errors.New(d.last)
	}
	return leakConn{name, d}, nil
}

type leakConn struct {
	dsn string
	d   *leakDriver
}

func (c leakConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c leakConn) Close() error                        { return nil }
func (c leakConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (c leakConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	c.d.last = fmt.Sprintf("query failed on %s", c.dsn)
	return nil, errors.New(c.d.last)
}

var (
	leakConnect = &leakDriver{failAt: "connect"}
	leakQuery   = &leakDriver{failAt: "query"}
)

func init() {
	sql.Register("oradb-leak-connect", leakConnect)
	sql.Register("oradb-leak-query", leakQuery)
}

// assertNoSecret renders err every way the codebase does and fails if the
// given password appears in any rendering.
func assertNoSecret(t *testing.T, label, secret string, err error) {
	t.Helper()
	forms := []string{err.Error(), fmt.Sprintf("%v", err), fmt.Sprintf("%+v", err), fmt.Sprintf("%s", err),
		fmt.Sprint(fmt.Errorf("outer: %w", err))}
	for u := errors.Unwrap(err); u != nil; u = errors.Unwrap(u) {
		forms = append(forms, u.Error(), fmt.Sprintf("%+v", u))
	}
	for _, f := range forms {
		if strings.Contains(f, secret) {
			t.Errorf("%s leaks the password: %q", label, f)
		}
	}
}

// TestG_SEC_2_1_DriverErrorsNeverLeakThePassword is gate G-SEC-2.
//
// SETUP: a driver whose errors quote the full DSN back, and, separately, a
// bare wrapped error doing the same.
// STIMULUS: classify it; run it through Open and Check's failure paths.
// ASSERTION: the password appears in no rendering: Error(), Unwrap chain,
// %w, %v, %+v.
// WOULD HAVE CAUGHT: Check once wrapped the round-trip error without
// redacting, and a Classify that kept the raw error in Err.
// Non-vacuity: the same error unredacted DOES contain the password.
func TestG_SEC_2_1_DriverErrorsNeverLeakThePassword(t *testing.T) {
	s := sample()
	t.Setenv(PasswordEnv, probe)
	dsn := s.dsn(probe)

	raw := fmt.Errorf("exec: %w", fmt.Errorf("driver: connect %s failed", dsn))
	if !strings.Contains(raw.Error(), probe) {
		t.Fatal("non-vacuity: the raw error does not contain the password, test proves nothing")
	}

	e, ok := NewRedactor(s, probe).Classify(raw)
	if !ok {
		t.Fatal("Classify returned not-ok")
	}
	assertNoSecret(t, "Classify", probe, e)
	assertNoSecret(t, "Classify.Err", probe, e.Err)

	// Password quoted on its own: raw, QueryEscape and PathEscape spellings
	// (go-ora's BuildUrl uses PathEscape: "p@ss%2Fword:1").
	odd := "p@ss/word:1"
	e2, _ := NewRedactor(sample(), odd).Classify(errors.New(
		"bad credential p%40ss%2Fword%3A1 and p@ss/word:1 and p@ss%2Fword:1"))
	for _, form := range []string{odd, "p%40ss", "p@ss%2Fword"} {
		if strings.Contains(e2.Error(), form) {
			t.Errorf("escaped password form %q leaks: %q", form, e2)
		}
	}

	// Open and Check, for a plain and an awkward password. The leak drivers
	// record their raw error; it must contain the DSN (non-vacuity) and the
	// returned error must not contain the password.
	for _, pw := range []string{probe, odd} {
		t.Setenv(PasswordEnv, pw)

		_, err := open(context.Background(), "oradb-leak-connect", s)
		if err == nil || !strings.Contains(err.Error(), "dial failed") {
			t.Fatalf("Open: want dial error, got %v", err)
		}
		if !strings.Contains(leakConnect.last, s.dsn(pw)) {
			t.Fatalf("non-vacuity: raw Open error does not quote the DSN: %q", leakConnect.last)
		}
		assertNoSecret(t, "Open", pw, err)
		if !strings.Contains(err.Error(), s.Describe()) {
			t.Errorf("Open: want Describe() in redacted text, got %q", err)
		}

		_, err = check(context.Background(), "oradb-leak-query", s)
		if err == nil || !strings.Contains(err.Error(), "query failed") {
			t.Fatalf("Check: want round-trip error, got %v", err)
		}
		if !strings.Contains(leakQuery.last, s.dsn(pw)) {
			t.Fatalf("non-vacuity: raw Check error does not quote the DSN: %q", leakQuery.last)
		}
		assertNoSecret(t, "Check", pw, err)
	}
}

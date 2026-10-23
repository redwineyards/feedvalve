package oradb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/oradb/oratest"
)

// fakeDB opens an oratest fake through OpenDriver with the given settings.
func fakeDB(t *testing.T, s DBSettings) (*DB, *oratest.Fake) {
	t.Helper()
	f := oratest.New()
	t.Cleanup(f.Close)
	d, err := OpenDriver(context.Background(), oratest.DriverName, f.DSN(), s)
	if err != nil {
		t.Fatalf("OpenDriver: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, f
}

func poolSettings() DBSettings {
	s := sample()
	s.MaxOpenConns, s.MaxIdleConns = 3, 2
	return s
}

func TestOpenDriverAppliesPoolSettingsAndPings(t *testing.T) {
	t.Setenv(PasswordEnv, "")
	d, f := fakeDB(t, poolSettings())
	// Hand-written expectations: MaxOpenConns 3 from the settings above, and
	// OpenDriver's own ping is the only operation so far.
	if got := d.Stats().MaxOpenConnections; got != 3 {
		t.Errorf("MaxOpenConnections = %d, want 3", got)
	}
	if got := f.Counts().Pings; got != 1 {
		t.Errorf("pings after open = %d, want 1", got)
	}

	// MaxIdleConns: hold three sessions at once (MaxOpen 3 allows it), release
	// all, and the pool keeps exactly MaxIdleConns idle. database/sql's own
	// default is 2, so the second half uses 1 to prove the setting is applied.
	hold := func(d *DB, n int) {
		var ss []*Session
		for i := 0; i < n; i++ {
			s, err := d.Acquire(context.Background())
			if err != nil {
				t.Fatalf("Acquire %d: %v", i, err)
			}
			ss = append(ss, s)
		}
		for _, s := range ss {
			s.Release()
		}
	}
	hold(d, 3)
	if got := d.Stats().Idle; got != 2 {
		t.Errorf("Idle after releasing 3 with MaxIdleConns 2 = %d, want 2", got)
	}
	s1 := poolSettings()
	s1.MaxIdleConns = 1
	d1, _ := fakeDB(t, s1)
	hold(d1, 3)
	if got := d1.Stats().Idle; got != 1 {
		t.Errorf("Idle after releasing 3 with MaxIdleConns 1 = %d, want 1", got)
	}
}

func TestPingIsBoundedByConnectTimeout(t *testing.T) {
	s := poolSettings()
	s.ConnectTimeoutSec = 1
	d, f := fakeDB(t, s)
	f.SetLatency(30 * time.Second)

	start := time.Now()
	err := d.PingContext(context.Background())
	if err == nil {
		t.Fatal("ping against a 30s latency succeeded under a 1s ConnectTimeout")
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Errorf("ping took %v; ConnectTimeout (1s) did not bound it", el)
	}
	// The failure is the deadline, which classifies as a session problem; an
	// early unrelated failure would not.
	if e, ok := d.Classify(err); !ok || e.Class != ErrSession {
		t.Errorf("Classify(%v) = %+v, %v; want Session (deadline)", err, e, ok)
	}
}

func TestPingFailureAfterOpenIsRedacted(t *testing.T) {
	t.Setenv(PasswordEnv, probe)
	d, f := fakeDB(t, poolSettings())
	raw := "dial failed for user/" + probe + "@host"
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpPing {
			return oratest.Result{Err: errors.New(raw)}
		}
		return oratest.Result{}
	})
	err := d.PingContext(context.Background())
	if err == nil {
		t.Fatal("PingContext succeeded though ping fails")
	}
	assertNoSecret(t, "PingContext", probe, err)
	if !strings.Contains(err.Error(), "dial failed") {
		t.Errorf("redaction lost the cause: %q", err)
	}
}

func TestOpenDriverFailureIsRedacted(t *testing.T) {
	t.Setenv(PasswordEnv, probe)
	f := oratest.New()
	defer f.Close()
	raw := "dial failed for user/" + probe + "@host"
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpPing {
			return oratest.Result{Err: errors.New(raw)}
		}
		return oratest.Result{}
	})
	if !strings.Contains(raw, probe) {
		t.Fatal("non-vacuity: raw error lacks the password")
	}
	_, err := OpenDriver(context.Background(), oratest.DriverName, f.DSN(), poolSettings())
	if err == nil {
		t.Fatal("OpenDriver succeeded though ping fails")
	}
	assertNoSecret(t, "OpenDriver", probe, err)
	if !strings.Contains(err.Error(), "dial failed") {
		t.Errorf("redaction lost the cause: %q", err)
	}
}

// TestG_SEC_1_2_TheWrappersPrintNoSecret re-asserts G-SEC-1 for the types that
// hold the DSN and password.
//
// SETUP: a DB and a Redactor holding a known password.
// STIMULUS: every fmt verb on value and pointer.
// ASSERTION: the password appears in none.
// WOULD HAVE CAUGHT: dropping String/GoString -- %+v then prints the fields.
// Non-vacuity: the same types with their methods stripped DO leak.
func TestG_SEC_1_2_TheWrappersPrintNoSecret(t *testing.T) {
	t.Setenv(PasswordEnv, probe)
	d, _ := fakeDB(t, poolSettings())
	r := NewRedactor(sample(), probe)

	// A defined type over the same underlying struct carries none of the
	// methods, so it shows what the verbs would print without String/GoString.
	type bareDB DB
	type bareRedactor Redactor
	if out := fmt.Sprintf("%+v", bareRedactor(r)); !strings.Contains(out, probe) {
		t.Fatal("non-vacuity: a method-less Redactor does not print its password")
	}
	if out := fmt.Sprintf("%+v", bareDB(*d)); !strings.Contains(out, probe) {
		t.Fatal("non-vacuity: a method-less DB does not print the password of the redactor OpenDriver built")
	}
	built := d.red // the Redactor OpenDriver itself built
	for _, v := range []any{d, *d, &built, built, &r, r} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			if out := fmt.Sprintf(verb, v); strings.Contains(out, probe) {
				t.Errorf("%s of %T leaks the password: %q", verb, v, out)
			}
		}
	}
}

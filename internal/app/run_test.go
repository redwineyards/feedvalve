package app_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"feedvalve/internal/app"
	"feedvalve/internal/oradb"
)

func write(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func run(t *testing.T, opts app.Options) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := app.Run(context.Background(), opts, &out)
	return out.String(), err
}

// TestSelfCheckNeedsNeitherPasswordNorNetwork is the requirement, not a
// convenience. -selfcheck exists to be run from a build host before
// deployment, and one that quietly required credentials would be useless
// there.
func TestSelfCheckNeedsNeitherPasswordNorNetwork(t *testing.T) {
	t.Setenv(oradb.PasswordEnv, "")

	out, err := run(t, app.Options{SelfCheck: true})
	if err != nil {
		t.Fatalf("selfcheck with no config and no password must pass: %v", err)
	}
	if app.ExitCode(err) != app.ExitOK {
		t.Errorf("exit code = %d, want %d", app.ExitCode(err), app.ExitOK)
	}

	for _, want := range []string{
		"PASSED",
		"MD_TOKEN_QUOTE", "181/256",
		"MD_SECURITY_MASTER", "37/256",
		"MD_MARKET_STATUS", "142/256",
		// The rules that cannot run yet are named by identifier, so a
		// reader can look them up in Vol 4 section 2.2 rather than guess
		// what "some checks were skipped" means.
		"NOT checked -- V-T1",
		"NOT checked -- V-T6",
		"NOT checked -- V-X7",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("selfcheck output omits %q:\n%s", want, out)
		}
	}
}

// TestSelfCheckFailureExitsThree keeps the code distinct from a startup
// failure, which is what lets CI branch on "the configuration is wrong"
// without also catching "the database was down".
func TestSelfCheckFailureExitsThree(t *testing.T) {
	path := write(t, "bad.json", `{"udp":{"stallBudgetMs":500},"stats":{"logLevel":"trace"}}`)

	out, err := run(t, app.Options{SelfCheck: true, ConfigPath: path})
	if err == nil {
		t.Fatal("a broken configuration must fail selfcheck")
	}
	if got := app.ExitCode(err); got != app.ExitSelfCheckFailed {
		t.Errorf("exit code = %d, want %d", got, app.ExitSelfCheckFailed)
	}
	if !app.AlreadyReported(err) {
		t.Error("the failure should be marked reported, or main prints it a second time")
	}
	if !strings.Contains(out, "FAILED") {
		t.Errorf("output does not say FAILED:\n%s", out)
	}
	// Both problems, not just the first.
	for _, want := range []string{"stallBudgetMs", "logLevel"} {
		if !strings.Contains(out, want) {
			t.Errorf("output omits %q; all problems should be reported at once:\n%s", want, out)
		}
	}
}

func TestRejectedCombinations(t *testing.T) {
	t.Run("both one-shot modes", func(t *testing.T) {
		_, err := run(t, app.Options{SelfCheck: true, DBCheck: true})
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Errorf("got %v, want a mutual-exclusion error", err)
		}
	})

	t.Run("dbcheck with no db configured", func(t *testing.T) {
		_, err := run(t, app.Options{DBCheck: true})
		if err == nil || !strings.Contains(err.Error(), "V-DB1") {
			t.Errorf("got %v, want rule V-DB1 to reject it", err)
		}
		if got := app.ExitCode(err); got != app.ExitStartup {
			t.Errorf("exit code = %d, want %d", got, app.ExitStartup)
		}
	})

	t.Run("missing config file", func(t *testing.T) {
		_, err := run(t, app.Options{ConfigPath: "no-such-file.json"})
		if err == nil {
			t.Fatal("a missing config file must fail")
		}
		if got := app.ExitCode(err); got != app.ExitStartup {
			t.Errorf("exit code = %d, want %d", got, app.ExitStartup)
		}
	})
}

func TestDBCheckRequiresThePassword(t *testing.T) {
	t.Setenv(oradb.PasswordEnv, "")
	path := write(t, "db.json", `{"db":{"host":"db.invalid","service":"S","user":"U"}}`)

	_, err := run(t, app.Options{DBCheck: true, ConfigPath: path})
	if !errors.Is(err, oradb.ErrNoPassword) {
		t.Errorf("got %v, want ErrNoPassword", err)
	}
	if got := app.ExitCode(err); got != app.ExitStartup {
		t.Errorf("exit code = %d, want %d", got, app.ExitStartup)
	}
}

// TestDBCheckFailureDoesNotLeakThePassword is the test this whole path exists
// to satisfy.
//
// The connection error comes from the driver, which has the DSN -- password
// included -- and quotes connection details back in its messages. On this
// host operational logs are append-only and long-lived, so one leak is
// permanent. The failure is exercised against an unresolvable host so the
// driver really does produce an error rather than the test asserting over a
// path that never ran.
func TestDBCheckFailureDoesNotLeakThePassword(t *testing.T) {
	const probe = "hunter2-UNIQUE-TOKEN"
	t.Setenv(oradb.PasswordEnv, probe)
	path := write(t, "db.json",
		`{"db":{"host":"db.invalid.example","port":1521,"service":"SVC","user":"feeduser"}}`)

	out, err := run(t, app.Options{DBCheck: true, ConfigPath: path})
	if err == nil {
		t.Fatal("connecting to an unresolvable host should fail")
	}

	for name, text := range map[string]string{
		"stdout":        out,
		"error":         err.Error(),
		"wrapped chain": errors.Unwrap(err).Error(),
	} {
		if strings.Contains(text, probe) {
			t.Errorf("the password reached %s: %s", name, text)
		}
	}

	// And the message must still identify the target, or it is useless.
	if !strings.Contains(err.Error(), "feeduser") || !strings.Contains(err.Error(), "db.invalid.example") {
		t.Errorf("error does not identify the target: %v", err)
	}
}

// TestRunModeRequiresADatabase is rule V-DB1.
//
// poc no-ops the write when no database is configured, which is right for a
// proof-of-concept and wrong for a relay whose only purpose is to persist: it
// would start, look healthy, and store nothing. Decoder-only operation is
// -selfcheck, never an accident.
func TestRunModeRequiresADatabase(t *testing.T) {
	_, err := run(t, app.Options{})
	if err == nil {
		t.Fatal("the relay must not start with nowhere to persist to")
	}
	if !strings.Contains(err.Error(), "V-DB1") {
		t.Errorf("want rule V-DB1 to reject it: %v", err)
	}
	if got := app.ExitCode(err); got != app.ExitStartup {
		t.Errorf("exit code = %d, want %d", got, app.ExitStartup)
	}
}

// TestDefaultModeFailsStartupWithoutACredential replaces a test that used to
// assert the relay said "not built yet". P1-09 built it, so the honest
// assertion changed: with a db object configured and no FEEDVALVE_DB_PASSWORD,
// startup must stop at step 11 with exit 1.
//
// That it stops THERE rather than later is the property worth pinning. sql.Open
// does not connect, so a process that deferred the credential check would bind
// the socket, start draining the feed, and fail on the first write at 09:00:03
// instead of refusing to start.
func TestDefaultModeFailsStartupWithoutACredential(t *testing.T) {
	path := write(t, "db.json", `{"db":{"host":"h","service":"s","user":"u"}}`)

	_, err := run(t, app.Options{ConfigPath: path})
	if err == nil {
		t.Fatal("startup succeeded with no database credential")
	}
	if got := app.ExitCode(err); got != app.ExitStartup {
		t.Errorf("exit code = %d, want %d", got, app.ExitStartup)
	}
	if !strings.Contains(err.Error(), "FEEDVALVE_DB_PASSWORD") {
		t.Errorf("message should name the missing credential: %v", err)
	}
}

func TestExitCodeMapping(t *testing.T) {
	if got := app.ExitCode(nil); got != app.ExitOK {
		t.Errorf("nil -> %d, want %d", got, app.ExitOK)
	}
	// An error with no code attached is a startup failure, not a silent 0.
	if got := app.ExitCode(errors.New("plain")); got != app.ExitStartup {
		t.Errorf("plain error -> %d, want %d", got, app.ExitStartup)
	}
	wrapped := &app.ExitError{Code: app.ExitTerminal, Err: errors.New("x")}
	if got := app.ExitCode(wrapped); got != app.ExitTerminal {
		t.Errorf("ExitError -> %d, want %d", got, app.ExitTerminal)
	}

	// 2 must stay unused by design, so a panic exit can never be mistaken
	// for a designed condition.
	if app.ExitPanic != 2 {
		t.Errorf("ExitPanic = %d, want 2", app.ExitPanic)
	}
}

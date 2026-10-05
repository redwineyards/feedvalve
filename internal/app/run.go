package app

import (
	"context"
	"errors"
	"fmt"
	"io"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/oradb"
)

// Exit codes, from Vol 0's sibling Vol 4 section 1.2.
//
// The supervisor restarts on non-zero, so a requested stop must be
// distinguishable from a failure or every planned shutdown becomes a restart.
// Codes 1, 2 and 4 all mean "restart" and differ only diagnostically -- which
// is the point: echo $? at 03:00 answers "did it fail to start, did it panic,
// or did it hit a terminal condition?" before anybody opens a log.
const (
	// ExitOK is a completed shutdown, or a one-shot mode that passed.
	ExitOK = 0
	// ExitStartup is a startup failure: invalid configuration, no password,
	// Oracle unreachable at open, socket bind refused.
	ExitStartup = 1
	// ExitPanic is RESERVED. The Go runtime exits 2 on an unrecovered panic,
	// and it is deliberately not claimed for a designed condition, so a
	// panic exit can never be mistaken for one.
	ExitPanic = 2
	// ExitSelfCheckFailed is -selfcheck finding a broken rule. Separate from
	// ExitStartup so CI can branch on it.
	ExitSelfCheckFailed = 3
	// ExitTerminal is a terminal runtime condition after a successful start.
	ExitTerminal = 4
)

// ExitError carries the code a failure should produce. main maps it; nothing
// in this tree calls os.Exit, because os.Exit skips every deferred cleanup.
type ExitError struct {
	Code int
	Err  error

	// Reported means the failure has already been written to the user in a
	// mode's own format, so main must not print it a second time. Without
	// it, -selfcheck on a broken file prints the same multi-line rejection
	// twice, which reads like two separate problems.
	Reported bool
}

func (e *ExitError) Error() string { return e.Err.Error() }
func (e *ExitError) Unwrap() error { return e.Err }

func exitf(code int, format string, a ...any) error {
	return &ExitError{Code: code, Err: fmt.Errorf(format, a...)}
}

// ExitCode maps an error from Run to a process exit code. A nil error is
// ExitOK; an error carrying no code is a startup failure.
func ExitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	var ee *ExitError
	if errors.As(err, &ee) {
		return ee.Code
	}
	return ExitStartup
}

// AlreadyReported reports whether Run has already shown this failure to the
// user, so main can avoid printing it twice.
func AlreadyReported(err error) bool {
	var ee *ExitError
	return errors.As(err, &ee) && ee.Reported
}

// Options are the parsed command-line flags.
//
// There is no password field and there never will be one: a flag is visible
// in ps to every user on the host. The password comes only from
// FEEDVALVE_DB_PASSWORD.
type Options struct {
	// ConfigPath is -config. Empty means built-in defaults, which is a
	// valid configuration.
	ConfigPath string

	// SelfCheck is -selfcheck: validate everything that can be validated
	// without connecting or binding, then exit.
	SelfCheck bool

	// DBCheck is -dbcheck: connect, prove a round trip, report the server
	// version, then exit. Read-only.
	DBCheck bool
}

// Run is the whole process, with main holding nothing but flag parsing and
// the exit code.
//
// The split is not stylistic. A fatal logger calls os.Exit, which skips every
// deferred cleanup, so the Fatalf-on-error style could never have closed the
// database or drained the queue. Startup failures return errors instead, and
// every defer in this call chain runs before the process ends.
func Run(ctx context.Context, opts Options, out io.Writer) error {
	if opts.SelfCheck && opts.DBCheck {
		return exitf(ExitStartup, "-selfcheck and -dbcheck are mutually exclusive: "+
			"one deliberately does not connect and the other exists only to connect")
	}

	mode := config.ModeRun
	switch {
	case opts.SelfCheck:
		mode = config.ModeSelfCheck
	case opts.DBCheck:
		mode = config.ModeDBCheck
	}

	cfg, dests, err := loadAll(opts.ConfigPath, mode)
	if err != nil {
		// A broken configuration under -selfcheck is the ANSWER, not a
		// startup failure, and CI branches on the difference.
		if opts.SelfCheck {
			fmt.Fprintf(out, "selfcheck: FAILED\n%v\n", err)
			return &ExitError{Code: ExitSelfCheckFailed, Err: err, Reported: true}
		}
		return &ExitError{Code: ExitStartup, Err: err}
	}

	switch {
	case opts.SelfCheck:
		return selfCheck(cfg, dests, out)
	case opts.DBCheck:
		return dbCheck(ctx, cfg, out)
	}

	// The relay itself. P1-09 builds the lifecycle -- startup ordering with
	// the socket bound last, signal handling, and the ordered drain -- and
	// the phases after it supply the goroutines to order.
	return exitf(ExitStartup, "the relay is not built yet: P1-01 to P1-05 deliver the "+
		"package tree, the value types, the field map and the configuration. "+
		"Use -selfcheck or -dbcheck; the receive path arrives in Phase 2")
}

// loadAll resolves the configuration and the field map, then validates them
// TOGETHER.
//
// Together, because the rules that matter most span both. ASM-15 compares a
// destination's row key against its contributors' shard keys; ASM-16a sums a
// transcode's contributed column widths, which only the field map knows.
// Validating each half alone would pass a pair that cannot work.
func loadAll(path string, mode config.Mode) (*config.Config, *destinations.Set, error) {
	var (
		cfg *config.Config
		err error
	)
	// Unvalidated on purpose: validating the configuration here and the
	// schema a moment later would report only the first set of problems,
	// and the operator would fix one layer per run. ValidateAll below is
	// the single gate, so a range error and an ASM-15 violation come back
	// in one message.
	if path == "" {
		cfg = config.Defaults()
	} else if cfg, err = config.LoadUnvalidated(path); err != nil {
		return nil, nil, err
	}

	var dests *destinations.Set
	if f := cfg.Runtime.DestinationsFile; f != "" {
		dests, err = destinations.Load(f)
	} else {
		dests, err = destinations.Default()
	}
	if err != nil {
		return nil, nil, err
	}

	if err := config.ValidateAll(cfg, dests, nil, mode); err != nil {
		return nil, nil, err
	}
	return cfg, dests, nil
}

// selfCheck validates everything checkable without a network or a password,
// and says what it checked.
//
// Running with NEITHER FEEDVALVE_DB_PASSWORD nor network access is the
// requirement, not an accident: it has to be usable from a build host as a
// pre-deployment check, and one that quietly required credentials would be
// useless there.
func selfCheck(cfg *config.Config, dests *destinations.Set, out io.Writer) error {
	fmt.Fprintf(out, "selfcheck: configuration OK (%d transcodes declared)\n", len(cfg.Transcodes))

	for _, d := range dests.All() {
		fmt.Fprintf(out, "selfcheck: destination %d %-20s table=%-20s image=%3d/%d bytes  "+
			"columns=%2d  contributors=%d\n",
			d.ID, d.Name, d.Table, d.ImageLen, model.ImageSize, len(d.Columns), d.NContributors)
	}

	fmt.Fprintf(out, "selfcheck: shards=%d datagramQueue=%d window=%dms writers=%d\n",
		cfg.Runtime.Shards, cfg.Runtime.DatagramQueueCapacity,
		cfg.Flush.WindowMs, cfg.Writers.Count)

	if cfg.HasDB {
		fmt.Fprintf(out, "selfcheck: database %s (password %s)\n",
			cfg.DB.Describe(), passwordState())
	} else {
		fmt.Fprintln(out, "selfcheck: no database configured")
	}

	// Stated rather than implied, so nobody reads a pass as more than it
	// is. The list comes from the validator itself rather than from a
	// string here, so it shrinks automatically as rules become checkable.
	for rule, why := range config.Unchecked() {
		fmt.Fprintf(out, "selfcheck: NOT checked -- %s: %s\n", rule, why)
	}
	fmt.Fprintln(out, "selfcheck: NOT checked -- V-X7: db.connMaxLifetimeMinutes against the "+
		"Oracle profile's IDLE_TIME. Un-checkable from here by nature; the observable "+
		"is a recurring ORA-03113 on first reuse of a session the server already closed")

	fmt.Fprintln(out, "selfcheck: PASSED")
	return nil
}

func passwordState() string {
	if oradb.HasPassword() {
		return "set"
	}
	return "not set -- required by -dbcheck and at startup, not by -selfcheck"
}

// dbCheck connects, proves a round trip and reports the server version. It
// writes nothing to the database.
func dbCheck(ctx context.Context, cfg *config.Config, out io.Writer) error {
	// No "is a db configured?" check here: rule V-DB1 in ModeDBCheck has
	// already rejected that case during load, and a second check would be
	// a second place for the answer to change.
	fmt.Fprintf(out, "dbcheck: connecting to %s\n", cfg.DB.Describe())

	version, err := oradb.Check(ctx, cfg.DB)
	if err != nil {
		return &ExitError{Code: ExitStartup, Err: err}
	}

	fmt.Fprintf(out, "dbcheck: round trip OK\ndbcheck: server %s\ndbcheck: PASSED\n", version)
	return nil
}

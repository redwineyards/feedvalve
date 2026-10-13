package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
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

	// Stderr is where the loggers and the stats emission go. Vol 4 section
	// 6.7: "the entire observability surface is stderr". Injected rather than
	// read from os.Stderr so the whole lifecycle is testable in-process, which
	// is what makes the startup-ordering and drain gates possible; nil means
	// os.Stderr.
	Stderr io.Writer

	// Version is the build stamp from main.version, reported on the startup
	// line so a running process can be matched to a commit. Empty is fine --
	// it reads as "unstamped", which is itself the answer to "which build is
	// this?"
	Version string
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

	return relay(ctx, opts, cfg, dests)
}

// relay is startup steps 6 to 17 and the ordered shutdown: the lifecycle.
//
// The step numbers are Vol 4 section 1.4's, and the ORDER is the contract --
// consumers before producers, every allocation before the socket, the socket
// last. Two deviations from that section's numbering are deliberate and
// marked below.
func relay(ctx context.Context, opts Options, cfg *config.Config, dests *destinations.Set) error {
	stderr := opts.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}

	// Step 9 BEFORE step 6, which is the first deviation. Section 1.4 orders
	// the loggers at 6 and the registry at 9; nothing in 6 needs the registry,
	// but the logger wants it, because log_events_suppressed{class} is what
	// makes the rate limiter visible. Validation -- the reason step 6 is not
	// earlier still -- already happened at step 4, so moving the registry
	// ahead of the loggers costs nothing and means there is exactly one
	// AppLogger for the life of the process instead of an unattached one
	// replaced moments later.
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg)
	reg.SetIntervalSec(cfg.Stats.IntervalSec)
	// Freeze ends resolution. Every accessor panics afterwards (gate G-OBS-1),
	// which is what enforces Vol 0 section 8.4's MUST that instruments are
	// resolved once at startup and never from a hot path.
	reg.Freeze()

	// Step 6: the loggers. appLogger to stderr, debugLogger gated per
	// transcode. There is NO packet logger: poc's full-payload logger wrote
	// several times the wire rate to a world-readable append-only file, and a
	// secret that lands there is there permanently.
	lg := obs.NewAppLogger(stderr, obs.ParseLevel(cfg.Stats.LogLevel),
		cfg.Stats.EventRatePerMin, reg)
	dbg := obs.NewDebugLogger(stderr, cfg.Debug.TraceTranscodes, cfg.Debug.HexDump)

	// Step 7: the effective configuration, as one INFO line. The single most
	// useful artifact in a postmortem, and safe to dump wholesale because
	// Config has no password field at any depth (Vol 0 section 7.5) -- the
	// password comes only from the environment and never enters Config.
	lg.Infof("starting feedvalve version=%s goos=%s goarch=%s", versionOf(opts),
		runtime.GOOS, runtime.GOARCH)
	lg.Infof("starting with the effective configuration: %+v", cfg)

	// Step 8: GOMAXPROCS, BEFORE the first goroutine, so every goroutine is
	// scheduled under the intended parallelism.
	//
	// NEVER runtime.NumCPU: the LPAR is capped at entitled capacity 4 while
	// the runtime may observe 32 (ADR-009), and sizing anything from a
	// detected CPU count would silently over-commit the partition. Gate
	// G-BLD-3 scans for that call anywhere in the tree.
	runtime.GOMAXPROCS(cfg.Runtime.GOMAXPROCS)

	// Step 11: the database. sql.Open does not connect, so Open pings under
	// db.connectTimeoutSec -- turning a bad host, a wrong service name or a
	// wrong credential into a startup failure rather than a first-write
	// failure at 09:00:03. The password is read here and never enters Config.
	//
	// The deferred Close is registered HERE, at open time, and therefore runs
	// after the drain in Graph.Run's body. That ordering is the whole reason
	// the drain is not itself a defer (section 1.1).
	var db *sql.DB
	if cfg.HasDB {
		var err error
		db, err = openDB(ctx, cfg.DB)
		if err != nil {
			return &ExitError{Code: ExitStartup, Err: err}
		}
		defer func() {
			if err := db.Close(); err != nil {
				lg.Errorf(obsClassStartup, "closing the database: %v", err)
			}
		}()
		lg.Infof("database connected %s", cfg.DB.Describe())
	}

	// Steps 10 and 12: the derived state and the pre-allocation.
	g, err := Build(Deps{
		Cfg: cfg, Store: storeOf(cfg), DB: db, Reg: reg,
		Log: lg, Debug: dbg, Dests: dests, Stderr: stderr, Inst: inst,
	})
	if err != nil {
		return err
	}

	// Steps 15 to 17, then the drain.
	return g.Run(ctx)
}

// versionOf reports the build stamp, or "unstamped" when nothing set it.
func versionOf(opts Options) string {
	if opts.Version == "" {
		return "unstamped"
	}
	return opts.Version
}

// openDB is oradb.Open behind a variable, so gate G-LIFE-3 -- "OpenDB
// succeeds; the socket bind is rigged to fail; run returns a non-nil error AND
// db.Close() was called" -- can be exercised without an Oracle instance. Tests
// replace it; nothing else may.
//
// The seam is here rather than in oradb because what G-LIFE-3 tests is the
// LIFECYCLE's ordering: that the deferred Close registered at step 11 still
// runs when step 16 fails. oradb's own round trip is covered by its stub-driver
// tests and by -dbcheck.
var openDB = oradb.Open

// storeOf publishes a configuration into a Store so the generation gauge and
// the Phase 5 reload path have one to swap.
//
// The second deviation from section 1.4, and a small one: the section has the
// Store existing from step 3. config.Store's publish is unexported, so the
// only way in is Swap, which is also where validation against the previous
// generation lives. Phase 5 owns the reload; this just establishes generation 1
// so config_generation reads 1 rather than 0 on a process that never reloaded.
func storeOf(cfg *config.Config) *config.Store {
	var s config.Store
	if _, err := s.Swap(cfg); err != nil {
		// Unreachable: cfg has already passed the same rule set at step 4.
		// Returning the empty store rather than panicking keeps a validation
		// surprise out of the startup path.
		return &s
	}
	return &s
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

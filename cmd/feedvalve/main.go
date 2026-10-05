// Command feedvalve relays the NSEIL commodity-derivatives NNF broadcast feed
// into Oracle.
//
// Usage:
//
//	feedvalve [-config path] [-selfcheck | -dbcheck]
//
//	-config     JSON configuration file. Omitted, the built-in defaults are
//	            used, which are themselves a valid configuration.
//	-selfcheck  Validate the configuration and the field map, then exit.
//	            Does NOT connect and does NOT require FEEDVALVE_DB_PASSWORD,
//	            so it is usable from a build host that has neither.
//	-dbcheck    Connect, prove a round trip with SELECT 1 FROM dual, report
//	            the server version, then exit. Read-only. Requires the
//	            password.
//
// There is no password flag and there will never be one: flags are visible in
// ps to every user on the host. The password comes only from
// FEEDVALVE_DB_PASSWORD.
//
// # The main/run split
//
// main holds nothing but flag parsing and the exit code. Everything else is
// app.Run, which returns an error.
//
// That is not stylistic. A fatal logger calls os.Exit, which skips every
// deferred cleanup, so the Fatalf-on-error style could never have closed the
// database or drained the queue -- poc was restructured for exactly this
// reason (poc/gobrdclnt.go:19-28). This file is the only place in the tree
// permitted to call os.Exit, and gate G-BLD-3 enforces that with a source
// scan.
//
// # A note on the timezone database, and OQ-L5
//
// Vol 0 section 7.2 specifies importing time/tzdata here, so that
// liveness.tradingWindow.tz resolves without the host supplying a zoneinfo
// database -- AIX may ship none. That import is NOT present because it does
// not link in this toolchain:
//
//	time/tzdata.init.0: relocation target
//	time.registerLoadFromEmbeddedTZData not defined
//
// Reproduced with go1.23.3 on a two-line program with no other dependency,
// for aix/ppc64, linux/amd64 and the native target alike, and it survives
// "go build -a", so it is neither a stale cache nor specific to the
// cross-compile. The push linkname directive is present in the installed
// time/zoneinfo_read.go, so the source is right and the link step is not.
//
// Consequence, which P1-11's LPAR smoke test must settle: the process relies
// on the HOST for zoneinfo, and config validation currently treats an
// unresolvable timezone as a hard error. If the LPAR has no zoneinfo
// database, the process will refuse to start over a feature that only
// downgrades log severity outside market hours. OQ-L5 notes that failing open
// -- treating every hour as in-window -- is the safe default; adopting it is
// a change to section 7.2's stated rule and is left as a decision.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"feedvalve/internal/app"
)

func main() {
	var opts app.Options

	flag.StringVar(&opts.ConfigPath, "config", "",
		"JSON configuration file; omitted, built-in defaults are used")
	flag.BoolVar(&opts.SelfCheck, "selfcheck", false,
		"validate the configuration and field map, then exit; does not connect")
	flag.BoolVar(&opts.DBCheck, "dbcheck", false,
		"connect, verify a round trip, report the server version, then exit")
	flag.Parse()

	// SIGINT and SIGTERM cancel the context rather than killing the
	// process, so the shutdown path runs. P1-09 adds the ordered drain
	// behind this; the cancellation is wired from the start because
	// retrofitting it later is how drain bugs become permanent.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := app.Run(ctx, opts, os.Stdout)
	if err != nil && !app.AlreadyReported(err) {
		fmt.Fprintf(os.Stderr, "feedvalve: %v\n", err)
	}

	// The one os.Exit in the tree, and it is after every defer above has
	// been registered and app.Run has returned -- so nothing is skipped.
	os.Exit(app.ExitCode(err))
}

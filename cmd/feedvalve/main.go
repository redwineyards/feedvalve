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
//	            the server version, then compare the destination tables'
//	            catalogue with the field map (Vol 0 section 7.6) and exit
//	            non-zero on any mismatch, NOT NULL data columns included.
//	            Read-only. Requires the password.
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
// # The timezone database, and OQ-L5 -- resolved
//
// time/tzdata is imported below, which is what Vol 0 section 7.2 and Vol 4
// section 8.7 specify: liveness.tradingWindow.tz then resolves without the host
// supplying a zoneinfo database, and AIX may ship none. About 450 KB of pure
// Go, no cgo.
//
// This file previously carried eighteen lines explaining why the import was
// ABSENT -- a link failure, "time.registerLoadFromEmbeddedTZData not defined",
// reported as reproducing on go1.23.3 for aix/ppc64, linux/amd64 and the native
// target alike, and as surviving go build -a. P1-11 re-tested every one of those
// cases and they all SUCCEED: the linkname directives are present in the
// installed GOROOT, the cross-compile links, and the native binary resolves
// Asia/Kolkata from the embedded database. Whatever the original failure was, it
// does not reproduce, so the import is restored and the comment is replaced by
// this record of why it ever went missing.
//
// That closes OQ-L5 on the build side. Section 7.2's rule -- an unresolvable
// timezone is a hard error -- therefore stands as written, and OQ-L5's
// alternative of failing open and treating every hour as in-window is not
// needed. G-AIX-1 assertion 3 still confirms it on the target, because a
// successful cross-compile is not proof that the embedded database works at
// runtime on AIX.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	// Embeds the IANA database so time.LoadLocation never depends on the host
	// having one. See the note above: this import was absent for a link
	// failure that no longer reproduces.
	_ "time/tzdata"

	"feedvalve/internal/app"
)

// version is stamped at build time:
//
//	go build -trimpath -ldflags "-X main.version=$(git rev-parse --short HEAD)"
//
// Vol 4 section 8's deployment topology specifies the artefact as built that
// way, and nothing existed for the flag to set. It is reported on the startup
// line so the running process can be matched to a commit, which is the first
// question a postmortem asks.
var version = "dev"

func main() {
	var opts app.Options

	flag.StringVar(&opts.ConfigPath, "config", "",
		"JSON configuration file; omitted, built-in defaults are used")
	flag.BoolVar(&opts.SelfCheck, "selfcheck", false,
		"validate the configuration and field map, then exit; does not connect")
	flag.BoolVar(&opts.DBCheck, "dbcheck", false,
		"connect, verify a round trip, report the server version, then exit")
	flag.Parse()
	opts.Version = version

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

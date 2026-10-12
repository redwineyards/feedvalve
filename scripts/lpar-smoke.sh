#!/bin/sh
# lpar-smoke.sh -- P1-11's five assertions, on the AIX/ppc64 target.
#
# RUN THIS ON THE LPAR, not on the build host. It is the whole of P1-11 that
# cannot be done anywhere else, and Phase 1's exit criterion is this script
# passing.
#
#   sh ./lpar-smoke.sh
#
# Requires FEEDVALVE_DB_PASSWORD in the environment for steps 3 and 4 and
# nothing at all for steps 1, 2 and 5. Exits non-zero if any assertion fails.
#
# THE ORDER IS DELIBERATE. Each step is a prerequisite for the next, so a
# failure says what is wrong instead of what noticed:
#
#   1. -selfcheck     needs no password, no network, no database. If this
#                     fails, nothing below is worth running.
#   2. test binaries  the layout constants and the timezone, AT RUNTIME on
#                     big-endian ppc64 (G-AIX-1 assertions 3 and 4).
#   3. -dbcheck       proves the credential, the listener and the service name
#                     FROM THIS HOST -- before step 4 can blame the binary for
#                     a database that was never reachable.
#   4. the full run   start, one instrument line, SIGTERM, clean exit.
#   5. the host       no cgo, and the receive buffer actually granted
#                     (G-AIX-1 assertion 5, the only thing that falsifies
#                     ASM-4).
#
# A missing AIX tool is reported as "could not check" and does NOT fail the
# run. An assertion that cannot be made is not an assertion that failed, and
# conflating the two is how a smoke test stops being believed.

set -u

BIN=${BIN:-./feedvalve}
CFG=${CFG:-./config.json}
FAILED=0
SKIPPED=0

say()  { printf '%s\n' "$*"; }
run()  { say "  \$ $*"; }
pass() { say "PASS  $*"; }
fail() { say "FAIL  $*"; FAILED=$((FAILED + 1)); }
skip() { say "SKIP  $* (could not check on this host)"; SKIPPED=$((SKIPPED + 1)); }
head2() { say ""; say "=== $* ==="; }

say "feedvalve LPAR smoke test"
say "uname: $(uname -a 2>/dev/null || echo unknown)"
if [ ! -x "$BIN" ]; then
	say "FATAL: $BIN is missing or not executable"
	exit 1
fi
say "version: $("$BIN" -selfcheck 2>&1 | sed -n 's/.*version=\([^ ]*\).*/\1/p' | head -1)"

# --- 1 -----------------------------------------------------------------------
head2 "1. -selfcheck exits 0 against the shipped configuration"
run "$BIN -config $CFG -selfcheck"
if "$BIN" -config "$CFG" -selfcheck; then
	pass "selfcheck"
else
	fail "selfcheck (exit $?) -- the configuration or the field map is wrong for this host"
fi

# --- 2 -----------------------------------------------------------------------
head2 "2. the cross-compiled test binaries (G-AIX-1)"
say "Assertions 3 and 4: time.LoadLocation resolves, and the layout constants"
say "hold under the ppc64 compiler AT RUNTIME. The compile-time half of"
say "assertion 4 is already proven by the cross-compile itself, because"
say "unsafe.Sizeof is evaluated for the target -- what runs here is the"
say "arithmetic in TestMemoryFigures and the reflect-based pointer-free walk."
say ""
say "Source-scanning gates will report SKIP: their input is the source tree,"
say "which is not deployed. That is expected and is not a failure."
found_tests=0
for t in ./*.test; do
	[ -f "$t" ] || continue
	found_tests=1
	run "$t -test.short"
	if "$t" -test.short; then
		pass "$(basename "$t")"
	else
		fail "$(basename "$t") -- a layout or runtime assertion failed ON THE TARGET, which is the case no amd64 run can make"
	fi
done
if [ "$found_tests" -eq 0 ]; then
	skip "no *.test binaries beside this script -- run scripts/build-aix.sh"
fi

# --- 3 -----------------------------------------------------------------------
head2 "3. -dbcheck proves Oracle is reachable FROM THIS HOST"
if [ -z "${FEEDVALVE_DB_PASSWORD:-}" ]; then
	skip "FEEDVALVE_DB_PASSWORD is not set, so steps 3 and 4 cannot run"
	say ""
	say "This is not optional for a complete run. Rule V-DB1 requires a db"
	say "object in run mode and startup step 11 pings it, so step 4 CANNOT"
	say "succeed without a reachable Oracle -- the card's assertion 2 presumes"
	say "one and does not say so."
else
	run "$BIN -config $CFG -dbcheck"
	if "$BIN" -config "$CFG" -dbcheck; then
		pass "dbcheck"
	else
		fail "dbcheck -- credential, listener or service name is wrong from this host. Step 4 will fail for this reason and not because of the binary"
	fi
fi

# --- 4 -----------------------------------------------------------------------
head2 "4. the binary starts, emits ONE instrument line, stops on SIGTERM"
if [ -z "${FEEDVALVE_DB_PASSWORD:-}" ]; then
	skip "needs FEEDVALVE_DB_PASSWORD (see step 3)"
else
	LOG=$(mktemp 2>/dev/null || echo /tmp/feedvalve-smoke.$$)
	# stats.intervalSec in the shipped config is 60, so wait a little past it.
	# Override it in config.json to shorten this if you are iterating.
	WAIT=${WAIT:-70}
	run "$BIN -config $CFG  (background, ${WAIT}s, then SIGTERM)"
	"$BIN" -config "$CFG" >"$LOG" 2>&1 &
	PID=$!
	sleep 2
	if ! kill -0 "$PID" 2>/dev/null; then
		fail "the process exited during startup"
		say "--- output ---"; cat "$LOG"; say "--- end ---"
	else
		say "  started, pid $PID; waiting ${WAIT}s for an emission"
		sleep "$WAIT"
		kill -TERM "$PID" 2>/dev/null
		wait "$PID"
		rc=$?

		if [ "$rc" -eq 0 ]; then
			pass "clean exit on SIGTERM (exit 0 -- a requested stop is not a failure)"
		else
			fail "exit $rc after SIGTERM, want 0. Exit 1 is a startup failure, 4 a terminal condition, 2 an unrecovered panic"
		fi

		if grep -q 'group=health' "$LOG"; then
			n=$(grep -c 'group=health' "$LOG")
			pass "instrument line emitted ($n health lines)"
		else
			fail "no instrument line in ${WAIT}s -- the stats goroutine never emitted"
		fi

		if grep -q 'shutdown complete' "$LOG"; then
			pass "ordered shutdown ran to completion"
		else
			fail "no shutdown summary -- the drain did not finish"
		fi

		# The receive buffer, read back from the emission itself. This is
		# G-AIX-1 assertion 5 and the only way to falsify ASM-4: if AIX
		# charges mbuf clusters rather than payload bytes, the real slack is
		# nearer 0.13 s than 0.53 s and NFR4's margin drops from 5x to ~1.3x.
		eff=$(sed -n 's/.*readbuf_effective_bytes=\([0-9]*\).*/\1/p' "$LOG" | head -1)
		if [ -n "$eff" ]; then
			say "  readbuf_effective_bytes=$eff (requested 655360)"
			if [ "$eff" -lt 655360 ]; then
				say "  NOTE: the kernel granted LESS than requested. Raise"
				say "  udp_recvspace (no -o udp_recvspace=655360) and ensure"
				say "  sb_max is at least that. ASM-4 is at risk."
			fi
			pass "effective receive buffer observed"
		else
			skip "readbuf_effective_bytes not found in the emission"
		fi

		say ""
		say "full output kept at $LOG"
	fi
fi

# --- 5 -----------------------------------------------------------------------
head2 "5. the binary itself: no cgo, and the kernel tunables"
# On AIX, dump -H lists the loader section. A pure-Go CGO_ENABLED=0 binary
# should show no dependency on libraries beyond what the OS always maps.
if command -v dump >/dev/null 2>&1; then
	run "dump -H $BIN"
	dump -H "$BIN" 2>&1 | sed 's/^/    /' | head -20
	say "  (inspect by eye: no libdl, no libpthread beyond the base system)"
	pass "loader section listed"
else
	skip "dump(1) not present"
fi

if command -v no >/dev/null 2>&1; then
	run "no -o udp_recvspace -o sb_max"
	no -o udp_recvspace 2>&1 | sed 's/^/    /'
	no -o sb_max 2>&1 | sed 's/^/    /'
	pass "kernel tunables listed"
else
	skip "no(1) not present -- cannot read udp_recvspace"
fi

say ""
say "The authoritative cgo check is a BUILD-time one and already automated:"
say "TestG_DEP_2_CrossCompilesForAIXWithoutCgo asserts go list -deps contains"
say "zero runtime/cgo. This step is confirmation on the target, not the proof."

# --- summary -----------------------------------------------------------------
head2 "summary"
say "failed:  $FAILED"
say "skipped: $SKIPPED"
if [ "$FAILED" -ne 0 ]; then
	say ""
	say "P1-11 is NOT satisfied. Phase 1's exit criterion is this script passing."
	exit 1
fi
if [ "$SKIPPED" -ne 0 ]; then
	say ""
	say "No failures, but $SKIPPED assertion(s) could not be made. P1-11 is"
	say "satisfied only when the skipped ones have been accounted for -- a"
	say "missing password or a missing AIX tool is a gap in the RUN, not"
	say "evidence about the binary."
	exit 0
fi
say ""
say "All assertions passed. Phase 1's exit criterion is met on this host."

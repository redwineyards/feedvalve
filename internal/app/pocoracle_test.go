// The oracle half of gate G-DIFF-1: poc, run as a subprocess with -debug, and
// the canonical form both sides are reduced to before they are compared.
//
// req section 7 both mandates validating the new decoder against poc and
// forbids reusing its code. ADR-012 resolves that by making the oracle poc's
// OUTPUT: nothing here links, imports or copies poc. It executes a binary and
// reads text, and the one import-direction consequence -- that executing is not
// importing -- is asserted separately by gate G-DEP-1.
//
// poc is NOT MODIFIED and must not need to be (Vol 1 section 8.6). Everything
// awkward about its output is modelled here rather than fixed there.
//
// # Why both sides go through one parser
//
// The two format strings are byte-identical -- poc/reader.go:30 and
// internal/obs/debug.go's fieldFormat are both
//
//	"DEBUG field=%-30s offset=%d-%d hex=%x value=%v"
//
// and internal/obs/debug_test.go already pins ours against a re-declared copy
// of poc's. So the internal trace is captured by pointing the PRODUCTION
// renderer at a buffer, not by a test-only formatter, and one parser reads both
// streams. That is deliberate: it makes the gate validate the trace an operator
// actually reads.
package app

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// span is one traced field, reduced to what section 8.6 says to diff on: the
// absolute offset range, the bytes, and the decoded value. The name is carried
// for diagnostics and for the mapping table, NOT as a comparison key -- poc
// prints header.sequenceNumber where this design prints BCSeqNo, and that
// rename is deliberate (Vol 0 section 10.5).
type span struct {
	name   string
	lo, hi int
	hex    string
	value  string
}

func (s span) String() string {
	return fmt.Sprintf("%-32s offset=%d-%d hex=%s value=%s", s.name, s.lo, s.hi, s.hex, s.value)
}

// fieldLinesFrom keeps only the field-trace lines in a stream and strips
// everything before the DEBUG token.
//
// Slicing from "DEBUG field=" rather than cutting a fixed 27 characters: both
// loggers are built with log.LstdFlags|log.Lmicroseconds today, but a parser
// that hard-codes the width of a timestamp breaks silently if either logger's
// flags change, and silently is the one way this harness must not break.
//
// This single predicate also drops everything else poc writes -- the INFO
// listening line, the per-packet header echo, the net_id warnings, the
// shutdown lines, and the multi-line hex dump, whose first line is
// "DEBUG from=" and whose continuations carry no timestamp at all.
func fieldLinesFrom(stream string) []string {
	var out []string
	for _, line := range strings.Split(stream, "\n") {
		if i := strings.Index(line, "DEBUG field="); i >= 0 {
			out = append(out, strings.TrimRight(line[i:], "\r"))
		}
	}
	return out
}

// parseSpan reads one line in the shared format.
//
// value is taken to END OF LINE, which is required rather than lazy: a pad read
// reports "(alignment padding, ignored)", which contains spaces, and poc's
// netIdPrefix line appends "(bytes 1-7 ignored per spec)" after the value. A
// parser that split on whitespace would truncate both.
func parseSpan(t *testing.T, line string) span {
	t.Helper()

	rest, ok := cutAfter(line, "DEBUG field=")
	if !ok {
		t.Fatalf("not a field line: %q", line)
	}
	nameRaw, rest, ok := strings.Cut(rest, " offset=")
	if !ok {
		t.Fatalf("no offset= in %q", line)
	}
	offs, rest, ok := strings.Cut(rest, " hex=")
	if !ok {
		t.Fatalf("no hex= in %q", line)
	}
	hex, value, ok := strings.Cut(rest, " value=")
	if !ok {
		t.Fatalf("no value= in %q", line)
	}
	loStr, hiStr, ok := strings.Cut(offs, "-")
	if !ok {
		t.Fatalf("offset %q is not lo-hi in %q", offs, line)
	}
	lo, err := strconv.Atoi(loStr)
	if err != nil {
		t.Fatalf("offset lo %q in %q: %v", loStr, line, err)
	}
	hi, err := strconv.Atoi(hiStr)
	if err != nil {
		t.Fatalf("offset hi %q in %q: %v", hiStr, line, err)
	}
	// %-30s pads the name to thirty columns, so the padding comes off here.
	// Field names cannot contain whitespace -- wire's
	// TestFieldIDNameTableIsComplete rejects it -- so trimming is lossless.
	return span{name: strings.TrimSpace(nameRaw), lo: lo, hi: hi, hex: hex, value: value}
}

func cutAfter(s, sep string) (string, bool) {
	i := strings.Index(s, sep)
	if i < 0 {
		return "", false
	}
	return s[i+len(sep):], true
}

func parseSpans(t *testing.T, lines []string) []span {
	t.Helper()
	out := make([]span, 0, len(lines))
	for _, l := range lines {
		out = append(out, parseSpan(t, l))
	}
	return out
}

// splitPasses breaks a field stream into parse passes.
//
// A single parse reads through one cursor, so its offsets STRICTLY INCREASE. An
// offset that does not advance is therefore a new pass over the same payload,
// and that is the whole rule.
//
// It is needed because poc re-enters parseBroadcastHeader 2 or 3 times per
// sub-packet -- once in processBroadcastPackData for the length lookup (only
// when the sub-packet is uncompressed), once in processPacket for the
// transaction code, and once more inside parseMS* -- so one sub-packet produces
// several passes, of which only the last is complete. Internal parses each
// sub-packet exactly once and therefore always yields exactly one pass.
func splitPasses(spans []span) [][]span {
	var passes [][]span
	var cur []span
	prevLo := -1
	for _, s := range spans {
		if s.lo <= prevLo {
			passes = append(passes, cur)
			cur = nil
		}
		cur = append(cur, s)
		prevLo = s.lo
	}
	if len(cur) > 0 {
		passes = append(passes, cur)
	}
	return passes
}

// pocSubPackets reduces poc's raw stderr to one field stream per sub-packet, in
// FIFO order.
//
// Windowing is on poc's own per-sub-packet hex-dump line ("DEBUG from=…
// packet=i/N broadcast data"), which it emits after decompression and before
// parsing. Within a window the sub-packet's stream is the FIRST pass of MAXIMAL
// LENGTH, and both halves of that rule earn their place:
//
//   - maximal, because the complete pass is the one that read the body; the
//     earlier passes stopped at the header.
//   - first, because the window can also contain the length-lookup pass for the
//     NEXT sub-packet, which poc emits before that sub-packet's own hex-dump
//     line. For a header-only code every pass in the window is the same length,
//     so without "first" the tie could be broken by the next sub-packet's
//     header -- a different message, at identical offsets, with plausible
//     values. Exactly the class of bug this gate exists to catch, so it is not
//     left to chance.
//
// Anything before the first window is the length-lookup pass of sub-packet 1
// and is dropped: it is a strict prefix of what window 1 contains.
func pocSubPackets(t *testing.T, raw string) [][]span {
	t.Helper()

	var windows []string
	var cur strings.Builder
	started := false
	for _, line := range strings.Split(raw, "\n") {
		if strings.Contains(line, "DEBUG from=") && strings.Contains(line, "broadcast data") {
			if started {
				windows = append(windows, cur.String())
			}
			cur.Reset()
			started = true
			continue
		}
		if started {
			cur.WriteString(line)
			cur.WriteString("\n")
		}
	}
	if started {
		windows = append(windows, cur.String())
	}

	out := make([][]span, 0, len(windows))
	for _, w := range windows {
		passes := splitPasses(parseSpans(t, fieldLinesFrom(w)))
		best := -1
		for i, p := range passes {
			if best < 0 || len(p) > len(passes[best]) {
				best = i
			}
		}
		if best < 0 {
			out = append(out, nil)
			continue
		}
		out = append(out, passes[best])
	}
	return out
}

// pocBinary builds poc once per test binary and returns its path.
//
// Both guards are mandatory rather than defensive. scripts/build-aix.sh
// cross-compiles a test binary per internal/* package and scripts/lpar-smoke.sh
// runs them with -test.short ON THE LPAR, where there is no Go toolchain and no
// source tree -- so this has to skip there, and a SKIP is the honest answer:
// an assertion that could not be made is not one that failed.
//
// (The equivalent helpers in testenv_test.go are in package app_test, and Go
// does not share helpers between a package's internal and external test
// packages, so the two checks are repeated here rather than exported.)
var (
	pocBuildOnce sync.Once
	pocBuildPath string
	pocBuildErr  error
)

func pocBinary(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("builds and runs poc as a subprocess; slow, and the deployment target " +
			"has neither a toolchain nor a source tree")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no Go toolchain on PATH: this gate builds poc and runs it. It belongs " +
			"on the build host and is not being quietly disabled -- " +
			"TestTheBuildHostHasEverythingTheGatesNeed fails if it skips here.")
	}
	root := findDiffModuleRoot()
	if root == "" {
		t.Skip("no source tree: this gate builds poc from the module's own sources, " +
			"which the cross-compiled test binary does not have on the LPAR.")
	}

	pocBuildOnce.Do(func() {
		// Built into the OS temp dir rather than a t.TempDir(), because the
		// binary is shared by every subtest and t.TempDir() is removed when
		// the test that created it finishes.
		dir, err := mkTempDir("feedvalve-poc-oracle")
		if err != nil {
			pocBuildErr = err
			return
		}
		out := filepath.Join(dir, "gobrdclnt"+exeSuffix())
		cmd := exec.Command("go", "build", "-o", out, "./poc")
		cmd.Dir = root
		if b, err := cmd.CombinedOutput(); err != nil {
			pocBuildErr = fmt.Errorf("go build ./poc: %v\n%s", err, b)
			return
		}
		pocBuildPath = out
	})

	if pocBuildErr != nil {
		t.Fatalf("building the poc oracle failed: %v.\n"+
			"poc must stay buildable: gate G-DIFF-1 is the only test that can prove this "+
			"decoder agrees with the one it replaces, and poc's output is the oracle "+
			"(ADR-012). Nothing in Phase 2 may break it.", pocBuildErr)
	}
	return pocBuildPath
}

// freeUDPPort picks a port by binding and releasing one.
//
// poc cannot be asked to report its own: -udp 127.0.0.1:0 binds an ephemeral
// port, but the INFO listening line echoes the FLAG STRING rather than
// LocalAddr, so the port would be unknowable. The bind-and-release race is
// accepted; a collision shows up as poc failing to start, which the readiness
// wait reports with its output rather than hanging.
func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a UDP port: %v", err)
	}
	port := c.LocalAddr().(*net.UDPAddr).Port
	if err := c.Close(); err != nil {
		t.Fatalf("releasing the reserved UDP port: %v", err)
	}
	return port
}

// pocTrace runs poc over one datagram and returns its field stream per
// sub-packet.
func pocTrace(t *testing.T, netID int, datagram []byte) [][]span {
	t.Helper()
	return pocSubPackets(t, pocRawTrace(t, netID, datagram))
}

// pocRawTrace is pocTrace without the reduction, for tests that need to assert
// on what poc did or did not emit at all.
func pocRawTrace(t *testing.T, netID int, datagram []byte) string {
	t.Helper()

	bin := pocBinary(t)
	port := freeUDPPort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// -out is passed explicitly: the default is tcp_reply.log, which would be
	// created in this package's directory and appended to on every run. -config
	// is omitted so no database is configured and the per-packet write no-ops.
	cmd := exec.Command(bin,
		"-debug",
		"-udp", addr,
		"-out", filepath.Join(t.TempDir(), "packets.log"),
		"-netid", strconv.Itoa(netID),
	)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("StderrPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the poc oracle: %v", err)
	}

	var (
		mu    sync.Mutex
		sb    strings.Builder
		ready = make(chan struct{})
		done  = make(chan struct{})
	)
	go func() {
		defer close(done)
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		signalled := false
		for sc.Scan() {
			line := sc.Text()
			mu.Lock()
			sb.WriteString(line)
			sb.WriteString("\n")
			mu.Unlock()
			if !signalled && strings.Contains(line, "INFO listening udp=") {
				signalled = true
				close(ready)
			}
		}
		if !signalled {
			close(ready) // so a failed start does not hang the test
		}
		_, _ = io.Copy(io.Discard, stderr)
	}()

	t.Cleanup(func() {
		// Kill rather than signal: Windows cannot deliver SIGTERM to a child,
		// and it is safe because log writes are unbuffered and the debug
		// stream does not go through poc's capped writer.
		_ = cmd.Process.Kill()
		<-done
		_ = cmd.Wait()
	})

	select {
	case <-ready:
	case <-time.After(20 * time.Second):
		t.Fatalf("the poc oracle never reported INFO listening on %s; it writes that line "+
			"once the socket is bound and there is no other handshake. stderr so far:\n%s",
			addr, snapshot(&mu, &sb))
	}
	// There is no "listening" line on a bind failure, so an early exit lands
	// here with whatever poc said about it.
	if s := snapshot(&mu, &sb); !strings.Contains(s, "INFO listening udp=") {
		t.Fatalf("the poc oracle exited before binding %s:\n%s", addr, s)
	}

	// Everything poc had said before the datagram, so the wait below can tell
	// "it has not answered yet" from "it has answered and gone quiet".
	baseline := snapshot(&mu, &sb)

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dialling the poc oracle: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write(datagram); err != nil {
		t.Fatalf("sending the %d-byte datagram: %v", len(datagram), err)
	}

	// Quiescence rather than a fixed sleep: wait until the stream has GROWN
	// past the baseline and then stopped growing. UDP to loopback is not
	// acknowledged, so there is nothing to block on, and a sleep long enough
	// to be safe on a loaded machine would be paid by every subtest.
	//
	// The condition is deliberately "any new output", not "a parsed packet".
	// A datagram poc REJECTS -- a MessageLength below the header size, say --
	// never reaches processPacket and so never produces the per-sub-packet
	// hex-dump line, only an ERROR. Requiring the dump would turn that case,
	// which is one of the cases this gate must cover, into a timeout.
	const quiet = 400 * time.Millisecond
	deadline := time.Now().Add(20 * time.Second)
	last := baseline
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
		now := snapshot(&mu, &sb)
		if now != last {
			last = now
			stableSince = time.Now()
			continue
		}
		if now != baseline && time.Since(stableSince) >= quiet {
			return now
		}
	}
	t.Fatalf("the poc oracle said nothing new in response to a %d-byte datagram within "+
		"the timeout. stderr:\n%s", len(datagram), last)
	return ""
}

func snapshot(mu *sync.Mutex, sb *strings.Builder) string {
	mu.Lock()
	defer mu.Unlock()
	return sb.String()
}

// findDiffModuleRoot locates the module root, or returns "" when there is no
// source tree -- the normal case on the LPAR. The same resolution
// testenv_test.go does, repeated because that file is package app_test.
func findDiffModuleRoot() string {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return ""
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return ""
	}
	return root
}

func mkTempDir(pattern string) (string, error) { return os.MkdirTemp("", pattern) }

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

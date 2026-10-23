package obs

import (
	"fmt"
	"io"
	"log"
	"strings"
	"sync/atomic"
)

// fieldFormat is the per-field trace line, kept VERBATIM from poc/reader.go.
//
// This is a compatibility obligation, not a preference. Vol 4 section 6.3: the
// differential harness of gate G-DIFF-1 diffs this exact stream against poc's,
// so a changed column, a changed width or a dropped token breaks the one test
// that can prove this decoder agrees with the one it replaces.
//
// Note the leading DEBUG token. Vol 4 section 6.3 quotes the example without
// it, but the quote is hand-typed prose while this is the format string the
// bytes actually came from -- and section 6.3 says the format is "kept verbatim
// from poc/reader.go:30", which is this. The format string wins; the doc's
// example is the thing that is wrong, and the test below asserts against the
// format rather than against the example.
//
// %-30s pads the field name to thirty columns, which is what lines the values
// up into a readable column and is also why the example's spacing cannot be
// counted on by eye.
const fieldFormat = "DEBUG field=%-30s offset=%d-%d hex=%x value=%v"

// DebugLogger is the per-transcode field trace: poc's -debug, carried over as
// an operator-triggered mode rather than a global flag.
//
// Offsets are ABSOLUTE within the broadcast data, including the 8-byte prefix
// (bible section 3.4). Two reasons, both load-bearing: they line up 1:1 against
// a hexdump -C and against a wire capture, and the differential harness depends
// on it. Getting the prefix wrong shifts every field by 8 bytes, which section
// 3.4 calls "the single most common decoding error".
type DebugLogger struct {
	lg *log.Logger

	// snap is the current trace selection, published whole so a reload can
	// replace it while the receive goroutine reads it. A reader loads the
	// pointer once and sees one consistent (codes, hexDump) pair; the snapshot
	// is never mutated after Set stores it.
	snap atomic.Pointer[debugSnap]
}

// debugSnap is an immutable trace selection: the transaction codes being
// traced, from debug.traceTranscodes (usually empty, occasionally one), and
// debug.hexDump.
type debugSnap struct {
	codes   []int
	hexDump bool
}

// NewDebugLogger builds the trace logger of startup step 6. An empty codes
// slice disables it entirely, which is the normal state.
func NewDebugLogger(w io.Writer, codes []int, hexDump bool) *DebugLogger {
	d := &DebugLogger{lg: log.New(w, "", log.LstdFlags|log.Lmicroseconds)}
	d.Set(codes, hexDump)
	return d
}

// Set applies a reloaded debug.traceTranscodes and debug.hexDump. It copies
// codes and publishes a new immutable snapshot, so it is safe to call while the
// receive goroutine is reading, and the readers below stay allocation-free.
func (d *DebugLogger) Set(codes []int, hexDump bool) {
	s := &debugSnap{hexDump: hexDump}
	if len(codes) > 0 {
		s.codes = append(s.codes, codes...)
	}
	d.snap.Store(s)
}

// Enabled reports whether this transaction code is being traced.
//
// CALL THIS AT THE CALL SITE, not just inside the trace methods:
//
//	if dbg.Enabled(code) { dbg.Field("token", lo, hi, raw, v) }
//
// The guard is mandatory on the receive path and the reason is subtle. Field's
// value parameter is an any, so the caller boxes it -- and for anything but a
// very small integer, boxing allocates. poc has exactly this bug: logField
// checks debugEnabled INSIDE itself (poc/reader.go:30), after the call site has
// already boxed the value, so with -debug off it still allocates once per field
// per message. At 10,000 messages/s and tens of fields each, that is the
// largest single source of garbage in the process, spent entirely on lines
// nobody asked for.
//
// Enabled is a length check and at most a short linear scan: debug.traceTranscodes
// holds nothing in normal operation and one code when an operator is
// diagnosing, so a map would be slower as well as larger.
func (d *DebugLogger) Enabled(code int) bool {
	codes := d.snap.Load().codes
	if len(codes) == 0 {
		return false
	}
	for _, c := range codes {
		if c == code {
			return true
		}
	}
	return false
}

// Any reports whether ANY transcode is being traced.
//
// The cheap pre-check for a caller that must do work just to learn which code
// it is holding. Resolving a transaction code out of a payload is not free, and
// in normal operation debug.traceTranscodes is empty -- so a caller asks this
// first and skips the resolution entirely, which is what keeps the receive path
// the same cost traced or not.
func (d *DebugLogger) Any() bool { return len(d.snap.Load().codes) > 0 }

// HexDumpEnabled reports whether debug.hexDump is set, so a caller can skip
// building a dump nobody will print.
func (d *DebugLogger) HexDumpEnabled() bool { return d.snap.Load().hexDump }

// Field traces one parsed field. lo and hi are absolute offsets within the
// broadcast data, prefix included, so they index the hex dump directly.
//
// It does NOT re-check Enabled. The caller has to, for the allocation reason on
// Enabled, and a second check here would imply the first was optional.
func (d *DebugLogger) Field(name string, lo, hi int, raw []byte, value any) {
	d.lg.Printf(fieldFormat, name, lo, hi, raw, value)
}

// HexDump writes a hexdump -C style dump of a payload, in poc's format
// (poc/reader.go:98-128), so a trace and a capture can be read side by side.
func (d *DebugLogger) HexDump(data []byte) {
	if !d.snap.Load().hexDump {
		return
	}
	d.lg.Printf("DEBUG payload %d bytes\n%s", len(data), HexDump(data))
}

// HexDump renders data in the hexdump -C layout poc produces: an 8-digit
// offset, sixteen bytes with a gap after the eighth, then the printable
// rendering between pipes.
//
// Exported because the format is shared with the differential harness, and
// because a caller that has already decided to dump wants the string rather
// than a logger call.
func HexDump(data []byte) string {
	var b strings.Builder
	for i := 0; i < len(data); i += 16 {
		end := min(i+16, len(data))
		line := data[i:end]

		fmt.Fprintf(&b, "%08x  ", i)
		for j := range 16 {
			if j < len(line) {
				fmt.Fprintf(&b, "%02x ", line[j])
			} else {
				b.WriteString("   ")
			}
			if j == 7 {
				b.WriteByte(' ')
			}
		}
		b.WriteString(" |")
		for _, c := range line {
			if c >= 0x20 && c < 0x7f {
				b.WriteByte(c)
			} else {
				b.WriteByte('.')
			}
		}
		b.WriteString("|\n")
	}
	return b.String()
}

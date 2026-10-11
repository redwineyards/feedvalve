package obs

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestDebugEnabledOnlyForNamedTranscodes(t *testing.T) {
	d := NewDebugLogger(discard{}, []int{7305}, false)
	if !d.Enabled(7305) {
		t.Error("Enabled(7305) = false for a code debug.traceTranscodes names")
	}
	if d.Enabled(7208) {
		t.Error("Enabled(7208) = true for a code nobody asked to trace")
	}
}

// TestDebugDisabledByDefaultAndFree is the normal state of the process. The
// trace is an operator-triggered mode, not a global flag, so with
// debug.traceTranscodes empty the check must cost nothing at all -- it sits on
// the receive path at 10,000 messages/s.
func TestDebugDisabledByDefaultAndFree(t *testing.T) {
	d := NewDebugLogger(discard{}, nil, false)
	if d.Enabled(7208) {
		t.Error("an empty trace list enabled a code")
	}
	if allocs := testing.AllocsPerRun(200, func() { d.Enabled(7208) }); allocs > 0 {
		t.Errorf("Enabled allocates %.1f times with tracing off", allocs)
	}
}

// TestFieldFormatIsVerbatimFromPoc is the compatibility obligation.
//
// Vol 4 section 6.3 says the format is "kept verbatim from poc/reader.go:30"
// and that the differential harness of gate G-DIFF-1 "diffs this exact stream
// against poc's". So the assertion is built FROM the format string rather than
// from the document's hand-typed example: the example in section 6.3 drops the
// leading DEBUG token that poc actually emits, and its column padding cannot be
// counted by eye. Generating the expectation from the format is the only way
// this test cannot inherit the document's typo.
func TestFieldFormatIsVerbatimFromPoc(t *testing.T) {
	var buf bytes.Buffer
	d := NewDebugLogger(&buf, []int{7305}, false)
	d.Field("header.transactionCode", 18, 20, []byte{0x1c, 0x89}, 7305)

	// poc/reader.go:30 -- the format this must not drift from.
	const pocFormat = "DEBUG field=%-30s offset=%d-%d hex=%x value=%v"
	want := fmt.Sprintf(pocFormat, "header.transactionCode", 18, 20, []byte{0x1c, 0x89}, 7305)

	line := strings.TrimRight(buf.String(), "\n")
	if !strings.HasSuffix(line, want) {
		t.Errorf("the trace line is not byte-identical to poc's format.\n got: %q\nwant suffix: %q\n"+
			"Gate G-DIFF-1 diffs this exact stream against poc's output, so a changed "+
			"column, width or token breaks the only test that can prove this decoder "+
			"agrees with the one it replaces.", line, want)
	}

	// And the pieces an operator reads, spelled out so a failure says which
	// one moved.
	for _, want := range []string{
		"field=header.transactionCode",
		"offset=18-20",
		"hex=1c89",
		"value=7305",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("the trace line is missing %q: %q", want, line)
		}
	}
}

// TestFieldOffsetsAreAbsoluteWithinTheBroadcastData is the invariant that makes
// the trace usable at all.
//
// The offsets include the 8-byte prefix (bible section 3.4), so they line up
// 1:1 against a hexdump -C and against a wire capture. Getting the prefix wrong
// shifts every field by eight bytes, which section 3.4 calls "the single most
// common decoding error" -- and the trace is the tool used to find exactly that,
// so a trace with relative offsets would hide the bug it exists to expose.
//
// obs cannot enforce this: the caller supplies the offsets. What it can do is
// pass them through unchanged, which is what this pins -- an obs that helpfully
// re-based them would silently break the capture comparison.
func TestFieldOffsetsAreAbsoluteWithinTheBroadcastData(t *testing.T) {
	var buf bytes.Buffer
	d := NewDebugLogger(&buf, []int{7305}, false)
	// The worked example from the spec: token at absolute offset 48, which is
	// 40 bytes into the broadcast header plus the 8-byte prefix.
	d.Field("token", 48, 52, []byte{0x00, 0x01, 0x81, 0xcd}, 98765)

	line := buf.String()
	if !strings.Contains(line, "offset=48-52") {
		t.Errorf("the offsets were not passed through unchanged: %q", line)
	}
	if !strings.Contains(line, "hex=000181cd") || !strings.Contains(line, "value=98765") {
		t.Errorf("the hex or value does not match the specified example: %q", line)
	}
}

func TestHexDumpMatchesPocLayout(t *testing.T) {
	data := []byte{
		0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x1c, 0x89, 0x41, 0x42, 0x43, 0x7f, 0x20, 0xff,
		0x01,
	}
	got := HexDump(data)

	want := "" +
		"00000000  08 00 00 00 00 00 00 00  1c 89 41 42 43 7f 20 ff  |..........ABC. .|\n" +
		"00000010  01                                                |.|\n"
	if got != want {
		t.Errorf("hex dump layout changed.\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestHexDumpIsSuppressedUnlessConfigured(t *testing.T) {
	var buf bytes.Buffer
	d := NewDebugLogger(&buf, []int{7305}, false)
	d.HexDump([]byte{1, 2, 3})
	if buf.Len() != 0 {
		t.Errorf("a dump was written with debug.hexDump off: %q", buf.String())
	}
	if d.HexDumpEnabled() {
		t.Error("HexDumpEnabled reports true when hexDump is off")
	}

	d = NewDebugLogger(&buf, []int{7305}, true)
	d.HexDump([]byte{1, 2, 3})
	if !strings.Contains(buf.String(), "00000000  01 02 03") {
		t.Errorf("no dump written with debug.hexDump on: %q", buf.String())
	}
}

// TestTraceListIsCopied guards against the caller mutating the configured slice
// afterwards. config rebuilds a whole Config on reload rather than mutating the
// live one, so this should never happen -- but a logger that aliased its
// configuration would make a reload change tracing without going through the
// reload path at all.
func TestTraceListIsCopied(t *testing.T) {
	codes := []int{7305}
	d := NewDebugLogger(discard{}, codes, false)
	codes[0] = 7208
	if d.Enabled(7208) {
		t.Error("mutating the caller's slice changed what is traced")
	}
	if !d.Enabled(7305) {
		t.Error("the originally configured code is no longer traced")
	}
}

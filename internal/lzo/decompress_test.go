package lzo

// Correctness and rejection tests for the LZO1Z port.
//
// Two halves, and the second is the reason this package exists in a market-data
// relay rather than in a compression library:
//
//   - the round-trips, which prove the port decodes what an independent
//     implementation encoded (ported from poc/decompress_test.go);
//   - the rejection tests, which prove that untrusted input -- truncated,
//     corrupt, or claiming more output than the buffer holds -- costs one
//     counted reject and never a panic, a short decode, or a write out of
//     bounds. Compressed sub-packets arrive from a UDP socket.

import (
	"bytes"
	"errors"
	"testing"
)

func TestDecompressMatchesReferenceFixtures(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"single byte", []byte{0x42}},
		{"short literal text", []byte("Hello, World! This is a short test string.")},
		{"highly repetitive", bytes.Repeat([]byte("AB"), 500)},
		{"all zeros 1KB", make([]byte, 1024)},
		{"all zeros 512B (typical sub-packet size)", make([]byte, 512)},
		{"random incompressible 2KB", randomBytes(t, 2048, 1)},
		{"random incompressible 64B", randomBytes(t, 64, 2)},
		{"synthetic MSSecurityUpdateInfo-shaped (294B)", buildSyntheticBroadcastMessage(999, 294)},
		{"synthetic MSBroadcastOnlyMBP-shaped (470B)", buildSyntheticBroadcastMessage(12345, mbpShapedLen)},
		{"long run then literal tail", append(bytes.Repeat([]byte{0x00}, 600), []byte("tail-literal-bytes-that-dont-repeat-1234567890")...)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compressed := compressWithReference(t, tc.data)

			// Both variants, because the tests are the only place the
			// unchecked one may be called, and the claim that the bounds
			// checks change nothing on well-formed input is worth holding.
			t.Run("Decompress", func(t *testing.T) {
				dst := make([]byte, len(tc.data))
				n, err := Decompress(dst, compressed)
				if err != nil {
					t.Fatalf("Decompress: %v", err)
				}
				if n != len(tc.data) {
					t.Fatalf("Decompress returned n=%d, want %d", n, len(tc.data))
				}
				if !bytes.Equal(dst[:n], tc.data) {
					t.Fatalf("Decompress output mismatch:\ngot  %x\nwant %x", dst[:n], tc.data)
				}
			})

			t.Run("DecompressSafe", func(t *testing.T) {
				dst := make([]byte, len(tc.data))
				n, err := DecompressSafe(dst, compressed)
				if err != nil {
					t.Fatalf("DecompressSafe: %v", err)
				}
				if n != len(tc.data) {
					t.Fatalf("DecompressSafe returned n=%d, want %d", n, len(tc.data))
				}
				if !bytes.Equal(dst[:n], tc.data) {
					t.Fatalf("DecompressSafe output mismatch:\ngot  %x\nwant %x", dst[:n], tc.data)
				}
			})
		})
	}
}

// TestDecompressSafeRoundTripsRandomSizes reruns the round-trip across many
// sizes, concentrated around the length-encoding boundaries (15/16/17,
// 255/256/257, 511/512/513), where an off-by-one in the literal/match length
// continuation logic would hide.
func TestDecompressSafeRoundTripsRandomSizes(t *testing.T) {
	sizes := []int{1, 2, 3, 15, 16, 17, 31, 32, 33, 255, 256, 257, 511, 512, 513, 1000, 4096}
	for _, size := range sizes {
		t.Run("", func(t *testing.T) {
			data := randomBytes(t, size, int64(size)+100)
			// Bias toward compressibility for some sizes by zeroing half the bytes.
			for i := 0; i < len(data)/2; i++ {
				data[i] = 0
			}

			compressed := compressWithReference(t, data)
			dst := make([]byte, len(data))
			n, err := DecompressSafe(dst, compressed)
			if err != nil {
				t.Fatalf("size=%d DecompressSafe: %v", size, err)
			}
			if !bytes.Equal(dst[:n], data) {
				t.Fatalf("size=%d output mismatch", size)
			}
		})
	}
}

// TestG_FRM_4_TruncatedStreamIsRejected is the decompressor's half of gate
// G-FRM-4 (Vol 4 section 7.3): a compressed sub-packet whose LZO1Z stream is
// truncated must make DecompressSafe return an error, with no panic. The other
// half of that gate -- that the NEXT sub-packet in the same datagram still
// decodes -- belongs to framing and lands with P2-03, because the cursor
// advances by iCompLen, which was read from the wire before the decompressor
// ran (Vol 1 section 5.6).
//
// The property asserted here is the one the card names: rejected, not silently
// SHORT-DECODED. Stated as an implication rather than "err != nil" so it stays
// exactly true -- a cut that happens to land on a valid end-of-stream marker
// may legitimately report success, but then its output must be the complete,
// byte-exact message. Anything else is a short decode: a truncated message
// handed downstream as if whole, which in this system means a fabricated
// security that looks real.
// Two streams, because the MBP-shaped message is sparse and compresses to 47
// bytes -- almost all match opcodes -- so on its own it would leave the literal
// paths, and the length-continuation loops a long literal run drives, untested
// against truncation. The 2 KB incompressible fixture is the other extreme.
func TestG_FRM_4_TruncatedStreamIsRejected(t *testing.T) {
	mbp, mbpComp := mbpShapedFixture(t)
	literal := randomBytes(t, 2048, 1)

	for _, tc := range []struct {
		name            string
		data            []byte
		compressed      []byte
		mostlyRejecting bool
	}{
		{"match-heavy (470B MBP-shaped, 47B compressed)", mbp, mbpComp, true},
		{"literal-heavy (2KB incompressible)", literal, compressWithReference(t, literal), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rejected := 0
			for cut := 1; cut < len(tc.compressed); cut++ {
				truncated := tc.compressed[:len(tc.compressed)-cut]
				dst := make([]byte, len(tc.data))

				n, err := safeCall(t, dst, truncated)
				if err != nil {
					rejected++
					continue
				}
				if n != len(tc.data) || !bytes.Equal(dst[:n], tc.data) {
					t.Fatalf("truncated input (cut last %d of %d bytes) decoded SHORT and reported "+
						"success: n=%d, want %d, equal=%v",
						cut, len(tc.compressed), n, len(tc.data), bytes.Equal(dst[:n], tc.data))
				}
			}

			// A floor, not an exact count: without it, a change that made
			// truncation mostly "succeed" could leave the loop above passing
			// while the gate had stopped meaning anything.
			if tc.mostlyRejecting && rejected < len(tc.compressed)/2 {
				t.Fatalf("only %d of %d truncations were rejected; G-FRM-4 expects truncation to "+
					"be the detected case, not the exception", rejected, len(tc.compressed)-1)
			}
			t.Logf("%d of %d truncations rejected outright; any that reported success returned the "+
				"complete message, never a short decode", rejected, len(tc.compressed)-1)
		})
	}
}

// TestDecompressSafeDistinguishesTheFourReasons pins the four sentinels to
// four different causes.
//
// This is not pedantry about error values: each one is a label slot on
// decompress_failed{reason} (Vol 1 section 5.4, addition A-01), and the whole
// point of the label is that an operator can tell "the wire is corrupt" from
// "ASM-14 is broken, raise the buffer" without reading a packet capture. If two
// causes collapsed onto one sentinel the counter would still move and the
// runbook entry it points at would be the wrong one.
func TestDecompressSafeDistinguishesTheFourReasons(t *testing.T) {
	data, compressed := mbpShapedFixture(t)

	t.Run("input_overrun: the stream is truncated", func(t *testing.T) {
		dst := make([]byte, len(data))
		_, err := safeCall(t, dst, compressed[:len(compressed)/2])
		if !errors.Is(err, ErrInputOverrun) {
			t.Fatalf("got %v, want ErrInputOverrun", err)
		}
	})

	t.Run("output_overrun: the message outgrows dst", func(t *testing.T) {
		dst := make([]byte, len(data)/2) // the stream is intact; the buffer is not big enough
		_, err := safeCall(t, dst, compressed)
		if !errors.Is(err, ErrOutputOverrun) {
			t.Fatalf("got %v, want ErrOutputOverrun", err)
		}
	})

	t.Run("input_not_consumed: iCompLen overstates the compressed length", func(t *testing.T) {
		// A complete stream with trailing bytes behind it, which is what a
		// too-large iCompLen looks like from in here: a framing question
		// rather than a compression one.
		padded := append(append([]byte{}, compressed...), 0xDE, 0xAD, 0xBE, 0xEF)
		dst := make([]byte, len(data))
		_, err := safeCall(t, dst, padded)
		if !errors.Is(err, ErrInputNotConsumed) {
			t.Fatalf("got %v, want ErrInputNotConsumed", err)
		}
	})

	t.Run("lookbehind_overrun: a match points before the output", func(t *testing.T) {
		// Found by scanning the single-byte mutation space in a fixed order
		// rather than hardcoding an offset: a corrupt match distance is not
		// something a hand-written stream demonstrates convincingly, and a
		// pinned magic offset would quietly stop testing this if the fixture
		// ever changed. The scan is deterministic, so the case it finds is
		// the same case on every run.
		off, val, found := findMutationYielding(t, compressed, len(data), ErrLookbehindOverrun)
		if !found {
			t.Fatal("no single-byte mutation of the frozen stream produced ErrLookbehindOverrun; " +
				"the sentinel may have become unreachable, which would silently empty one of the " +
				"four decompress_failed{reason} slots")
		}
		t.Logf("reached via compressed[%d] = %#02x (was %#02x)", off, val, compressed[off])
	})
}

// TestDecompressSafeRejectsEveryUndersizedDst sweeps every possible undersized
// destination for one known-good stream.
//
// Vol 1 section 5.3 makes a specific claim about MaxDecompressedLen: an
// undersized buffer is "a COUNTED REJECT, never memory corruption". That claim
// rests on every output-writing branch carrying its own bound check. A branch
// that is missing one does not corrupt memory here -- Go's own bounds check
// fires first -- but it panics, which on the receive path means the recover
// boundary eats a sub-packet for a reason nobody can diagnose. So this asserts
// the stronger, useful thing: ErrOutputOverrun, from every size, with no panic.
func TestDecompressSafeRejectsEveryUndersizedDst(t *testing.T) {
	data, compressed := mbpShapedFixture(t)

	// Backed by a larger array with a canary tail. In Go a write past
	// len(dst) panics rather than reaching the canary, so this cannot fail
	// on its own -- it is here to say, for a reader coming from the C
	// original where it very much could, exactly which guarantee the port
	// inherits from the language and which one it has to earn itself.
	const canaryLen = 64
	backing := make([]byte, len(data)+canaryLen)
	canary := bytes.Repeat([]byte{0xA5}, canaryLen)

	for size := range len(data) {
		copy(backing[len(data):], canary)
		dst := backing[:size]

		n, err := safeCall(t, dst, compressed)
		if !errors.Is(err, ErrOutputOverrun) {
			t.Fatalf("len(dst)=%d (need %d): got n=%d err=%v, want ErrOutputOverrun",
				size, len(data), n, err)
		}
		if n > size {
			t.Fatalf("len(dst)=%d: returned n=%d, which is past the end of dst", size, n)
		}
		if !bytes.Equal(backing[len(data):], canary) {
			t.Fatalf("len(dst)=%d: wrote past the destination slice", size)
		}
	}

	// And the exact size still works, so the sweep above is not passing
	// because the decompressor refuses everything.
	dst := make([]byte, len(data))
	if n, err := safeCall(t, dst, compressed); err != nil || n != len(data) {
		t.Fatalf("exact-size dst: n=%d err=%v, want %d and no error", n, err, len(data))
	}
}

// TestDecompressSafeSurvivesCorruptInput mutates one byte at a time across the
// whole stream, with a fixed set of replacement values chosen to hit the opcode
// class boundaries the decoder branches on (17, 16, 32, 64) as well as the
// zero-continuation paths.
//
// Three claims, all of them about what a corrupt datagram must cost:
//
//   - no panic escapes this package. The receiver's recover boundary (Vol 1
//     section 5.5) is the last line, not the first;
//   - every error is one of the four sentinels, so every failure has a
//     decompress_failed{reason} slot to land in and none falls through as an
//     unlabelled surprise;
//   - all four sentinels are actually reachable from corrupt input. A reason
//     that cannot occur is a label that will never move, and one worth
//     noticing if a future edit makes it so.
//
// What it deliberately does NOT claim is that corruption is detected. It
// mostly is not: around 60% of these single-byte mutations decode with no
// error at all, because a mutated literal byte is still a valid literal byte
// and LZO1Z carries no checksum. The logged counts say so out loud. That is
// the gap Vol 1 section 5.7's cross-check closes -- a correct compressed
// packet satisfies decompressedLen == 8 + MessageLength -- and it is why that
// check is a requirement rather than a nicety. It needs the parsed header, so
// it belongs to P2-02, not here.
func TestDecompressSafeSurvivesCorruptInput(t *testing.T) {
	data, compressed := mbpShapedFixture(t)

	seen := map[error]int{}
	ok, total := 0, 0

	mutationSweep(t, compressed, len(data), func(off int, val byte, n int, err error) {
		total++
		switch {
		case err == nil:
			ok++
			if n > len(data) {
				t.Fatalf("compressed[%d]=%#02x: reported success with n=%d, past len(dst)=%d",
					off, val, n, len(data))
			}
		case errors.Is(err, ErrInputOverrun),
			errors.Is(err, ErrOutputOverrun),
			errors.Is(err, ErrLookbehindOverrun),
			errors.Is(err, ErrInputNotConsumed):
			seen[unwrapSentinel(err)]++
		default:
			t.Fatalf("compressed[%d]=%#02x: error %v is none of the four sentinels, so it has no "+
				"decompress_failed{reason} slot", off, val, err)
		}
	})

	for _, want := range []error{ErrInputOverrun, ErrOutputOverrun, ErrLookbehindOverrun, ErrInputNotConsumed} {
		if seen[want] == 0 {
			t.Errorf("no single-byte corruption produced %v; that reason slot is unreachable", want)
		}
	}
	t.Logf("%d mutations: %d decoded without error, rejections %v", total, ok, countsByName(seen))
}

// TestDecompressSafeAllocatesNothing is the decompressor's share of G-ALLOC-1
// (Vol 4 section 7.3), asserted as a test rather than left to a benchmark a
// human has to read. The whole-receive-iteration version arrives in P2-09; a
// decompressor that allocated would make that gate unreachable, and this
// catches it at the one place where the cause is unambiguous.
func TestDecompressSafeAllocatesNothing(t *testing.T) {
	data, compressed := mbpShapedFixture(t)
	dst := make([]byte, MaxDecompressedLen) // as the receiver sizes it

	if n := testing.AllocsPerRun(1000, func() {
		if _, err := DecompressSafe(dst, compressed); err != nil {
			t.Fatalf("DecompressSafe: %v", err)
		}
	}); n != 0 {
		t.Fatalf("DecompressSafe allocates %v times per call, want 0 (len(data)=%d)", n, len(data))
	}
}

// safeCall runs DecompressSafe and turns an escaping panic into a test
// failure naming the input, instead of a stack trace that kills the run.
// Returning an error is the contract on malformed input; panicking is the
// defect these tests exist to catch.
func safeCall(t *testing.T, dst, src []byte) (n int, err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("DecompressSafe panicked on %d compressed bytes into a %d-byte dst: %v",
				len(src), len(dst), r)
		}
	}()
	return DecompressSafe(dst, src)
}

// mutationValues are the single-byte replacements the corruption sweep uses.
// They straddle the opcode class boundaries the decoder branches on -- >17 for
// the first-literal-run shortcut, and 16 / 32 / 64 for M1 / M4 / M3 / M2 -- plus
// 0x00, which is what drives the length-continuation loops.
var mutationValues = []byte{0x00, 0x01, 0x0f, 0x10, 0x11, 0x12, 0x20, 0x3f, 0x40, 0x80, 0xc0, 0xff}

// mutationSweep calls visit once for every (offset, replacement) pair over a
// copy of src, in a fixed order. The destination is reused across iterations
// and zeroed between them, which keeps the sweep allocation-free and means a
// mutation cannot pass by reading what the previous one wrote.
func mutationSweep(t *testing.T, src []byte, dstLen int, visit func(off int, val byte, n int, err error)) {
	t.Helper()

	mutated := make([]byte, len(src))
	dst := make([]byte, dstLen)

	for off := range src {
		for _, val := range mutationValues {
			if src[off] == val {
				continue
			}
			copy(mutated, src)
			mutated[off] = val
			for i := range dst {
				dst[i] = 0
			}
			n, err := safeCall(t, dst, mutated)
			visit(off, val, n, err)
		}
	}
}

// findMutationYielding returns the first single-byte mutation, in the sweep's
// fixed order, that makes DecompressSafe return want.
func findMutationYielding(t *testing.T, src []byte, dstLen int, want error) (off int, val byte, found bool) {
	t.Helper()
	mutationSweep(t, src, dstLen, func(o int, v byte, _ int, err error) {
		if found || !errors.Is(err, want) {
			return
		}
		off, val, found = o, v, true
	})
	return off, val, found
}

func unwrapSentinel(err error) error {
	for _, s := range []error{ErrInputOverrun, ErrOutputOverrun, ErrLookbehindOverrun, ErrInputNotConsumed} {
		if errors.Is(err, s) {
			return s
		}
	}
	return err
}

func countsByName(seen map[error]int) map[string]int {
	out := make(map[string]int, len(seen))
	for err, n := range seen {
		out[err.Error()] = n
	}
	return out
}

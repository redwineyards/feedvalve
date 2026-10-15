// The literal-only LZO1Z encoder the differential harness uses to build a
// COMPRESSED sub-packet, and the round trip that proves it is a valid stream.
//
// Why this exists at all. Gate G-DIFF-1 requires "a compressed and an
// uncompressed sub-packet in one datagram" (Vol 1 section 8.6 item 4) with a
// decoder actually running on the DECOMPRESSED bytes -- the one thing P2-09's
// allocation gate explicitly left to this card. There is no compressor in this
// module and there must not be one (gate G-DEP-3): compressed fixtures come
// from poc/testdata/lzo_fixtures.json, keyed by the SHA-256 of their input, and
// no frozen input is a valid broadcast message.
//
// So the stream is built here, by hand, using only literal runs. That is not a
// compressor and it does not pretend to be one: it emits the payload verbatim
// between a length prefix and an end-of-stream marker, which is a well-formed
// LZO1Z stream that happens to be six bytes LARGER than its input.
//
// THE INDEPENDENCE RULE IS NOT WEAKENED BY THIS. The frozen fixtures exist
// because a decompressor checked only against a compressor from the same
// codebase agrees with itself even when both are wrong; internal/lzo and poc
// are each already validated that way, by their own frozen sets. Here the
// compressed bytes are transport, not the thing under test -- the thing under
// test is the DECODER, and the oracle is poc's trace. And because poc runs its
// own independently written port of LZO1Z over these same bytes, a datagram
// built this way cross-checks the two decompressors as a side effect: if they
// disagreed about the plaintext, every field after the disagreement would
// differ and the gate would say so.
//
// The expansion is not a defect to optimise away. A diagnostic fixture needs
// distinct bytes at every offset so that a one-byte offset slip changes the hex
// of every field after it (see newDiffFixture), and distinct bytes do not
// compress. A real compressor would emit a comparable size.
package app

import (
	"bytes"
	"fmt"
	"testing"

	"feedvalve/internal/lzo"
)

// lzoEOFMarker is LZO1Z's end-of-stream marker, and it is REQUIRED rather than
// decorative.
//
// Two separate reasons, both checked by hand against internal/lzo/decompress.go
// and both easy to get wrong:
//
//  1. It is what terminates the stream. 0x11 is >= 16, so it enters the M4
//     branch with t&8 == 0 (leaving mPos == op) and t&7 == 1 (so the long-length
//     path is skipped). The two zero bytes then subtract nothing, so mPos is
//     still op -- which is exactly the `if mPos == op { goto eofFound }`
//     condition. eofFound returns nil only when ip == ipEnd, so the three bytes
//     must be the last three.
//
//  2. It is what makes the literal run fit. The safe-mode bound before the
//     literal copy is `ipEnd-ip < t+6` (an input overrun otherwise), and after
//     the length prefix ipEnd-ip is exactly len(payload)+3 == t+6. Drop the
//     marker and a correct stream is rejected as truncated.
var lzoEOFMarker = []byte{0x11, 0x00, 0x00}

// lzoLiteralOnly encodes payload as a single LZO1Z literal run.
//
// The length encoding is the awkward part and follows internal/lzo's reader
// exactly. The run length the decoder computes is t+3, so t = len(payload)-3,
// and t is written one of two ways:
//
//   - t in [1,15]: one byte, t itself. A first byte below 16 is a literal run,
//     and a non-zero one carries the whole length.
//   - t >= 16: a 0x00 lead-in selects the long form, then one 0x00 per 255,
//     then a remainder byte f with t = 255*k + 15 + f.
//
// f MUST NOT BE ZERO. The decoder consumes the 255s with `for src[ip] == 0`, so
// a zero remainder would be eaten as another 255 and the length would come out
// 255 too large -- a stream that decompresses to the wrong size rather than
// failing. Where the arithmetic lands on f == 0 the encoder steps k back one
// and uses f = 255 instead.
func lzoLiteralOnly(t *testing.T, payload []byte) []byte {
	t.Helper()

	if len(payload) < 19 {
		// Below 19 the single-byte form would be needed (t < 16), which no
		// fixture here uses: the smallest broadcast payload is the 48-byte
		// header-only message. Refusing is better than carrying an untested
		// branch.
		t.Fatalf("lzoLiteralOnly: payload is %d bytes; this encoder covers the long "+
			"literal form only, which needs at least 19 (t >= 16)", len(payload))
	}

	runLen := len(payload) - 3

	// Choose k and f such that runLen == 255*k + 15 + f, with 1 <= f <= 255.
	k := 0
	for runLen-255*k-15 > 255 {
		k++
	}
	f := runLen - 255*k - 15
	if f == 0 {
		k--
		f = 255
	}
	if k < 0 || f < 1 || f > 255 {
		t.Fatalf("lzoLiteralOnly: cannot encode run length %d as 255*%d+15+%d", runLen, k, f)
	}

	stream := make([]byte, 0, 2+k+len(payload)+len(lzoEOFMarker))
	stream = append(stream, 0x00) // the long-literal lead-in
	for range k {
		stream = append(stream, 0x00) // +255 each
	}
	stream = append(stream, byte(f))
	stream = append(stream, payload...)
	stream = append(stream, lzoEOFMarker...)
	return stream
}

// TestLiteralOnlyStreamRoundTrips is step one of P2-10, deliberately standing
// alone: everything else in the differential harness is built on this encoder,
// so it is proven in isolation first.
//
// Setup: payloads at and around the length-encoding boundaries, including the
// two real fixture sizes (302 for 7305, 478 for 7208) and the k=0/k=1 seam at
// runLen 270/271. Stimulus: encode, then DecompressSafe. Assertion: the output
// is byte-identical to the input and no error is reported.
//
// What it would have caught: an off-by-one in the length arithmetic. That is
// the failure mode worth guarding, because it does not fail -- a run length 255
// too large reads past the payload into the marker and SUCCEEDS with the wrong
// size, which downstream looks like a protocol-level length mismatch rather
// than a broken fixture.
func TestLiteralOnlyStreamRoundTrips(t *testing.T) {
	sizes := []int{
		19,  // runLen 16, the smallest long form
		20,  // runLen 17
		48,  // a header-only message
		60,  // 6503's payload
		273, // runLen 270, the last k=0 length
		274, // runLen 271, the first k=1 length
		302, // 7305 / 7340: 8 prefix + 294 declared
		478, // 7208: 8 prefix + 470 declared
		512,
	}

	for _, n := range sizes {
		t.Run(fmt.Sprintf("payload=%d", n), func(t *testing.T) {
			payload := make([]byte, n)
			for i := range payload {
				// Position-dependent, so a misaligned copy cannot pass by
				// landing on an identical byte.
				payload[i] = byte(i*31 + 7)
			}

			stream := lzoLiteralOnly(t, payload)
			if len(stream) <= n {
				t.Fatalf("stream is %d bytes for a %d-byte payload; a literal-only "+
					"encoding must EXPAND, so a shorter one means the length prefix is wrong",
					len(stream), n)
			}

			dst := make([]byte, n+64)
			got, err := lzo.DecompressSafe(dst, stream)
			if err != nil {
				t.Fatalf("DecompressSafe rejected a literal-only stream of %d bytes: %v.\n"+
					"The encoding is 0x00 <255s> <f> || payload || 11 00 00; see lzoEOFMarker "+
					"for why the marker is load-bearing rather than cosmetic.", len(stream), err)
			}
			if got != n {
				t.Fatalf("decompressed %d bytes, want %d -- the run-length encoding is off", got, n)
			}
			if !bytes.Equal(dst[:got], payload) {
				t.Fatalf("decompressed bytes differ from the payload for n=%d", n)
			}
		})
	}
}

// TestLiteralOnlyStreamNeedsItsMarker pins the second reason lzoEOFMarker
// exists, because the first one (termination) is the only one that is obvious.
//
// Truncating the three marker bytes must FAIL. If it ever starts succeeding,
// the `ipEnd-ip < t+6` bound has changed and the comment on lzoEOFMarker is
// wrong -- which matters, because the next person sizing a fixture will believe
// it.
func TestLiteralOnlyStreamNeedsItsMarker(t *testing.T) {
	payload := make([]byte, 302)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}

	stream := lzoLiteralOnly(t, payload)
	truncated := stream[:len(stream)-len(lzoEOFMarker)]

	dst := make([]byte, len(payload)+64)
	if _, err := lzo.DecompressSafe(dst, truncated); err == nil {
		t.Fatal("DecompressSafe accepted a stream with the end-of-stream marker removed; " +
			"the literal-run bound ipEnd-ip < t+6 is what the marker satisfies, so this " +
			"must be rejected as truncated")
	}
}

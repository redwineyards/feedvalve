package lzo

import "errors"

// The four sentinels below are not four spellings of "decompression failed".
// They mean four genuinely different things to an operator (Vol 1 section 5.4),
// and they map 1:1 onto obs.DecompressReason -- the {reason} label on
// decompress_failed, Vol 1 section 14 addition A-01, built in P1-07 and sitting
// in internal/obs/dim.go with its four values already named. Collapsing them
// would throw away a distinction this port makes for free and leave an operator
// unable to tell a truncated stream (wire corruption, escalate) from an output
// overrun (ASM-14 broken, raise the buffer) -- different problems with
// different fixes.
//
// The mapping is deliberately NOT performed here. lzo imports nothing outside
// the standard library, which is what keeps the binary free of cgo and
// liblzo2, and internal/app/deps_test.go enforces it; importing obs for a label
// index would trade that for a switch statement. So the switch lives at the
// call site, in the receiver, where the counter is incremented.
var (
	// ErrInputOverrun is returned when the compressed input is truncated:
	// an opcode or match needed more input bytes than were available.
	// Corresponds to LZO_E_INPUT_OVERRUN.
	//
	// Operationally: wire corruption, or iCompLen disagreeing with the bytes
	// actually sent. Capture and escalate.
	ErrInputOverrun = errors.New("lzo1z: input overrun")

	// ErrOutputOverrun is returned when decompression would write more
	// bytes than fit in the destination buffer.
	// Corresponds to LZO_E_OUTPUT_OVERRUN.
	//
	// Operationally: ASM-14 is broken -- a capacity problem with a known fix.
	// Completely different from the other three. See MaxDecompressedLen.
	ErrOutputOverrun = errors.New("lzo1z: output overrun")

	// ErrLookbehindOverrun is returned when a match references a
	// position before the start of the output buffer (corrupt distance).
	// Corresponds to LZO_E_LOOKBEHIND_OVERRUN.
	//
	// Operationally: wire corruption.
	ErrLookbehindOverrun = errors.New("lzo1z: lookbehind overrun")

	// ErrInputNotConsumed is returned when the end-of-stream marker was
	// found but trailing bytes remain in the input.
	// Corresponds to LZO_E_INPUT_NOT_CONSUMED.
	//
	// Operationally: iCompLen is larger than the real compressed length -- a
	// framing question, not a compression one.
	ErrInputNotConsumed = errors.New("lzo1z: input not consumed")
)

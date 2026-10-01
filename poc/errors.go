package main

import "errors"

// Errors returned by Decompress / DecompressSafe, mirroring the LZO_E_*
// codes returned by the reference C implementation (liblzo2 lzoconf.h).
var (
	// ErrInputOverrun is returned when the compressed input is truncated:
	// an opcode or match needed more input bytes than were available.
	// Corresponds to LZO_E_INPUT_OVERRUN.
	ErrInputOverrun = errors.New("lzo1z: input overrun")

	// ErrOutputOverrun is returned when decompression would write more
	// bytes than fit in the destination buffer.
	// Corresponds to LZO_E_OUTPUT_OVERRUN.
	ErrOutputOverrun = errors.New("lzo1z: output overrun")

	// ErrLookbehindOverrun is returned when a match references a
	// position before the start of the output buffer (corrupt distance).
	// Corresponds to LZO_E_LOOKBEHIND_OVERRUN.
	ErrLookbehindOverrun = errors.New("lzo1z: lookbehind overrun")

	// ErrInputNotConsumed is returned when the end-of-stream marker was
	// found but trailing bytes remain in the input.
	// Corresponds to LZO_E_INPUT_NOT_CONSUMED.
	ErrInputNotConsumed = errors.New("lzo1z: input not consumed")
)

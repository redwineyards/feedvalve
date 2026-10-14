// This file is a pure Go port of the LZO1Z decompression algorithm from the
// reference C implementation (liblzo2, Markus F.X.J. Oberhumer,
// GPLv2-or-later): the LZO1Z branches of lzo1x_d.ch's DO_DECOMPRESS, with
// config1z.h's M2_MAX_OFFSET. The C source is not vendored in this repo;
// comments below cite it by file and label.
//
// Only decompression is implemented: DecompressSafe (bounds-checked, a
// port of lzo1z_decompress_safe) and Decompress (unchecked, a port of
// lzo1z_decompress). The preset-dictionary variant is out of scope.
//
// Carried over from poc/decompress.go unchanged, which is what Vol 1 section
// 5.2 asks for in as many words: DecompressSafe "is poc/decompress.go:28
// unchanged". The goto/label control flow below mirrors the C source line for
// line and is deliberately not tidied -- its value is that it has been
// exercised against an independent implementation's output since before this
// package existed, and a restructuring would spend that.
package lzo

// m2MaxOffset mirrors M2_MAX_OFFSET from liblzo2 config1z.h.
const m2MaxOffset = 0x0700

// maxUint511 mirrors the C expression `(lzo_uint)0 - 511` used by the
// TEST_IV/TEST_OV overflow guards in liblzo2 lzo1_d.ch.
const maxUint511 = ^uint(0) - 511

// MaxDecompressedLen is the size the receiver gives its single decompression
// buffer (Vol 0 section 9.1's decompBuf, allocated once before the read loop
// and reused per sub-packet). It lives here, next to the bounds checks that
// give it its meaning, rather than in wire -- wire holds protocol constants and
// this is not one.
//
// It is 4.16x the largest broadcast message the bible catalogues: 7202
// MS_TICKER_TRADE_DATA at 484 body + 8 prefix = 492 bytes (Vol 1 section 5.3).
//
// Two things it is NOT, both from section 5.3, because both have been assumed
// in the past:
//
//  1. It is not a correctness bound. DecompressSafe bounds-checks every write
//     against len(dst) and returns ErrOutputOverrun rather than writing past
//     it, so an undersized buffer is a COUNTED REJECT, never memory
//     corruption. A few compressed bytes can legitimately emit hundreds of
//     output bytes, so a corrupt or hostile sub-packet can ask for arbitrary
//     expansion; that is precisely why the packet path may call DecompressSafe
//     and nothing else.
//
//  2. It is not a proof about 7215/7216. Those two are compressed (bible
//     section 3.1) but their layouts are Chapter 8, outside the bible, so
//     their decompressed size is unknown -- which is what ASM-14 assumes away.
//     With 4x headroom over everything that CAN be sized, the first message to
//     break that assumption announces itself as
//     decompress_failed{reason=output_overrun} rather than as bad data.
//
// poc sizes the same buffer at 512 (maxDecompressedPacketLen), which clears the
// 492-byte worst case by twenty bytes and has no margin at all for a structure
// the bible does not cover. That confirms this number rather than contradicting
// it.
const MaxDecompressedLen = 2048

// DecompressSafe decompresses src into dst, a direct port of
// lzo1z_decompress_safe: every input read and output write is bounds
// checked against src/dst, and a match referencing before the start of
// dst is rejected. dst must be pre-sized to (at least) the expected
// decompressed length; use n, the returned count, to know how many
// bytes of dst were written.
//
// Returns ErrInputOverrun, ErrOutputOverrun, ErrLookbehindOverrun, or
// ErrInputNotConsumed on malformed input; nil on success.
//
// This is the only variant the packet path may call, because compressed
// sub-packets are untrusted network input (bible section 3.5).
//
// # dst[:n] is valid for exactly one sub-packet
//
// The receiver owns one decompression buffer and the next compressed
// sub-packet in the same datagram overwrites it (Vol 1 section 5.2). A Decoder
// that retained dst[a:b] in an UpdateUnit would hand a shard a window onto
// bytes the receive goroutine overwrites microseconds later, producing
// cross-contamination between unrelated tokens that looks exactly like a
// decode bug and reproduces in nothing. internal/model/unit.go states the
// invariant from the other side -- UpdateUnit is pointer-free and slice-free;
// this names the buffer whose reuse makes it a live hazard rather than a
// theoretical one.
func DecompressSafe(dst, src []byte) (n int, err error) {
	return decompress(dst, src, true)
}

// Decompress decompresses src into dst, a direct port of the unchecked
// lzo1z_decompress: it performs none of the bounds checks DecompressSafe
// does. The reference C variant has undefined behavior (out-of-bounds
// memory access) on malformed or truncated input; this Go port cannot
// reproduce that without unsafe code, so instead it may panic with an
// out-of-range slice access on such input. DecompressSafe should be
// preferred for any input that isn't already trusted/validated.
//
// Nothing on the receive path may call this. It exists so the tests can check
// the two variants agree on known-good fixtures, and the contrast between the
// two is what gate G-FRM-4 is written against.
func Decompress(dst, src []byte) (n int, err error) {
	return decompress(dst, src, false)
}

// decompress is a line-by-line port of the LZO1Z branches of
// liblzo2 lzo1x_d.ch's DO_DECOMPRESS, with dictionary support (COPY_DICT)
// omitted since it is out of scope. safe selects between the
// lzo1z_decompress_safe (true) and lzo1z_decompress (false) behavior,
// corresponding to whether LZO_TEST_OVERRUN was defined when the C source
// was compiled.
//
// C pointers (ip, op, m_pos) are represented as indices into src/dst.
// All local variables are declared up front so that the goto-based
// control flow below (mirroring the C goto/label structure exactly) is
// legal Go: a goto must never jump into a variable's scope.
func decompress(dst, src []byte, safe bool) (n int, err error) {
	var (
		op, ip   int
		t        int
		mPos     int
		lastMOff int
		off      int
	)
	ipEnd := len(src)
	opEnd := len(dst)

	op = 0
	ip = 0

	// NEED_IP(1)
	if safe && ipEnd-ip < 1 {
		return op, ErrInputOverrun
	}
	if src[ip] > 17 {
		t = int(src[ip]) - 17
		ip++
		if t < 4 {
			goto matchNext
		}
		if safe && opEnd-op < t {
			return op, ErrOutputOverrun
		}
		if safe && ipEnd-ip < t+3 {
			return op, ErrInputOverrun
		}
		for t > 0 {
			dst[op] = src[ip]
			op++
			ip++
			t--
		}
		goto firstLiteralRun
	}

outerLoop:
	// NEED_IP(3)
	if safe && ipEnd-ip < 3 {
		return op, ErrInputOverrun
	}
	t = int(src[ip])
	ip++
	if t >= 16 {
		goto match
	}
	// a literal run
	if t == 0 {
		for src[ip] == 0 {
			t += 255
			ip++
			if safe && uint(t) > maxUint511 {
				return op, ErrInputOverrun
			}
			if safe && ipEnd-ip < 1 {
				return op, ErrInputOverrun
			}
		}
		t += 15 + int(src[ip])
		ip++
	}
	// copy literals: the actual number of literal bytes is t+3 (see
	// liblzo2 lzo1x_d.ch lines 114-163 -- every optimized/fallback
	// variant of this block advances ip/op by t+3, not t).
	if safe && opEnd-op < t+3 {
		return op, ErrOutputOverrun
	}
	if safe && ipEnd-ip < t+6 {
		return op, ErrInputOverrun
	}
	{
		n := t + 3
		for i := range n {
			dst[op+i] = src[ip+i]
		}
		op += n
		ip += n
	}

firstLiteralRun:
	t = int(src[ip])
	ip++
	if t >= 16 {
		goto match
	}
	t = (1 + m2MaxOffset) + (t << 6) + (int(src[ip]) >> 2)
	ip++
	mPos = op - t
	lastMOff = t
	if safe && (mPos < 0 || mPos >= op) {
		return op, ErrLookbehindOverrun
	}
	if safe && opEnd-op < 3 {
		return op, ErrOutputOverrun
	}
	dst[op] = dst[mPos]
	dst[op+1] = dst[mPos+1]
	dst[op+2] = dst[mPos+2]
	op += 3
	goto matchDone

match:
	if t >= 64 { // M2
		off = t & 0x1f
		mPos = op
		if off >= 0x1c {
			mPos -= lastMOff
		} else {
			off = 1 + (off << 6) + (int(src[ip]) >> 2)
			ip++
			mPos -= off
			lastMOff = off
		}
		t = (t >> 5) - 1
		if safe && (mPos < 0 || mPos >= op) {
			return op, ErrLookbehindOverrun
		}
		if safe && opEnd-op < t+2 {
			return op, ErrOutputOverrun
		}
		goto copyMatch
	} else if t >= 32 { // M3
		t &= 31
		if t == 0 {
			for src[ip] == 0 {
				t += 255
				ip++
				if safe && uint(t) > maxUint511 {
					return op, ErrOutputOverrun
				}
				if safe && ipEnd-ip < 1 {
					return op, ErrInputOverrun
				}
			}
			t += 31 + int(src[ip])
			ip++
			if safe && ipEnd-ip < 2 {
				return op, ErrInputOverrun
			}
		}
		off = 1 + (int(src[ip]) << 6) + (int(src[ip+1]) >> 2)
		mPos = op - off
		lastMOff = off
		ip += 2
	} else if t >= 16 { // M4 (long distance)
		mPos = op - ((t & 8) << 11)
		t &= 7
		if t == 0 {
			for src[ip] == 0 {
				t += 255
				ip++
				if safe && uint(t) > maxUint511 {
					return op, ErrOutputOverrun
				}
				if safe && ipEnd-ip < 1 {
					return op, ErrInputOverrun
				}
			}
			t += 7 + int(src[ip])
			ip++
			if safe && ipEnd-ip < 2 {
				return op, ErrInputOverrun
			}
		}
		mPos -= (int(src[ip]) << 6) + (int(src[ip+1]) >> 2)
		ip += 2
		if mPos == op {
			goto eofFound
		}
		mPos -= 0x4000
		lastMOff = op - mPos
	} else { // M1
		t = 1 + (t << 6) + (int(src[ip]) >> 2)
		ip++
		mPos = op - t
		lastMOff = t
		if safe && (mPos < 0 || mPos >= op) {
			return op, ErrLookbehindOverrun
		}
		if safe && opEnd-op < 2 {
			return op, ErrOutputOverrun
		}
		dst[op] = dst[mPos]
		dst[op+1] = dst[mPos+1]
		op += 2
		goto matchDone
	}

	// shared "copy match" prologue for the M3/M4 fall-through case
	// (M2 already checked and jumped straight to copyMatch).
	if safe && (mPos < 0 || mPos >= op) {
		return op, ErrLookbehindOverrun
	}
	if safe && opEnd-op < t+2 {
		return op, ErrOutputOverrun
	}

copyMatch:
	dst[op] = dst[mPos]
	dst[op+1] = dst[mPos+1]
	op += 2
	mPos += 2
	for t > 0 {
		dst[op] = dst[mPos]
		op++
		mPos++
		t--
	}

matchDone:
	t = int(src[ip-1]) & 3
	if t == 0 {
		goto outerLoop
	}

matchNext:
	// copy 1-3 literal bytes, then read the next opcode
	if safe && opEnd-op < t {
		return op, ErrOutputOverrun
	}
	if safe && ipEnd-ip < t+3 {
		return op, ErrInputOverrun
	}
	dst[op] = src[ip]
	op++
	ip++
	if t > 1 {
		dst[op] = src[ip]
		op++
		ip++
		if t > 2 {
			dst[op] = src[ip]
			op++
			ip++
		}
	}
	t = int(src[ip])
	ip++
	goto match

eofFound:
	if ip == ipEnd {
		return op, nil
	}
	if ip < ipEnd {
		return op, ErrInputNotConsumed
	}
	return op, ErrInputOverrun
}

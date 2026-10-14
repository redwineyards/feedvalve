package wire

// The netId oracle (Vol 1 section 6.2).
//
// payload[0] repeats the segment netId (bible section 3.2, section 3.4,
// Appendix A). Comparing it against the configured segment costs one byte
// comparison and is the cheapest misframing detector in the system -- but what
// makes it valuable is WHERE the mismatch lands, which is why this package
// provides only the predicate and the caller supplies the sub-packet index:
//
//	sub-packet 1      -> wrong segment, or the encoding changed
//	sub-packet 2 or N -> the PREVIOUS sub-packet's length was computed wrongly,
//	                     leaving the cursor misaligned
//
// A mismatch is logged and counted (netid_mismatch), NEVER used to drop. The
// encoding assumption is itself unverified: bible section 3.2 declares cNetId
// as CHAR[2] holding a small number, so both the binary byte and the ASCII
// digit are accepted (poc/gobrdclnt.go:209-211). Dropping on a check whose
// encoding we are guessing at would blind the feed on a guess, and gate
// G-FRM-5 exists to stop exactly that: "turning the frame-alignment oracle
// into a filter". Nothing in this file can drop anything -- it returns a bool.

// NetIDMatches reports whether an observed netId byte carries the expected
// segment. expected == 0 disables the comparison (Vol 0 section 7.2), in which
// case every byte matches and observed values are still worth logging at
// startup.
//
// Both encodings are accepted for a single-digit expectation -- the binary byte
// 8 and the ASCII digit '8' -- because the bible declares the field CHAR but
// fills it with a number, and we do not know which the feed sends. The ASCII
// arm only applies to 0..9, since a two-character decimal has no single-byte
// ASCII form and guessing one would start accepting unrelated bytes.
func NetIDMatches(observed byte, expected int) bool {
	if expected == 0 {
		return true
	}
	if int(observed) == expected {
		return true
	}
	return expected >= 1 && expected <= 9 && observed == byte('0'+expected)
}

// EnvelopeNetIDMatches reports whether the envelope's cNetId carries the
// expected segment. It is CHAR[2] holding a small number, so a match in EITHER
// byte is accepted (poc/gobrdclnt.go:218-227) -- the same reasoning as
// NetIDMatches, one level out: we do not know whether the number is written
// left- or right-aligned in the pair.
//
// Checked once per datagram, against the same expectation as each sub-packet's
// payload[0] (Vol 1 section 6.2). A short slice cannot match, because a
// two-byte field that did not arrive is a framing problem rather than a segment
// problem and must not be reported as agreement.
func EnvelopeNetIDMatches(netID []byte, expected int) bool {
	if expected == 0 {
		return true
	}
	if len(netID) < 2 {
		return false
	}
	return NetIDMatches(netID[0], expected) || NetIDMatches(netID[1], expected)
}

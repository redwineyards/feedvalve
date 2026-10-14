package decode

// Layout verification as a build-time gate -- gate G-LAY-1, ADR-012 and Vol 0
// section 2 rule 7.
//
// The technique is poc/exchange_communication.go:132-139 unchanged: a constant
// expression that fails to COMPILE when the arithmetic stops reconciling.
// Nothing here runs. The cost is one unused constant per decoder and the
// benefit is that a layout cannot drift from the specification by accident --
// only deliberately, by editing the number the gate is written against too.
//
// # Two forms, and using the wrong one is a silent weakening
//
// EXACT (two-sided). Compiles only when slack == 0: if slack > 0 then
// uint(-slack) overflows, and if slack < 0 then uint(slack) does. Use it for a
// DECLARED PACKET LENGTH, which the bible's Part V states exactly:
//
//	const mbpLayoutSlack = 470 - (wire.BroadcastHeaderSize + 2 + mbpMaxRecords*mbpRecordSize)
//	const _ = uint(mbpLayoutSlack) + uint(-mbpLayoutSlack)
//
// AT-MOST (one-sided). Compiles only when slack >= 0. Use it for a BOUND, such
// as "this destination's image fits imageSize" or "this decoder's fan-out fits
// the buffer":
//
//	const _ = uint(fanoutBufUnits - maxDecoderUnits)
//
// Choosing the one-sided form where the specification states an exact length
// is the silent weakening: it keeps compiling while the layout shrinks, and a
// layout that is too short decodes the early fields correctly and turns the
// later ones into noise. Nothing alarms.
//
// # Where the per-decoder gates live
//
// With each decoder, in its own file, next to the field sums it checks (P2-06).
// This file holds the idiom and the reasoning; it deliberately does not
// pre-declare constants for decoders that do not exist, because the arithmetic
// is only meaningful beside the reader calls it describes.
//
// Each decoder's gate is the bible Part V row for its structure, written as
// arithmetic the compiler checks: 7208 declared 470 with an interior pad at
// record offset 17 and a stride of 214; 7200 declared 410, field sum 409,
// delta +1; 7305/7340 declared 294 with NO padding anywhere; the 6511 family
// declared 320 with a trailing pad at 319; 6541 header-only at 8 + 40; 6503
// declared 52, whose LONG at offset 46 is the pack(2) proof.
//
// # The 7305 gate is the one that must NOT be "fixed"
//
// Bible section 2.3 warns: "if it already equals the declared length with no
// padding, add none -- do not align blindly".
// ST_SEC_ELIGIBILITY_PER_MARKET is a 3-byte struct whose 4-element array
// occupies 12 bytes, the one odd-sized struct in the protocol that is not
// padded to an even length (Appendix B item 8). A gate written as 4*4 = 16
// would compile, produce 298 against a declared 294, and fail the build --
// which is the gate WORKING. A gate "corrected" to 298 so the build passes
// would ship a parser that is four bytes wrong from offset 116 onward, and
// every field after it would be plausible garbage.
//
// # What the gate does not catch, stated so nobody assumes it does
//
// It verifies that OUR declared sizes reconcile with THE BIBLE's declared
// length. It cannot verify that either matches the wire. That is what
// body_len_mismatch{code} (section 6.7) and the differential harness
// (section 8.6) are for -- three independent checks on the same property,
// which for the class of bug Appendix B documents is proportionate rather than
// paranoid.

// # Where a gate lives
//
// NEXT TO THE CONSTANT IT PINS, in the same file, which wire/const.go already
// does for G-BLD-1 and which this file deliberately does not do: there are no
// gate expressions here, only the idiom and the reasoning.
//
// That placement is not tidiness. The mutation harness
// (internal/app/gates_test.go) changes one constant and requires the resulting
// compile error to name THE FILE IT CHANGED -- otherwise a mutation that
// happened to break some unrelated expression would pass as proof that the gate
// works. A gate declared away from its constant fails that check, which is how
// this paragraph came to be written: the first version of P2-05 put
// maxRegistered's gate here and the harness refused it.
//
// So maxRegistered's gate is in registry.go, maxDecoderUnits' is in decoder.go,
// and each decoder's will be in its own file.

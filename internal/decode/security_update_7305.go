package decode

import (
	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// 7305 and 7340, MS_SECURITY_UPDATE_INFO -- bible section 4.3, Part V.
// Destination 1 MD_SECURITY_MASTER, one unit per message, NOT compressed.
//
// ONE IMPLEMENTATION, TWO REGISTRATIONS. 7340
// (BCAST_SEC_MSTR_CHNG_PERIODIC) is the identical structure re-sent
// periodically during the day for every security that changed, so the decoder
// is shared and Code() distinguishes them -- which is what gives them different
// contributor slots and different packets_by_transcode buckets while sharing
// one layout and one blob shape.
//
// # Declared 294, field sum 294, delta ZERO: no padding anywhere
//
// This is the structure where ADDING alignment is the error. Bible section 2.3
// is explicit: "MS_SECURITY_UPDATE_INFO sums to exactly 294 and contains an
// unpadded 3-byte ST_SEC_ELIGIBILITY_PER_MARKET; adding alignment there would
// break it."
//
// ST_SEC_ELIGIBILITY_PER_MARKET is a 3-byte struct -- Eligibility BIT 1 plus
// Reserved BIT 7 in byte 0, Status SHORT in bytes 1 and 2 (bible section 4.14)
// -- and it is THE ONE ODD-SIZED STRUCT IN THE PROTOCOL THAT IS NOT PADDED TO
// EVEN (Appendix B item 8). Its array of four therefore occupies 12 bytes and
// not 16. A gate written as 4*4 would compile, produce 298 against the declared
// 294, and FAIL THE BUILD -- which is the gate working. A gate "corrected" to
// 298 so the build passes would ship a parser four bytes wrong from offset 116
// onward, and every field after it would be plausible garbage.
const (
	secDeclaredLen = 294 // bible section 4.3
	secInfoLen     = 30  // SEC_INFO, bible Chapter 5
	secEligStride  = 3   // *** NOT 4 *** -- Appendix B item 8
	secEligCount   = 4
	secEligLen     = secEligCount * secEligStride // 12, not 16

	// Payload offsets, from the table in Vol 1 section 9.4.
	secBody0 = wire.BroadcastDataPrefixLen + wire.BroadcastHeaderSize // 48
)

// The layout gate (G-LAY-1), two-sided: Part V states 294 exactly and the
// field sum reaches it with no padding.
//
//	40 header + 4 Token + 30 SEC_INFO + 2 + 8 + 4 + 4 + 12 CreditRating
//	+ 12 eligibility + 2 + 4*6 dates + 4 + 4 + 4 + 4 + 25 Name + 1 Reserved
//	+ 4*9 dates + 2 + 4*5 + 6 + 10 + 4 + 4 + 4 + 2 + 4 + 1 + 25 Remark + 4
const secLayoutSlack = secDeclaredLen - (wire.BroadcastHeaderSize +
	4 + // Token
	secInfoLen + // SEC_INFO
	2 + 8 + 4 + 4 + 12 + // PermittedToTrade, IssuedCapital, Warning, Freeze, CreditRating
	secEligLen + // ST_SEC_ELIGIBILITY_PER_MARKET[4] -- 12, the whole point
	2 + 4 + 4 + 4 + 4 + 4 + // IssueRate .. MinimumLotQuantity
	4 + 4 + // BoardLotQuantity, TickSize
	25 + 1 + // Name, Reserved (a real field whose FUNCTION is alignment)
	4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + // ListingDate .. NoDeliveryEndDate
	2 + // ST_ELIGIBILITY_INDICATORS
	4 + 4 + 4 + 4 + 4 + // BookClosure/Exercise dates, Multiplier
	6 + 10 + 4 + 4 + 4 + // AssetInstrument, AssetName, tokens and values
	2 + // ST_PURPOSE
	4 + // LocalUpdateDateTime
	1 + 25 + // DeleteFlag, Remark
	4) // BasePrice

const _ = uint(secLayoutSlack) + uint(-secLayoutSlack)

// SEC_INFO's own sum, and the eligibility array's stride -- the two places this
// structure is most often got wrong.
const (
	secInfoSlack = secInfoLen - (6 + 10 + 2 + 4 + 4 + 2 + 2)

	// THE GATE THAT MUST NOT BE "FIXED". Four elements of three bytes is 12.
	// Writing 4*4 here compiles and then fails secLayoutSlack above with a
	// total of 298, which is the gate catching the error rather than the error
	// reaching the wire.
	secEligSlack = secEligLen - 12
)

const (
	_ = uint(secInfoSlack) + uint(-secInfoSlack)
	_ = uint(secEligSlack) + uint(-secEligSlack)
)

// Destination 1's mask bits, from internal/destinations/mapping.json.
const (
	s1Symbol      uint8 = 0
	s1Series      uint8 = 1
	s1ExpiryDate  uint8 = 2
	s1StrikePrice uint8 = 3
	s1BoardLotQty uint8 = 4
	s1TickSize    uint8 = 5
	s1BasePrice   uint8 = 6
	s1DeleteFlag  uint8 = 7
	s1EffectiveTS uint8 = 8
)

func init() {
	// The same constructor under both codes. Each gets its OWN instance, so
	// each carries its own Code() and its own registry entry -- which is what
	// keeps their contributor slots and counters separate.
	register(7305, func(c Counters, e *RegistryEntry) Decoder {
		return &securityUpdate{code: 7305, cnt: c, entry: e}
	})
	register(7340, func(c Counters, e *RegistryEntry) Decoder {
		return &securityUpdate{code: 7340, cnt: c, entry: e}
	})
}

type securityUpdate struct {
	code  model.TransCode
	cnt   Counters
	entry *RegistryEntry
	r     wire.FieldReader
	trace wire.TraceSink
}

// SetTrace attaches the field-trace sink for the next Decode, or nil.
func (d *securityUpdate) SetTrace(t wire.TraceSink) { d.trace = t }

func (d *securityUpdate) Code() model.TransCode { return d.code }
func (d *securityUpdate) BodyLen() int          { return secDeclaredLen }
func (d *securityUpdate) MaxUnits() int         { return 1 }

// Decode reads the one security this message describes.
//
// Forty-seven fields with no padding anywhere is exactly where an implementer
// needs the checklist rather than prose, so every field appears below in wire
// order -- skipped ones included. The payload offset is in the comment column,
// which makes this readable straight against bible section 4.3's table.
func (d *securityUpdate) Decode(h model.MsgHeader, payload []byte,
	out []model.UpdateUnit) (int, error) {

	if len(payload) < wire.BroadcastDataPrefixLen+secDeclaredLen {
		return 0, ErrShortBody
	}
	if len(out) < 1 {
		return 0, nil
	}

	e := d.entry
	spec := &e.Contrib[destSecurityMaster]
	u := &out[0]
	r := &d.r
	r.Reset(payload[secBody0:wire.BroadcastDataPrefixLen+secDeclaredLen], secBody0, d.trace)

	stamp(u, h, e)

	token := r.I32(wire.FieldToken) // pay  48
	u.Token = token
	contribute(u, destSecurityMaster, model.SingleKey(uint32(token)), e)

	// SEC_INFO, bible Chapter 5: 30 bytes, four of whose seven fields are
	// stored.
	r.Skip(wire.FieldInstrumentName, 6) // pay  52

	// Symbol and Series are FIXED-WIDTH TEXT, not length-prefixed: their
	// widths come from the wire and never vary, so the whole 10 or 2 bytes are
	// copied verbatim -- blanks included -- and the writer right-trims at bind
	// time. Bible section 2.6: blank-padded, NEVER NUL-terminated, so a
	// strlen-style read is a bug. Copying verbatim is also what keeps the
	// digest stable (Vol 0 section 5.2 normalisation rule 3).
	put(u, spec, s1Symbol, r.Raw(wire.FieldSymbol, 10))     // pay  58
	put(u, spec, s1Series, r.Raw(wire.FieldSeries, 2))      // pay  68
	put(u, spec, s1ExpiryDate, r.Raw(wire.FieldExpiry, 4))  // pay  70, Time1980
	put(u, spec, s1StrikePrice, r.Raw(wire.FieldStrike, 4)) // pay  74, scaled

	r.Skip(wire.FieldOptionType, 2) // pay  78
	r.Skip(wire.FieldCALevel, 2)    // pay  80

	r.Skip(wire.FieldPermittedToTrade, 2) // pay  82
	r.Skip(wire.FieldIssuedCapital, 8)    // pay  84, DOUBLE
	r.Skip(wire.FieldWarningQuantity, 4)  // pay  92
	r.Skip(wire.FieldFreezeQuantity, 4)   // pay  96
	r.Skip(wire.FieldCreditRating, 12)    // pay 100

	// *** STRIDE 3, NOT 4 *** -- the one unpadded odd-sized struct in the
	// protocol. Four of them occupy 12 bytes. Skipped as one run because no
	// destination-1 column consumes it, but the LENGTH is load-bearing:
	// reading 16 here shifts every field below by four bytes.
	r.Skip(wire.FieldSecEligibility, secEligLen) // pay 112

	r.Skip(wire.FieldIssueRate, 2)           // pay 124
	r.Skip(wire.FieldIssueStartDate, 4)      // pay 126
	r.Skip(wire.FieldInterestPaymentDate, 4) // pay 130
	r.Skip(wire.FieldIssueMaturityDate, 4)   // pay 134
	r.Skip(wire.FieldMarginPercentage, 4)    // pay 138
	r.Skip(wire.FieldMinimumLotQuantity, 4)  // pay 142

	put(u, spec, s1BoardLotQty, r.Raw(wire.FieldBoardLotQuantity, 4)) // pay 146
	put(u, spec, s1TickSize, r.Raw(wire.FieldTickSize, 4))            // pay 150, scaled

	r.Skip(wire.FieldName, 25) // pay 154

	// A REAL FIELD, not implicit padding (bible section 4.3's warning). Its
	// function is to re-align the LONG that follows the 25-byte Name, but it
	// is in the specification's table, so it is read with Skip and not Pad --
	// a distinction that is visible in the trace and is the honest one.
	r.Skip(wire.FieldReserved1, 1) // pay 179

	r.Skip(wire.FieldListingDate, 4)           // pay 180
	r.Skip(wire.FieldExpulsionDate, 4)         // pay 184
	r.Skip(wire.FieldReAdmissionDate, 4)       // pay 188
	r.Skip(wire.FieldRecordDate, 4)            // pay 192
	r.Skip(wire.FieldLowPriceRange, 4)         // pay 196
	r.Skip(wire.FieldHighPriceRange, 4)        // pay 200
	r.Skip(wire.FieldExpiry, 4)                // pay 204, the outer ExpiryDate
	r.Skip(wire.FieldNoDeliveryStartDate, 4)   // pay 208
	r.Skip(wire.FieldNoDeliveryEndDate, 4)     // pay 212
	r.Skip(wire.FieldEligibilityIndicators, 2) // pay 216, bit field
	r.Skip(wire.FieldBookClosureStartDate, 4)  // pay 218
	r.Skip(wire.FieldBookClosureEndDate, 4)    // pay 222
	r.Skip(wire.FieldExerciseStartDate, 4)     // pay 226
	r.Skip(wire.FieldExerciseEndDate, 4)       // pay 230
	r.Skip(wire.FieldMultiplier, 4)            // pay 234
	r.Skip(wire.FieldAssetInstrument, 6)       // pay 238
	r.Skip(wire.FieldAssetName, 10)            // pay 244
	r.Skip(wire.FieldUnderlyingToken, 4)       // pay 254
	r.Skip(wire.FieldIntrinsicValue, 4)        // pay 258
	r.Skip(wire.FieldExtrinsicValue, 4)        // pay 262
	r.Skip(wire.FieldSTPurpose, 2)             // pay 266, bit field
	r.Skip(wire.FieldLocalUpdateDateTime, 4)   // pay 268

	put(u, spec, s1DeleteFlag, r.Raw(wire.FieldDeleteFlag, 1)) // pay 272, 'Y' / 'N'

	r.Skip(wire.FieldRemark, 25) // pay 273

	put(u, spec, s1BasePrice, r.Raw(wire.FieldBasePrice, 4)) // pay 298, scaled

	putI32(u, spec, s1EffectiveTS, h.LogTime)

	if !sealBlob(u, d.code, d.cnt) {
		return 0, nil
	}
	return 1, nil
}

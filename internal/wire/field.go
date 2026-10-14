package wire

// FieldID names a parsed field without formatting a string (ADR-L07).
//
// It is a uint16 index into the table below. Passing one costs nothing and
// resolving it to text happens only when a trace sink is attached, which is
// only when an operator is diagnosing one transcode. poc builds the name of
// every field with fmt.Sprintf unconditionally -- a 7208 record is ~28 named
// fields plus 40 ladder fields formatted twice each, around 1.6 million
// allocations per second at the planning rate -- and that is the cost this
// type exists to remove.
type FieldID uint16

// The header fields of both layouts (bible section 2.8), in wire order.
//
// Names keep the BIBLE's spelling, not poc's, per Vol 0 section 10.5: this
// design traces BCSeqNo where poc traces header.sequenceNumber, so a grep for
// either finds the right thing. That rename is deliberate and it is why the
// differential harness of gate G-DIFF-1 diffs on (offset, hex, value) with an
// explicit name-mapping table rather than on names (Vol 1 section 8.6).
//
// The two layouts share four entries -- ErrorCode and MessageLength are at the
// same offset in both (bible section 2.8), and TransactionCode and LogTime are
// at DIFFERENT offsets but mean the same field, which is exactly why the parse
// is per-kind rather than one shared sequence.
const (
	FieldInvalid FieldID = iota

	// Common to both layouts.
	FieldTransactionCode
	FieldLogTime
	FieldErrorCode
	FieldMessageLength
	FieldAlphaChar
	FieldTimeStamp2

	// BCAST_HEADER only.
	FieldReserved1
	FieldReserved2
	FieldBCSeqNo
	FieldReserved3
	FieldReserved4
	FieldFiller2

	// MESSAGE_HEADER only.
	FieldTraderId
	FieldTimestamp
	FieldTimeStamp1

	numFieldIDs
)

// fieldNames is the index FieldID points into. Decoder field names join it in
// P2-05 and P2-06; today it holds the header fields and nothing else, because
// the headers are all Vol 1 section 6 parses.
var fieldNames = [numFieldIDs]string{
	FieldInvalid: "<invalid>",

	FieldTransactionCode: "TransactionCode",
	FieldLogTime:         "LogTime",
	FieldErrorCode:       "ErrorCode",
	FieldMessageLength:   "MessageLength",
	FieldAlphaChar:       "AlphaChar",
	FieldTimeStamp2:      "TimeStamp2",

	FieldReserved1: "Reserved1",
	FieldReserved2: "Reserved2",
	FieldBCSeqNo:   "BCSeqNo",
	FieldReserved3: "Reserved3",
	FieldReserved4: "Reserved4",
	FieldFiller2:   "Filler2",

	FieldTraderId:   "TraderId",
	FieldTimestamp:  "Timestamp",
	FieldTimeStamp1: "TimeStamp1",
}

// String resolves the name. Only a trace sink calls it, so the bounds check
// costs nothing on the hot path -- where it is never reached at all.
func (f FieldID) String() string {
	if f >= numFieldIDs {
		return "<unknown field>"
	}
	return fieldNames[f]
}

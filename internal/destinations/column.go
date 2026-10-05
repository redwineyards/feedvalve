package destinations

import "fmt"

// ColKind says how the writer converts an image field at bind time. The image
// itself holds WIRE FORM -- big-endian, wire width, unscaled, unconverted
// (Vol 0 section 4.1 R1) -- so every conversion named here happens once per
// written row at bind time, off the hot path.
type ColKind uint8

const (
	// KindLong is a 4-byte signed LONG (bible section 2.7).
	KindLong ColKind = iota
	// KindShort is a 2-byte signed SHORT.
	KindShort
	// KindChar1 is a single flag character, such as 'Y'/'N' or '+'/'-'.
	KindChar1
	// KindDouble is an 8-byte IEEE-754 big-endian double.
	KindDouble
	// KindText is character data stored at its declared maximum width and
	// zero-filled beyond the valid length (R5).
	//
	// Where the valid length comes from is LenBit, not the kind. LenBit >= 0
	// names another image field holding the length; LenBit == -1 means the
	// length is implicit and the writer right-trims blanks at bind time
	// (bible section 2.6: blank-padded, never NUL-terminated). The second form
	// is what Vol 0 section 4.4 calls "Text-fixed".
	KindText
	// KindTime1980 is a LONG of seconds since 1 Jan 1980, converted to an
	// Oracle TIMESTAMP at bind time (bible section 2.5; Unix offset 315532800).
	KindTime1980
	// KindGroup is an opaque run expanding to several SQL columns (R7).
	// Destination 0's 120-byte MBP ladder is one bit and forty columns.
	KindGroup
)

var kindNames = map[string]ColKind{
	"long":     KindLong,
	"short":    KindShort,
	"char1":    KindChar1,
	"double":   KindDouble,
	"text":     KindText,
	"time1980": KindTime1980,
	"group":    KindGroup,
}

func (k ColKind) String() string {
	for name, kind := range kindNames {
		if kind == k {
			return name
		}
	}
	return fmt.Sprintf("ColKind(%d)", uint8(k))
}

// parseKind resolves a mapping-file kind name.
func parseKind(s string) (ColKind, error) {
	k, ok := kindNames[s]
	if !ok {
		return 0, fmt.Errorf("unknown column kind %q", s)
	}
	return k, nil
}

// Column is one maskable image field.
//
// One bit is one image FIELD, which may be several SQL columns (R7): splitting
// destination 0's ladder into forty bits would spend forty of the sixty-four
// available bits to express a distinction the exchange never makes, since 7208
// always supplies the whole ladder.
type Column struct {
	Bit    uint8   // ColMask bit index, and the index of this column in Destination.Columns
	Offset uint16  // byte offset within Entry.Image; DERIVED from the declared order (R2)
	Width  uint16  // bytes occupied, at the wire width
	Kind   ColKind // how the writer converts it at bind time
	Scaled bool    // divide by 10^Scale at bind time (bible section 2.4)
	Scale  uint8   // the divisor's exponent. Scaled says WHETHER; this says BY WHAT
	LenBit int8    // mask bit of the field supplying this field's valid length;
	// -1 when the length is implicit. Destination 2's message_text
	// takes its length from message_len, a DIFFERENT image field,
	// and without this the binder cannot be generated from the
	// column table at all
	SQL []string // one or more SQL column names, in bind order (R7)

	// Pre-validation metadata: the DDL facts the mirror needs to refuse a
	// value before Oracle has to (Vol 0 section 5A).
	//
	// These live here, in destinations, and not in oradb, because mirror
	// validates at merge time and mirror may not import the database
	// packages (section 1.3).
	SQLPrecision uint8  // NUMBER(p,s) total digits; 0 means "not a NUMBER"
	SQLMaxBytes  uint16 // VARCHAR2/CHAR byte capacity; 0 means "not character"
	NotNull      bool   // the column is NOT NULL in the DDL. ADVISORY ONLY --
	// writes are MERGE, so the insert branch fires only for a
	// row Oracle does not have, and the mirror knows only
	// whether IT has written the row. Warn, never quarantine
	Domain []string // permitted values of a flag column; nil is unconstrained
}

// Name is the column's first SQL name, for log lines and error messages. A
// group column has several; this is the one that identifies it.
func (c *Column) Name() string {
	if len(c.SQL) == 0 {
		return ""
	}
	return c.SQL[0]
}

// End is the offset one past this column's last image byte.
func (c *Column) End() uint16 { return c.Offset + c.Width }

// HasExplicitLength reports whether another image field supplies this field's
// valid length. When false, a text field is blank-padded to its full width and
// the writer right-trims at bind time.
func (c *Column) HasExplicitLength() bool { return c.LenBit >= 0 }

// RowKeyRule says how a destination's RowKey is derived from a unit.
//
// ASM-15 constrains this and nothing else does: the key MUST be derivable from
// the shard key of every contributing transcode. Violate it and two shards
// write one row, which produces intermittent lost updates and is invisible
// until production. The cross-check against the transcode table is a
// -selfcheck rule (P1-06); this type is what it checks.
type RowKeyRule uint8

const (
	// KeyToken derives the row key from the unit's token.
	KeyToken RowKeyRule = iota
	// KeyMarketType derives it from the market type, for the single
	// market-status row per market.
	KeyMarketType
)

var rowKeyNames = map[string]RowKeyRule{
	"token":      KeyToken,
	"marketType": KeyMarketType,
}

func (r RowKeyRule) String() string {
	for name, rule := range rowKeyNames {
		if rule == r {
			return name
		}
	}
	return fmt.Sprintf("RowKeyRule(%d)", uint8(r))
}

// parseRowKey resolves a mapping-file row-key rule.
//
// Vol 0 section 7.2 also allows "a declared composite". None of the three
// destinations needs one, and RowKey already encodes the two-component form
// (model.CompositeKey), so the parser rejects anything else by name rather
// than guessing -- a row key silently defaulting to the wrong field is exactly
// the ASM-15 failure.
func parseRowKey(s string) (RowKeyRule, error) {
	r, ok := rowKeyNames[s]
	if !ok {
		return 0, fmt.Errorf("unknown rowKey %q (want \"token\" or \"marketType\"; "+
			"composites are not yet declared by any destination)", s)
	}
	return r, nil
}

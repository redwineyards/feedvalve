package destinations

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"feedvalve/internal/model"
)

// UpdatedAtColumn is the writer-owned clock column. It is in every statement
// and in no mapping: R4 makes it unmasked and derived (Vol 0 section 4.1), so a
// data column of that name would be bound twice.
const UpdatedAtColumn = "updated_at"

// ErrEmptyMask is returned for a mask that selects no data column. A MERGE
// with an empty SET list is ORA-00936, which points nowhere near the cause
// (V3 section 3.5), so the generator refuses instead of emitting it.
var ErrEmptyMask = errors.New("destinations: column mask selects no column")

// BindRole says where a bind's value comes from.
type BindRole uint8

const (
	// RoleKey is the destination's RowKey. It has no image bytes.
	RoleKey BindRole = iota
	// RoleData is a slice of Entry.Image, located by Offset and Width.
	RoleData
	// RoleUpdatedAt is the writer's batch clock. It has no image bytes.
	RoleUpdatedAt
)

// Bind describes one placeholder. BindPlan[i] is placeholder :i+1.
//
// For RoleData, Offset and Width are ABSOLUTE within Entry.Image and already
// include a ladder record's position, so a binder reads exactly
// image[Offset:Offset+Width] and converts it by Kind. Scale is the divisor
// exponent the SQL applies; it is zero for an unscaled column. The binder never
// divides: Oracle does, in the statement text (V3 section 3.2).
type Bind struct {
	Role   BindRole
	Name   string  // the SQL column this bind feeds
	Bit    int     // ColMask bit; -1 for the key and updated_at
	Kind   ColKind // wire form of the bytes at Offset
	Offset uint16
	Width  uint16
	LenBit int8  // as Column.LenBit; -1 when the length is implicit or not text
	Scale  uint8 // 0 when the SQL does not divide
	Ladder int   // index into the group's SQL list; -1 outside a group
}

// BindPlan is the ordered bind list of one statement.
type BindPlan []Bind

// A group column expands record by record: attribute 0 of record 0, attribute
// 1, attribute 2, then record 1, and so on, which is the order the mapping
// lists its SQL names in. Destination 0's ladder is MBP_INFORMATION[10]: ten
// 12-byte records of Quantity LONG, Price LONG, NumberOfOrders SHORT and the
// BbBuySellFlag SHORT the mapping discards (Vol 0 section 4.3). Only the price
// is scaled. That layout is protocol knowledge, so it is written out here
// rather than inferred from the column names.
const ladderRecordSize = 12

var ladderAttrs = [...]struct {
	kind   ColKind
	offset uint16
	width  uint16
	scaled bool
}{
	{KindLong, 0, 4, false},  // quantity
	{KindLong, 4, 4, true},   // price
	{KindShort, 8, 2, false}, // number of orders
}

// bindsOf appends the binds one column contributes.
func bindsOf(plan BindPlan, c *Column) (BindPlan, error) {
	if c.Kind != KindGroup {
		b := Bind{Role: RoleData, Name: c.SQL[0], Bit: int(c.Bit), Kind: c.Kind,
			Offset: c.Offset, Width: c.Width, LenBit: -1, Ladder: -1}
		if c.Kind == KindText {
			b.LenBit = c.LenBit
		}
		if c.Scaled {
			b.Scale = c.Scale
		}
		return append(plan, b), nil
	}

	n := len(ladderAttrs)
	if len(c.SQL)%n != 0 || len(c.SQL)/n*ladderRecordSize != int(c.Width) {
		return nil, fmt.Errorf("group %q: %d SQL columns do not fill %d bytes as %d-byte records of %d attributes",
			c.Name(), len(c.SQL), c.Width, ladderRecordSize, n)
	}
	for j, name := range c.SQL {
		a := ladderAttrs[j%n]
		b := Bind{Role: RoleData, Name: name, Bit: int(c.Bit), Kind: a.kind,
			Offset: c.Offset + uint16(j/n*ladderRecordSize) + a.offset, Width: a.width,
			LenBit: -1, Ladder: j}
		if a.scaled && c.Scaled {
			b.Scale = c.Scale
		}
		plan = append(plan, b)
	}
	return plan, nil
}

// divisor renders 10^scale without arithmetic that could overflow.
func divisor(scale uint8) string { return "1" + strings.Repeat("0", int(scale)) }

// StatementFor generates the MERGE for one destination and column mask, and
// the bind plan that goes with it (V3 sections 3.1 to 3.5).
//
// It is pure: no database/sql, no state, deterministic for its inputs. The text
// starts with MERGE, holds no literal and no comment, numbers its placeholders
// :1..:n ascending and each exactly once, names the key only in ON and INSERT,
// divides scaled columns by 10^Scale, and always carries updated_at.
//
// Every identifier is re-checked here even though the loader checked it, so a
// Destination built any other way cannot put a name into SQL unvalidated.
func StatementFor(d *Destination, mask model.ColMask) (string, BindPlan, error) {
	if mask.Empty() {
		return "", nil, ErrEmptyMask
	}
	if extra := mask.Without(d.FullMask()); !extra.Empty() {
		return "", nil, fmt.Errorf("destinations: %s: mask %#x names bits this destination does not declare (%#x)",
			d.Name, uint64(mask), uint64(extra))
	}
	if err := validIdent(d.Table); err != nil {
		return "", nil, fmt.Errorf("destinations: %s: table: %w", d.Name, err)
	}
	if err := validIdent(d.KeyColumn); err != nil {
		return "", nil, fmt.Errorf("destinations: %s: keyColumn: %w", d.Name, err)
	}

	plan := BindPlan{{Role: RoleKey, Name: d.KeyColumn, Bit: -1, Kind: KindLong, LenBit: -1, Ladder: -1}}
	for i := range d.Columns {
		c := &d.Columns[i]
		if !mask.Has(c.Bit) {
			continue
		}
		for _, name := range c.SQL {
			if err := validIdent(name); err != nil {
				return "", nil, fmt.Errorf("destinations: %s: %w", d.Name, err)
			}
		}
		var err error
		if plan, err = bindsOf(plan, c); err != nil {
			return "", nil, fmt.Errorf("destinations: %s: %w", d.Name, err)
		}
	}
	plan = append(plan, Bind{Role: RoleUpdatedAt, Name: UpdatedAtColumn, Bit: -1, Kind: KindTime1980, LenBit: -1, Ladder: -1})

	// value is the source-row reference for a bind, with the scaling the
	// statement performs.
	value := func(b Bind) string {
		if b.Scale > 0 {
			return "s." + b.Name + " / " + divisor(b.Scale)
		}
		return "s." + b.Name
	}

	var sb strings.Builder
	sb.WriteString("MERGE INTO " + d.Table + " t\nUSING (SELECT\n")
	for i, b := range plan {
		sb.WriteString("  :" + strconv.Itoa(i+1) + " AS " + b.Name)
		if i < len(plan)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("  FROM dual) s\nON (t." + d.KeyColumn + " = s." + d.KeyColumn + ")\nWHEN MATCHED THEN UPDATE SET\n")
	// The key is skipped: Oracle forbids updating a column named in ON.
	for i, b := range plan[1:] {
		sb.WriteString("  t." + b.Name + " = " + value(b))
		if i < len(plan)-2 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("WHEN NOT MATCHED THEN INSERT (\n")
	for i, b := range plan {
		sb.WriteString("  " + b.Name)
		if i < len(plan)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString(")\nVALUES (\n")
	for i, b := range plan {
		sb.WriteString("  " + value(b))
		if i < len(plan)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString(")")
	return sb.String(), plan, nil
}

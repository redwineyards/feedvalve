package writer

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
)

// DefaultTime1980UnixOffsetSec is the Unix time of 1980-01-01T00:00:00 UTC
// (bible section 2.5). It is configuration (runtime.time1980UnixOffsetSec);
// this is the value a Binder starts with.
const DefaultTime1980UnixOffsetSec = 315532800

// char1Strings holds every one-byte string, so a flag column binds without
// allocating.
var char1Strings = func() (t [256]string) {
	for i := range t {
		t[i] = string([]byte{byte(i)})
	}
	return
}()

// bcol is one bind position: its plan entry, where a text length lives, and
// the one typed arena that matches its kind. Arenas are allocated once at the
// ceiling and re-sliced to the run length; they are concretely typed because
// go-ora types a whole array from its first element, and an untyped nil in an
// []any would make the column NCHAR (V3 section 5.6 note 4).
type bcol struct {
	bind   destinations.Bind
	lenOff int // image offset of the length field for explicit-length text, else -1
	dest   int
	i64    []int64
	str    []sql.NullString
	f64    []sql.NullFloat64
	tm     []sql.NullTime
}

// fit re-slices the column's arena to n elements, reporting false when the
// arena holds fewer (which a correct Binder never sees; the check makes the
// failure an error before any exec instead of a panic or a silent truncation).
func (c *bcol) fit(n int) bool {
	switch {
	case c.i64 != nil:
		if cap(c.i64) < n {
			return false
		}
		c.i64 = c.i64[:n]
	case c.f64 != nil:
		if cap(c.f64) < n {
			return false
		}
		c.f64 = c.f64[:n]
	case c.str != nil:
		if cap(c.str) < n {
			return false
		}
		c.str = c.str[:n]
	default:
		if cap(c.tm) < n {
			return false
		}
		c.tm = c.tm[:n]
	}
	return true
}

// box returns the arena as the bind argument.
func (c *bcol) box() any {
	switch {
	case c.i64 != nil:
		return c.i64
	case c.f64 != nil:
		return c.f64
	case c.str != nil:
		return c.str
	}
	return c.tm
}

// Binder turns the masked fields of a run of BatchRows into positional bind
// arrays for one statement identity (V3 section 5.1). It follows
// destinations.StatementFor's BindPlan exactly: position i is placeholder
// :i+1. A Binder is not safe for concurrent use.
type Binder struct {
	// Inst receives the bind_nonfinite and bind_bad_time counts; nil is fine.
	Inst *Instruments
	// Time1980UnixOffsetSec converts wire seconds to Unix seconds.
	Time1980UnixOffsetSec int64

	mask    model.ColMask
	ceiling int
	cols    []bcol
	args    []any
	lastN   int // length the boxed args were built for; 0 when stale
}

// NewBinder builds the binder for one (destination, mask) statement identity.
// ceiling is the largest run Bind will be given.
func NewBinder(d *destinations.Destination, mask model.ColMask, ceiling int) (*Binder, error) {
	if ceiling < 1 {
		return nil, fmt.Errorf("writer: binder ceiling %d must be at least 1", ceiling)
	}
	_, plan, err := destinations.StatementFor(d, mask)
	if err != nil {
		return nil, err
	}
	b := &Binder{Time1980UnixOffsetSec: DefaultTime1980UnixOffsetSec, mask: mask,
		ceiling: ceiling, cols: make([]bcol, len(plan)), args: make([]any, len(plan))}
	for i, p := range plan {
		c := &b.cols[i]
		c.bind, c.lenOff, c.dest = p, -1, int(d.ID)
		if p.Role == destinations.RoleData {
			if int(p.Offset)+int(p.Width) > model.ImageSize {
				return nil, fmt.Errorf("writer: %s: column %s ends at %d, past the %d-byte image",
					d.Name, p.Name, int(p.Offset)+int(p.Width), model.ImageSize)
			}
			if p.Kind == destinations.KindText && p.LenBit >= 0 {
				lc := d.Column(uint8(p.LenBit))
				if lc == nil || lc.Kind != destinations.KindShort {
					return nil, fmt.Errorf("writer: %s: column %s takes its length from bit %d, which is not a short",
						d.Name, p.Name, p.LenBit)
				}
				c.lenOff = int(lc.Offset)
			}
		}
		switch {
		case p.Role == destinations.RoleUpdatedAt:
			c.tm = make([]sql.NullTime, ceiling)
		case p.Role == destinations.RoleKey, p.Kind == destinations.KindLong, p.Kind == destinations.KindShort:
			c.i64 = make([]int64, ceiling)
		case p.Kind == destinations.KindDouble:
			c.f64 = make([]sql.NullFloat64, ceiling)
		case p.Kind == destinations.KindTime1980:
			c.tm = make([]sql.NullTime, ceiling)
		case p.Kind == destinations.KindChar1, p.Kind == destinations.KindText:
			c.str = make([]sql.NullString, ceiling)
		default:
			return nil, fmt.Errorf("writer: %s: column %s has kind %s, which cannot be bound", d.Name, p.Name, p.Kind)
		}
	}
	return b, nil
}

// Bind fills the arenas from rows and returns the positional argument slice:
// one concretely typed slice per placeholder, each exactly len(rows) long,
// position 0 the row key and the last position updatedAt, the same value for
// every row of the batch. args is owned by the Binder and valid until the
// next Bind. The boxed slice headers are reused while len(rows) repeats, so a
// steady-state call allocates only for text values.
func (b *Binder) Bind(rows []model.BatchRow, updatedAt time.Time) ([]any, error) {
	return b.bind(rows, nil, len(rows), updatedAt)
}

// BindIdx is Bind over the rows of a batch named by idx, in idx order: the
// form the writer uses, because the grouper returns absolute indices into
// Batch.Rows and Rows is never copied or reordered (D7). Same arenas, same
// mask check, same W-3 length assertion. An index outside rows is an error,
// not a panic.
func (b *Binder) BindIdx(rows []model.BatchRow, idx []int32, updatedAt time.Time) ([]any, error) {
	for _, i := range idx {
		if i < 0 || int(i) >= len(rows) {
			return nil, fmt.Errorf("writer: bind index %d outside %d rows", i, len(rows))
		}
	}
	return b.bind(rows, idx, len(idx), updatedAt)
}

// bind is the shared body: row r is rows[r], or rows[idx[r]] when idx is set.
func (b *Binder) bind(rows []model.BatchRow, idx []int32, n int, updatedAt time.Time) ([]any, error) {
	if n == 0 || n > b.ceiling {
		return nil, fmt.Errorf("writer: bind of %d rows outside 1..%d", n, b.ceiling)
	}
	at := func(r int) *model.BatchRow {
		if idx != nil {
			return &rows[idx[r]]
		}
		return &rows[r]
	}
	for r := 0; r < n; r++ {
		if m := at(r).Mask; m != b.mask {
			return nil, fmt.Errorf("writer: row %d has mask %#x, statement is %#x", r, uint64(m), uint64(b.mask))
		}
	}
	for i := range b.cols {
		if !b.cols[i].fit(n) {
			b.lastN = 0
			return nil, fmt.Errorf("writer: bind arena for %s holds fewer than %d rows", b.cols[i].bind.Name, n)
		}
	}
	for r := 0; r < n; r++ {
		row := at(r)
		img := &row.Image
		for i := range b.cols {
			c := &b.cols[i]
			switch {
			case c.bind.Role == destinations.RoleKey:
				c.i64[r] = int64(row.Row.Lo())
			case c.bind.Role == destinations.RoleUpdatedAt:
				c.tm[r] = sql.NullTime{Time: updatedAt, Valid: true}
			case c.i64 != nil:
				c.i64[r] = readInt(img, c)
			case c.f64 != nil:
				c.f64[r] = b.readDouble(img, c, true)
			case c.str != nil:
				c.str[r] = readString(img, c)
			default:
				c.tm[r] = b.readTime(img, c, true)
			}
		}
	}
	if n != b.lastN {
		for i := range b.cols {
			b.args[i] = b.cols[i].box()
		}
		b.lastN = n
	}
	// W-3: go-ora binds min(len) rows and silently drops the rest, so a short
	// slice loses writes and reports success. Refuse before any exec.
	if err := checkLens(b.args, n); err != nil {
		b.lastN = 0
		return nil, err
	}
	return b.args, nil
}

// checkLens verifies every bind position is a slice of exactly n elements.
func checkLens(args []any, n int) error {
	for i, a := range args {
		var l int
		switch v := a.(type) {
		case []int64:
			l = len(v)
		case []sql.NullString:
			l = len(v)
		case []sql.NullFloat64:
			l = len(v)
		case []sql.NullTime:
			l = len(v)
		default:
			return fmt.Errorf("writer: bind position %d has type %T, want a typed slice", i+1, a)
		}
		if l != n {
			return fmt.Errorf("writer: bind position %d has %d elements, want %d", i+1, l, n)
		}
	}
	return nil
}

// BindRow is the phase-2 form: scalar arguments for one row, same plan, same
// positions, same statement. It allocates freely; it runs only after a
// row-data error. It does not touch Inst: the failing batch's Bind already
// counted each bad value, so counts are per Bind of the batch, not per retry.
func (b *Binder) BindRow(row *model.BatchRow, updatedAt time.Time) ([]any, error) {
	if row.Mask != b.mask {
		return nil, fmt.Errorf("writer: row has mask %#x, statement is %#x", uint64(row.Mask), uint64(b.mask))
	}
	args := make([]any, len(b.cols))
	for i := range b.cols {
		c := &b.cols[i]
		switch {
		case c.bind.Role == destinations.RoleKey:
			args[i] = int64(row.Row.Lo())
		case c.bind.Role == destinations.RoleUpdatedAt:
			args[i] = sql.NullTime{Time: updatedAt, Valid: true}
		case c.i64 != nil:
			args[i] = readInt(&row.Image, c)
		case c.f64 != nil:
			args[i] = b.readDouble(&row.Image, c, false)
		case c.str != nil:
			args[i] = readString(&row.Image, c)
		default:
			args[i] = b.readTime(&row.Image, c, false)
		}
	}
	return args, nil
}

// readInt reads a big-endian LONG or SHORT, sign-extended. The value is the
// RAW integer: scaling is Oracle's job, in the statement text (V3 section 5.3).
func readInt(img *[model.ImageSize]byte, c *bcol) int64 {
	o := c.bind.Offset
	if c.bind.Kind == destinations.KindShort {
		return int64(int16(binary.BigEndian.Uint16(img[o:])))
	}
	return int64(int32(binary.BigEndian.Uint32(img[o:])))
}

func (b *Binder) readDouble(img *[model.ImageSize]byte, c *bcol, counted bool) sql.NullFloat64 {
	f := math.Float64frombits(binary.BigEndian.Uint64(img[c.bind.Offset:]))
	if math.IsNaN(f) || math.IsInf(f, 0) {
		if counted {
			b.count(c, true)
		}
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: f, Valid: true}
}

func (b *Binder) readTime(img *[model.ImageSize]byte, c *bcol, counted bool) sql.NullTime {
	s := int32(binary.BigEndian.Uint32(img[c.bind.Offset:]))
	if s <= 0 {
		if counted {
			b.count(c, false)
		}
		return sql.NullTime{}
	}
	return sql.NullTime{Time: time.Unix(int64(s)+b.Time1980UnixOffsetSec, 0).UTC(), Valid: true}
}

func (b *Binder) count(c *bcol, nonfinite bool) {
	if b.Inst == nil || c.bind.Bit < 0 || c.bind.Bit >= model.MaxColumns {
		return
	}
	if nonfinite {
		add(b.Inst.BindNonfinite[c.dest][c.bind.Bit])
	} else {
		add(b.Inst.BindBadTime[c.dest][c.bind.Bit])
	}
}

// readString converts a char1 or text field. NUL char1 is NULL; text is cut to
// its valid length (an unsigned 16-bit value from another field when LenBit
// says so, clamped to the field width), right-trimmed of blanks AND NULs (a
// deliberate merge of the old fixed-text row: V3 5.2 trims blanks only, but a
// NUL tail is padding on this wire), and an empty result is NULL. Bytes are
// passed through as-is; no UTF-8 validation.
func readString(img *[model.ImageSize]byte, c *bcol) sql.NullString {
	o, w := int(c.bind.Offset), int(c.bind.Width)
	if c.bind.Kind == destinations.KindChar1 {
		if img[o] == 0 {
			return sql.NullString{}
		}
		return sql.NullString{String: char1Strings[img[o]], Valid: true}
	}
	n := w
	if c.lenOff >= 0 {
		n = min(int(binary.BigEndian.Uint16(img[c.lenOff:])), w)
	}
	t := bytes.TrimRight(img[o:o+n], " \x00")
	if len(t) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{String: string(t), Valid: true}
}

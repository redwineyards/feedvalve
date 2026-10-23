package destinations

import (
	"fmt"
	"strings"

	"feedvalve/internal/model"
)

// validateSet runs the whole rule set over a loaded mapping.
//
// This is layer 2 of the ASM-16b gate (Vol 0 section 4.7) and it runs in all
// three places that matter -- startup, -selfcheck, and before a SIGHUP swap --
// from this one function, so the three cannot drift. A mapping that fails is
// rejected WHOLESALE: a half-applied field map is worse than a stale one,
// because the masks and the images would disagree about what bit 3 means.
func validateSet(s *Set) error {
	if len(s.list) != model.MaxDestinations {
		return fmt.Errorf("destinations: %d declared, exactly %d are required",
			len(s.list), model.MaxDestinations)
	}

	seenName := map[string]model.DestID{}
	for i := range s.list {
		d := &s.list[i]

		if prev, dup := seenName[d.Name]; dup {
			return fmt.Errorf("destinations: %q is declared twice, as %d and %d", d.Name, prev, d.ID)
		}
		seenName[d.Name] = d.ID

		if err := validateDestination(d); err != nil {
			return fmt.Errorf("destinations: %s: %w", d.Name, err)
		}
	}
	return nil
}

func validateDestination(d *Destination) error {
	if d.Name == "" {
		return fmt.Errorf("name is empty")
	}
	if err := validIdent(d.Table); err != nil {
		return fmt.Errorf("table: %w", err)
	}
	if err := validIdent(d.KeyColumn); err != nil {
		return fmt.Errorf("keyColumn: %w", err)
	}
	if strings.EqualFold(d.KeyColumn, UpdatedAtColumn) {
		return fmt.Errorf("keyColumn %q collides with the writer-owned %s column", d.KeyColumn, UpdatedAtColumn)
	}
	if d.NContributors == 0 {
		return fmt.Errorf("no contributors declared; nothing could ever write this destination")
	}

	// ASM-16b. The build-time gate in layout.go pins the three shipped
	// lengths; this catches a destination the build could not see.
	if d.ImageLen > model.ImageSize {
		return fmt.Errorf("image is %d bytes, over the %d-byte Entry.Image by %d. "+
			"Either trim a column or raise ImageSize -- at 400,000 rows each extra "+
			"byte costs 400 kB of arena",
			d.ImageLen, model.ImageSize, d.ImageLen-model.ImageSize)
	}
	if len(d.Columns) > model.MaxColumns {
		return fmt.Errorf("%d columns declared, at most %d fit ColMask",
			len(d.Columns), model.MaxColumns)
	}

	seenSQL := map[string]uint8{}
	for i := range d.Columns {
		c := &d.Columns[i]

		// Bit indices are declared in the file AND required to equal the
		// index. The redundancy is the point: it is what turns a reordered
		// column list into a failed load instead of a silent re-mapping of
		// every live entry and every stored digest (R8).
		if int(c.Bit) != i {
			return fmt.Errorf("column %d (%q) declares bit %d; bit must equal the "+
				"column's index, because the declared order IS the bit order",
				i, c.Name(), c.Bit)
		}
		if err := validateColumn(d, c); err != nil {
			return fmt.Errorf("column %d (%q): %w", i, c.Name(), err)
		}

		for _, name := range c.SQL {
			// Statement aliases are case-insensitive in Oracle, so the
			// key and updated_at collisions are compared folded.
			if strings.EqualFold(name, d.KeyColumn) {
				return fmt.Errorf("SQL column %q (bit %d) is also the keyColumn; the key "+
					"is bound once and never in the SET list", name, c.Bit)
			}
			if strings.EqualFold(name, UpdatedAtColumn) {
				return fmt.Errorf("SQL column %q (bit %d) is reserved for the writer's clock", name, c.Bit)
			}
			if prev, dup := seenSQL[name]; dup {
				return fmt.Errorf("SQL column %q is declared by both bit %d and bit %d",
					name, prev, c.Bit)
			}
			seenSQL[name] = c.Bit
		}
	}
	return nil
}

// kindWidth is the fixed wire width each scalar kind must occupy. Text and
// group are variable by nature and are excluded.
var kindWidth = map[ColKind]uint16{
	KindLong:     4,
	KindShort:    2,
	KindChar1:    1,
	KindDouble:   8,
	KindTime1980: 4,
}

func validateColumn(d *Destination, c *Column) error {
	if c.Width == 0 {
		return fmt.Errorf("width is zero")
	}
	if len(c.SQL) == 0 {
		return fmt.Errorf("no SQL column names")
	}
	for _, name := range c.SQL {
		if err := validIdent(name); err != nil {
			return err
		}
	}

	// A width that disagrees with its kind is a transcription error, and a
	// silent one: the image still packs, the offsets still add up, and every
	// field after it reads the wrong bytes.
	if want, fixed := kindWidth[c.Kind]; fixed && c.Width != want {
		return fmt.Errorf("kind %s is %d bytes on the wire but width is %d", c.Kind, want, c.Width)
	}

	// Only one kind expands to several columns (R7).
	if len(c.SQL) > 1 && c.Kind != KindGroup {
		return fmt.Errorf("kind %s declares %d SQL columns; only a group expands to more than one",
			c.Kind, len(c.SQL))
	}

	if c.Scaled {
		switch c.Kind {
		case KindLong, KindShort, KindDouble, KindGroup:
		default:
			return fmt.Errorf("kind %s cannot be scaled", c.Kind)
		}
		if c.Scale == 0 {
			return fmt.Errorf("scaled but scale is 0; Scaled says whether to divide " +
				"and Scale says by what, so a zero divisor exponent is a missing value")
		}
	}

	// LenBit must name a declared field, and that field must be able to hold
	// a length. Dangling or self-referential length fields are how a binder
	// reads past the end of a value.
	if c.HasExplicitLength() {
		if c.Kind != KindText {
			return fmt.Errorf("lenBit is set but kind is %s; only text takes its length "+
				"from another field", c.Kind)
		}
		if int(c.LenBit) >= len(d.Columns) {
			return fmt.Errorf("lenBit %d names a bit this destination does not declare", c.LenBit)
		}
		if uint8(c.LenBit) == c.Bit {
			return fmt.Errorf("lenBit points at itself")
		}
		if lk := d.Columns[c.LenBit].Kind; lk != KindShort && lk != KindLong {
			return fmt.Errorf("lenBit %d names a %s column; a length must be a short or a long",
				c.LenBit, lk)
		}
	}

	// Pre-validation metadata (section 5A). A character capacity wider than
	// the image field is a mapping that promises Oracle more than the image
	// can carry.
	if c.SQLMaxBytes > 0 && uint16(c.SQLMaxBytes) > c.Width {
		return fmt.Errorf("sqlMaxBytes %d exceeds the %d image bytes this field occupies",
			c.SQLMaxBytes, c.Width)
	}
	if len(c.Domain) > 0 {
		if c.Kind != KindChar1 && c.Kind != KindText {
			return fmt.Errorf("domain is set but kind is %s; a domain constrains "+
				"character values", c.Kind)
		}
		seen := map[string]bool{}
		for _, v := range c.Domain {
			if v == "" {
				return fmt.Errorf("domain contains an empty value")
			}
			if seen[v] {
				return fmt.Errorf("domain lists %q twice", v)
			}
			seen[v] = true
		}
	}
	return nil
}

package destinations

import (
	"fmt"

	"feedvalve/internal/model"
)

// Destination is one target table. Immutable after startup.
//
// Immutable is not a style preference. The declared column order fixes every
// ColMask bit index and every image offset (Vol 0 section 4.1 R8), so a reload
// that inserted a column would renumber the masks and re-lay 400,000 live
// images and every stored digest in one step. That is why the whole
// destinations array is startup-only configuration.
type Destination struct {
	ID        model.DestID
	Name      string     // "MD_TOKEN_QUOTE"
	Table     string     // the Oracle table name
	Key       RowKeyRule // how RowKey is derived from a unit (ASM-15)
	KeyColumn string     // the SQL column holding RowKey: in ON and INSERT, never in SET (V3 3.1)
	Columns   []Column   // declared order == bit order == image order
	ImageLen  int        // sum of the column widths; at most model.ImageSize (ASM-16b)

	// Contributors are the transaction codes that may write this
	// destination, in SLOT ORDER: ascending code, assigned once at startup.
	//
	// The slot is what indexes Entry.LastSeq, LastTime and LastRecv, so a
	// reload that added or reordered a contributor would silently reassign
	// every stored ordering value to the wrong transcode. Hence startup-only.
	Contributors  [model.MaxContributors]model.TransCode
	NContributors int
}

// Column returns the column occupying mask bit i, or nil.
func (d *Destination) Column(bit uint8) *Column {
	if int(bit) >= len(d.Columns) {
		return nil
	}
	return &d.Columns[bit]
}

// Slot returns the contributor slot of a transaction code within this
// destination, and whether it contributes at all.
//
// Slots are per-destination, not per-unit: one transcode may occupy different
// slots in different destinations, which is why Contribution carries Slot
// rather than UpdateUnit.
func (d *Destination) Slot(code model.TransCode) (uint8, bool) {
	for i := range d.NContributors {
		if d.Contributors[i] == code {
			return uint8(i), true
		}
	}
	return 0, false
}

// Contributes reports whether code may write this destination.
func (d *Destination) Contributes(code model.TransCode) bool {
	_, ok := d.Slot(code)
	return ok
}

// FullMask is the mask with every declared column set. Useful to bound a
// contribution and to assert that no mask names a bit this destination has
// not declared.
func (d *Destination) FullMask() model.ColMask {
	var m model.ColMask
	for i := range d.Columns {
		m = m.Set(d.Columns[i].Bit)
	}
	return m
}

// Set is the loaded destination table: every destination, immutable, indexed
// by DestID and by name.
type Set struct {
	list   []Destination
	byName map[string]*Destination
}

// Len is the number of destinations.
func (s *Set) Len() int { return len(s.list) }

// All returns every destination in DestID order.
func (s *Set) All() []Destination { return s.list }

// ByID returns the destination with the given id, or nil.
func (s *Set) ByID(id model.DestID) *Destination {
	if int(id) >= len(s.list) {
		return nil
	}
	return &s.list[id]
}

// ByName returns the destination with the given name, or nil.
func (s *Set) ByName(name string) *Destination { return s.byName[name] }

// MustByName is for tests and startup wiring, where a missing destination is a
// programming error rather than a runtime condition.
func (s *Set) MustByName(name string) *Destination {
	d := s.ByName(name)
	if d == nil {
		panic(fmt.Sprintf("destinations: no destination named %q", name))
	}
	return d
}

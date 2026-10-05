package destinations

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"slices"

	"feedvalve/internal/model"
)

// defaultMapping is the shipped field map, embedded so a binary is complete on
// its own and -selfcheck needs no file to validate something real.
//
//go:embed mapping.json
var defaultMapping []byte

// On-disk shapes. Separate from Destination and Column because the file
// declares less than the loaded form holds: offsets are derived, slots are
// assigned, and the ID is the array index.
//
// Every shape carries its _note and _comment fields explicitly, because the
// decoder rejects unknown keys. A mapping file is the one place a typo would
// otherwise be silent -- "sqlPresicion" ignored means a column loses its
// pre-validation and nobody finds out until Oracle refuses a row.
type fileMapping struct {
	Comment      json.RawMessage   `json:"_comment"`
	Destinations []fileDestination `json:"destinations"`
}

type fileDestination struct {
	Comment      json.RawMessage `json:"_contributors"`
	Name         string          `json:"name"`
	Table        string          `json:"table"`
	RowKey       string          `json:"rowKey"`
	ImageLen     int             `json:"imageLen"`
	Contributors []int           `json:"contributors"`
	Columns      []fileColumn    `json:"columns"`
}

type fileColumn struct {
	Note         json.RawMessage `json:"_note"`
	Bit          uint8           `json:"bit"`
	Width        uint16          `json:"width"`
	Kind         string          `json:"kind"`
	Scaled       bool            `json:"scaled"`
	Scale        uint8           `json:"scale"`
	LenBit       *int8           `json:"lenBit"` // pointer: absent means -1, not 0
	SQL          []string        `json:"sql"`
	SQLPrecision uint8           `json:"sqlPrecision"`
	SQLMaxBytes  uint16          `json:"sqlMaxBytes"`
	NotNull      bool            `json:"notNull"`
	Domain       []string        `json:"domain"`
}

// Default returns the destination table from the embedded mapping.
func Default() (*Set, error) { return Parse(defaultMapping) }

// Load reads a mapping file from disk. An operator-supplied file replaces the
// embedded one wholesale; there is no merging, because a partially overridden
// field map is harder to reason about than a wrong one.
func Load(path string) (*Set, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("destinations: reading %s: %w", path, err)
	}
	s, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("destinations: %s: %w", path, err)
	}
	return s, nil
}

// Parse decodes and validates a mapping. It returns a usable Set or an error;
// it never returns a partially built one.
func Parse(raw []byte) (*Set, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var f fileMapping
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("decoding mapping: %w", err)
	}

	set := &Set{
		list:   make([]Destination, 0, len(f.Destinations)),
		byName: make(map[string]*Destination, len(f.Destinations)),
	}

	for i, fd := range f.Destinations {
		d, err := buildDestination(model.DestID(i), fd)
		if err != nil {
			name := fd.Name
			if name == "" {
				name = fmt.Sprintf("destinations[%d]", i)
			}
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		set.list = append(set.list, d)
	}

	for i := range set.list {
		set.byName[set.list[i].Name] = &set.list[i]
	}

	if err := validateSet(set); err != nil {
		return nil, err
	}
	return set, nil
}

// buildDestination turns one file entry into a Destination, deriving what the
// file deliberately does not declare.
func buildDestination(id model.DestID, fd fileDestination) (Destination, error) {
	d := Destination{
		ID:      id,
		Name:    fd.Name,
		Table:   fd.Table,
		Columns: make([]Column, 0, len(fd.Columns)),
	}

	key, err := parseRowKey(fd.RowKey)
	if err != nil {
		return d, err
	}
	d.Key = key

	// Contributor slots: ascending transaction code, assigned once, here.
	// Sorting rather than trusting the file's order means a reordered
	// "contributors" array cannot change a slot, and slots index the stored
	// ordering state.
	codes := make([]int, len(fd.Contributors))
	copy(codes, fd.Contributors)
	slices.Sort(codes)
	if len(codes) > model.MaxContributors {
		return d, fmt.Errorf("%d contributors declared, at most %d are possible "+
			"(MaxContributors sizes Entry's ordering arrays)", len(codes), model.MaxContributors)
	}
	for i, c := range codes {
		if c <= 0 || c > 0xFFFF {
			return d, fmt.Errorf("contributor %d is not a valid transaction code", c)
		}
		if i > 0 && codes[i-1] == c {
			return d, fmt.Errorf("transaction code %d is declared twice as a contributor", c)
		}
		d.Contributors[i] = model.TransCode(c)
	}
	d.NContributors = len(codes)

	// Offsets are the running sum of the declared widths: packed, declared
	// order, no padding (R2). Nothing reads the image through a typed
	// pointer, so there is nothing to align to.
	var offset uint16
	for ci, fc := range fd.Columns {
		kind, err := parseKind(fc.Kind)
		if err != nil {
			return d, fmt.Errorf("column %d (%q): %w", ci, firstName(fc.SQL), err)
		}

		lenBit := int8(-1)
		if fc.LenBit != nil {
			lenBit = *fc.LenBit
		}

		d.Columns = append(d.Columns, Column{
			Bit:          fc.Bit,
			Offset:       offset,
			Width:        fc.Width,
			Kind:         kind,
			Scaled:       fc.Scaled,
			Scale:        fc.Scale,
			LenBit:       lenBit,
			SQL:          fc.SQL,
			SQLPrecision: fc.SQLPrecision,
			SQLMaxBytes:  fc.SQLMaxBytes,
			NotNull:      fc.NotNull,
			Domain:       fc.Domain,
		})
		offset += fc.Width
	}
	d.ImageLen = int(offset)

	// The file declares imageLen as well. It is checked, not used: a width
	// typo then fails the load instead of quietly producing a different
	// layout that is internally consistent and wrong.
	if fd.ImageLen != d.ImageLen {
		return d, fmt.Errorf("declared imageLen %d but the column widths sum to %d",
			fd.ImageLen, d.ImageLen)
	}

	return d, nil
}

func firstName(sql []string) string {
	if len(sql) == 0 {
		return ""
	}
	return sql[0]
}

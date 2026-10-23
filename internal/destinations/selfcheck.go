package destinations

import (
	"errors"
	"fmt"
	"regexp"

	"feedvalve/internal/model"
)

// selfCheckIdent is the identifier rule -selfcheck holds every generated name
// to (V3 section 11): a letter, then up to 29 letters, digits or underscores.
// It is narrower than the loader's validIdent on length (30, not 128) because
// the target server may be pre-12.2, and -selfcheck is the pre-deployment gate.
// It is written out here rather than shared with ident.go so the two cannot
// drift together unnoticed.
var selfCheckIdent = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,29}$`)

// CheckStatements generates StatementFor for every destination at every
// single-bit mask and at the full mask, and checks every identifier the
// statement uses (table, key column, every bind name). It returns how many
// statements it generated. It touches no database, password or network.
func CheckStatements(set *Set) (int, error) {
	var errs []error
	n := 0
	for i := range set.All() {
		d := &set.All()[i]
		masks := make([]model.ColMask, 0, len(d.Columns)+1)
		for j := range d.Columns {
			masks = append(masks, model.ColMask(0).Set(d.Columns[j].Bit))
			// Column names appear quoted in the statement text; check them
			// directly rather than relying on bind names equalling them.
			for _, name := range d.Columns[j].SQL {
				if !selfCheckIdent.MatchString(name) {
					errs = append(errs, fmt.Errorf("%s column %q is not [A-Za-z][A-Za-z0-9_]{0,29}", d.Name, name))
				}
			}
		}
		masks = append(masks, d.FullMask())
		for _, m := range masks {
			text, plan, err := StatementFor(d, m)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s mask %#x: %w", d.Name, uint64(m), err))
				continue
			}
			n++
			if text == "" || len(plan) == 0 {
				errs = append(errs, fmt.Errorf("%s mask %#x: empty statement", d.Name, uint64(m)))
			}
			names := []string{d.Table, d.KeyColumn}
			for _, b := range plan {
				names = append(names, b.Name)
			}
			for _, name := range names {
				if !selfCheckIdent.MatchString(name) {
					errs = append(errs, fmt.Errorf("%s mask %#x: identifier %q is not [A-Za-z][A-Za-z0-9_]{0,29}",
						d.Name, uint64(m), name))
				}
			}
		}
	}
	return n, errors.Join(errs...)
}

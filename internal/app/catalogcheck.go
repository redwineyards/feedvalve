package app

import (
	"context"
	"fmt"
	"strings"

	"feedvalve/internal/destinations"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

// The catalogue check, Vol 0 7.6. oradb reads ALL_TAB_COLUMNS and returns
// neutral rows; the comparison against the field map is a pure function here,
// because app is the only package that sees both halves.

// catalogueFindings reads the catalogue for every destination's table and
// compares it with the field map. An error means the catalogue could not be
// read; a finding is a disagreement with the mapping.
func catalogueFindings(ctx context.Context, db *oradb.DB, dests *destinations.Set) ([]string, error) {
	tables := make([]string, 0, dests.Len())
	for _, d := range dests.All() {
		// Oracle stores unquoted identifiers upper-case.
		tables = append(tables, strings.ToUpper(d.Table))
	}
	cols, err := oradb.Catalog(ctx, db, tables)
	if err != nil {
		return nil, err
	}
	return compareCatalogue(dests, cols), nil
}

// errorLogger is the one method logCatalogue needs.
type errorLogger interface {
	Errorf(class obs.Class, format string, a ...any)
}

// logCatalogue is the startup half: one ERROR per finding, then proceed. The
// fix is DDL by a DBA, and it must not need a redeploy; a database problem must
// never stop the relay. A catalogue that cannot be read is one ERROR too.
func logCatalogue(ctx context.Context, lg errorLogger, db *oradb.DB, dests *destinations.Set) {
	findings, err := catalogueFindings(ctx, db, dests)
	if err != nil {
		lg.Errorf(obsClassStartup, "catalogue check could not run: %v", err)
		return
	}
	for _, f := range findings {
		lg.Errorf(obsClassStartup, "catalogue mismatch: %s", f)
	}
	if len(findings) > 0 {
		// The startup class is rate-limited, so the count says how many
		// findings there were even if some lines were suppressed.
		lg.Errorf(obsClassStartup, "catalogue check: %d mismatch(es) against the field map", len(findings))
	}
}

// compareCatalogue is pure. For each destination it expects the key column, every
// SQL column of every mapped field and updated_at, and reports one line per
// disagreement: missing table or column, wrong type family, short NUMBER
// precision, short character capacity, and any data column that is NOT NULL
// (Vol 3 10.4: that turns the first partial-mask insert into ORA-01400,
// permanently). The key and updated_at may be NOT NULL and are not flagged.
//
// Not compared: scale, CHECK domains, grants. Ladder group columns get the
// family and nullability checks only, since the mapping carries no per-element
// precision for them.
func compareCatalogue(dests *destinations.Set, cols []oradb.CatalogColumn) []string {
	byTable := map[string]map[string]oradb.CatalogColumn{}
	for _, c := range cols {
		t := strings.ToUpper(c.Table)
		if byTable[t] == nil {
			byTable[t] = map[string]oradb.CatalogColumn{}
		}
		byTable[t][strings.ToUpper(c.Name)] = c
	}

	var out []string
	for _, d := range dests.All() {
		table := strings.ToUpper(d.Table)
		have, ok := byTable[table]
		if !ok {
			out = append(out, fmt.Sprintf("%s: table not found in the current schema", table))
			continue
		}
		find := func(name string) (oradb.CatalogColumn, bool) {
			c, ok := have[strings.ToUpper(name)]
			if !ok {
				out = append(out, fmt.Sprintf("%s.%s: column not found", table, strings.ToUpper(name)))
			}
			return c, ok
		}
		bad := func(c oradb.CatalogColumn, format string, a ...any) {
			out = append(out, fmt.Sprintf("%s.%s: %s", table, c.Name, fmt.Sprintf(format, a...)))
		}

		if c, ok := find(d.KeyColumn); ok && !isNumber(c) {
			bad(c, "key column is %s, want NUMBER", c.DataType)
		}
		for i := range d.Columns {
			col := &d.Columns[i]
			for _, name := range col.SQL {
				c, ok := find(name)
				if !ok {
					continue
				}
				switch col.Kind {
				case destinations.KindTime1980:
					if !isTimestamp(c) {
						bad(c, "type is %s, want TIMESTAMP", c.DataType)
					}
				case destinations.KindChar1, destinations.KindText:
					if !isCharacter(c) {
						bad(c, "type is %s, want VARCHAR2 or CHAR", c.DataType)
					} else if col.SQLMaxBytes > 0 && c.Bytes < int(col.SQLMaxBytes) {
						bad(c, "capacity is %d bytes, want at least %d", c.Bytes, col.SQLMaxBytes)
					}
				case destinations.KindGroup:
					// Ladder columns: family only; no per-element precision.
					if !isNumber(c) {
						bad(c, "type is %s, want NUMBER", c.DataType)
					}
				default:
					if !isNumber(c) {
						bad(c, "type is %s, want NUMBER", c.DataType)
					} else if c.Precision > 0 && c.Precision < int(col.SQLPrecision) {
						bad(c, "precision is %d, want at least %d", c.Precision, col.SQLPrecision)
					}
				}
				if !c.Nullable {
					bad(c, "NOT NULL on a data column; a partial-mask insert will fail with ORA-01400")
				}
			}
		}
		if c, ok := find(destinations.UpdatedAtColumn); ok && !isTimestamp(c) {
			bad(c, "type is %s, want TIMESTAMP", c.DataType)
		}
	}
	return out
}

func isNumber(c oradb.CatalogColumn) bool { return strings.ToUpper(c.DataType) == "NUMBER" }

func isTimestamp(c oradb.CatalogColumn) bool {
	return strings.HasPrefix(strings.ToUpper(c.DataType), "TIMESTAMP")
}

func isCharacter(c oradb.CatalogColumn) bool {
	switch strings.ToUpper(c.DataType) {
	case "VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR":
		return true
	}
	return false
}

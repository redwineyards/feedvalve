package oradb

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CatalogColumn is one row of ALL_TAB_COLUMNS, kept neutral: it names nothing
// from destinations, so oradb stays below the packages that know what the
// columns are meant to be. The comparison lives in app (Vol 0 7.6).
type CatalogColumn struct {
	Table     string
	Name      string
	DataType  string // e.g. "NUMBER", "VARCHAR2", "TIMESTAMP(6)"
	Precision int    // DATA_PRECISION; 0 when NULL (a bare NUMBER, or not a number)
	Bytes     int    // DATA_LENGTH, the declared byte capacity
	Nullable  bool
}

// CatalogQuery reads one table's columns. The OWNER filter is required, not
// tidy: ALL_TAB_COLUMNS lists every table the account can SEE, so without it a
// same-named table in another schema would be compared in place of ours.
// CURRENT_SCHEMA is the schema an unqualified table name in a MERGE resolves
// to. The bind is positional (:1), never ?.
const CatalogQuery = `SELECT table_name, column_name, data_type, data_precision, data_length, nullable ` +
	`FROM all_tab_columns ` +
	`WHERE owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AND table_name = :1 ` +
	`ORDER BY column_id`

// Catalog returns the columns of each named table in the current schema. Names
// are passed exactly as given (Oracle stores unquoted identifiers upper-case).
// A table that does not exist contributes no rows; that is a finding for the
// caller, not an error here. Read-only. Errors are redacted.
func Catalog(ctx context.Context, d *DB, tables []string) ([]CatalogColumn, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(d.s.QueryTimeoutSec)*time.Second)
	defer cancel()

	var out []CatalogColumn
	for _, t := range tables {
		cols, err := catalogTable(ctx, d, t)
		if err != nil {
			return nil, fmt.Errorf("reading the catalogue for %s: %w", t, d.red.redact(err))
		}
		out = append(out, cols...)
	}
	return out, nil
}

func catalogTable(ctx context.Context, d *DB, table string) ([]CatalogColumn, error) {
	rows, err := d.db.QueryContext(ctx, CatalogQuery, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CatalogColumn
	for rows.Next() {
		var (
			c        CatalogColumn
			prec, ln sql.NullInt64
			nullable string
		)
		if err := rows.Scan(&c.Table, &c.Name, &c.DataType, &prec, &ln, &nullable); err != nil {
			return nil, err
		}
		// The query already filters on the table; a row for another one can
		// only be a stand-in driver serving one canned text to every table.
		// Dropping it keeps a per-table result per-table.
		if c.Table != table {
			continue
		}
		c.Precision, c.Bytes, c.Nullable = int(prec.Int64), int(ln.Int64), nullable == "Y"
		out = append(out, c)
	}
	return out, rows.Err()
}

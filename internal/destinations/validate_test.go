package destinations

import (
	"encoding/json"
	"strings"
	"testing"
)

func i8(v int8) *int8 { return &v }

// base is a minimal mapping that validates. Each test mutates one thing and
// requires the load to fail, so a rule that stopped working shows up as a
// mapping that was accepted rather than as a passing assertion.
func base() fileMapping {
	return fileMapping{Destinations: []fileDestination{
		{
			Name: "D0", Table: "T0", RowKey: "token", ImageLen: 6,
			Contributors: []int{7208},
			Columns: []fileColumn{
				{Bit: 0, Width: 4, Kind: "long", SQL: []string{"alpha"}},
				{Bit: 1, Width: 2, Kind: "short", SQL: []string{"beta"}},
			},
		},
		{
			Name: "D1", Table: "T1", RowKey: "token", ImageLen: 2,
			Contributors: []int{7305},
			Columns: []fileColumn{
				{Bit: 0, Width: 2, Kind: "short", SQL: []string{"gamma"}},
			},
		},
		{
			Name: "D2", Table: "T2", RowKey: "marketType", ImageLen: 1,
			Contributors: []int{6511},
			Columns: []fileColumn{
				{Bit: 0, Width: 1, Kind: "char1", SQL: []string{"delta"}},
			},
		},
	}}
}

func parseFile(t *testing.T, f fileMapping) (*Set, error) {
	t.Helper()
	raw, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return Parse(raw)
}

func TestBaseMappingIsValid(t *testing.T) {
	if _, err := parseFile(t, base()); err != nil {
		t.Fatalf("the base fixture must validate, otherwise every rejection test "+
			"below could be passing for the wrong reason: %v", err)
	}
}

func TestRejections(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*fileMapping)
		want   string // substring the error must contain
	}{
		{
			name:   "duplicate bit index",
			mutate: func(f *fileMapping) { f.Destinations[0].Columns[1].Bit = 0 },
			want:   "bit must equal",
		},
		{
			name:   "bit index out of order",
			mutate: func(f *fileMapping) { f.Destinations[0].Columns[0].Bit = 1 },
			want:   "bit must equal",
		},
		{
			name: "SQL identifier with a quote",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].SQL = []string{"a\"; DROP TABLE x--"}
			},
			want: "only letters, digits and underscore",
		},
		{
			name:   "SQL identifier starting with a digit",
			mutate: func(f *fileMapping) { f.Destinations[0].Columns[0].SQL = []string{"1col"} },
			want:   "must start with a letter",
		},
		{
			name:   "table name with a dot",
			mutate: func(f *fileMapping) { f.Destinations[0].Table = "schema.table" },
			want:   "only letters, digits and underscore",
		},
		{
			name:   "empty SQL name list",
			mutate: func(f *fileMapping) { f.Destinations[0].Columns[0].SQL = nil },
			want:   "no SQL column names",
		},
		{
			name: "duplicate SQL column within a destination",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[1].SQL = []string{"alpha"}
			},
			want: "declared by both",
		},
		{
			name: "image larger than ImageSize",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns = append(f.Destinations[0].Columns,
					fileColumn{Bit: 2, Width: 300, Kind: "text", LenBit: i8(-1), SQL: []string{"big"}})
				f.Destinations[0].ImageLen = 306
			},
			want: "over the 256-byte Entry.Image",
		},
		{
			name:   "declared imageLen disagrees with the widths",
			mutate: func(f *fileMapping) { f.Destinations[0].ImageLen = 99 },
			want:   "declared imageLen 99 but the column widths sum to 6",
		},
		{
			name: "ninth contributor",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Contributors = []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
			},
			want: "at most 8 are possible",
		},
		{
			name: "duplicate contributor",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Contributors = []int{7208, 7208}
			},
			want: "declared twice as a contributor",
		},
		{
			name:   "no contributors",
			mutate: func(f *fileMapping) { f.Destinations[0].Contributors = nil },
			want:   "no contributors declared",
		},
		{
			name:   "unknown rowKey",
			mutate: func(f *fileMapping) { f.Destinations[0].RowKey = "isin" },
			want:   "unknown rowKey",
		},
		{
			name:   "unknown column kind",
			mutate: func(f *fileMapping) { f.Destinations[0].Columns[0].Kind = "decimal" },
			want:   "unknown column kind",
		},
		{
			// imageLen is adjusted to match, so the sum check cannot fire
			// first. Without that this case passes for the wrong reason:
			// the mapping is rejected, but as an arithmetic error rather
			// than as the kind/width mismatch it is meant to exercise.
			name: "width disagrees with the kind",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].Width = 7
				f.Destinations[0].ImageLen = 9
			},
			want: "is 4 bytes on the wire but width is 7",
		},
		{
			name: "several SQL columns on a non-group kind",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].SQL = []string{"alpha", "alpha2"}
			},
			want: "only a group expands to more than one",
		},
		{
			name: "lenBit naming an undeclared bit",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].Kind = "text"
				f.Destinations[0].Columns[0].LenBit = i8(9)
			},
			want: "names a bit this destination does not declare",
		},
		{
			name: "lenBit pointing at itself",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].Kind = "text"
				f.Destinations[0].Columns[0].LenBit = i8(0)
			},
			want: "points at itself",
		},
		{
			name: "lenBit naming a non-numeric column",
			mutate: func(f *fileMapping) {
				f.Destinations[2].Columns = append(f.Destinations[2].Columns,
					fileColumn{Bit: 1, Width: 4, Kind: "text", LenBit: i8(0), SQL: []string{"txt"}})
				f.Destinations[2].ImageLen = 5
			},
			want: "a length must be a short or a long",
		},
		{
			name: "scaled with a zero scale",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].Scaled = true
			},
			want: "scale is 0",
		},
		{
			name: "scaled text",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].Kind = "text"
				f.Destinations[0].Columns[0].LenBit = i8(-1)
				f.Destinations[0].Columns[0].Scaled = true
				f.Destinations[0].Columns[0].Scale = 2
			},
			want: "cannot be scaled",
		},
		{
			name: "sqlMaxBytes wider than the image field",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].SQLMaxBytes = 64
			},
			want: "exceeds the 4 image bytes",
		},
		{
			name: "domain on a numeric column",
			mutate: func(f *fileMapping) {
				f.Destinations[0].Columns[0].Domain = []string{"Y", "N"}
			},
			want: "a domain constrains character values",
		},
		{
			name:   "duplicate destination name",
			mutate: func(f *fileMapping) { f.Destinations[1].Name = "D0" },
			want:   "is declared twice",
		},
		{
			name:   "wrong number of destinations",
			mutate: func(f *fileMapping) { f.Destinations = f.Destinations[:2] },
			want:   "2 declared, exactly 3 are required",
		},
		{
			name:   "empty table name",
			mutate: func(f *fileMapping) { f.Destinations[0].Table = "" },
			want:   "empty identifier",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := base()
			tc.mutate(&f)

			set, err := parseFile(t, f)
			if err == nil {
				t.Fatalf("mapping was ACCEPTED; expected it to be rejected with %q "+
					"(loaded %d destinations)", tc.want, set.Len())
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not explain the problem.\n got: %v\nwant substring: %q",
					err, tc.want)
			}
		})
	}
}

// TestUnknownKeyIsRejected is separate because it cannot be expressed by
// mutating the typed fixture -- the whole point is a key the struct does not
// have.
//
// A mapping file is the one place a typo would otherwise be silent:
// "sqlPresicion" quietly ignored means a column loses its pre-validation and
// nobody finds out until Oracle refuses a row in production.
func TestUnknownKeyIsRejected(t *testing.T) {
	raw, err := json.Marshal(base())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	withTypo := strings.Replace(string(raw), `"sqlPrecision":0`, `"sqlPresicion":0`, 1)
	if withTypo == string(raw) {
		// Field ordering changed; inject into the first column object instead.
		withTypo = strings.Replace(string(raw), `"bit":0`, `"sqlPresicion":7,"bit":0`, 1)
	}

	if _, err := Parse([]byte(withTypo)); err == nil {
		t.Fatal("a mapping with an unknown key was accepted; a misspelled field must " +
			"fail the load rather than silently lose its meaning")
	} else if !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("rejected, but not as an unknown field: %v", err)
	}
}

func TestValidIdent(t *testing.T) {
	for _, ok := range []string{"a", "A1", "last_traded_price", "MD_TOKEN_QUOTE", "x_9"} {
		if err := validIdent(ok); err != nil {
			t.Errorf("validIdent(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{
		"", "1a", "_x", "a b", "a-b", "a.b", `a"b`, "a'b", "a;b", "a--b", "a$b", "a#b",
		strings.Repeat("x", maxIdentLen+1),
	} {
		if err := validIdent(bad); err == nil {
			t.Errorf("validIdent(%q) = nil, want an error", bad)
		}
	}
}

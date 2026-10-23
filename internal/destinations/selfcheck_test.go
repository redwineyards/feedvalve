package destinations

import (
	"strings"
	"testing"
)

// TestG_SC_1_SelfCheckStatementsDefaultMapping.
// SETUP: the embedded mapping. STIMULUS: CheckStatements.
// ASSERTION: no error; count equals the hand-counted statements, one per
// declared column plus the full mask, per destination.
// WOULD HAVE CAUGHT: a mask that generates an empty or invalid statement.
func TestG_SC_1_SelfCheckStatementsDefaultMapping(t *testing.T) {
	set, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	// Hand count from the embedded mapping: MD_TOKEN_QUOTE 16 columns,
	// MD_SECURITY_MASTER 9, MD_MARKET_STATUS 6; one statement per column plus
	// the full mask: 17 + 10 + 7.
	const want = 34
	n, err := CheckStatements(set)
	if err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("generated %d statements, want %d", n, want)
	}
}

// TestG_SC_1_SelfCheckRejectsLongIdentifier is the non-vacuity check: a 31-byte
// column name passes the loader (limit 128) and must fail the selfcheck.
func TestG_SC_1_SelfCheckRejectsLongIdentifier(t *testing.T) {
	long := "A" + strings.Repeat("b", 30) // 31 bytes
	cases := []struct {
		name   string
		mutate func(d *Destination)
		bad    string
	}{
		{"column", func(d *Destination) { d.Columns[0].SQL[0] = long }, long},
		{"table", func(d *Destination) { d.Table = long }, long},
		{"key column", func(d *Destination) { d.KeyColumn = long }, long},
		{"leading digit", func(d *Destination) { d.Table = "1ABC" }, "1ABC"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			set, err := Default()
			if err != nil {
				t.Fatal(err)
			}
			c.mutate(&set.All()[0])
			_, err = CheckStatements(set)
			if err == nil || !strings.Contains(err.Error(), c.bad) {
				t.Fatalf("err = %v, want a rejection naming %q", err, c.bad)
			}
		})
	}
}

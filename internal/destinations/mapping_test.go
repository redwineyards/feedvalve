package destinations

import (
	"slices"
	"testing"

	"feedvalve/internal/model"
)

// TestShippedMappingLoads is the positive case: the embedded field map parses,
// validates, and produces the three destinations Vol 0 sections 4.3 to 4.5
// specify, at the image lengths those sections derive.
func TestShippedMappingLoads(t *testing.T) {
	set, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	if set.Len() != model.MaxDestinations {
		t.Fatalf("loaded %d destinations, want %d", set.Len(), model.MaxDestinations)
	}

	for _, tc := range []struct {
		name         string
		id           model.DestID
		imageLen     int
		columns      int
		contributors []model.TransCode
		key          RowKeyRule
	}{
		{"MD_TOKEN_QUOTE", 0, 181, 16, []model.TransCode{7200, 7208}, KeyToken},
		{"MD_SECURITY_MASTER", 1, 37, 9, []model.TransCode{7305, 7340}, KeyToken},
		{"MD_MARKET_STATUS", 2, 142, 6, []model.TransCode{6511, 6521, 6522, 6531, 6571}, KeyMarketType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := set.ByName(tc.name)
			if d == nil {
				t.Fatalf("no destination named %q", tc.name)
			}
			if d.ID != tc.id {
				t.Errorf("ID = %d, want %d", d.ID, tc.id)
			}
			if d.ImageLen != tc.imageLen {
				t.Errorf("ImageLen = %d, want %d", d.ImageLen, tc.imageLen)
			}
			if len(d.Columns) != tc.columns {
				t.Errorf("%d columns, want %d", len(d.Columns), tc.columns)
			}
			if d.Key != tc.key {
				t.Errorf("Key = %s, want %s", d.Key, tc.key)
			}
			if got := d.Contributors[:d.NContributors]; !slices.Equal(got, tc.contributors) {
				t.Errorf("contributors = %v, want %v (ascending code order)", got, tc.contributors)
			}
		})
	}
}

// TestShippedLengthsMatchTheBuildGate closes the loop between the data and
// gate G-LAY-3. The gate pins constants the compiler can see; mapping.json is
// data it cannot. Two sources of truth are only safe when something checks
// them against each other, and this is that something.
func TestShippedLengthsMatchTheBuildGate(t *testing.T) {
	set, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	for name, want := range shippedImageLens {
		d := set.ByName(name)
		if d == nil {
			t.Errorf("layout.go pins %s but mapping.json does not declare it", name)
			continue
		}
		if d.ImageLen != want {
			t.Errorf("%s: mapping.json computes %d bytes, layout.go's build gate asserts %d",
				name, d.ImageLen, want)
		}
	}
}

// TestOffsetsArePackedInDeclaredOrder checks rule R2 directly: offsets are the
// running sum of the widths, with no padding anywhere. Nothing reads the image
// through a typed pointer, so there is nothing to align to, and packing is
// what makes 181 bytes fit where an aligned layout would not.
func TestOffsetsArePackedInDeclaredOrder(t *testing.T) {
	set, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	for _, d := range set.All() {
		var want uint16
		for i := range d.Columns {
			c := &d.Columns[i]
			if c.Offset != want {
				t.Errorf("%s column %d (%s): offset %d, want %d -- the image must be packed",
					d.Name, i, c.Name(), c.Offset, want)
			}
			want += c.Width
		}
		if int(want) != d.ImageLen {
			t.Errorf("%s: offsets end at %d but ImageLen is %d", d.Name, want, d.ImageLen)
		}
		if d.ImageLen > model.ImageSize {
			t.Errorf("%s: ImageLen %d exceeds ImageSize %d", d.Name, d.ImageLen, model.ImageSize)
		}
	}
}

// TestLadderIsOneBitAndThirtyColumns pins R7's worked example. Splitting the
// ladder into separate bits would spend 30 of the 64 available on a
// distinction 7208 never makes: it always supplies the whole ladder.
func TestLadderIsOneBitAndThirtyColumns(t *testing.T) {
	set, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	d := set.MustByName("MD_TOKEN_QUOTE")

	var ladder *Column
	for i := range d.Columns {
		if d.Columns[i].Kind == KindGroup {
			ladder = &d.Columns[i]
			break
		}
	}
	if ladder == nil {
		t.Fatal("no group column in MD_TOKEN_QUOTE")
	}

	// 10 records of 12 bytes. Three usable fields per record -- quantity,
	// price, orders -- and BbBuySellFlag discarded because the side is
	// positional. 3 x 5 levels x 2 sides = 30.
	if ladder.Width != 120 {
		t.Errorf("ladder width = %d, want 120 (10 records x 12 bytes)", ladder.Width)
	}
	if len(ladder.SQL) != 30 {
		t.Errorf("ladder expands to %d columns, want 30 (quantity, price, orders "+
			"x 5 levels x 2 sides)", len(ladder.SQL))
	}
}

// TestSlotLookup checks the per-destination slot assignment. Slots index
// Entry's ordering arrays, so one transcode occupying different slots in
// different destinations is normal and must work.
func TestSlotLookup(t *testing.T) {
	set, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	status := set.MustByName("MD_MARKET_STATUS")

	for want, code := range []model.TransCode{6511, 6521, 6522, 6531, 6571} {
		got, ok := status.Slot(code)
		if !ok {
			t.Errorf("%d does not contribute to MD_MARKET_STATUS", code)
			continue
		}
		if int(got) != want {
			t.Errorf("%d is slot %d, want %d (ascending code order)", code, got, want)
		}
	}
	if _, ok := status.Slot(7208); ok {
		t.Error("7208 should not contribute to MD_MARKET_STATUS")
	}
	if status.NContributors > model.MaxContributors {
		t.Errorf("%d contributors exceeds MaxContributors %d",
			status.NContributors, model.MaxContributors)
	}
}

// TestMessageTextTakesItsLengthFromAnotherField pins the LenBit mechanism.
// Without it the binder cannot be generated from the column table at all:
// message_text is 128 bytes whose valid prefix is given by message_len, and
// there is no NUL terminator to scan for (bible 2.6).
func TestMessageTextTakesItsLengthFromAnotherField(t *testing.T) {
	set, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	d := set.MustByName("MD_MARKET_STATUS")

	text := d.Column(2)
	if text == nil || text.Name() != "message_text" {
		t.Fatal("bit 2 of MD_MARKET_STATUS should be message_text")
	}
	if !text.HasExplicitLength() {
		t.Fatal("message_text must take its length from another field")
	}
	lenCol := d.Column(uint8(text.LenBit))
	if lenCol == nil || lenCol.Name() != "message_len" {
		t.Errorf("message_text.LenBit points at %v, want message_len", lenCol)
	}

	// Destination 1's symbol is the other form: fixed width, blank-padded,
	// right-trimmed at bind time. Vol 0 section 4.4 calls it "Text-fixed";
	// it is KindText with an implicit length.
	sym := set.MustByName("MD_SECURITY_MASTER").Column(0)
	if sym.Kind != KindText || sym.HasExplicitLength() {
		t.Errorf("symbol should be text with an implicit length, got kind=%s lenBit=%d",
			sym.Kind, sym.LenBit)
	}
}

// TestFullMask checks that every declared bit is reachable and that nothing
// beyond the declared columns is.
func TestFullMask(t *testing.T) {
	set, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	for _, d := range set.All() {
		m := d.FullMask()
		if m.Count() != len(d.Columns) {
			t.Errorf("%s: FullMask has %d bits for %d columns", d.Name, m.Count(), len(d.Columns))
		}
		if m.Has(uint8(len(d.Columns))) {
			t.Errorf("%s: FullMask sets a bit past the declared columns", d.Name)
		}
	}
}

// shippedImageLens maps destination name to the length layout.go's build gate
// asserts, so the loader test can prove mapping.json and those constants agree.
// Two sources of truth are only safe when something checks them against each
// other.
var shippedImageLens = map[string]int{
	"MD_TOKEN_QUOTE":     quoteImageLen,
	"MD_SECURITY_MASTER": masterImageLen,
	"MD_MARKET_STATUS":   statusImageLen,
}

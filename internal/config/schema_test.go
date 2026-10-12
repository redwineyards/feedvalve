package config

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"feedvalve/internal/destinations"
)

func shippedDests(t *testing.T) *destinations.Set {
	t.Helper()
	d, err := destinations.Default()
	if err != nil {
		t.Fatalf("loading the shipped field map: %v", err)
	}
	return d
}

// valid is a configuration that passes every rule against the shipped field
// map. Each test below breaks exactly one thing in it.
//
// It declares the real contributors of the real destinations, because
// anything less would trip V-D7's "declared but not configured" arm and every
// test would then be passing for the wrong reason.
func valid() *Config {
	c := Defaults()
	c.HasDB = true
	c.DB.Host, c.DB.Service, c.DB.User = "db.internal", "SVC", "feedvalve"
	c.DB.Port, c.DB.ConnectTimeoutSec = 1521, 10
	c.DB.QueryTimeoutSec, c.DB.ConnMaxLifetimeMinutes = 30, 30
	c.applyDerivedPool()

	quote := []string{"last_traded_price"}
	master := []string{"symbol"}
	status := []string{"transcode"}

	for _, code := range []int{7200, 7208} {
		c.Transcodes = append(c.Transcodes, TranscodeConfig{
			Code: code, Class: "rate-limited",
			Contributions: []ContributionConfig{{Destination: "MD_TOKEN_QUOTE", Columns: quote}},
		})
	}
	for _, code := range []int{7305, 7340} {
		c.Transcodes = append(c.Transcodes, TranscodeConfig{
			Code: code, Class: "rate-limited",
			Contributions: []ContributionConfig{{Destination: "MD_SECURITY_MASTER", Columns: master}},
		})
	}
	for _, code := range []int{6511, 6521, 6522, 6531, 6571} {
		c.Transcodes = append(c.Transcodes, TranscodeConfig{
			Code: code, Class: "non-rate-limited",
			Contributions: []ContributionConfig{{Destination: "MD_MARKET_STATUS", Columns: status}},
		})
	}
	return c
}

func TestValidBaseline(t *testing.T) {
	if err := ValidateAll(valid(), shippedDests(t), nil, ModeRun); err != nil {
		t.Fatalf("the baseline must validate, or every rejection test below could be "+
			"passing for the wrong reason:\n%v", err)
	}
}

// rulesOf extracts the rule identifiers from a validation error.
func rulesOf(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		return nil
	}
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("error is %T, not *ValidationError: %v", err, err)
	}
	return ve.Rules()
}

// TestG_SC_1_EachRuleFiresExactlyOnce is the done-when of P1-06: one deliberately
// broken configuration per rule identifier, each producing THAT identifier
// and no other.
//
// "And no other" is the part that does the work. Without it a rule can look
// implemented while a different rule catches every input that would have
// exercised it -- which is exactly what happened in P1-04, where an image
// length check fired before the kind/width rule it was meant to test.
// It carries gate G-SC-1's identifier because it IS G-SC-1 (Vol 4 section
// 7.2): "the shipped config.json ... zero failures; and separately, one
// deliberately-broken configuration per rule id, each producing exactly that id
// and no other". The gate was implemented in P1-06 without its label, so a
// reviewer grepping for G-SC-1 found nothing -- which P1-10 noticed while
// counting the gates and is the cheapest possible fix.
func TestG_SC_1_EachRuleFiresExactlyOnce(t *testing.T) {
	dests := shippedDests(t)

	for _, tc := range []struct {
		rule   string
		mode   Mode
		mutate func(*Config)
	}{
		{RuleRange, ModeRun, func(c *Config) { c.Runtime.Shards = 0 }},
		{RuleStatementFailWriters, ModeRun, func(c *Config) {
			c.Writers.Count = 2
			c.Writers.StatementFailWriters = 3
			c.applyDerivedPool()
		}},
		{RuleHeartbeatOrder, ModeRun, func(c *Config) { c.Liveness.HeartbeatDownSec = c.Liveness.HeartbeatWarnSec }},
		{RuleBatchSizeCeiling, ModeRun, func(c *Config) { c.Flush.MaxBatchSize = c.Runtime.MaxBatchSizeCeiling + 1 }},
		{RuleGeneralShardRange, ModeRun, func(c *Config) { c.Runtime.GeneralShard = c.Runtime.Shards }},
		{RuleRetryOrder, ModeRun, func(c *Config) { c.Writers.RetryMaxMs = c.Writers.RetryInitialMs - 1 }},
		{RuleStallBudget, ModeRun, func(c *Config) { c.UDP.StallBudgetMs = 500 }},

		// V-D7 cannot be isolated against the shipped map -- pointing a
		// transcode at a differently-keyed destination also makes it a
		// non-contributor of that destination, so V-T3 fires too. It gets
		// its own fixture in TestRowKeyDerivableIsEnforced.
		{RuleClass, ModeRun, func(c *Config) { c.Transcodes[0].Class = "urgent" }},
		{RuleContribDest, ModeRun, func(c *Config) {
			c.Transcodes[0].Contributions[0].Destination = "MD_NO_SUCH_TABLE"
		}},
		{RuleContribColumn, ModeRun, func(c *Config) {
			c.Transcodes[0].Contributions[0].Columns = []string{"no_such_column"}
		}},

		{RuleDBRequired, ModeRun, func(c *Config) { c.HasDB = false }},
		{RuleDBFields, ModeRun, func(c *Config) { c.DB.Host = "" }},
		{RuleTimezone, ModeRun, func(c *Config) { c.Liveness.TradingWindow.TZ = "Mars/Olympus" }},
		{RuleTradingTimes, ModeRun, func(c *Config) { c.Liveness.TradingWindow.Start = "9am" }},
		{RuleTraceCodes, ModeRun, func(c *Config) { c.Debug.TraceTranscodes = []int{9999} }},
		{RuleSourceList, ModeRun, func(c *Config) { c.UDP.SourceAllowList = []string{"not-an-ip"} }},
	} {
		t.Run(tc.rule, func(t *testing.T) {
			c := valid()
			tc.mutate(c)

			err := ValidateAll(c, dests, nil, tc.mode)
			if err == nil {
				t.Fatalf("configuration was ACCEPTED; expected %s to fire", tc.rule)
			}
			got := rulesOf(t, err)
			if !slices.Equal(got, []string{tc.rule}) {
				t.Errorf("fired %v, want exactly [%s]\n%v", got, tc.rule, err)
			}
		})
	}
}

// TestReloadRulesFireExactlyOnce covers the rules that only exist when there
// is a previous configuration to compare against.
func TestReloadRulesFireExactlyOnce(t *testing.T) {
	dests := shippedDests(t)

	for _, tc := range []struct {
		rule   string
		mutate func(*Config)
	}{
		{RuleStartupOnly, func(c *Config) { c.Runtime.Shards = 8 }},
		{RuleCeilingFixed, func(c *Config) { c.Runtime.MaxBatchSizeCeiling = 2000 }},
		{RuleTranscodeMembers, func(c *Config) {
			c.Transcodes[0].Contributions[0].Destination = "MD_SECURITY_MASTER"
			c.Transcodes[0].Contributions[0].Columns = []string{"symbol"}
		}},
	} {
		t.Run(tc.rule, func(t *testing.T) {
			prev, next := valid(), valid()
			tc.mutate(next)

			err := ValidateAll(next, dests, prev, ModeRun)
			if err == nil {
				t.Fatalf("reload was ACCEPTED; expected %s to fire", tc.rule)
			}
			got := rulesOf(t, err)
			if !slices.Contains(got, tc.rule) {
				t.Errorf("fired %v, want %s among them\n%v", got, tc.rule, err)
			}
		})
	}

	t.Run("an unchanged reload is accepted", func(t *testing.T) {
		if err := ValidateAll(valid(), dests, valid(), ModeRun); err != nil {
			t.Errorf("reloading an identical configuration must be accepted: %v", err)
		}
	})
}

// TestRowKeyDerivableIsEnforced is V-D7 / ASM-15 -- the single most valuable
// rule in the set, because the defect it catches is otherwise invisible until
// production: two shards writing one row, losing updates intermittently
// because neither mirror entry sees the other's columns.
//
// It needs a purpose-built field map. Against the shipped one, pointing a
// transcode at a differently-keyed destination also makes it a
// non-contributor of that destination, so V-T3 fires as well and the test
// would not be about ASM-15 at all. Here 7208 is a DECLARED contributor of
// both, so the only thing wrong is the keying.
func TestRowKeyDerivableIsEnforced(t *testing.T) {
	const mapping = `{"destinations":[
	  {"name":"D_TOKEN","table":"D_TOKEN","rowKey":"token","imageLen":4,
	   "contributors":[7208],
	   "columns":[{"bit":0,"width":4,"kind":"long","sql":["v"]}]},
	  {"name":"D_MARKET","table":"D_MARKET","rowKey":"marketType","imageLen":4,
	   "contributors":[7208],
	   "columns":[{"bit":0,"width":4,"kind":"long","sql":["w"]}]},
	  {"name":"D_SPARE","table":"D_SPARE","rowKey":"token","imageLen":1,
	   "contributors":[7305],
	   "columns":[{"bit":0,"width":1,"kind":"char1","sql":["f"]}]}
	]}`

	dests, err := destinations.Parse([]byte(mapping))
	if err != nil {
		t.Fatalf("test mapping must load: %v", err)
	}

	base := func() *Config {
		c := Defaults()
		c.HasDB = true
		c.DB.Host, c.DB.Service, c.DB.User = "h", "s", "u"
		c.DB.Port, c.DB.ConnectTimeoutSec, c.DB.QueryTimeoutSec, c.DB.ConnMaxLifetimeMinutes = 1521, 10, 30, 30
		c.applyDerivedPool()
		c.Transcodes = []TranscodeConfig{
			{Code: 7208, Class: "rate-limited", Contributions: []ContributionConfig{
				{Destination: "D_TOKEN", Columns: []string{"v"}},
			}},
			{Code: 7305, Class: "rate-limited", Contributions: []ContributionConfig{
				{Destination: "D_SPARE", Columns: []string{"f"}},
			}},
		}
		return c
	}

	// One transcode feeding one row-key rule: fine.
	if err := ValidateAll(base(), dests, nil, ModeRun); err != nil {
		t.Fatalf("the baseline must validate: %v", err)
	}

	// The same transcode now feeds a token-keyed AND a marketType-keyed
	// destination. Its units are routed by one shard key, so rows of one
	// of them are written from a shard that does not own them.
	c := base()
	c.Transcodes[0].Contributions = append(c.Transcodes[0].Contributions,
		ContributionConfig{Destination: "D_MARKET", Columns: []string{"w"}})

	err = ValidateAll(c, dests, nil, ModeRun)
	if err == nil {
		t.Fatal("a transcode feeding differently-keyed destinations must be rejected (ASM-15)")
	}
	if got := rulesOf(t, err); !slices.Equal(got, []string{RuleRowKeyDerivable}) {
		t.Errorf("fired %v, want exactly [%s]\n%v", got, RuleRowKeyDerivable, err)
	}

	// The other arm: a destination declaring a contributor that is not
	// configured. Its contributions never arrive, its slot stays empty,
	// and the ordering guard is silently weaker for that row.
	c = base()
	c.Transcodes = c.Transcodes[:1] // drop 7305, still a declared contributor of D_SPARE
	if err := ValidateAll(c, dests, nil, ModeRun); err == nil {
		t.Error("a declared contributor that is never configured must be rejected")
	} else if got := rulesOf(t, err); !slices.Contains(got, RuleRowKeyDerivable) {
		t.Errorf("fired %v, want %s among them", got, RuleRowKeyDerivable)
	}
}

// TestBlobFitsIsEnforced is V-T5 / ASM-16a, and it needs its own field map:
// against the shipped one, no token-keyed transcode can exceed 256 bytes
// without ALSO crossing into the marketType destination and tripping V-D7.
// Using a purpose-built map is what keeps this test about the blob bound.
func TestBlobFitsIsEnforced(t *testing.T) {
	const mapping = `{"destinations":[
	  {"name":"D_WIDE","table":"D_WIDE","rowKey":"token","imageLen":250,
	   "contributors":[7208],
	   "columns":[{"bit":0,"width":250,"kind":"text","lenBit":-1,"sql":["wide"]}]},
	  {"name":"D_ALSO","table":"D_ALSO","rowKey":"token","imageLen":20,
	   "contributors":[7208],
	   "columns":[{"bit":0,"width":20,"kind":"text","lenBit":-1,"sql":["alsowide"]}]},
	  {"name":"D_STATUS","table":"D_STATUS","rowKey":"marketType","imageLen":1,
	   "contributors":[6511],
	   "columns":[{"bit":0,"width":1,"kind":"char1","sql":["flag"]}]}
	]}`

	dests, err := destinations.Parse([]byte(mapping))
	if err != nil {
		t.Fatalf("test mapping must load: %v", err)
	}

	c := Defaults()
	c.HasDB = true
	c.DB.Host, c.DB.Service, c.DB.User = "h", "s", "u"
	c.DB.Port, c.DB.ConnectTimeoutSec, c.DB.QueryTimeoutSec, c.DB.ConnMaxLifetimeMinutes = 1521, 10, 30, 30
	c.applyDerivedPool()
	c.Transcodes = []TranscodeConfig{
		{Code: 7208, Class: "rate-limited", Contributions: []ContributionConfig{
			{Destination: "D_WIDE", Columns: []string{"wide"}},
			{Destination: "D_ALSO", Columns: []string{"alsowide"}},
		}},
		{Code: 6511, Class: "non-rate-limited", Contributions: []ContributionConfig{
			{Destination: "D_STATUS", Columns: []string{"flag"}},
		}},
	}

	// 250 + 20 = 270, over UpdateUnit.Blob's 256. Both destinations are
	// token-keyed, so V-D7 stays quiet and only the blob bound fires.
	err = ValidateAll(c, dests, nil, ModeRun)
	if err == nil {
		t.Fatal("270 bytes of contributions must not fit a 256-byte blob")
	}
	if got := rulesOf(t, err); !slices.Equal(got, []string{RuleBlobFits}) {
		t.Errorf("fired %v, want exactly [%s]\n%v", got, RuleBlobFits, err)
	}

	// Trimming one contribution brings it under and the pair validates,
	// which proves the bound rather than a permanently broken fixture.
	c.Transcodes[0].Contributions = c.Transcodes[0].Contributions[:1]
	if err := ValidateAll(c, dests, nil, ModeRun); err != nil {
		t.Errorf("250 bytes should fit: %v", err)
	}
}

// TestSelfCheckRelaxesTheDatabaseRule: -selfcheck must run where the real
// thing would not. It exists to be used from a build host before deployment,
// and one that required a database would be useless there.
func TestSelfCheckRelaxesTheDatabaseRule(t *testing.T) {
	dests := shippedDests(t)

	c := valid()
	c.HasDB = false

	if err := ValidateAll(c, dests, nil, ModeRun); err == nil {
		t.Error("ModeRun must require a db object: a relay whose purpose is to persist " +
			"must not start with nowhere to persist to")
	}
	if err := ValidateAll(c, dests, nil, ModeSelfCheck); err != nil {
		t.Errorf("ModeSelfCheck must not require a db object: %v", err)
	}
	if err := ValidateAll(c, dests, nil, ModeDBCheck); err == nil {
		t.Error("ModeDBCheck must require a db object")
	}
}

// TestG_CFG_1_EveryNumericKeyHasARangeCheck is gate G-CFG-1.
//
// It walks the on-disk shape and requires an entry in the range table for
// every numeric key. Without it a key added to the schema ships unchecked,
// and the first thing to notice is production -- the validator would accept
// any value at all for it while looking complete.
func TestG_CFG_1_EveryNumericKeyHasARangeCheck(t *testing.T) {
	covered := map[string]bool{}
	for _, r := range rangeRules {
		covered[r.key] = true
	}
	for _, r := range dbRangeRules {
		covered[r.key] = true
	}

	// Checked by a dedicated rule rather than by a numeric range.
	exempt := map[string]bool{
		"ordering.guardSuppressionRatio": true, // a float, bounded by its own check
	}

	var missing []string
	walkNumericKeys(t, reflect.TypeOf(fileConfig{}), "", func(path string) {
		if !covered[path] && !exempt[path] {
			missing = append(missing, path)
		}
	})

	if len(missing) > 0 {
		slices.Sort(missing)
		t.Errorf("these numeric keys have no range check, so any value would be accepted:\n  %s\n"+
			"Add them to rangeRules, or to the exempt list with the rule that covers them.",
			strings.Join(missing, "\n  "))
	}
}

// walkNumericKeys visits every numeric leaf of the on-disk shape, building
// the JSON path as it goes. Transcode elements are skipped: they are array
// members validated per element, not single keys.
func walkNumericKeys(t *testing.T, typ reflect.Type, prefix string, visit func(string)) {
	t.Helper()
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return
	}

	for i := range typ.NumField() {
		f := typ.Field(i)
		tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if tag == "" || strings.HasPrefix(tag, "_") {
			continue
		}
		path := tag
		if prefix != "" {
			path = prefix + "." + tag
		}

		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		switch ft.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Float32, reflect.Float64:
			visit(path)
		case reflect.Struct:
			walkNumericKeys(t, ft, path, visit)
		case reflect.Slice:
			// Array-valued keys are validated per element.
		}
	}
}

// TestStrictKeysIsV_STRICT1 ties the unknown-key rejection to its identifier.
// It happens at decode time rather than in the rule set, so it is checked
// here rather than in the per-rule table.
func TestStrictKeysIsV_STRICT1(t *testing.T) {
	_, err := Parse([]byte(`{"udp":{"stallBudgetMillis":40}}`))
	if err == nil {
		t.Fatal("an unknown key must be rejected (V-STRICT1)")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("rejected, but not as an unknown field: %v", err)
	}
}

// TestNoPasswordKeyIsV_DB3: a file carrying a password has already leaked it,
// and refusing to start is the only response that makes the operator rotate
// it. The strict decoder is what enforces this, since the field does not
// exist to be matched.
func TestNoPasswordKeyIsV_DB3(t *testing.T) {
	for _, bad := range []string{
		`{"db":{"password":"hunter2"}}`,
		`{"db":{"passwd":"hunter2"}}`,
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("%s was accepted; a config carrying a password must be refused", bad)
		}
	}
}

// TestUncheckedIsHonest: the rules that cannot run yet must be reported by
// the validator itself, not by a hand-written string somewhere, so the list
// shrinks automatically as they become checkable.
func TestUncheckedIsHonest(t *testing.T) {
	u := Unchecked()
	for _, want := range []string{RuleDecoderExists, RuleColSpecWidth} {
		if _, ok := u[want]; !ok {
			t.Errorf("%s should be listed as not yet checkable", want)
		}
	}
	for rule, why := range u {
		if why == "" {
			t.Errorf("%s is listed as unchecked with no reason", rule)
		}
	}

	// And a rule that IS implemented must not still be listed.
	for _, implemented := range []string{RuleRowKeyDerivable, RuleContribColumn, RuleBlobFits} {
		if _, ok := u[implemented]; ok {
			t.Errorf("%s is implemented but still listed as unchecked", implemented)
		}
	}
}

func TestValidationErrorReportsEveryRule(t *testing.T) {
	c := valid()
	c.UDP.StallBudgetMs = 500
	c.Stats.LogLevel = "trace"
	c.Liveness.TradingWindow.TZ = "Mars/Olympus"

	err := ValidateAll(c, shippedDests(t), nil, ModeRun)
	got := rulesOf(t, err)
	want := []string{RuleRange, RuleStallBudget, RuleTimezone}
	slices.Sort(want)

	if !slices.Equal(got, want) {
		t.Errorf("fired %v, want %v -- all problems should be reported at once\n%v", got, want, err)
	}
}

func TestProblemJSONShape(t *testing.T) {
	// Problems are plain data so a future -selfcheck -json can emit them
	// without the rule identifiers being buried in prose.
	b, err := json.Marshal(Problem{Rule: RuleRange, Msg: "x"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), "V-RANGE") {
		t.Errorf("rule identifier missing from %s", b)
	}
}

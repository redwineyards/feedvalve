package config

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"feedvalve/internal/destinations"
)

// vol0StartupOnly is Vol 0 section 7.2's "startup-only" column, typed by hand
// from docs/lld/00-foundations.md. It is deliberately NOT derived from
// StartupOnlyKeys or from the getter table: a list compared with itself agrees
// with itself even when it is wrong.
var vol0StartupOnly = []string{
	"udp.listen",
	"runtime.shards",
	"runtime.generalShard",
	"runtime.shardQueueCapacity",
	"runtime.datagramQueueCapacity",
	"runtime.maxInFlightPerShard",
	"runtime.maxBatchSizeCeiling",
	"runtime.expectedRows",
	"runtime.arenaChunkRows",
	"db.host",
	"db.port",
	"db.service",
	"db.user",
}

// implementationStartupOnly is startup-only here but absent from Vol 0's table:
// the field-map path. V-R2 compares the map's content; this key is the path
// that selects it, and moving it without a restart is the same defect.
var implementationStartupOnly = []string{"runtime.destinationsFile"}

// startupOnlyMutators change exactly one startup-only key. Hand-written, so a
// getter wired to the wrong field is caught by the key it fails to trip.
var startupOnlyMutators = map[string]func(*Config){
	"udp.listen":                    func(c *Config) { c.UDP.Listen = ":9999" },
	"runtime.shards":                func(c *Config) { c.Runtime.Shards++ },
	"runtime.generalShard":          func(c *Config) { c.Runtime.GeneralShard++ },
	"runtime.shardQueueCapacity":    func(c *Config) { c.Runtime.ShardQueueCapacity *= 2 },
	"runtime.datagramQueueCapacity": func(c *Config) { c.Runtime.DatagramQueueCapacity *= 2 },
	"runtime.maxInFlightPerShard":   func(c *Config) { c.Runtime.MaxInFlightPerShard++ },
	"runtime.maxBatchSizeCeiling":   func(c *Config) { c.Runtime.MaxBatchSizeCeiling++ },
	"runtime.expectedRows":          func(c *Config) { c.Runtime.ExpectedRows++ },
	"runtime.arenaChunkRows":        func(c *Config) { c.Runtime.ArenaChunkRows++ },
	"runtime.destinationsFile":      func(c *Config) { c.Runtime.DestinationsFile = "other.json" },
	"db.host":                       func(c *Config) { c.DB.Host = "other-host-9" },
	"db.port":                       func(c *Config) { c.DB.Port++ },
	"db.service":                    func(c *Config) { c.DB.Service = "OTHERSVC" },
	"db.user":                       func(c *Config) { c.DB.User = "other-user-9" },
}

// TestG_CFG_2_StartupOnlyListMatchesVol0 is gate G-CFG-2.
//
// SETUP: the hand-written Vol 0 section 7.2 startup-only list, plus the one
// documented implementation addition.
// STIMULUS: compare it with StartupOnlyKeys in BOTH directions, then change
// each listed key alone on a copy of a valid configuration and reload.
// ASSERTION: the two lists are equal as sets; every key trips its own rule
// (V-R4 for the ceiling, V-R1 otherwise) and exactly that one problem.
// WOULD HAVE CAUGHT: a key added to or dropped from the getter table without
// the spec moving (drift in either direction), and a getter reading the wrong
// field, which a list-only comparison cannot see.
// NON-VACUITY: the mutator map must cover every key, and a key missing from the
// table is reported by the reverse comparison below.
func TestG_CFG_2_StartupOnlyListMatchesVol0(t *testing.T) {
	want := append(slices.Clone(vol0StartupOnly), implementationStartupOnly...)
	got := slices.Clone(StartupOnlyKeys)
	slices.Sort(want)
	slices.Sort(got)
	for _, k := range want {
		if !slices.Contains(got, k) {
			t.Errorf("%s is startup-only in Vol 0 section 7.2 but not in StartupOnlyKeys", k)
		}
	}
	for _, k := range got {
		if !slices.Contains(want, k) {
			t.Errorf("%s is in StartupOnlyKeys but Vol 0 section 7.2 does not mark it startup-only", k)
		}
	}
	if len(want) == 0 || len(got) != len(want) {
		t.Fatalf("lists differ in length: StartupOnlyKeys %d, Vol 0 %d", len(got), len(want))
	}

	for _, key := range got {
		t.Run(key, func(t *testing.T) {
			mutate, ok := startupOnlyMutators[key]
			if !ok {
				t.Fatalf("no mutator for %s: the test is not covering it", key)
			}
			prev, next := valid(), valid()
			mutate(next)
			next.applyDerivedPool()

			// Validate alone: the mutation may also break an unrelated
			// range rule (shards, generalShard); only the reload rule
			// for THIS key is asserted.
			var p problems
			validateReload(next, prev, &p)
			wantRule := RuleStartupOnly
			if key == "runtime.maxBatchSizeCeiling" {
				wantRule = RuleCeilingFixed
			}
			if len(p) != 1 || p[0].Rule != wantRule || p[0].KeyPath != key {
				t.Fatalf("changing %s alone produced %+v, want exactly one %s on that key", key, p, wantRule)
			}
		})
	}
}

// TestG_CFG_2_V_R1ReportsKeyAndBothValuesExceptForDB: V-R1 names the key, the
// running value and the attempted value -- and for a db.* key the values are
// withheld, because a db.* value is a credential-adjacent secret.
func TestG_CFG_2_V_R1ReportsKeyAndBothValuesExceptForDB(t *testing.T) {
	prev, next := valid(), valid()
	next.Runtime.Shards = 8
	next.DB.Host = "secret-host.internal"

	err := Validate(next, prev)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %v", err)
	}
	var shards, host *Problem
	for i := range ve.Problems {
		switch ve.Problems[i].KeyPath {
		case "runtime.shards":
			shards = &ve.Problems[i]
		case "db.host":
			host = &ve.Problems[i]
		}
	}
	if shards == nil || shards.Running != "4" || shards.Attempt != "8" {
		t.Errorf("runtime.shards problem = %+v, want running 4 attempted 8", shards)
	}
	if host == nil || host.Running != "" || host.Attempt != "" {
		t.Errorf("db.host problem = %+v, want the key and NO values", host)
	}
	if strings.Contains(err.Error(), "secret-host.internal") || strings.Contains(err.Error(), "db.internal") {
		t.Errorf("a db.* value leaked into the error text:\n%v", err)
	}
}

// TestRuleErrorIsDerivedFromTheFirstProblem pins RuleError's shape.
func TestRuleErrorIsDerivedFromTheFirstProblem(t *testing.T) {
	prev, next := valid(), valid()
	next.Runtime.Shards = 8
	err := Validate(next, prev)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %v", err)
	}
	re := ve.RuleError()
	if re.Rule != ve.Problems[0].Rule || re.KeyPath != "runtime.shards" ||
		re.Running != "4" || re.Attempt != "8" || re.Detail != ve.Problems[0].Msg {
		t.Errorf("RuleError = %+v does not mirror Problems[0] = %+v", re, ve.Problems[0])
	}
	if (&ValidationError{}).RuleError() != (RuleError{}) {
		t.Error("an empty ValidationError must yield the zero RuleError")
	}
}

// TestG_SEC_3_PasswordKeyIsRejectedWithoutTheValue is gate G-SEC-3.
//
// SETUP: configuration files carrying a password at several depths, with a
// recognisable secret as the value.
// STIMULUS: Parse each, and Load one from disk.
// ASSERTION: every one is rejected by V-DB3 alone, the key path is named, the
// message tells the operator to rotate, and the secret is nowhere in the error.
// WOULD HAVE CAUGHT: accepting-and-ignoring the key (a live password left in
// version control with nobody told), reporting it as a generic "unknown field"
// (no rotate instruction), and echoing the value.
// NON-VACUITY: the same file without the password key parses.
func TestG_SEC_3_PasswordKeyIsRejectedWithoutTheValue(t *testing.T) {
	const secret = "hunter2-S3CRET-value"
	for name, tc := range map[string]struct{ json, path string }{
		"db.password":            {`{"db":{"host":"h","password":"` + secret + `"}}`, "db.password"},
		"top level":              {`{"password":"` + secret + `"}`, "password"},
		"nested in an array":     {`{"transcodes":[{"code":7208,"x":{"Password":"` + secret + `"}}]}`, "transcodes[0].x.Password"},
		"alongside an unknown":   {`{"db":{"password":"` + secret + `","typo":1}}`, "db.password"},
		"numeric value is never": {`{"db":{"password":123456789}}`, "db.password"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.json))
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("want *ValidationError, got %T: %v", err, err)
			}
			if !slices.Equal(ve.Rules(), []string{RuleDBNoPassword}) {
				t.Fatalf("fired %v, want exactly [%s]", ve.Rules(), RuleDBNoPassword)
			}
			if re := ve.RuleError(); re.KeyPath != tc.path {
				t.Errorf("key path = %q, want %q", re.KeyPath, tc.path)
			}
			msg := err.Error()
			if !strings.Contains(msg, "rotate") {
				t.Errorf("message does not tell the operator to rotate the credential:\n%s", msg)
			}
			if strings.Contains(msg, secret) || strings.Contains(msg, "123456789") {
				t.Errorf("the password value leaked into the error:\n%s", msg)
			}
		})
	}

	t.Run("through Load", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "c.json")
		if err := os.WriteFile(path, []byte(`{"db":{"password":"`+secret+`"}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(path)
		var ve *ValidationError
		if !errors.As(err, &ve) || !ve.Has(RuleDBNoPassword) {
			t.Fatalf("Load did not surface V-DB3: %v", err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("Load leaked the value:\n%v", err)
		}
	})

	t.Run("non-vacuity: no password key parses", func(t *testing.T) {
		if _, err := Parse([]byte(`{"db":{"host":"h","service":"s","user":"u"}}`)); err != nil {
			t.Errorf("a file without a password key must parse: %v", err)
		}
	})
}

// TestV_STRICT1IsRaisedAsARule: an unknown key is a V-STRICT1 problem naming
// the key, not an anonymous decode error.
func TestV_STRICT1IsRaisedAsARule(t *testing.T) {
	_, err := Parse([]byte(`{"flush":{"windowMillis":50}}`))
	var ve *ValidationError
	if !errors.As(err, &ve) || !slices.Equal(ve.Rules(), []string{RuleStrictKeys}) {
		t.Fatalf("want exactly V-STRICT1, got %v", err)
	}
	if re := ve.RuleError(); re.KeyPath != "windowMillis" || !strings.Contains(re.Detail, "unknown field") {
		t.Errorf("RuleError = %+v, want the key windowMillis and the decoder's reason", re)
	}
	if _, err := Parse([]byte(`{"flush":`)); err == nil {
		t.Fatal("malformed JSON must still be rejected")
	} else if errors.As(err, &ve) {
		t.Errorf("a syntax error is not an unknown-key rule: %v", err)
	}
}

// remapped returns the shipped mapping with each (old, new) substitution
// applied, parsed as a destination set.
func remapped(t *testing.T, subs ...string) *destinations.Set {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "destinations", "mapping.json"))
	if err != nil {
		t.Skipf("shipped mapping.json not reachable from this test (%v)", err)
	}
	s := string(raw)
	for i := 0; i < len(subs); i += 2 {
		if !strings.Contains(s, subs[i]) {
			t.Fatalf("mapping.json no longer contains %q; the fixture is stale", subs[i])
		}
		s = strings.Replace(s, subs[i], subs[i+1], 1)
	}
	set, err := destinations.Parse([]byte(s))
	if err != nil {
		t.Fatalf("edited mapping does not load: %v", err)
	}
	return set
}

// TestG_SC_1_V_R2FiresExactlyOnceOnAnEditedFieldMap is G-SC-1 for V-R2.
//
// SETUP: the running configuration and the shipped field map as the running set.
// STIMULUS: reload against (a) an identical map, (b) a map with a renamed table,
// (c) a map with a renamed SQL column, (d) both edits at once, (e) no running
// set at all.
// ASSERTION: (a) and (e) accepted; (b)-(d) rejected by V-R2 and no other rule,
// with exactly ONE V-R2 problem even when two destinations differ.
// WOULD HAVE CAUGHT: a field-map reload silently renumbering live masks, V-R2
// firing once per edit, and V-R2 firing with nothing to compare.
func TestG_SC_1_V_R2FiresExactlyOnceOnAnEditedFieldMap(t *testing.T) {
	running := shippedDests(t)
	cfg := valid()

	check := func(t *testing.T, next *destinations.Set, wantReject bool) {
		t.Helper()
		err := ValidateAll(cfg, next, running, valid(), ModeRun)
		if !wantReject {
			if err != nil {
				t.Fatalf("accepted map was rejected: %v", err)
			}
			return
		}
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("edited field map was ACCEPTED (err=%v)", err)
		}
		if !slices.Equal(ve.Rules(), []string{RuleDestStartupOnly}) {
			t.Fatalf("fired %v, want exactly [%s]\n%v", ve.Rules(), RuleDestStartupOnly, err)
		}
		if len(ve.Problems) != 1 {
			t.Errorf("V-R2 fired %d times, want once:\n%v", len(ve.Problems), err)
		}
	}

	t.Run("identical map is accepted", func(t *testing.T) {
		check(t, remapped(t), false)
	})
	t.Run("renamed table", func(t *testing.T) {
		check(t, remapped(t, `"table": "MD_TOKEN_QUOTE"`, `"table": "MD_TOKEN_QUOTE_V2"`), true)
	})
	t.Run("renamed column", func(t *testing.T) {
		check(t, remapped(t, `"sql": ["series"]`, `"sql": ["series_code"]`), true)
	})
	t.Run("two destinations edited still fire once", func(t *testing.T) {
		check(t, remapped(t,
			`"table": "MD_TOKEN_QUOTE"`, `"table": "MD_TOKEN_QUOTE_V2"`,
			`"sql": ["series"]`, `"sql": ["series_code"]`), true)
	})
	t.Run("no running set: nothing to compare", func(t *testing.T) {
		if err := ValidateAll(cfg, remapped(t, `"table": "MD_TOKEN_QUOTE"`, `"table": "X_V2"`), nil, valid(), ModeRun); err != nil {
			t.Errorf("with no running set V-R2 must be silent: %v", err)
		}
	})
	t.Run("startup is never a reload", func(t *testing.T) {
		if err := ValidateAll(cfg, running, nil, nil, ModeRun); err != nil {
			t.Errorf("startup validation must not apply reload-only rules: %v", err)
		}
	})
}

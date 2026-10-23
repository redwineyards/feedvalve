package app

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"feedvalve/internal/config"
)

// TestG_SC_1_ReloadLoadPathAppliesV_R2 is G-SC-1 for the reload load path.
//
// SETUP: a config file and a field-map file on disk, loaded once with loadAll
// as the "running" pair.
// STIMULUS: loadForReload against (a) the unchanged files, (b) the field-map
// file edited (one table renamed), (c) a startup-only key edited in the config.
// ASSERTION: (a) is accepted and returns an equivalent pair; (b) is rejected by
// V-R2 alone, once; (c) is rejected by V-R1 alone naming the key.
// WOULD HAVE CAUGHT: the reload path validating with prev == nil (every
// reload-only rule silently off), or reading the destinations file but never
// comparing it with the running set.
// NON-VACUITY: (b) and (c) are the same files as (a) with one edit each.
func TestG_SC_1_ReloadLoadPathAppliesV_R2(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "destinations", "mapping.json"))
	if err != nil {
		t.Skipf("shipped mapping.json not reachable (%v)", err)
	}
	dir := t.TempDir()
	mapPath := filepath.Join(dir, "mapping.json")
	cfgPath := filepath.Join(dir, "config.json")
	writeFile := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfgBody := func(shards string) string {
		return `{"runtime":{"destinationsFile":` + quote(mapPath) + `,"shards":` + shards + `},
		 "db":{"host":"h","service":"s","user":"u"}}`
	}
	writeFile(mapPath, string(src))
	writeFile(cfgPath, cfgBody("4"))

	prev, prevDests, err := loadAll(cfgPath, config.ModeRun)
	if err != nil {
		t.Fatalf("loading the running pair: %v", err)
	}

	t.Run("unchanged files are accepted", func(t *testing.T) {
		cfg, dests, err := loadForReload(cfgPath, prev, prevDests)
		if err != nil {
			t.Fatalf("an unchanged reload must be accepted: %v", err)
		}
		if cfg == prev || dests == prevDests {
			t.Error("a reload must return a freshly loaded pair, not the running one")
		}
	})

	t.Run("edited field map is V-R2", func(t *testing.T) {
		edited := strings.Replace(string(src), `"table": "MD_TOKEN_QUOTE"`, `"table": "MD_TOKEN_QUOTE_V2"`, 1)
		if edited == string(src) {
			t.Fatal("the fixture edit did not apply")
		}
		writeFile(mapPath, edited)
		defer writeFile(mapPath, string(src))

		_, _, err := loadForReload(cfgPath, prev, prevDests)
		var ve *config.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("edited field map was ACCEPTED (err=%v)", err)
		}
		if !slices.Equal(ve.Rules(), []string{config.RuleDestStartupOnly}) || len(ve.Problems) != 1 {
			t.Errorf("fired %v (%d problems), want V-R2 exactly once\n%v", ve.Rules(), len(ve.Problems), err)
		}
	})

	t.Run("edited startup-only key is V-R1", func(t *testing.T) {
		writeFile(cfgPath, cfgBody("8"))
		defer writeFile(cfgPath, cfgBody("4"))

		_, _, err := loadForReload(cfgPath, prev, prevDests)
		var ve *config.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("a startup-only change was ACCEPTED (err=%v)", err)
		}
		if re := ve.RuleError(); re.Rule != config.RuleStartupOnly || re.KeyPath != "runtime.shards" ||
			re.Running != "4" || re.Attempt != "8" {
			t.Errorf("RuleError = %+v, want V-R1 on runtime.shards 4 -> 8", re)
		}
	})

	t.Run("no config file is not a reload", func(t *testing.T) {
		if _, _, err := loadForReload("", prev, prevDests); err == nil {
			t.Error("an empty path must not silently load defaults over a running configuration")
		}
	})
}

func quote(s string) string {
	b := strings.Builder{}
	b.WriteByte('"')
	b.WriteString(strings.ReplaceAll(s, `\`, `\\`))
	b.WriteByte('"')
	return b.String()
}

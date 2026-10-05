package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// TestDefaultsAreValid matters more than it looks. -selfcheck has to mean
// something on a host with no configuration file, and a shipped default set
// that failed its own rules would make every rejection test below meaningless.
func TestDefaultsAreValid(t *testing.T) {
	c := Defaults()
	if err := Validate(c, nil); err != nil {
		t.Fatalf("the default configuration must validate: %v", err)
	}

	// Spot-check a few defaults against the key table rather than all of
	// them; the point is that resolve ran, not to restate the table.
	if c.UDP.Listen != ":9000" || c.UDP.StallBudgetMs != 50 {
		t.Errorf("udp defaults wrong: %+v", c.UDP)
	}
	if c.Runtime.Shards != 4 || c.Runtime.DatagramQueueCapacity != 8192 {
		t.Errorf("runtime defaults wrong: %+v", c.Runtime)
	}
	if c.Flush.WindowMs != 200 {
		t.Errorf("flush.windowMs = %d, want 200", c.Flush.WindowMs)
	}
	if c.Ordering.Guard != "auto" {
		t.Errorf("ordering.guard = %q, want auto", c.Ordering.Guard)
	}
}

// TestAbsentIsNotZero is the reason every field in the file shape is a
// pointer. Without that, "sourceEnforce": false and omitting the key are the
// same input, and no default could ever be anything but the zero value.
func TestAbsentIsNotZero(t *testing.T) {
	absent, err := Parse([]byte(`{"udp":{}}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if absent.UDP.ExpectedNetID != 8 {
		t.Errorf("omitted expectedNetId = %d, want the default 8", absent.UDP.ExpectedNetID)
	}

	explicit, err := Parse([]byte(`{"udp":{"expectedNetId":0}}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if explicit.UDP.ExpectedNetID != 0 {
		t.Errorf("explicit expectedNetId=0 became %d; an explicit zero must survive, "+
			"because 0 is how the netId check is disabled", explicit.UDP.ExpectedNetID)
	}
}

// TestFullConfigRoundTrips exercises a file that sets EVERY key, so a field
// present in the resolved shape but missing from the on-disk shape -- or
// spelled differently between them -- shows up here rather than as a key an
// operator sets and nothing reads.
func TestFullConfigRoundTrips(t *testing.T) {
	const full = `{
	  "udp": {"listen": ":9100", "readBufferBytes": 131072, "expectedNetId": 9,
	          "sourceAllowList": ["10.0.0.1"], "sourceEnforce": true, "stallBudgetMs": 40},
	  "runtime": {"gomaxprocs": 8, "shards": 8, "generalShard": 2,
	              "shardQueueCapacity": 8192, "datagramQueueCapacity": 16384,
	              "maxInFlightPerShard": 4, "maxBatchSizeCeiling": 2000,
	              "expectedRows": 400000, "arenaChunkRows": 8192,
	              "time1980UnixOffsetSec": 315552600, "destinationsFile": "dest.json"},
	  "flush": {"windowMs": 250, "maxBatchSize": 1500, "drainTimeoutMs": 7000,
	            "epochStallWarnMs": 45000},
	  "writers": {"count": 6, "statementFailWriters": 3, "statementFailWindowSec": 90,
	              "failFastSeconds": 45, "failFastWrites": 50, "retryInitialMs": 200,
	              "retryMaxMs": 8000, "connIdleReleaseSec": 120, "stmtCacheSize": 512},
	  "db": {"host": "db.internal", "port": 1522, "service": "FEEDPRD", "user": "feedvalve",
	         "connectTimeoutSec": 15, "queryTimeoutSec": 45, "connMaxLifetimeMinutes": 20},
	  "ordering": {"guard": "on", "guardSuppressionRatio": 0.25, "guardMinSamples": 500,
	               "guardEvalIntervalSec": 10, "gapScope": "per-transcode",
	               "gapAuditPackets": 1000},
	  "stats": {"intervalSec": 30, "logLevel": "warn", "eventRatePerMin": 20},
	  "liveness": {"heartbeatWarnSec": 15, "heartbeatDownSec": 40, "startupGraceSec": 90,
	               "tradingWindow": {"start": "08:30", "end": "22:00", "tz": "UTC"}},
	  "debug": {"traceTranscodes": [7208, 7305], "hexDump": true},
	  "transcodes": [
	    {"code": 7208, "class": "rate-limited", "expectedRatePerSec": 2000,
	     "contributions": [{"destination": "MD_TOKEN_QUOTE", "columns": ["last_traded_price"]}]}
	  ]
	}`

	c, err := Parse([]byte(full))
	if err != nil {
		t.Fatalf("a file setting every key must load: %v", err)
	}

	// Every value must be the file's, not a default. Checking a sample from
	// each section is enough to catch a section wired to the wrong struct.
	for _, tc := range []struct {
		name      string
		got, want any
	}{
		{"udp.listen", c.UDP.Listen, ":9100"},
		{"udp.sourceEnforce", c.UDP.SourceEnforce, true},
		{"runtime.generalShard", c.Runtime.GeneralShard, 2},
		{"runtime.destinationsFile", c.Runtime.DestinationsFile, "dest.json"},
		{"runtime.time1980UnixOffsetSec", c.Runtime.Time1980UnixOffsetSec, 315552600},
		{"flush.maxBatchSize", c.Flush.MaxBatchSize, 1500},
		{"writers.stmtCacheSize", c.Writers.StmtCacheSize, 512},
		{"db.port", c.DB.Port, 1522},
		{"db.maxOpenConns (derived)", c.DB.MaxOpenConns, 7},
		{"ordering.guardSuppressionRatio", c.Ordering.GuardSuppressionRatio, 0.25},
		{"stats.logLevel", c.Stats.LogLevel, "warn"},
		{"liveness.tradingWindow.tz", c.Liveness.TradingWindow.TZ, "UTC"},
		{"debug.hexDump", c.Debug.HexDump, true},
		{"transcodes[0].code", c.Transcodes[0].Code, 7208},
		{"transcodes[0].contributions[0].destination",
			c.Transcodes[0].Contributions[0].Destination, "MD_TOKEN_QUOTE"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
	if len(c.Debug.TraceTranscodes) != 2 || len(c.UDP.SourceAllowList) != 1 {
		t.Errorf("slice-valued keys did not survive: %+v / %+v",
			c.Debug.TraceTranscodes, c.UDP.SourceAllowList)
	}
}

// TestUnknownKeyIsRejected: a typo'd key silently ignored means the operator
// believes they changed something and did not.
func TestUnknownKeyIsRejected(t *testing.T) {
	for _, bad := range []string{
		`{"udp":{"stallBudget":40}}`,       // truncated
		`{"udp":{"stallBudgetMillis":40}}`, // plausible but wrong
		`{"runtime":{"maxBatchSize":100}}`, // right name, wrong section
		`{"flush":{"windowMillis":100}}`,   // wrong unit suffix
		`{"nosuchsection":{"x":1}}`,        // unknown top-level object
		`{"db":{"password":"hunter2"}}`,    // the one that matters most
	} {
		t.Run(bad, func(t *testing.T) {
			_, err := Parse([]byte(bad))
			if err == nil {
				t.Fatal("accepted; a key the schema does not have must fail the load")
			}
			if !strings.Contains(err.Error(), "unknown field") {
				t.Errorf("rejected, but not as an unknown field: %v", err)
			}
		})
	}
}

// TestKeyMatchingIsCaseInsensitive records Go's behaviour rather than wishing
// it away: encoding/json falls back to a case-insensitive match, so
// "stallBudgetMS" resolves to StallBudgetMs instead of being unknown.
//
// This is benign and worth pinning. The operator's value IS applied, so the
// failure mode strictness exists to prevent -- a change that silently does
// nothing -- does not occur. A test asserting the opposite would be asserting
// something false about the standard library.
func TestKeyMatchingIsCaseInsensitive(t *testing.T) {
	c, err := Parse([]byte(`{"udp":{"stallBudgetMS":40}}`))
	if err != nil {
		t.Fatalf("case-variant key should resolve, not fail: %v", err)
	}
	if c.UDP.StallBudgetMs != 40 {
		t.Errorf("stallBudgetMS applied as %d, want 40", c.UDP.StallBudgetMs)
	}
}

func TestRejections(t *testing.T) {
	for _, tc := range []struct {
		name string
		json string
		want string
	}{
		{"stall budget over NFR4's whole budget", `{"udp":{"stallBudgetMs":150}}`, "udp.stallBudgetMs"},
		{"stall budget zero", `{"udp":{"stallBudgetMs":0}}`, "udp.stallBudgetMs"},
		{"read buffer too small", `{"udp":{"readBufferBytes":1024}}`, "udp.readBufferBytes"},
		{"netId out of range", `{"udp":{"expectedNetId":256}}`, "udp.expectedNetId"},
		{"empty listen", `{"udp":{"listen":""}}`, "udp.listen is empty"},

		{"datagram queue not a power of two", `{"runtime":{"datagramQueueCapacity":5000}}`, "not a power of two"},
		{"datagram queue too small", `{"runtime":{"datagramQueueCapacity":512}}`, "runtime.datagramQueueCapacity"},
		{"generalShard outside shards", `{"runtime":{"shards":4,"generalShard":4}}`, "runtime.generalShard"},
		{"gomaxprocs zero", `{"runtime":{"gomaxprocs":0}}`, "runtime.gomaxprocs"},

		{"maxBatchSize over the ceiling", `{"runtime":{"maxBatchSizeCeiling":100},"flush":{"maxBatchSize":500}}`, "flush.maxBatchSize"},
		{"window too short", `{"flush":{"windowMs":5}}`, "flush.windowMs"},

		{"statementFailWriters over count", `{"writers":{"count":2,"statementFailWriters":3}}`, "writers.statementFailWriters"},
		{"retryMax below retryInitial", `{"writers":{"retryInitialMs":5000,"retryMaxMs":100}}`, "below writers.retryInitialMs"},
		{"writers count zero", `{"writers":{"count":0}}`, "writers.count"},

		{"heartbeat down not after warn", `{"liveness":{"heartbeatWarnSec":30,"heartbeatDownSec":30}}`, "must exceed heartbeatWarnSec"},
		{"bad trading window time", `{"liveness":{"tradingWindow":{"start":"9am"}}}`, "want HH:MM"},
		{"unresolvable timezone", `{"liveness":{"tradingWindow":{"tz":"Mars/Olympus"}}}`, "does not resolve"},

		{"bad guard", `{"ordering":{"guard":"maybe"}}`, "ordering.guard"},
		{"bad gap scope", `{"ordering":{"gapScope":"sometimes"}}`, "ordering.gapScope"},
		{"suppression ratio above 1", `{"ordering":{"guardSuppressionRatio":1.5}}`, "guardSuppressionRatio"},
		{"suppression ratio zero", `{"ordering":{"guardSuppressionRatio":0}}`, "guardSuppressionRatio"},

		{"bad log level", `{"stats":{"logLevel":"trace"}}`, "stats.logLevel"},

		{"db without host", `{"db":{"service":"S","user":"U"}}`, "db.host is required"},
		{"db port out of range", `{"db":{"host":"h","service":"s","user":"u","port":70000}}`, "db.port"},

		{"transcode code zero", `{"transcodes":[{"code":0}]}`, "not a valid transaction code"},
		{"duplicate transcode", `{"transcodes":[{"code":7208},{"code":7208}]}`, "declared twice"},
		{"bad class", `{"transcodes":[{"code":7208,"class":"urgent"}]}`, "class"},
		{"contribution with no destination", `{"transcodes":[{"code":7208,"contributions":[{"columns":["a"]}]}]}`, "names no destination"},
		{"contribution with no columns", `{"transcodes":[{"code":7208,"contributions":[{"destination":"D"}]}]}`, "declares no columns"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Parse([]byte(tc.json))
			if err == nil {
				t.Fatalf("configuration was ACCEPTED; expected rejection mentioning %q\n%+v",
					tc.want, c)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not explain the problem.\n got: %v\nwant substring: %q",
					err, tc.want)
			}
		})
	}
}

// TestAllProblemsReportedAtOnce: an operator fixing a configuration should
// not have to restart once per mistake.
func TestAllProblemsReportedAtOnce(t *testing.T) {
	_, err := Parse([]byte(`{"udp":{"stallBudgetMs":500},"stats":{"logLevel":"trace"},
		"ordering":{"guard":"maybe"}}`))
	if err == nil {
		t.Fatal("expected rejection")
	}
	for _, want := range []string{"udp.stallBudgetMs", "stats.logLevel", "ordering.guard"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error omits %q; all problems should be reported together:\n%v", want, err)
		}
	}
	if !strings.Contains(err.Error(), "3 problems") {
		t.Errorf("error does not count the problems: %v", err)
	}
}

// TestDerivedPoolFollowsWriterCount is contradiction C5. Without it, the
// documented remedy for a slow write path -- raise writers -- silently does
// nothing past a separately fixed pool limit, which is a throughput cliff on
// the exact lever the runbook names.
func TestDerivedPoolFollowsWriterCount(t *testing.T) {
	c, err := Parse([]byte(`{"writers":{"count":8},
		"db":{"host":"h","service":"s","user":"u"}}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.DB.MaxOpenConns != 9 || c.DB.MaxIdleConns != 9 {
		t.Errorf("pool = %d open / %d idle, want 9/9 (writers.count+1)",
			c.DB.MaxOpenConns, c.DB.MaxIdleConns)
	}

	c.Writers.Count = 16
	c.applyDerivedPool()
	if err := Validate(c, nil); err != nil {
		t.Errorf("after reapplying the derived pool the config should validate: %v", err)
	}

	// And the validator notices when it was not reapplied.
	c.Writers.Count = 2
	if err := Validate(c, nil); err == nil ||
		!strings.Contains(err.Error(), "do not follow writers.count+1") {
		t.Errorf("a stale derived pool should be rejected, got: %v", err)
	}
}

func TestReloadClasses(t *testing.T) {
	base := func(t *testing.T) *Config {
		t.Helper()
		c, err := Parse([]byte(`{"runtime":{"shards":4},"writers":{"count":4},
			"transcodes":[{"code":7208,"contributions":[{"destination":"D0","columns":["a"]}]}]}`))
		if err != nil {
			t.Fatalf("parse base: %v", err)
		}
		return c
	}

	t.Run("reloadable keys are accepted", func(t *testing.T) {
		prev, next := base(t), base(t)
		next.Flush.WindowMs = 500
		next.Writers.Count = 8
		next.applyDerivedPool()
		next.Ordering.Guard = "off"
		next.Stats.LogLevel = "warn"

		if err := Validate(next, prev); err != nil {
			t.Errorf("reloadable changes should be accepted: %v", err)
		}
	})

	// C2 in full. The contradiction section 7.3 resolves is that HLD
	// section 6.9 called maxBatchSize "reloadable downward only" while
	// justifying it with a reason -- the row buffers are pre-allocated to
	// the ceiling -- that permits raising it too. Both directions must work
	// inside the ceiling, so testing only the rejection above it would
	// leave the half that was actually contradicted unchecked.
	t.Run("maxBatchSize is reloadable in both directions", func(t *testing.T) {
		for _, to := range []int{1, 250, 1000} {
			prev, next := base(t), base(t)
			prev.Flush.MaxBatchSize = 500
			next.Flush.MaxBatchSize = to

			if err := Validate(next, prev); err != nil {
				t.Errorf("500 -> %d within the ceiling should be accepted: %v", to, err)
			}
		}

		// And still rejected past the ceiling.
		prev, next := base(t), base(t)
		next.Flush.MaxBatchSize = next.Runtime.MaxBatchSizeCeiling + 1
		if err := Validate(next, prev); err == nil {
			t.Error("maxBatchSize above the ceiling should be rejected")
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"shards", func(c *Config) { c.Runtime.Shards = 8 }, "runtime.shards is startup-only"},
		{"listen", func(c *Config) { c.UDP.Listen = ":9999" }, "udp.listen is startup-only"},
		{"generalShard", func(c *Config) { c.Runtime.GeneralShard = 1 }, "runtime.generalShard is startup-only"},
		{"shardQueueCapacity", func(c *Config) { c.Runtime.ShardQueueCapacity = 8192 }, "runtime.shardQueueCapacity is startup-only"},
		{"datagramQueueCapacity", func(c *Config) { c.Runtime.DatagramQueueCapacity = 16384 }, "runtime.datagramQueueCapacity is startup-only"},
		{"maxBatchSizeCeiling", func(c *Config) { c.Runtime.MaxBatchSizeCeiling = 2000 }, "runtime.maxBatchSizeCeiling is startup-only"},
		{"expectedRows", func(c *Config) { c.Runtime.ExpectedRows = 400000 }, "runtime.expectedRows is startup-only"},
		{"destinationsFile", func(c *Config) { c.Runtime.DestinationsFile = "other.json" }, "runtime.destinationsFile is startup-only"},
		{"transcode set", func(c *Config) {
			c.Transcodes = append(c.Transcodes, TranscodeConfig{
				Code: 7305, Class: "rate-limited",
				Contributions: []ContributionConfig{{Destination: "D1", Columns: []string{"x"}}},
			})
		}, "membership is startup-only"},
		{"contribution target", func(c *Config) {
			c.Transcodes[0].Contributions[0].Destination = "D2"
		}, "contribution membership changed"},
	} {
		t.Run("startup-only: "+tc.name, func(t *testing.T) {
			prev, next := base(t), base(t)
			tc.mutate(next)

			err := Validate(next, prev)
			if err == nil {
				t.Fatalf("changing %s on reload was accepted; it must be rejected", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not name the rule.\n got: %v\nwant substring: %q", err, tc.want)
			}
		})
	}

	// C1's other half: which COLUMNS a transcode supplies is reloadable,
	// even though which destinations it feeds is not.
	t.Run("contribution columns are reloadable", func(t *testing.T) {
		prev, next := base(t), base(t)
		next.Transcodes[0].Contributions[0].Columns = []string{"a", "b"}
		if err := Validate(next, prev); err != nil {
			t.Errorf("changing the column list should be accepted: %v", err)
		}
	})
}

func TestStore(t *testing.T) {
	first := Defaults()
	s := NewStore(first)

	if s.Gen() != 1 {
		t.Errorf("first generation = %d, want 1 (0 must mean \"nothing published\")", s.Gen())
	}
	if s.Load() != first {
		t.Error("Load returned a different pointer than was published")
	}

	second := Defaults()
	second.Flush.WindowMs = 300
	if _, err := s.Swap(second); err != nil {
		t.Fatalf("valid swap: %v", err)
	}
	if s.Gen() != 2 || s.Load().Gen != 2 {
		t.Errorf("after swap Gen = %d / %d, want 2", s.Gen(), s.Load().Gen)
	}
	if s.Load().Flush.WindowMs != 300 {
		t.Error("swap did not publish the new configuration")
	}

	// A rejected swap must change nothing at all -- not the pointer, not
	// the generation. All-or-nothing is the whole property.
	bad := Defaults()
	bad.Runtime.Shards = 99
	if _, err := s.Swap(bad); err == nil {
		t.Fatal("a startup-only change should have been rejected")
	}
	if s.Gen() != 2 {
		t.Errorf("a rejected swap advanced the generation to %d", s.Gen())
	}
	if s.Load().Flush.WindowMs != 300 || s.Load().Runtime.Shards == 99 {
		t.Error("a rejected swap changed the published configuration")
	}
}

// TestNoPasswordAnywhere walks every type reachable from Config and from the
// on-disk shape, looking for anything that could hold the secret.
//
// The password comes only from the environment. A field named for it would
// not merely be redundant: Config is dumped wholesale into the startup
// "effective configuration" log line precisely because it cannot contain one,
// and that log is append-only and long-lived.
func TestNoPasswordAnywhere(t *testing.T) {
	for _, root := range []reflect.Type{
		reflect.TypeOf(Config{}),
		reflect.TypeOf(fileConfig{}),
	} {
		t.Run(root.Name(), func(t *testing.T) {
			walkFields(t, root, root.Name(), map[reflect.Type]bool{})
		})
	}
}

func walkFields(t *testing.T, typ reflect.Type, path string, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || seen[typ] {
		return
	}
	seen[typ] = true

	for i := range typ.NumField() {
		f := typ.Field(i)
		name := strings.ToLower(f.Name)
		tag := strings.ToLower(f.Tag.Get("json"))

		if strings.Contains(name, "password") || strings.Contains(name, "passwd") ||
			strings.Contains(name, "secret") || strings.Contains(tag, "password") {
			t.Errorf("%s.%s could hold the database password. It comes only from the "+
				"environment: Config is logged wholesale at startup because it cannot "+
				"contain one, and that log is append-only", path, f.Name)
		}
		walkFields(t, f.Type, path+"."+f.Name, seen)
	}
}

// TestConfigMarshalsWithoutSecrets is the companion to the walk above: the
// actual rendering, not just the shape.
func TestConfigMarshalsWithoutSecrets(t *testing.T) {
	t.Setenv("FEEDVALVE_DB_PASSWORD", "hunter2-UNIQUE-TOKEN")

	c, err := Parse([]byte(`{"db":{"host":"h","service":"s","user":"u"}}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, rendering := range []string{
		mustJSON(t, c),
		strings.ToLower(strings.Join([]string{c.DB.Describe()}, "")),
	} {
		if strings.Contains(rendering, "hunter2") {
			t.Errorf("a rendering of the configuration contains the password: %s", rendering)
		}
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"feedvalve/internal/oradb"
)

// The on-disk shape. Every scalar is a POINTER so that "absent from the file"
// is distinguishable from "explicitly set to the zero value" -- the pattern
// poc/config.go established. Without it, `"sourceEnforce": false` and omitting
// the key entirely are the same input, and a default can never be anything but
// the zero value.
type fileConfig struct {
	Comment json.RawMessage `json:"_comment"`

	UDP      *fileUDP      `json:"udp"`
	Runtime  *fileRuntime  `json:"runtime"`
	Flush    *fileFlush    `json:"flush"`
	Writers  *fileWriters  `json:"writers"`
	DB       *fileDB       `json:"db"`
	Ordering *fileOrdering `json:"ordering"`
	Stats    *fileStats    `json:"stats"`
	Liveness *fileLiveness `json:"liveness"`
	Debug    *fileDebug    `json:"debug"`

	Transcodes []fileTranscode `json:"transcodes"`
}

type fileUDP struct {
	Listen          *string   `json:"listen"`
	ReadBufferBytes *int      `json:"readBufferBytes"`
	ExpectedNetID   *int      `json:"expectedNetId"`
	SourceAllowList *[]string `json:"sourceAllowList"`
	SourceEnforce   *bool     `json:"sourceEnforce"`
	StallBudgetMs   *int      `json:"stallBudgetMs"`
}

type fileRuntime struct {
	GOMAXPROCS            *int    `json:"gomaxprocs"`
	Shards                *int    `json:"shards"`
	GeneralShard          *int    `json:"generalShard"`
	ShardQueueCapacity    *int    `json:"shardQueueCapacity"`
	DatagramQueueCapacity *int    `json:"datagramQueueCapacity"`
	MaxInFlightPerShard   *int    `json:"maxInFlightPerShard"`
	MaxBatchSizeCeiling   *int    `json:"maxBatchSizeCeiling"`
	ExpectedRows          *int    `json:"expectedRows"`
	ArenaChunkRows        *int    `json:"arenaChunkRows"`
	Time1980UnixOffsetSec *int    `json:"time1980UnixOffsetSec"`
	DestinationsFile      *string `json:"destinationsFile"`
}

type fileFlush struct {
	WindowMs         *int `json:"windowMs"`
	MaxBatchSize     *int `json:"maxBatchSize"`
	DrainTimeoutMs   *int `json:"drainTimeoutMs"`
	EpochStallWarnMs *int `json:"epochStallWarnMs"`
}

type fileWriters struct {
	Count                  *int `json:"count"`
	StatementFailWriters   *int `json:"statementFailWriters"`
	StatementFailWindowSec *int `json:"statementFailWindowSec"`
	FailFastSeconds        *int `json:"failFastSeconds"`
	FailFastWrites         *int `json:"failFastWrites"`
	RetryInitialMs         *int `json:"retryInitialMs"`
	RetryMaxMs             *int `json:"retryMaxMs"`
	ConnIdleReleaseSec     *int `json:"connIdleReleaseSec"`
	StmtCacheSize          *int `json:"stmtCacheSize"`
}

// fileDB has no password field, and adding one must stay impossible rather
// than merely discouraged: TestNoPasswordKeyAnywhere walks this struct.
type fileDB struct {
	Host                   *string `json:"host"`
	Port                   *int    `json:"port"`
	Service                *string `json:"service"`
	User                   *string `json:"user"`
	ConnectTimeoutSec      *int    `json:"connectTimeoutSec"`
	QueryTimeoutSec        *int    `json:"queryTimeoutSec"`
	ConnMaxLifetimeMinutes *int    `json:"connMaxLifetimeMinutes"`
}

type fileOrdering struct {
	Guard                 *string  `json:"guard"`
	GuardSuppressionRatio *float64 `json:"guardSuppressionRatio"`
	GuardMinSamples       *int     `json:"guardMinSamples"`
	GuardEvalIntervalSec  *int     `json:"guardEvalIntervalSec"`
	GapScope              *string  `json:"gapScope"`
	GapAuditPackets       *int     `json:"gapAuditPackets"`
}

type fileStats struct {
	IntervalSec     *int    `json:"intervalSec"`
	LogLevel        *string `json:"logLevel"`
	EventRatePerMin *int    `json:"eventRatePerMin"`
}

type fileLiveness struct {
	HeartbeatWarnSec *int               `json:"heartbeatWarnSec"`
	HeartbeatDownSec *int               `json:"heartbeatDownSec"`
	StartupGraceSec  *int               `json:"startupGraceSec"`
	TradingWindow    *fileTradingWindow `json:"tradingWindow"`
}

type fileTradingWindow struct {
	Start *string `json:"start"`
	End   *string `json:"end"`
	TZ    *string `json:"tz"`
}

type fileDebug struct {
	TraceTranscodes *[]int `json:"traceTranscodes"`
	HexDump         *bool  `json:"hexDump"`
}

type fileTranscode struct {
	Code               *int               `json:"code"`
	Class              *string            `json:"class"`
	Contributions      []fileContribution `json:"contributions"`
	ExpectedRatePerSec *int               `json:"expectedRatePerSec"`
}

type fileContribution struct {
	Destination *string   `json:"destination"`
	Columns     *[]string `json:"columns"`
}

// pick resolves an optional field against its default.
func pick[T any](p *T, def T) T {
	if p != nil {
		return *p
	}
	return def
}

// Defaults returns the configuration with every default applied and nothing
// else. It is a valid configuration, which is what makes -selfcheck
// meaningful on a host with no config file.
func Defaults() *Config {
	var empty fileConfig
	return resolve(empty)
}

// Load reads and validates a configuration file.
//
// A file that fails is rejected WHOLESALE, with the reason named. There is no
// partial application: a configuration half-applied is worse than a stale one,
// because the half that applied may contradict the half that did not.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}
	c, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return c, nil
}

// LoadUnvalidated reads and resolves a file WITHOUT running the rule set.
//
// It exists for the one caller that also has a field map: validating the
// configuration alone and then the pair separately means a file with a range
// error and a schema error reports only the first, and the operator fixes one
// problem per run. app uses this and then calls ValidateAll once, so every
// problem in both halves comes back together.
//
// Any other caller should use Load. The name is deliberately awkward.
func LoadUnvalidated(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}
	f, err := decode(raw)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return resolve(f), nil
}

// decode performs the strict JSON decode shared by both load paths.
func decode(raw []byte) (fileConfig, error) {
	var f fileConfig

	// V-DB3 first, over a generic tree. The strict decode below would also
	// refuse a password key, but as "unknown field" -- and a file that
	// carries one has already leaked it, which the operator must be told.
	if err := checkNoPasswordKey(raw); err != nil {
		return f, err
	}

	dec := json.NewDecoder(bytes.NewReader(raw))

	// V-STRICT1. Unknown keys are REJECTED, not ignored. A typo'd key
	// silently dropped means the operator believes they changed something
	// and did not -- during the incident they are trying to fix.
	dec.DisallowUnknownFields()

	if err := dec.Decode(&f); err != nil {
		const unknown = "json: unknown field "
		if msg := err.Error(); strings.HasPrefix(msg, unknown) {
			// encoding/json reports the leaf name only, not its path.
			key := strings.Trim(strings.TrimPrefix(msg, unknown), `"`)
			var p problems
			p.addKey(RuleStrictKeys, key, "", "", "unknown configuration key %q: %s. "+
				"A misspelt tuning key would otherwise do nothing at all, silently", key, msg)
			return f, p.err()
		}
		return f, fmt.Errorf("decoding configuration: %w", err)
	}
	return f, nil
}

// checkNoPasswordKey is V-DB3: no key named "password" at any depth, found by
// walking the file as generic JSON BEFORE the strict decode can hide it as an
// unknown field. It reports the key path only, never the value.
//
// Input that is not a JSON object is left for the real decode to reject.
func checkNoPasswordKey(raw []byte) error {
	var tree map[string]any
	if json.Unmarshal(raw, &tree) != nil {
		return nil
	}
	var p problems
	walkForPassword(&p, "", tree)
	return p.err()
}

func walkForPassword(p *problems, path string, v any) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		slices.Sort(keys) // map order is random; the message is read and diffed
		for _, k := range keys {
			kp := k
			if path != "" {
				kp = path + "." + k
			}
			if strings.EqualFold(k, "password") {
				p.addKey(RuleDBNoPassword, kp, "", "", "%s is present in the configuration file. "+
					"The file has already leaked that credential: rotate it, delete the key, and "+
					"supply the password only through FEEDVALVE_DB_PASSWORD", kp)
				continue
			}
			walkForPassword(p, kp, t[k])
		}
	case []any:
		for i, e := range t {
			walkForPassword(p, fmt.Sprintf("%s[%d]", path, i), e)
		}
	}
}

// Parse decodes, resolves and validates a configuration.
func Parse(raw []byte) (*Config, error) {
	f, err := decode(raw)
	if err != nil {
		return nil, err
	}
	c := resolve(f)
	if err := Validate(c, nil); err != nil {
		return nil, err
	}
	return c, nil
}

// resolve applies defaults to the file shape. It never fails; everything that
// can be wrong is Validate's job, so that one function reports every problem
// in one vocabulary.
func resolve(f fileConfig) *Config {
	c := &Config{}

	u := f.UDP
	if u == nil {
		u = &fileUDP{}
	}
	c.UDP = UDPConfig{
		Listen:          pick(u.Listen, ":9000"),
		ReadBufferBytes: pick(u.ReadBufferBytes, 655360),
		ExpectedNetID:   pick(u.ExpectedNetID, 8),
		SourceAllowList: pick(u.SourceAllowList, []string{}),
		SourceEnforce:   pick(u.SourceEnforce, false),
		StallBudgetMs:   pick(u.StallBudgetMs, 50),
	}

	r := f.Runtime
	if r == nil {
		r = &fileRuntime{}
	}
	c.Runtime = RuntimeConfig{
		GOMAXPROCS:            pick(r.GOMAXPROCS, 4),
		Shards:                pick(r.Shards, 4),
		GeneralShard:          pick(r.GeneralShard, 0),
		ShardQueueCapacity:    pick(r.ShardQueueCapacity, 4096),
		DatagramQueueCapacity: pick(r.DatagramQueueCapacity, 8192),
		MaxInFlightPerShard:   pick(r.MaxInFlightPerShard, 2),
		MaxBatchSizeCeiling:   pick(r.MaxBatchSizeCeiling, 1000),
		ExpectedRows:          pick(r.ExpectedRows, 200000),
		ArenaChunkRows:        pick(r.ArenaChunkRows, 4096),
		Time1980UnixOffsetSec: pick(r.Time1980UnixOffsetSec, 315532800),
		DestinationsFile:      pick(r.DestinationsFile, ""),
	}

	fl := f.Flush
	if fl == nil {
		fl = &fileFlush{}
	}
	c.Flush = FlushConfig{
		WindowMs:         pick(fl.WindowMs, 200),
		MaxBatchSize:     pick(fl.MaxBatchSize, 1000),
		DrainTimeoutMs:   pick(fl.DrainTimeoutMs, 5000),
		EpochStallWarnMs: pick(fl.EpochStallWarnMs, 30000),
	}

	w := f.Writers
	if w == nil {
		w = &fileWriters{}
	}
	c.Writers = WritersConfig{
		Count:                  pick(w.Count, 4),
		StatementFailWriters:   pick(w.StatementFailWriters, 2),
		StatementFailWindowSec: pick(w.StatementFailWindowSec, 60),
		FailFastSeconds:        pick(w.FailFastSeconds, 30),
		FailFastWrites:         pick(w.FailFastWrites, 20),
		RetryInitialMs:         pick(w.RetryInitialMs, 100),
		RetryMaxMs:             pick(w.RetryMaxMs, 5000),
		ConnIdleReleaseSec:     pick(w.ConnIdleReleaseSec, 60),
		StmtCacheSize:          pick(w.StmtCacheSize, 256),
	}

	if f.DB != nil {
		c.HasDB = true
		c.DB = oradb.DBSettings{
			Host:                   pick(f.DB.Host, ""),
			Port:                   pick(f.DB.Port, 1521),
			Service:                pick(f.DB.Service, ""),
			User:                   pick(f.DB.User, ""),
			ConnectTimeoutSec:      pick(f.DB.ConnectTimeoutSec, 10),
			QueryTimeoutSec:        pick(f.DB.QueryTimeoutSec, 30),
			ConnMaxLifetimeMinutes: pick(f.DB.ConnMaxLifetimeMinutes, 30),
		}
	}
	// Derived, never configured -- see the note on DBSettings.MaxOpenConns.
	c.applyDerivedPool()

	o := f.Ordering
	if o == nil {
		o = &fileOrdering{}
	}
	c.Ordering = OrderingConfig{
		Guard:                 pick(o.Guard, "auto"),
		GuardSuppressionRatio: pick(o.GuardSuppressionRatio, 0.5),
		GuardMinSamples:       pick(o.GuardMinSamples, 100),
		GuardEvalIntervalSec:  pick(o.GuardEvalIntervalSec, 5),
		GapScope:              pick(o.GapScope, "global"),
		GapAuditPackets:       pick(o.GapAuditPackets, 0),
	}

	s := f.Stats
	if s == nil {
		s = &fileStats{}
	}
	c.Stats = StatsConfig{
		IntervalSec:     pick(s.IntervalSec, 60),
		LogLevel:        pick(s.LogLevel, "info"),
		EventRatePerMin: pick(s.EventRatePerMin, 10),
	}

	l := f.Liveness
	if l == nil {
		l = &fileLiveness{}
	}
	tw := l.TradingWindow
	if tw == nil {
		tw = &fileTradingWindow{}
	}
	c.Liveness = LivenessConfig{
		HeartbeatWarnSec: pick(l.HeartbeatWarnSec, 12),
		HeartbeatDownSec: pick(l.HeartbeatDownSec, 27),
		StartupGraceSec:  pick(l.StartupGraceSec, 60),
		TradingWindow: TradingWindow{
			Start: pick(tw.Start, "09:00"),
			End:   pick(tw.End, "23:55"),
			TZ:    pick(tw.TZ, "Asia/Kolkata"),
		},
	}

	d := f.Debug
	if d == nil {
		d = &fileDebug{}
	}
	c.Debug = DebugConfig{
		TraceTranscodes: pick(d.TraceTranscodes, []int{}),
		HexDump:         pick(d.HexDump, false),
	}

	for _, ft := range f.Transcodes {
		t := TranscodeConfig{
			Code:               pick(ft.Code, 0),
			Class:              pick(ft.Class, "rate-limited"),
			ExpectedRatePerSec: pick(ft.ExpectedRatePerSec, 0),
		}
		for _, fc := range ft.Contributions {
			t.Contributions = append(t.Contributions, ContributionConfig{
				Destination: pick(fc.Destination, ""),
				Columns:     pick(fc.Columns, []string{}),
			})
		}
		c.Transcodes = append(c.Transcodes, t)
	}

	return c
}

// applyDerivedPool recomputes the pool sizes from writers.count.
//
// These follow writers.count on EVERY accepted reload. Otherwise the
// documented remedy for a slow write path -- "raise writers" -- silently does
// nothing past a separately fixed pool limit, and the operator sees no effect
// from the one lever the runbook gives them.
func (c *Config) applyDerivedPool() {
	c.DB.MaxOpenConns = c.Writers.Count + 1
	c.DB.MaxIdleConns = c.Writers.Count + 1
}

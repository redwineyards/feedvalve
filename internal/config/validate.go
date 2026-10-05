package config

import (
	"fmt"
	"math/bits"
	"net"
	"net/netip"
	"slices"
	"strings"
	"time"
)

// rangeRule is one key's absolute bounds, from Vol 0 section 7.2.
//
// Table-driven rather than a sequence of if statements, so that gate G-CFG-1
// can assert every numeric key in the file shape has an entry. Without that,
// a key added to the schema ships unchecked and the first thing that notices
// is production.
//
// Relative bounds -- maxBatchSize against its ceiling, statementFailWriters
// against writers.count -- are NOT here. Those are cross-field rules with
// their own identifiers, and they apply on top of these.
type rangeRule struct {
	key    string // the JSON path, which is what G-CFG-1 matches against
	get    func(*Config) int
	lo, hi int
}

// rangeRuleID overrides V-RANGE for a key whose bound has an identifier of
// its own.
//
// Exactly one key needs this, and the reason is worth stating. V-X6 and
// section 7.2's range for udp.stallBudgetMs are THE SAME BOUND -- 100 ms --
// so reporting both would make it the one key for which no input can trip
// exactly one rule, and "exactly one rule" is what the per-rule tests rest
// on. Every other cross-field rule is a genuinely different constraint from
// its key's absolute range and can fire alone: maxBatchSize 1500 is inside
// 1..10000 but outside a ceiling of 1000, and trips only V-X3.
var rangeRuleID = map[string]string{
	"udp.stallBudgetMs": RuleStallBudget,
}

func (r rangeRule) id() string {
	if id, ok := rangeRuleID[r.key]; ok {
		return id
	}
	return RuleRange
}

var rangeRules = []rangeRule{
	{"udp.readBufferBytes", func(c *Config) int { return c.UDP.ReadBufferBytes }, 65536, 4194304},
	{"udp.expectedNetId", func(c *Config) int { return c.UDP.ExpectedNetID }, 0, 255},
	{"udp.stallBudgetMs", func(c *Config) int { return c.UDP.StallBudgetMs }, 1, 100},

	{"runtime.gomaxprocs", func(c *Config) int { return c.Runtime.GOMAXPROCS }, 1, 64},
	{"runtime.shards", func(c *Config) int { return c.Runtime.Shards }, 1, 64},
	{"runtime.generalShard", func(c *Config) int { return c.Runtime.GeneralShard }, 0, 63},
	{"runtime.shardQueueCapacity", func(c *Config) int { return c.Runtime.ShardQueueCapacity }, 256, 65536},
	{"runtime.datagramQueueCapacity", func(c *Config) int { return c.Runtime.DatagramQueueCapacity }, 1024, 65536},
	{"runtime.maxInFlightPerShard", func(c *Config) int { return c.Runtime.MaxInFlightPerShard }, 1, 16},
	{"runtime.maxBatchSizeCeiling", func(c *Config) int { return c.Runtime.MaxBatchSizeCeiling }, 1, 10000},
	{"runtime.expectedRows", func(c *Config) int { return c.Runtime.ExpectedRows }, 1000, 5000000},
	{"runtime.arenaChunkRows", func(c *Config) int { return c.Runtime.ArenaChunkRows }, 256, 65536},
	{"runtime.time1980UnixOffsetSec", func(c *Config) int { return c.Runtime.Time1980UnixOffsetSec }, 0, 1<<31 - 1},

	{"flush.windowMs", func(c *Config) int { return c.Flush.WindowMs }, 10, 60000},
	{"flush.maxBatchSize", func(c *Config) int { return c.Flush.MaxBatchSize }, 1, 10000},
	{"flush.drainTimeoutMs", func(c *Config) int { return c.Flush.DrainTimeoutMs }, 0, 60000},
	{"flush.epochStallWarnMs", func(c *Config) int { return c.Flush.EpochStallWarnMs }, 1000, 600000},

	{"writers.count", func(c *Config) int { return c.Writers.Count }, 1, 32},
	{"writers.statementFailWriters", func(c *Config) int { return c.Writers.StatementFailWriters }, 1, 32},
	{"writers.statementFailWindowSec", func(c *Config) int { return c.Writers.StatementFailWindowSec }, 1, 3600},
	{"writers.failFastSeconds", func(c *Config) int { return c.Writers.FailFastSeconds }, 1, 3600},
	{"writers.failFastWrites", func(c *Config) int { return c.Writers.FailFastWrites }, 1, 100000},
	{"writers.retryInitialMs", func(c *Config) int { return c.Writers.RetryInitialMs }, 1, 60000},
	{"writers.retryMaxMs", func(c *Config) int { return c.Writers.RetryMaxMs }, 1, 60000},
	{"writers.connIdleReleaseSec", func(c *Config) int { return c.Writers.ConnIdleReleaseSec }, 5, 3600},
	{"writers.stmtCacheSize", func(c *Config) int { return c.Writers.StmtCacheSize }, 8, 4096},

	{"ordering.guardMinSamples", func(c *Config) int { return c.Ordering.GuardMinSamples }, 1, 1000000},
	{"ordering.guardEvalIntervalSec", func(c *Config) int { return c.Ordering.GuardEvalIntervalSec }, 1, 3600},
	{"ordering.gapAuditPackets", func(c *Config) int { return c.Ordering.GapAuditPackets }, 0, 10000000},

	{"stats.intervalSec", func(c *Config) int { return c.Stats.IntervalSec }, 1, 3600},
	{"stats.eventRatePerMin", func(c *Config) int { return c.Stats.EventRatePerMin }, 1, 10000},

	{"liveness.heartbeatWarnSec", func(c *Config) int { return c.Liveness.HeartbeatWarnSec }, 1, 3600},
	{"liveness.heartbeatDownSec", func(c *Config) int { return c.Liveness.HeartbeatDownSec }, 1, 86400},
	{"liveness.startupGraceSec", func(c *Config) int { return c.Liveness.StartupGraceSec }, 1, 86400},
}

// dbRangeRules apply only when a db object is present.
var dbRangeRules = []rangeRule{
	{"db.port", func(c *Config) int { return c.DB.Port }, 1, 65535},
	{"db.connectTimeoutSec", func(c *Config) int { return c.DB.ConnectTimeoutSec }, 1, 300},
	{"db.queryTimeoutSec", func(c *Config) int { return c.DB.QueryTimeoutSec }, 1, 600},
	{"db.connMaxLifetimeMinutes", func(c *Config) int { return c.DB.ConnMaxLifetimeMinutes }, 1, 1440},
}

// Validate runs every rule checkable from the configuration alone.
//
// Schema rules that need the destination table -- V-D7's ASM-15, V-T3, V-T4,
// V-T5 -- are in ValidateAll, because this package cannot invent a field map.
// Callers that have one should use ValidateAll; Validate is what Parse runs so
// that a malformed file fails before anything downstream sees it.
func Validate(c *Config, prev *Config) error {
	var p problems
	validateInto(&p, c, prev)
	return p.err()
}

func validateInto(p *problems, c, prev *Config) {
	for _, r := range rangeRules {
		checkRange(p, c, r)
	}
	validateUDP(c, p)
	validateRuntime(c, p)
	validateFlush(c, p)
	validateWriters(c, p)
	validateDB(c, p)
	validateOrdering(c, p)
	validateStats(c, p)
	validateLiveness(c, p)
	validateDebug(c, p)
	validateTranscodes(c, p)

	if prev != nil {
		validateReload(c, prev, p)
	}
}

func checkRange(p *problems, c *Config, r rangeRule) {
	if v := r.get(c); v < r.lo || v > r.hi {
		p.add(r.id(), "%s is %d, outside the valid range %d..%d", r.key, v, r.lo, r.hi)
	}
}

func oneOf(p *problems, rule, key, v string, allowed ...string) {
	if !slices.Contains(allowed, v) {
		p.add(rule, "%s is %q, want one of %s", key, v, strings.Join(allowed, ", "))
	}
}

func validateUDP(c *Config, p *problems) {
	if c.UDP.Listen == "" {
		p.add(RuleRange, "udp.listen is empty")
	}

	// V-X6 is the range-table entry for udp.stallBudgetMs, which carries
	// RuleStallBudget rather than V-RANGE: 100 ms IS NFR4's entire
	// read-loop budget, so a value above it breaks the requirement rather
	// than tuning within it, and the two bounds are the same bound.

	// V-SRC1. With sourceEnforce set, an unparseable entry silently blinds
	// the feed -- the datagram arrives and is discarded by a filter nobody
	// realises is misconfigured.
	for i, e := range c.UDP.SourceAllowList {
		if _, err := netip.ParseAddr(e); err == nil {
			continue
		}
		if _, _, err := net.ParseCIDR(e); err == nil {
			continue
		}
		p.add(RuleSourceList, "udp.sourceAllowList[%d] is %q, which is neither an IP "+
			"address nor a CIDR block", i, e)
	}
}

func validateRuntime(c *Config, p *problems) {
	r := &c.Runtime

	// The QD ring wraps by masking, so a non-power-of-two does not merely
	// waste space -- it silently corrupts the index and the ring starts
	// returning the wrong datagram.
	if r.DatagramQueueCapacity > 0 && bits.OnesCount(uint(r.DatagramQueueCapacity)) != 1 {
		p.add(RuleRange, "runtime.datagramQueueCapacity is %d, which is not a power of two. "+
			"The datagram ring wraps by masking; a non-power-of-two silently corrupts "+
			"the index rather than failing", r.DatagramQueueCapacity)
	}

	// V-X4. An out-of-range designated shard routes every general unit into
	// a shard that does not exist.
	if r.Shards > 0 && (r.GeneralShard < 0 || r.GeneralShard >= r.Shards) {
		p.add(RuleGeneralShardRange, "runtime.generalShard is %d, outside 0..%d (shards-1). "+
			"Every non-token-scoped unit is pinned to it", r.GeneralShard, r.Shards-1)
	}
}

func validateFlush(c *Config, p *problems) {
	// V-X3. The ceiling is startup-only and sizes the pre-allocated
	// Batch.Rows slices, so a larger live value would index past allocated
	// memory. Reloadable in BOTH directions inside the ceiling.
	if f := c.Flush.MaxBatchSize; f < 1 || f > c.Runtime.MaxBatchSizeCeiling {
		p.add(RuleBatchSizeCeiling, "flush.maxBatchSize is %d, outside 1..%d; the ceiling is "+
			"startup-only and sizes the pre-allocated batch row buffers",
			f, c.Runtime.MaxBatchSizeCeiling)
	}
}

func validateWriters(c *Config, p *problems) {
	w := &c.Writers

	// V-X1. Rejected, not clamped: a silently clamped safety threshold
	// makes the pool-stop rule unreachable and nothing says so.
	if w.StatementFailWriters > w.Count {
		p.add(RuleStatementFailWriters, "writers.statementFailWriters is %d but there are only "+
			"%d writers, so the pool-stop rule could never fire", w.StatementFailWriters, w.Count)
	}

	// V-X5. Backoff that shrinks is a busy loop against a dead database.
	if w.RetryMaxMs < w.RetryInitialMs {
		p.add(RuleRetryOrder, "writers.retryMaxMs (%d) is below writers.retryInitialMs (%d), "+
			"which makes the backoff shrink", w.RetryMaxMs, w.RetryInitialMs)
	}
}

func validateDB(c *Config, p *problems) {
	if !c.HasDB {
		return
	}
	for _, r := range dbRangeRules {
		checkRange(p, c, r)
	}

	// V-DB2.
	for key, v := range map[string]string{
		"db.host":    c.DB.Host,
		"db.service": c.DB.Service,
		"db.user":    c.DB.User,
	} {
		if v == "" {
			p.add(RuleDBFields, "%s is required when a db object is present", key)
		}
	}

	// C5. Derived, so a mismatch means the pool was not reapplied after a
	// change to writers.count -- which makes "raise writers", the one lever
	// the runbook gives for a slow write path, silently ineffective.
	if want := c.Writers.Count + 1; c.DB.MaxOpenConns != want || c.DB.MaxIdleConns != want {
		p.add(RuleRange, "db pool sizes (%d open, %d idle) do not follow writers.count+1 (%d); "+
			"they are derived and must be reapplied on every reload",
			c.DB.MaxOpenConns, c.DB.MaxIdleConns, want)
	}
}

func validateOrdering(c *Config, p *problems) {
	o := &c.Ordering
	oneOf(p, RuleRange, "ordering.guard", o.Guard, "on", "off", "auto")
	oneOf(p, RuleRange, "ordering.gapScope", o.GapScope, "global", "per-transcode", "off")

	if o.GuardSuppressionRatio <= 0 || o.GuardSuppressionRatio > 1 {
		p.add(RuleRange, "ordering.guardSuppressionRatio is %g, want 0 < r <= 1",
			o.GuardSuppressionRatio)
	}
}

func validateStats(c *Config, p *problems) {
	oneOf(p, RuleRange, "stats.logLevel", c.Stats.LogLevel, "info", "warn", "error")
}

func validateLiveness(c *Config, p *problems) {
	l := &c.Liveness

	// V-X2. An inverted pair makes the liveness machine skip its warning
	// state entirely, so the first notice of a dead feed is the last one.
	if l.HeartbeatDownSec <= l.HeartbeatWarnSec {
		p.add(RuleHeartbeatOrder, "liveness.heartbeatDownSec (%d) must exceed "+
			"heartbeatWarnSec (%d), otherwise the warning state is unreachable",
			l.HeartbeatDownSec, l.HeartbeatWarnSec)
	}

	// V-TW1.
	for key, v := range map[string]string{
		"liveness.tradingWindow.start": l.TradingWindow.Start,
		"liveness.tradingWindow.end":   l.TradingWindow.End,
	} {
		if _, err := time.Parse("15:04", v); err != nil {
			p.add(RuleTradingTimes, "%s is %q, want HH:MM", key, v)
		}
	}

	// V-TZ1. See cmd/feedvalve/main.go: time/tzdata does not link in this
	// toolchain, so this depends on the HOST having a zoneinfo database.
	if _, err := time.LoadLocation(l.TradingWindow.TZ); err != nil {
		p.add(RuleTimezone, "liveness.tradingWindow.tz %q does not resolve: %v",
			l.TradingWindow.TZ, err)
	}
}

func validateDebug(c *Config, p *problems) {
	// V-DBG1. A typo traces nothing, which reads as "the bug went away".
	declared := map[int]bool{}
	for _, t := range c.Transcodes {
		declared[t.Code] = true
	}
	for _, code := range c.Debug.TraceTranscodes {
		if !declared[code] {
			p.add(RuleTraceCodes, "debug.traceTranscodes names %d, which is not a configured "+
				"transcode; it would trace nothing and read as the bug having gone away", code)
		}
	}
}

func validateTranscodes(c *Config, p *problems) {
	seen := map[int]bool{}
	for i, t := range c.Transcodes {
		where := describeTranscode(i, t.Code)

		if t.Code <= 0 || t.Code > 0xFFFF {
			p.add(RuleRange, "%s: code is not a valid transaction code", where)
		}
		if seen[t.Code] {
			p.add(RuleRange, "%s: code is declared twice", where)
		}
		seen[t.Code] = true

		// V-T2.
		oneOf(p, RuleClass, where+".class", t.Class, "rate-limited", "non-rate-limited")

		if t.ExpectedRatePerSec < 0 {
			p.add(RuleRange, "%s: expectedRatePerSec is negative", where)
		}
		for j, con := range t.Contributions {
			if con.Destination == "" {
				p.add(RuleContribDest, "%s: contributions[%d] names no destination", where, j)
			}
			if len(con.Columns) == 0 {
				p.add(RuleContribColumn, "%s: contributions[%d] (%s) declares no columns",
					where, j, con.Destination)
			}
		}
	}
}

func describeTranscode(i, code int) string {
	if code > 0 {
		return fmt.Sprintf("transcodes[%d] (code %d)", i, code)
	}
	return fmt.Sprintf("transcodes[%d]", i)
}

// startupOnly names every key that cannot change without a restart, paired
// with its value.
//
// Spelled out rather than derived by reflection, because the list IS the
// decision: each entry is a claim that changing this at runtime would corrupt
// live state, and each deserves to be read.
func startupOnly(c *Config) map[string]any {
	return map[string]any{
		// Rebinding the socket drops whatever arrives during the gap, on a
		// circuit with no retransmission.
		"udp.listen": c.UDP.Listen,

		// Changing either re-keys every mirror and strands rows in the old
		// shard.
		"runtime.shards":       c.Runtime.Shards,
		"runtime.generalShard": c.Runtime.GeneralShard,

		// These size structures allocated once at construction. The
		// datagram ring in particular is a single allocation.
		"runtime.shardQueueCapacity":    c.Runtime.ShardQueueCapacity,
		"runtime.datagramQueueCapacity": c.Runtime.DatagramQueueCapacity,
		"runtime.maxInFlightPerShard":   c.Runtime.MaxInFlightPerShard,
		"runtime.expectedRows":          c.Runtime.ExpectedRows,
		"runtime.arenaChunkRows":        c.Runtime.ArenaChunkRows,

		// The field map fixes every ColMask bit index and image offset.
		// Reloading it would renumber the masks and re-lay every live image
		// and stored digest in one step.
		"runtime.destinationsFile": c.Runtime.DestinationsFile,

		"db.host":    c.DB.Host,
		"db.port":    c.DB.Port,
		"db.service": c.DB.Service,
		"db.user":    c.DB.User,
	}
}

func validateReload(c, prev *Config, p *problems) {
	now, before := startupOnly(c), startupOnly(prev)
	for key, nv := range now {
		if ov := before[key]; nv != ov {
			p.add(RuleStartupOnly, "%s is startup-only and changed from %v to %v; "+
				"the whole file is rejected and the running configuration is kept",
				key, ov, nv)
		}
	}

	// V-R4, separately identified because V-X3's bound depends on it: if
	// the ceiling could move, a live maxBatchSize could become legal
	// without the pre-allocated buffers growing to match.
	if c.Runtime.MaxBatchSizeCeiling != prev.Runtime.MaxBatchSizeCeiling {
		p.add(RuleCeilingFixed, "runtime.maxBatchSizeCeiling is startup-only and changed from "+
			"%d to %d; it sizes the pre-allocated batch row buffers",
			prev.Runtime.MaxBatchSizeCeiling, c.Runtime.MaxBatchSizeCeiling)
	}

	// V-R3. The set of codes contributing to a destination fixes the slot
	// assignment, and slots index the stored ordering state -- a reload
	// adding or reordering one would reassign every stored value to the
	// wrong transcode.
	if a, b := transcodeMembership(c), transcodeMembership(prev); !slices.Equal(a, b) {
		p.add(RuleTranscodeMembers, "the transcode set changed from %v to %v; membership is "+
			"startup-only because it fixes the contributor slot assignment, and slots "+
			"index the stored ordering state", b, a)
	}
	for i := range c.Transcodes {
		if i >= len(prev.Transcodes) {
			break
		}
		a, b := contributionTargets(c.Transcodes[i]), contributionTargets(prev.Transcodes[i])
		if !slices.Equal(a, b) {
			p.add(RuleTranscodeMembers, "transcodes[%d] (code %d): contribution membership "+
				"changed from %v to %v; which destinations a transcode feeds is "+
				"startup-only, though which columns it supplies is not",
				i, c.Transcodes[i].Code, b, a)
		}
	}
}

func transcodeMembership(c *Config) []int {
	out := make([]int, 0, len(c.Transcodes))
	for _, t := range c.Transcodes {
		out = append(out, t.Code)
	}
	slices.Sort(out)
	return out
}

func contributionTargets(t TranscodeConfig) []string {
	out := make([]string, 0, len(t.Contributions))
	for _, c := range t.Contributions {
		out = append(out, c.Destination)
	}
	slices.Sort(out)
	return out
}

package config

import (
	"fmt"
	"math/bits"
	"slices"
	"strings"
	"time"
)

// problems accumulates every rule a configuration breaks, so one load reports
// all of them rather than making an operator fix them one restart at a time.
type problems []string

func (p *problems) addf(format string, a ...any) { *p = append(*p, fmt.Sprintf(format, a...)) }

func (p *problems) intRange(name string, v, lo, hi int) {
	if v < lo || v > hi {
		p.addf("%s is %d, outside the valid range %d..%d", name, v, lo, hi)
	}
}

func (p *problems) oneOf(name, v string, allowed ...string) {
	if !slices.Contains(allowed, v) {
		p.addf("%s is %q, want one of %s", name, v, strings.Join(allowed, ", "))
	}
}

func (p *problems) err() error {
	if len(*p) == 0 {
		return nil
	}
	return fmt.Errorf("configuration rejected (%d problems):\n  - %s",
		len(*p), strings.Join(*p, "\n  - "))
}

// Validate runs the complete rule set.
//
// It is identical at startup, under -selfcheck, and on the SIGHUP path before
// the atomic swap, because it is one function called from all three. A file
// that fails is rejected WHOLESALE and the previous configuration keeps
// running.
//
// prev is the currently running configuration, or nil at startup. When it is
// non-nil, changes to startup-only keys are rejected -- also wholesale, not
// by ignoring the offending key, because an operator who changed shards and
// saw the process accept the file would reasonably believe it took effect.
//
// What Validate deliberately does NOT check: that a transcode's contributions
// name a declared destination and column, that every configured transcode has
// a registered decoder, and ASM-15's requirement that a destination's row key
// be derivable from every contributor's shard key. Those need the destination
// table and the decoder registry, and they are -selfcheck's job (P1-06).
func Validate(c *Config, prev *Config) error {
	var p problems

	validateUDP(c, &p)
	validateRuntime(c, &p)
	validateFlush(c, &p)
	validateWriters(c, &p)
	validateDB(c, &p)
	validateOrdering(c, &p)
	validateStats(c, &p)
	validateLiveness(c, &p)
	validateTranscodes(c, &p)

	if prev != nil {
		validateReload(c, prev, &p)
	}
	return p.err()
}

func validateUDP(c *Config, p *problems) {
	u := &c.UDP
	if u.Listen == "" {
		p.addf("udp.listen is empty")
	}
	p.intRange("udp.readBufferBytes", u.ReadBufferBytes, 65536, 4194304)
	p.intRange("udp.expectedNetId", u.ExpectedNetID, 0, 255)

	// The stall budget is NFR4's whole budget, and exceeding it means the
	// process can block the read loop for longer than the requirement
	// permits while believing it is compliant.
	p.intRange("udp.stallBudgetMs", u.StallBudgetMs, 1, 100)
}

func validateRuntime(c *Config, p *problems) {
	r := &c.Runtime
	p.intRange("runtime.gomaxprocs", r.GOMAXPROCS, 1, 64)
	p.intRange("runtime.shards", r.Shards, 1, 64)
	p.intRange("runtime.shardQueueCapacity", r.ShardQueueCapacity, 256, 65536)
	p.intRange("runtime.datagramQueueCapacity", r.DatagramQueueCapacity, 1024, 65536)
	p.intRange("runtime.maxInFlightPerShard", r.MaxInFlightPerShard, 1, 16)
	p.intRange("runtime.maxBatchSizeCeiling", r.MaxBatchSizeCeiling, 1, 10000)
	p.intRange("runtime.expectedRows", r.ExpectedRows, 1000, 5000000)
	p.intRange("runtime.arenaChunkRows", r.ArenaChunkRows, 256, 65536)
	p.intRange("runtime.time1980UnixOffsetSec", r.Time1980UnixOffsetSec, 0, 1<<31-1)

	// The QD ring wraps by masking, so a non-power-of-two does not merely
	// waste space -- it silently corrupts the index and the ring starts
	// returning the wrong datagram.
	if r.DatagramQueueCapacity > 0 && bits.OnesCount(uint(r.DatagramQueueCapacity)) != 1 {
		p.addf("runtime.datagramQueueCapacity is %d, which is not a power of two. "+
			"The datagram ring wraps by masking; a non-power-of-two silently "+
			"corrupts the index rather than failing", r.DatagramQueueCapacity)
	}

	if r.Shards > 0 && (r.GeneralShard < 0 || r.GeneralShard >= r.Shards) {
		p.addf("runtime.generalShard is %d, outside 0..%d (shards-1)", r.GeneralShard, r.Shards-1)
	}
}

func validateFlush(c *Config, p *problems) {
	f := &c.Flush
	p.intRange("flush.windowMs", f.WindowMs, 10, 60000)
	p.intRange("flush.drainTimeoutMs", f.DrainTimeoutMs, 0, 60000)
	p.intRange("flush.epochStallWarnMs", f.EpochStallWarnMs, 1000, 600000)

	// C2. Reloadable in BOTH directions within the ceiling: the row buffers
	// are sized by the startup-only ceiling, so any value inside it is
	// already allocated.
	p.intRange("flush.maxBatchSize", f.MaxBatchSize, 1, c.Runtime.MaxBatchSizeCeiling)
}

func validateWriters(c *Config, p *problems) {
	w := &c.Writers
	p.intRange("writers.count", w.Count, 1, 32)
	p.intRange("writers.statementFailWindowSec", w.StatementFailWindowSec, 1, 3600)
	p.intRange("writers.failFastSeconds", w.FailFastSeconds, 1, 3600)
	p.intRange("writers.failFastWrites", w.FailFastWrites, 1, 100000)
	p.intRange("writers.retryInitialMs", w.RetryInitialMs, 1, 60000)
	p.intRange("writers.connIdleReleaseSec", w.ConnIdleReleaseSec, 5, 3600)
	p.intRange("writers.stmtCacheSize", w.StmtCacheSize, 8, 4096)

	// C6. A reload lowering writers.count below the threshold is rejected,
	// not clamped. A silently clamped safety threshold is worse than a
	// refused file: the pool-stop rule would become unreachable and nothing
	// would say so.
	p.intRange("writers.statementFailWriters", w.StatementFailWriters, 1, w.Count)

	if w.RetryMaxMs < w.RetryInitialMs {
		p.addf("writers.retryMaxMs (%d) is below writers.retryInitialMs (%d)",
			w.RetryMaxMs, w.RetryInitialMs)
	}
	p.intRange("writers.retryMaxMs", w.RetryMaxMs, 1, 60000)
}

func validateDB(c *Config, p *problems) {
	if !c.HasDB {
		return
	}
	d := &c.DB
	if d.Host == "" {
		p.addf("db.host is required when a db object is present")
	}
	if d.Service == "" {
		p.addf("db.service is required when a db object is present")
	}
	if d.User == "" {
		p.addf("db.user is required when a db object is present")
	}
	p.intRange("db.port", d.Port, 1, 65535)
	p.intRange("db.connectTimeoutSec", d.ConnectTimeoutSec, 1, 300)
	p.intRange("db.queryTimeoutSec", d.QueryTimeoutSec, 1, 600)
	p.intRange("db.connMaxLifetimeMinutes", d.ConnMaxLifetimeMinutes, 1, 1440)

	// C5. These are derived, so a mismatch means applyDerivedPool was not
	// called after a change to writers.count -- which would make "raise
	// writers" silently ineffective past the old pool limit.
	if want := c.Writers.Count + 1; d.MaxOpenConns != want || d.MaxIdleConns != want {
		p.addf("db pool sizes (%d open, %d idle) do not follow writers.count+1 (%d); "+
			"they are derived and must be reapplied on every reload",
			d.MaxOpenConns, d.MaxIdleConns, want)
	}
}

func validateOrdering(c *Config, p *problems) {
	o := &c.Ordering
	p.oneOf("ordering.guard", o.Guard, "on", "off", "auto")
	p.oneOf("ordering.gapScope", o.GapScope, "global", "per-transcode", "off")
	p.intRange("ordering.guardMinSamples", o.GuardMinSamples, 1, 1000000)
	p.intRange("ordering.guardEvalIntervalSec", o.GuardEvalIntervalSec, 1, 3600)
	p.intRange("ordering.gapAuditPackets", o.GapAuditPackets, 0, 10000000)

	if o.GuardSuppressionRatio <= 0 || o.GuardSuppressionRatio > 1 {
		p.addf("ordering.guardSuppressionRatio is %g, want 0 < r <= 1", o.GuardSuppressionRatio)
	}
}

func validateStats(c *Config, p *problems) {
	s := &c.Stats
	p.intRange("stats.intervalSec", s.IntervalSec, 1, 3600)
	p.intRange("stats.eventRatePerMin", s.EventRatePerMin, 1, 10000)
	p.oneOf("stats.logLevel", s.LogLevel, "info", "warn", "error")
}

func validateLiveness(c *Config, p *problems) {
	l := &c.Liveness
	p.intRange("liveness.heartbeatWarnSec", l.HeartbeatWarnSec, 1, 3600)
	p.intRange("liveness.heartbeatDownSec", l.HeartbeatDownSec, 1, 86400)
	p.intRange("liveness.startupGraceSec", l.StartupGraceSec, 1, 86400)

	// C7. Down must be strictly later than warn, or the warning state is
	// unreachable and the first notice of a dead feed is the last one.
	if l.HeartbeatDownSec <= l.HeartbeatWarnSec {
		p.addf("liveness.heartbeatDownSec (%d) must exceed heartbeatWarnSec (%d), "+
			"otherwise the warning state is unreachable",
			l.HeartbeatDownSec, l.HeartbeatWarnSec)
	}

	for name, v := range map[string]string{
		"liveness.tradingWindow.start": l.TradingWindow.Start,
		"liveness.tradingWindow.end":   l.TradingWindow.End,
	} {
		if _, err := time.Parse("15:04", v); err != nil {
			p.addf("%s is %q, want HH:MM", name, v)
		}
	}

	// AIX may ship no zoneinfo database, which is why the binary imports
	// time/tzdata. If this fails it is a genuine configuration error, not a
	// platform one.
	if _, err := time.LoadLocation(l.TradingWindow.TZ); err != nil {
		p.addf("liveness.tradingWindow.tz %q does not resolve: %v", l.TradingWindow.TZ, err)
	}
}

func validateTranscodes(c *Config, p *problems) {
	seen := map[int]bool{}
	for i, t := range c.Transcodes {
		where := fmt.Sprintf("transcodes[%d]", i)
		if t.Code > 0 {
			where = fmt.Sprintf("transcodes[%d] (code %d)", i, t.Code)
		}

		if t.Code <= 0 || t.Code > 0xFFFF {
			p.addf("%s: code is not a valid transaction code", where)
		}
		if seen[t.Code] {
			p.addf("%s: code is declared twice", where)
		}
		seen[t.Code] = true

		p.oneOf(where+".class", t.Class, "rate-limited", "non-rate-limited")

		if t.ExpectedRatePerSec < 0 {
			p.addf("%s: expectedRatePerSec is negative", where)
		}
		for j, con := range t.Contributions {
			if con.Destination == "" {
				p.addf("%s: contributions[%d] names no destination", where, j)
			}
			if len(con.Columns) == 0 {
				p.addf("%s: contributions[%d] (%s) declares no columns",
					where, j, con.Destination)
			}
		}
	}
}

// startupOnly names every key that cannot change without a restart, paired
// with its value in a given configuration.
//
// Spelled out rather than derived by reflection, because the list IS the
// decision: each entry is a claim that changing this at runtime would corrupt
// live state, and each deserves to be read.
func startupOnly(c *Config) map[string]any {
	m := map[string]any{
		// Rebinding the socket drops whatever arrives during the gap, on a
		// circuit with no retransmission (C3).
		"udp.listen": c.UDP.Listen,

		// Changing either re-keys every mirror and strands rows in the old
		// shard (C4).
		"runtime.shards":       c.Runtime.Shards,
		"runtime.generalShard": c.Runtime.GeneralShard,

		// These size structures allocated once at construction. The
		// datagram ring in particular is a single allocation.
		"runtime.shardQueueCapacity":    c.Runtime.ShardQueueCapacity,
		"runtime.datagramQueueCapacity": c.Runtime.DatagramQueueCapacity,
		"runtime.maxInFlightPerShard":   c.Runtime.MaxInFlightPerShard,
		"runtime.maxBatchSizeCeiling":   c.Runtime.MaxBatchSizeCeiling,
		"runtime.expectedRows":          c.Runtime.ExpectedRows,
		"runtime.arenaChunkRows":        c.Runtime.ArenaChunkRows,

		// The field map fixes every ColMask bit index and image offset
		// (C1). Reloading it would renumber the masks and re-lay every
		// live image and stored digest in one step.
		"runtime.destinationsFile": c.Runtime.DestinationsFile,

		// Credentials and target are not reloadable.
		"db.host":    c.DB.Host,
		"db.port":    c.DB.Port,
		"db.service": c.DB.Service,
		"db.user":    c.DB.User,
	}
	return m
}

func validateReload(c, prev *Config, p *problems) {
	now, before := startupOnly(c), startupOnly(prev)
	for key, nv := range now {
		if ov := before[key]; nv != ov {
			p.addf("%s is startup-only and changed from %v to %v; "+
				"the whole file is rejected and the running configuration is kept",
				key, ov, nv)
		}
	}

	// Contributor membership is startup-only even though the column lists
	// inside it are not (C1). The set of codes contributing to a
	// destination fixes the slot assignment, and slots index the stored
	// ordering state -- a reload that added or reordered one would
	// reassign every stored value to the wrong transcode.
	if a, b := transcodeMembership(c), transcodeMembership(prev); !slices.Equal(a, b) {
		p.addf("the transcode set changed from %v to %v; membership is startup-only "+
			"because it fixes the contributor slot assignment, and slots index "+
			"the stored ordering state", b, a)
	}
	for i := range c.Transcodes {
		if i >= len(prev.Transcodes) {
			break
		}
		a, b := contributionTargets(c.Transcodes[i]), contributionTargets(prev.Transcodes[i])
		if !slices.Equal(a, b) {
			p.addf("transcodes[%d] (code %d): contribution membership changed from "+
				"%v to %v; which destinations a transcode feeds is startup-only, "+
				"though which columns it supplies is not",
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

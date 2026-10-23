package config

import (
	"time"

	"feedvalve/internal/oradb"
)

// Config is the whole runtime configuration.
//
// IMMUTABLE once published. A reload builds a new *Config, validates it
// wholesale, and swaps the pointer; nothing mutates a published Config. That
// is what lets every reader hold the pointer for a whole unit of work -- one
// datagram for the receiver, one flush tick for a shard, one batch for a
// writer -- rather than re-reading fields that could change underneath it.
type Config struct {
	// Gen is assigned monotonically on load and on swap.
	//
	// A shard needs "was this reloaded?" as a cheap exact test at epoch
	// open. Pointer comparison would also fire on a no-op re-publish, which
	// would make a shard rescan its quarantine set for nothing.
	Gen uint64

	UDP      UDPConfig
	Runtime  RuntimeConfig
	Flush    FlushConfig
	Writers  WritersConfig
	DB       oradb.DBSettings // no password field at any depth -- see oradb.DBSettings
	HasDB    bool             // whether the file declared a db object at all
	Ordering OrderingConfig
	Stats    StatsConfig
	Liveness LivenessConfig
	Debug    DebugConfig

	Transcodes []TranscodeConfig
}

// UDPConfig is the socket and the framing oracle.
type UDPConfig struct {
	Listen          string   // startup-only: rebinding drops datagrams on a circuit with no retransmission
	ReadBufferBytes int      // the kernel silently caps at udp_recvspace, so the effective value is logged
	ExpectedNetID   int      // diagnostic only; a mismatch is counted and logged, NEVER used to drop
	SourceAllowList []string // empty allows all
	SourceEnforce   bool     // off by default: a wrong list silently blinds the feed
	StallBudgetMs   int      // per READ-LOOP ITERATION, shared by every unit that datagram fans out
}

// RuntimeConfig sizes the structures that cannot be resized later.
type RuntimeConfig struct {
	GOMAXPROCS int // set explicitly, NEVER detected: an SMT-8 partition reports 32
	// logical CPUs while entitled to 4
	Shards                int
	GeneralShard          int // the designated shard for non-token units
	ShardQueueCapacity    int // Q1 depth
	DatagramQueueCapacity int // QD depth
	MaxInFlightPerShard   int
	MaxBatchSizeCeiling   int // sizes the pre-allocated Batch.Rows slices
	ExpectedRows          int // pre-sizes each shard's index
	ArenaChunkRows        int
	Time1980UnixOffsetSec int // 0 means the default 315532800 (writer.Params); a key and not a constant because the epoch's anchor
	// is an open question; this turns a wrong answer from
	// a rebuild into a reload

	// DestinationsFile overrides the embedded field map. Empty uses the
	// embedded one.
	DestinationsFile string
}

// FlushConfig is the coalescing window and its bounds.
type FlushConfig struct {
	WindowMs         int // the dominant term in rate-limited latency
	MaxBatchSize     int // caps a TRANSACTION, not a window's output
	DrainTimeoutMs   int // ONE shared budget for all shards, not one each
	EpochStallWarnMs int // an epoch that has stopped progressing, not one merely running slowly
}

// WritersConfig is the pool and its failure policy.
type WritersConfig struct {
	Count                  int // the design's lever for a write path that cannot keep up
	StatementFailWriters   int // same statement identity failing on N distinct writers stops the pool
	StatementFailWindowSec int
	FailFastSeconds        int // time-based AS WELL AS count-based, so a lightly loaded
	// system notices a dead database in 30 s not 20 minutes
	FailFastWrites     int
	RetryInitialMs     int
	RetryMaxMs         int
	ConnIdleReleaseSec int // a pinned *sql.Conn is EXEMPT from ConnMaxLifetime, so each
	// writer must release its own session when idle
	StmtCacheSize int
}

// OrderingConfig is the guard and the gap detector.
type OrderingConfig struct {
	Guard string // on | off | auto. "off" is safe by construction: ordering is
	// established structurally, and the guard is only defence
	GuardSuppressionRatio float64
	GuardMinSamples       int
	GuardEvalIntervalSec  int // deliberately NOT the 60 s stats interval: a frozen
	// transcode's rows stop updating entirely
	GapScope        string // global | per-transcode | off
	GapAuditPackets int    // maintain both hypotheses for n packets and report which
	// produced zero gaps; 0 is off
}

// StatsConfig is the emission line and the log budget.
type StatsConfig struct {
	IntervalSec     int
	LogLevel        string
	EventRatePerMin int // token bucket PER EVENT CLASS. Not decoration: a misframed
	// feed emits one WARN per sub-packet, and an uncapped logger
	// turns a data problem into an availability problem
}

// LivenessConfig is the heartbeat and the session window.
type LivenessConfig struct {
	HeartbeatWarnSec int // the pulse interval is NSEIL-configurable, so this is
	// configuration and never a constant
	HeartbeatDownSec int
	StartupGraceSec  int
	TradingWindow    TradingWindow
}

// TradingWindow downgrades liveness severity outside market hours. It never
// gates data.
type TradingWindow struct {
	Start string // "HH:MM"
	End   string
	TZ    string // IANA name; the binary imports time/tzdata because AIX may
	// lack a zoneinfo database
}

// DebugConfig is the per-transcode field trace, carried over from poc's
// -debug as an operator-triggered mode rather than a global flag.
type DebugConfig struct {
	TraceTranscodes []int
	HexDump         bool
}

// TranscodeConfig is one transaction code's class and contributions.
type TranscodeConfig struct {
	Code               int
	Class              string // rate-limited | non-rate-limited
	Contributions      []ContributionConfig
	ExpectedRatePerSec int // a non-rate-limited transcode observed above this is
	// warned: that combination is what turns flush
	// amplification into a capacity problem
}

// ContributionConfig names the columns one transcode supplies to one
// destination.
type ContributionConfig struct {
	Destination string   // must name a declared destination; membership is startup-only
	Columns     []string // must already be declared by that destination
}

// Convenience accessors, so callers are not converting units at every use.
func (c *Config) Window() time.Duration { return time.Duration(c.Flush.WindowMs) * time.Millisecond }
func (c *Config) StallBudget() time.Duration {
	return time.Duration(c.UDP.StallBudgetMs) * time.Millisecond
}
func (c *Config) DrainTimeout() time.Duration {
	return time.Duration(c.Flush.DrainTimeoutMs) * time.Millisecond
}
func (c *Config) StatsInterval() time.Duration {
	return time.Duration(c.Stats.IntervalSec) * time.Second
}

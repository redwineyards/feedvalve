package config

import (
	"fmt"
	"slices"
	"strings"
)

// Rule identifiers from Vol 4 section 2.2.
//
// Every rejection names the rule it broke. That is not decoration: it is what
// lets a test assert a deliberately-broken configuration trips exactly one
// rule and no other, which in turn is what stops a rule from appearing to
// work because a different rule happened to catch the same file first. P1-04
// had precisely that problem -- an image-length check firing before the
// kind/width rule it was meant to exercise.
const (
	// Range and type rules. Every key in Vol 0 section 7.2 has one, which
	// gate G-CFG-1 asserts rather than trusts.
	RuleRange = "V-RANGE"

	// Cross-field rules.
	RuleStatementFailWriters = "V-X1" // statementFailWriters <= writers.count
	RuleHeartbeatOrder       = "V-X2" // heartbeatDownSec > heartbeatWarnSec
	RuleBatchSizeCeiling     = "V-X3" // 1 <= maxBatchSize <= maxBatchSizeCeiling
	RuleGeneralShardRange    = "V-X4" // 0 <= generalShard < shards
	RuleRetryOrder           = "V-X5" // retryMaxMs >= retryInitialMs
	RuleStallBudget          = "V-X6" // stallBudgetMs <= 100, NFR4's whole budget
	// V-X7 is deliberately absent: db.connMaxLifetimeMinutes cannot be
	// validated against the Oracle profile's IDLE_TIME from here. Stated as
	// un-checkable rather than silently assumed; -selfcheck says so.

	// Destination schema rules.
	RuleDestCount        = "V-D1" // exactly three destinations
	RuleDestUnique       = "V-D2" // names unique, table non-empty
	RuleDestColumnCount  = "V-D3" // len(columns) <= maxColumns
	RuleDestImageFits    = "V-D4" // sum of widths <= imageSize (ASM-16b)
	RuleDestSQLUnique    = "V-D5" // sql names non-empty and unique within a destination
	RuleDestContributors = "V-D6" // contributors <= maxContributors
	RuleRowKeyDerivable  = "V-D7" // ASM-15 -- the single most valuable rule here

	// Transcode schema rules.
	RuleDecoderExists = "V-T1" // every transcode has a registered decoder (Phase 2)
	RuleClass         = "V-T2" // class is one of the two
	RuleContribDest   = "V-T3" // a contribution names a declared destination
	RuleContribColumn = "V-T4" // a contributed column is declared by that destination
	RuleBlobFits      = "V-T5" // sum of widths <= unitBlobSize (ASM-16a)
	RuleColSpecWidth  = "V-T6" // ColSpec.Width equals its mask's widths (Phase 2)

	// Environment and resource rules.
	RuleDBRequired   = "V-DB1"     // a db object is required outside the one-shot modes
	RuleDBFields     = "V-DB2"     // host, service, user non-empty
	RuleDBNoPassword = "V-DB3"     // no db.password key at any depth
	RuleTimezone     = "V-TZ1"     // tradingWindow.tz resolves
	RuleTradingTimes = "V-TW1"     // start and end parse as HH:MM
	RuleTraceCodes   = "V-DBG1"    // traceTranscodes are configured codes
	RuleSourceList   = "V-SRC1"    // sourceAllowList entries parse as IP or CIDR
	RuleStrictKeys   = "V-STRICT1" // an unknown JSON key is an error

	// Reload-only rules.
	RuleStartupOnly      = "V-R1" // startup-only keys equal their running value
	RuleDestStartupOnly  = "V-R2" // the destination set is startup-only in its entirety
	RuleTranscodeMembers = "V-R3" // the transcode set and its targets are startup-only
	RuleCeilingFixed     = "V-R4" // maxBatchSizeCeiling is startup-only
)

// Problem is one broken rule.
type Problem struct {
	Rule string // the V- identifier
	Msg  string // what is wrong, in the operator's terms

	// Optional structured detail, set by the rules that are about one key:
	// V-R1..V-R4, V-DB3 and V-STRICT1. Running and Attempt are empty for a
	// db.* key (a secret is never echoed) and for rules with no value to show.
	KeyPath string
	Running string
	Attempt string
}

// RuleError is the first problem of a rejection, flattened for the reload
// driver's one WARN line: which rule, which key, and both values.
type RuleError struct {
	Rule    string
	KeyPath string
	Running string
	Attempt string
	Detail  string
}

func (p Problem) String() string { return p.Rule + ": " + p.Msg }

// ValidationError is every rule a configuration broke.
//
// Every problem, not the first: an operator fixing a configuration should not
// have to restart once per mistake.
type ValidationError struct {
	Problems []Problem
}

func (e *ValidationError) Error() string {
	lines := make([]string, 0, len(e.Problems))
	for _, p := range e.Problems {
		lines = append(lines, p.String())
	}
	return fmt.Sprintf("configuration rejected (%d problems):\n  - %s",
		len(e.Problems), strings.Join(lines, "\n  - "))
}

// RuleError returns Problems[0] as a RuleError; the zero value if there is none.
func (e *ValidationError) RuleError() RuleError {
	if len(e.Problems) == 0 {
		return RuleError{}
	}
	p := e.Problems[0]
	return RuleError{Rule: p.Rule, KeyPath: p.KeyPath, Running: p.Running, Attempt: p.Attempt, Detail: p.Msg}
}

// Rules returns the distinct rule identifiers that fired, sorted. This is what
// a test asserts against.
func (e *ValidationError) Rules() []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range e.Problems {
		if !seen[p.Rule] {
			seen[p.Rule] = true
			out = append(out, p.Rule)
		}
	}
	slices.Sort(out)
	return out
}

// Has reports whether a given rule fired.
func (e *ValidationError) Has(rule string) bool {
	return slices.Contains(e.Rules(), rule)
}

// problems accumulates rule violations during a validation pass.
type problems []Problem

func (p *problems) add(rule, format string, a ...any) {
	*p = append(*p, Problem{Rule: rule, Msg: fmt.Sprintf(format, a...)})
}

// addKey is add for a rule that is about one key, recording the key path and,
// where there are values to show, the running and attempted ones.
func (p *problems) addKey(rule, key, running, attempt, format string, a ...any) {
	*p = append(*p, Problem{Rule: rule, Msg: fmt.Sprintf(format, a...),
		KeyPath: key, Running: running, Attempt: attempt})
}

func (p *problems) err() error {
	if len(*p) == 0 {
		return nil
	}
	return &ValidationError{Problems: *p}
}

package obs_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"feedvalve/internal/obs"
)

// backtick matches an inline code span on one line.
var backtick = regexp.MustCompile("`([^`\n]+)`")

// instrumentLike matches a lower_snake_case identifier containing an
// underscore: the shape of every instrument name in the ledger. Config keys are
// camelCase with dots, flags have a leading dash, environment variables and
// Oracle tables are upper case, so none of them looks like this.
var instrumentLike = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)+$`)

// runbookInstruments returns every instrument-shaped token in the runbook's
// code spans, with a label suffix ("name{k}") or an assignment ("name=1")
// trimmed off.
func runbookInstruments(doc string) map[string]bool {
	out := map[string]bool{}
	for _, m := range backtick.FindAllStringSubmatch(doc, -1) {
		tok := m[1]
		if i := strings.IndexAny(tok, "{="); i >= 0 {
			tok = tok[:i]
		}
		if instrumentLike.MatchString(tok) {
			out[tok] = true
		}
	}
	return out
}

// unknownToLedger returns the runbook names the ledger does not declare.
func unknownToLedger(doc string, ledger []string) []string {
	in := make(map[string]bool, len(ledger))
	for _, n := range ledger {
		in[n] = true
	}
	var bad []string
	for n := range runbookInstruments(doc) {
		if !in[n] {
			bad = append(bad, n)
		}
	}
	return bad
}

func readRunbook(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../docs/runbook.md")
	if err != nil {
		t.Fatalf("docs/runbook.md must exist (P5-15): %v", err)
	}
	return string(b)
}

// TestG_OBS_3_RunbookNamesEveryInstrumentFromTheLedger
//
// SETUP: docs/runbook.md and the obs ledger.
// STIMULUS: extract every instrument-shaped code span from the runbook.
// ASSERTION: each is a declared ledger name; the runbook names the full set
// Vol 4 section 6.6 assigns to the twelve questions; the check is not vacuous
// (it rejects a runbook with an invented name).
// WOULD HAVE CAUGHT: an instrument renamed in the ledger while the runbook,
// which the operator reads at 3 a.m., keeps telling them to read a counter
// that no longer exists -- an absent counter looks exactly like a healthy one.
func TestG_OBS_3_RunbookNamesEveryInstrumentFromTheLedger(t *testing.T) {
	doc := readRunbook(t)
	ledger := obs.LedgerNamesForTest()

	if bad := unknownToLedger(doc, ledger); len(bad) > 0 {
		t.Errorf("runbook names instruments absent from the ledger: %v", bad)
	}

	// Section 6.6's instrument set, plus the section 8.8 verification set.
	required := strings.Fields(`uptime_seconds config_generation circuit_state
		datagrams_received heartbeat_seen seq_gap_messages seq_gap_events
		shard_send_blocked shard_depth_max stall_budget_used_max_ns
		shard_loop_max_ns circuit_down_events circuit_outage_max_ns
		netid_mismatch body_len_mismatch record_count_clamped decompress_failed
		framing_abandoned panics_recovered writers_busy writers_configured
		writers_stopped epoch_duration_max_ns tick_skipped_epoch_open dirty_depth
		writer_health_state writes_failed write_retry row_rejected
		ordering_guard_disabled guard_superseded guard_offered session_state
		config_reload_rejected unknown_transcode unknown_transcode_other
		message_truncated datagrams_oversize datagrams_unexpected_source
		drain_abandoned_rows rows_written`)
	found := runbookInstruments(doc)
	for _, n := range required {
		if !found[n] {
			t.Errorf("runbook does not name %q, which Vol 4 section 6.6 or 8.8 requires", n)
		}
	}

	// Non-vacuity: the extractor finds names, and flags an invented one.
	if len(found) < len(required) {
		t.Fatalf("extractor found only %d names; the check is vacuous", len(found))
	}
	if bad := unknownToLedger(doc+"\nRead `seq_gap_mesages` now.\n", ledger); len(bad) != 1 || bad[0] != "seq_gap_mesages" {
		t.Errorf("a misspelled instrument was not flagged exactly once: %v", bad)
	}
}

// TestG_OBS_3_RunbookCoversTheOrderedQuestionsAndFixedPoints
//
// SETUP: docs/runbook.md.
// STIMULUS: look for each question heading, the named external dependency, the
// Q6a reload step, the Q10 pickup window, the deploy steps and every open
// question with its owner.
// ASSERTION: all present.
// WOULD HAVE CAUGHT: a runbook edited down to a subset, or one that drops the
// netstat dependency or the stats.intervalSec pickup window and so teaches the
// operator that 60 s of silence after a reload is a failure.
func TestG_OBS_3_RunbookCoversTheOrderedQuestionsAndFixedPoints(t *testing.T) {
	doc := readRunbook(t)

	for _, q := range []string{"Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q6a", "Q6b",
		"Q7", "Q8", "Q9", "Q10", "Q11", "Q12"} {
		if !regexp.MustCompile(`(?m)^### ` + q + ` `).MatchString(doc) {
			t.Errorf("runbook has no heading for %s", q)
		}
	}
	for _, s := range []string{
		"netstat -s -p udp",
		"debug.traceTranscodes", "debug.hexDump", "SIGHUP",
		"config reloaded generation=",
		"stats.intervalSec", "up to the previous `stats.intervalSec`",
		"-selfcheck", "-dbcheck", "kill -TERM", "startupGraceSec",
		"alongside",
	} {
		if !strings.Contains(doc, s) {
			t.Errorf("runbook is missing %q", s)
		}
	}
	for _, oq := range []string{"OQ-L3", "OQ-L4", "OQ-L5", "OQ-L6", "OQ-L7",
		"OQ-L8", "OQ-L9", "OQ-L10", "OQ-L11", "OQ-L12"} {
		row := regexp.MustCompile(`(?m)^\| ` + oq + ` \|.*\|.*\|\s*$`)
		if oq == "OQ-L6" {
			// L6 is not in the section 13 table; it is carried as prose.
			if !strings.Contains(doc, oq) {
				t.Errorf("runbook does not mention %s", oq)
			}
			continue
		}
		if !row.MatchString(doc) {
			t.Errorf("runbook's open-question table has no row with an owner for %s", oq)
		}
	}
}

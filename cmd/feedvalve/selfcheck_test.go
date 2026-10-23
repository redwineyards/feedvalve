package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"feedvalve/internal/app"
)

// TestG_SC_1_SelfcheckGeneratesStatementsWithoutCredentials.
// SETUP: no config file, no database password, no network used.
// STIMULUS: app.Run with SelfCheck.
// ASSERTION: it passes and reports the statement-count line.
// WOULD HAVE CAUGHT: -selfcheck skipping statement generation, or requiring
// a password to run it.
func TestG_SC_1_SelfcheckGeneratesStatementsWithoutCredentials(t *testing.T) {
	t.Setenv("FEEDVALVE_DB_PASSWORD", "")
	var out bytes.Buffer
	if err := app.Run(context.Background(), app.Options{SelfCheck: true}, &out); err != nil {
		t.Fatalf("selfcheck failed: %v\n%s", err, out.String())
	}
	s := out.String()
	if !strings.Contains(s, "statements generated") || !strings.Contains(s, "selfcheck: PASSED") {
		t.Fatalf("missing statement line or PASSED:\n%s", s)
	}
}

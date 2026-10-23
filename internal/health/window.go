package health

import (
	"fmt"
	"time"
)

// TradingWindow is the configured window, as strings. It is health's own type;
// app projects config's into it.
type TradingWindow struct {
	Start string // "HH:MM"
	End   string // "HH:MM"
	TZ    string // IANA zone name
}

// Window decides whether a moment is inside trading hours. It gates log
// severity only (Vol 4 section 3.5), never data. A Window that could not be
// built is "open": every moment is in-window, so a timezone or parsing problem
// leaves alarms at their louder setting instead of silencing them.
type Window struct {
	loc        *time.Location // nil: fail open
	start, end int            // minutes after midnight
}

// NewWindow builds a Window. When the zone or a time cannot be resolved it
// returns a fail-open Window AND a non-nil error describing why; the caller
// logs that error once. The Window is always usable.
func NewWindow(tw TradingWindow) (*Window, error) {
	// LoadLocation maps "" to UTC and "Local" to the host zone without error;
	// neither is a configured zone, so both fail open.
	if tw.TZ == "" || tw.TZ == "Local" {
		return &Window{}, fmt.Errorf("trading window zone %q is not an IANA zone name, treating every hour as in-window", tw.TZ)
	}
	loc, err := time.LoadLocation(tw.TZ)
	if err != nil {
		return &Window{}, fmt.Errorf("trading window zone %q does not resolve, treating every hour as in-window: %w", tw.TZ, err)
	}
	s, err := time.Parse("15:04", tw.Start)
	if err != nil {
		return &Window{}, fmt.Errorf("trading window start %q is not HH:MM, treating every hour as in-window: %w", tw.Start, err)
	}
	e, err := time.Parse("15:04", tw.End)
	if err != nil {
		return &Window{}, fmt.Errorf("trading window end %q is not HH:MM, treating every hour as in-window: %w", tw.End, err)
	}
	return &Window{loc: loc, start: s.Hour()*60 + s.Minute(), end: e.Hour()*60 + e.Minute()}, nil
}

// Contains reports whether t falls inside [start, end) in the window's zone. A
// window whose end is before its start spans midnight; start == end is the
// whole day. A nil or fail-open Window contains everything.
func (w *Window) Contains(t time.Time) bool {
	if w == nil || w.loc == nil {
		return true
	}
	lt := t.In(w.loc)
	m := lt.Hour()*60 + lt.Minute()
	switch {
	case w.start == w.end:
		return true
	case w.start < w.end:
		return m >= w.start && m < w.end
	default:
		return m >= w.start || m < w.end
	}
}

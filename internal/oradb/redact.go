package oradb

import (
	"errors"
	"net/url"
	"strings"
)

// redactDSN is the SECOND line of defence, not the first.
//
// The first is that callers build their own messages from Describe. This
// exists because a driver error can quote the connection string back at us,
// and on this host operational logs are append-only and long-lived, so one
// leak is permanent.
//
// It also scrubs the URL-escaped spellings of the password, because the DSN is
// built with BuildUrl and a driver that quotes one component of it would quote
// the escaped form.
//
// A scrubbed error is a fresh errors.New: the original chain is dropped on
// purpose, because unwrapping it would hand the secret straight back.
func redactDSN(err error, dsn, password string, s DBSettings) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	out := text
	if dsn != "" {
		out = strings.ReplaceAll(out, dsn, s.Describe())
	}
	if password != "" {
		out = strings.ReplaceAll(out, password, "[redacted]")
		if esc := url.QueryEscape(password); esc != password {
			out = strings.ReplaceAll(out, esc, "[redacted]")
		}
		if esc := url.PathEscape(password); esc != password {
			out = strings.ReplaceAll(out, esc, "[redacted]")
		}
	}
	if out == text {
		return err
	}
	return errors.New(out)
}

// Redactor holds what is needed to scrub a driver error: the settings and the
// password. It is built once next to the connection and handed to whatever
// classifies errors, so no call site has to remember to redact.
//
// The zero Redactor scrubs nothing; Classify on it still classifies.
type Redactor struct {
	dsn      string
	password string
	s        DBSettings
}

// NewRedactor builds a Redactor for s and its password. The DSN is derived
// here and kept unexported; nothing outside this package can read it back.
func NewRedactor(s DBSettings, password string) Redactor {
	return Redactor{dsn: s.dsn(password), password: password, s: s}
}

// String and GoString: see DB.String. A Redactor holds the password.
func (Redactor) String() string   { return "oradb.Redactor" }
func (Redactor) GoString() string { return "oradb.Redactor" }

// redact returns err with the DSN and password removed from its text.
func (r Redactor) redact(err error) error {
	return redactDSN(err, r.dsn, r.password, r.s)
}

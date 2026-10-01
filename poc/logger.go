package main

import (
	"io"
	"log"
	"os"
)

var (
	appLogger    = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	packetLogger = log.New(io.Discard, "", log.LstdFlags|log.Lmicroseconds)
	debugLogger  = log.New(io.Discard, "", log.LstdFlags|log.Lmicroseconds)
)

// debugEnabled gates fieldReader's per-field offset/hex/value logging and
// the pre-parse hex dump. Off by default since it's extremely verbose.
var debugEnabled bool

func initPacketLogger(output io.Writer) {
	packetLogger.SetOutput(output)
}

// cappedWriter stops writing once limit bytes have been written, logging the
// cutoff exactly once. The packet log records every payload plus its parsed
// form, which at market-open broadcast rates is several times the wire rate
// against an append-only file -- without a ceiling it fills the disk and takes
// the host down rather than just the relay. A limit of zero means unlimited.
//
// This is a floor, not a substitute for log rotation: it preserves the start
// of a session rather than the most recent packets.
type cappedWriter struct {
	w       io.Writer
	limit   int64
	written int64
	stopped bool
}

func newCappedWriter(w io.Writer, limit int64) *cappedWriter {
	return &cappedWriter{w: w, limit: limit}
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	if c.limit <= 0 {
		return c.w.Write(p)
	}
	if c.stopped {
		// Report the bytes as consumed: the logger has nothing useful to do
		// with an error here, and a short-write error would only add noise to
		// stderr for every packet from now on.
		return len(p), nil
	}
	if c.written+int64(len(p)) > c.limit {
		c.stopped = true
		appLogger.Printf("WARN packet log reached its %d-byte limit after %d bytes; no further packet data will be written (raise or disable with -maxout)",
			c.limit, c.written)
		return len(p), nil
	}
	n, err := c.w.Write(p)
	c.written += int64(n)
	return n, err
}

func initDebugLogger(output io.Writer) {
	debugLogger.SetOutput(output)
	debugEnabled = true
}

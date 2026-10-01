package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// fieldReader is a sequential big-endian cursor over a byte slice. Every
// read advances the cursor by exactly the number of bytes consumed, so
// offsets can never drift out of sync with the struct layout the way
// hand-written data[a:b] slices can. When debugEnabled is set, every read
// also logs the field name, its offset range, raw hex bytes, and decoded
// value to debugLogger.
type fieldReader struct {
	data []byte
	pos  int
	base int // added to pos when logging, so nested readers report true offsets within the full message
}

func newFieldReader(data []byte, base int) *fieldReader {
	return &fieldReader{data: data, base: base}
}

func (r *fieldReader) logField(name string, start, end int, raw []byte, value any) {
	if !debugEnabled {
		return
	}
	debugLogger.Printf("DEBUG field=%-30s offset=%d-%d hex=%x value=%v", name, r.base+start, r.base+end, raw, value)
}

// bytes reads n raw bytes and advances the cursor.
func (r *fieldReader) bytes(name string, n int) []byte {
	start := r.pos
	end := start + n
	raw := r.data[start:end]
	r.pos = end
	r.logField(name, start, end, raw, raw)
	return raw
}

// pad skips n alignment bytes and advances the cursor. Padding is read
// explicitly rather than by bumping the cursor silently so that it shows up in
// the -debug trace alongside real fields, keeping the logged offsets lined up
// 1:1 with the hex dump.
func (r *fieldReader) pad(name string, n int) {
	start := r.pos
	end := start + n
	raw := r.data[start:end]
	r.pos = end
	r.logField(name, start, end, raw, "(alignment padding, ignored)")
}

func (r *fieldReader) i16(name string) int16 {
	start := r.pos
	end := start + 2
	raw := r.data[start:end]
	v := int16(binary.BigEndian.Uint16(raw))
	r.pos = end
	r.logField(name, start, end, raw, v)
	return v
}

func (r *fieldReader) i32(name string) int32 {
	start := r.pos
	end := start + 4
	raw := r.data[start:end]
	v := int32(binary.BigEndian.Uint32(raw))
	r.pos = end
	r.logField(name, start, end, raw, v)
	return v
}

func (r *fieldReader) byte1(name string) byte {
	start := r.pos
	end := start + 1
	raw := r.data[start:end]
	v := raw[0]
	r.pos = end
	r.logField(name, start, end, raw, v)
	return v
}

func (r *fieldReader) f64(name string) float64 {
	start := r.pos
	end := start + 8
	raw := r.data[start:end]
	v := math.Float64frombits(binary.BigEndian.Uint64(raw))
	r.pos = end
	r.logField(name, start, end, raw, v)
	return v
}

// hexDump renders data in classic `hexdump -C` style (offset, 16 hex bytes,
// ascii preview), used to inspect a decompressed payload independently of
// field parsing -- e.g. to tell a decompression bug apart from an offset bug.
func hexDump(data []byte) string {
	var b strings.Builder
	for i := 0; i < len(data); i += 16 {
		end := i + 16
		if end > len(data) {
			end = len(data)
		}
		line := data[i:end]

		fmt.Fprintf(&b, "%08x  ", i)
		for j := 0; j < 16; j++ {
			if j < len(line) {
				fmt.Fprintf(&b, "%02x ", line[j])
			} else {
				b.WriteString("   ")
			}
			if j == 7 {
				b.WriteByte(' ')
			}
		}
		b.WriteString(" |")
		for _, c := range line {
			if c >= 0x20 && c < 0x7f {
				b.WriteByte(c)
			} else {
				b.WriteByte('.')
			}
		}
		b.WriteString("|\n")
	}
	return b.String()
}

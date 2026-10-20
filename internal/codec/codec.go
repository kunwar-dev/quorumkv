// Package codec provides a small, allocation-conscious binary encoding based
// on uvarints and length-prefixed byte strings. It is used for WAL records
// and replicated KV commands, where gob or JSON would add overhead to every
// entry on the hot path.
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrTruncated is returned when input ends before a value is complete.
var ErrTruncated = errors.New("codec: truncated input")

// Encoder appends values to Buf.
type Encoder struct{ Buf []byte }

// Byte appends a single byte.
func (e *Encoder) Byte(b byte) { e.Buf = append(e.Buf, b) }

// Uvarint appends v as an unsigned varint.
func (e *Encoder) Uvarint(v uint64) { e.Buf = binary.AppendUvarint(e.Buf, v) }

// Bytes appends a length-prefixed byte string.
func (e *Encoder) Bytes(b []byte) {
	e.Uvarint(uint64(len(b)))
	e.Buf = append(e.Buf, b...)
}

// String appends a length-prefixed string.
func (e *Encoder) String(s string) {
	e.Uvarint(uint64(len(s)))
	e.Buf = append(e.Buf, s...)
}

// Decoder reads values produced by Encoder. After the first failure every
// method returns a zero value and Err reports the failure.
type Decoder struct {
	buf []byte
	err error
}

// NewDecoder returns a Decoder over b.
func NewDecoder(b []byte) *Decoder { return &Decoder{buf: b} }

// Err returns the first decoding error, if any.
func (d *Decoder) Err() error { return d.err }

// Remaining returns the undecoded tail of the input.
func (d *Decoder) Remaining() []byte { return d.buf }

// Len returns the number of undecoded bytes.
func (d *Decoder) Len() int { return len(d.buf) }

func (d *Decoder) fail(what string) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: %s", ErrTruncated, what)
	}
}

// Byte reads a single byte.
func (d *Decoder) Byte() byte {
	if d.err != nil {
		return 0
	}
	if len(d.buf) == 0 {
		d.fail("byte")
		return 0
	}
	b := d.buf[0]
	d.buf = d.buf[1:]
	return b
}

// Uvarint reads an unsigned varint.
func (d *Decoder) Uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.fail("uvarint")
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

// Bytes reads a length-prefixed byte string into a fresh slice. An empty
// string decodes as nil.
func (d *Decoder) Bytes() []byte {
	n := d.Uvarint()
	if d.err != nil {
		return nil
	}
	if uint64(len(d.buf)) < n {
		d.fail("bytes")
		return nil
	}
	var b []byte
	if n > 0 {
		b = append([]byte(nil), d.buf[:n]...)
	}
	d.buf = d.buf[n:]
	return b
}

// String reads a length-prefixed string.
func (d *Decoder) String() string { return string(d.Bytes()) }

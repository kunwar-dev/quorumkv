package wal

import (
	"encoding/binary"
	"fmt"

	"github.com/kunwar-dev/quorumkv/raft"
)

// encoder builds compact binary payloads using uvarints.
type encoder struct{ buf []byte }

func (e *encoder) byte(b byte)      { e.buf = append(e.buf, b) }
func (e *encoder) uvarint(v uint64) { e.buf = binary.AppendUvarint(e.buf, v) }
func (e *encoder) bytes(b []byte) {
	e.uvarint(uint64(len(b)))
	e.buf = append(e.buf, b...)
}

func (e *encoder) entries(es []raft.Entry) {
	e.uvarint(uint64(len(es)))
	for _, en := range es {
		e.uvarint(en.Index)
		e.uvarint(en.Term)
		e.byte(byte(en.Type))
		e.bytes(en.Data)
	}
}

// decoder reads payloads written by encoder. The first error sticks.
type decoder struct {
	buf []byte
	err error
}

func (d *decoder) fail(what string) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: truncated %s", ErrCorrupt, what)
	}
}

func (d *decoder) uvarint() uint64 {
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

func (d *decoder) byte() byte {
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

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if uint64(len(d.buf)) < n {
		d.fail("bytes")
		return nil
	}
	b := append([]byte(nil), d.buf[:n]...)
	d.buf = d.buf[n:]
	return b
}

func (d *decoder) entries() []raft.Entry {
	n := d.uvarint()
	if d.err != nil || n > uint64(len(d.buf)) { // each entry needs at least 1 byte
		d.fail("entries")
		return nil
	}
	es := make([]raft.Entry, 0, n)
	for range n {
		e := raft.Entry{Index: d.uvarint(), Term: d.uvarint(), Type: raft.EntryType(d.byte())}
		e.Data = d.bytes()
		if d.err != nil {
			return nil
		}
		if len(e.Data) == 0 {
			e.Data = nil
		}
		es = append(es, e)
	}
	return es
}

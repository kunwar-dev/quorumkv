package wal

import (
	"fmt"

	"github.com/kunwar-dev/quorumkv/internal/codec"
	"github.com/kunwar-dev/quorumkv/raft"
)

func encodeHardState(hs raft.HardState) []byte {
	var e codec.Encoder
	e.Byte(byte(recHardState))
	e.Uvarint(hs.Term)
	e.String(hs.VotedFor)
	return e.Buf
}

func encodeEntries(es []raft.Entry) []byte {
	var e codec.Encoder
	e.Byte(byte(recEntries))
	e.Uvarint(uint64(len(es)))
	for _, en := range es {
		e.Uvarint(en.Index)
		e.Uvarint(en.Term)
		e.Byte(byte(en.Type))
		e.Bytes(en.Data)
	}
	return e.Buf
}

func encodeTruncate(index uint64) []byte {
	var e codec.Encoder
	e.Byte(byte(recTruncate))
	e.Uvarint(index)
	return e.Buf
}

func decodeHardState(d *codec.Decoder) (raft.HardState, error) {
	hs := raft.HardState{Term: d.Uvarint(), VotedFor: d.String()}
	return hs, wrap(d.Err())
}

func decodeEntries(d *codec.Decoder) ([]raft.Entry, error) {
	n := d.Uvarint()
	// Each entry occupies at least four bytes, which bounds the allocation
	// even if a corrupt record claims an enormous count.
	if d.Err() != nil || n > uint64(d.Len()) {
		return nil, fmt.Errorf("%w: bad entry count", ErrCorrupt)
	}
	es := make([]raft.Entry, 0, n)
	for range n {
		e := raft.Entry{Index: d.Uvarint(), Term: d.Uvarint(), Type: raft.EntryType(d.Byte())}
		e.Data = d.Bytes()
		if d.Err() != nil {
			return nil, wrap(d.Err())
		}
		es = append(es, e)
	}
	return es, nil
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrCorrupt, err)
}

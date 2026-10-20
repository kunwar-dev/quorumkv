package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/kunwar-dev/quorumkv/internal/codec"
	"github.com/kunwar-dev/quorumkv/raft"
)

var snapMagic = []byte("QKVSNAP1")

// Snapshot file layout:
//
//	magic(8) | crc32c(4) of the rest | uvarint index | uvarint term | data
func writeSnapshot(dir string, snap raft.Snapshot, durable bool) error {
	var body codec.Encoder
	body.Uvarint(snap.Meta.Index)
	body.Uvarint(snap.Meta.Term)
	body.Buf = append(body.Buf, snap.Data...)

	var buf bytes.Buffer
	buf.Write(snapMagic)
	var sum [4]byte
	binary.LittleEndian.PutUint32(sum[:], crc32.Checksum(body.Buf, crcTable))
	buf.Write(sum[:])
	buf.Write(body.Buf)

	tmpPath := filepath.Join(dir, snapshotFile+".tmp")
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: write snapshot: %w", err)
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return fmt.Errorf("wal: write snapshot: %w", err)
	}
	if durable {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("wal: fsync snapshot: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("wal: write snapshot: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, snapshotFile)); err != nil {
		return fmt.Errorf("wal: install snapshot: %w", err)
	}
	if durable {
		return syncDir(dir)
	}
	return nil
}

// readSnapshot returns nil, nil when no snapshot exists. Because snapshots
// are installed with an atomic rename, a corrupt file indicates real damage
// rather than a torn write, so it is reported instead of ignored.
func readSnapshot(path string) (*raft.Snapshot, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("wal: read snapshot: %w", err)
	}
	if len(raw) < len(snapMagic)+4 || !bytes.Equal(raw[:len(snapMagic)], snapMagic) {
		return nil, fmt.Errorf("%w: bad snapshot header in %s", ErrCorrupt, path)
	}
	sum := binary.LittleEndian.Uint32(raw[len(snapMagic):])
	body := raw[len(snapMagic)+4:]
	if crc32.Checksum(body, crcTable) != sum {
		return nil, fmt.Errorf("%w: snapshot checksum mismatch in %s", ErrCorrupt, path)
	}
	d := codec.NewDecoder(body)
	meta := raft.SnapshotMeta{Index: d.Uvarint(), Term: d.Uvarint()}
	if err := wrap(d.Err()); err != nil {
		return nil, err
	}
	return &raft.Snapshot{Meta: meta, Data: append([]byte(nil), d.Remaining()...)}, nil
}

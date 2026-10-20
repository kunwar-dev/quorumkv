// Package wal provides crash-safe, file-backed storage for Raft.
//
// State is kept in two files inside a data directory:
//
//	snapshot  the latest application snapshot, replaced atomically
//	wal.log   an append-only log of checksummed records describing every
//	          change to the hard state and the log since that snapshot
//
// Every mutation is written and fsynced before the call returns, which is the
// durability contract raft.Storage requires. Each record carries a CRC-32C
// checksum; on startup the log is replayed and any torn or corrupt tail left
// by a crash mid-write is detected and truncated. When a snapshot is saved,
// the log is compacted by atomically rewriting it to contain only the state
// that follows the snapshot.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/kunwar-dev/quorumkv/internal/codec"
	"github.com/kunwar-dev/quorumkv/raft"
)

const (
	walFile      = "wal.log"
	snapshotFile = "snapshot"
	headerSize   = 8         // 4-byte length + 4-byte CRC
	maxRecord    = 256 << 20 // sanity bound; larger lengths imply corruption
)

type recordType byte

const (
	recHardState recordType = iota + 1
	recEntries
	recTruncate
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt reports an unreadable snapshot file.
var ErrCorrupt = errors.New("wal: corrupt data")

// Options configures a Storage.
type Options struct {
	// NoSync skips fsync. It is unsafe and exists only for benchmarks and tests.
	NoSync bool
	// Logger receives recovery diagnostics. Defaults to slog.Default().
	Logger *slog.Logger
}

// Storage is a durable raft.Storage backed by a directory on disk.
type Storage struct {
	dir    string
	opts   Options
	logger *slog.Logger

	mu    sync.Mutex
	f     *os.File
	state *raft.MemoryStorage // authoritative in-memory mirror of what is on disk
	hs    raft.HardState
	buf   []byte
}

var _ raft.Storage = (*Storage)(nil)

// Open loads (or creates) the storage in dir, recovering from any torn write.
func Open(dir string, opts Options) (*Storage, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}
	s := &Storage{dir: dir, opts: opts, logger: opts.Logger, state: raft.NewMemoryStorage()}

	snap, err := readSnapshot(filepath.Join(dir, snapshotFile))
	if err != nil {
		return nil, err
	}
	if snap != nil {
		if err := s.state.SaveSnapshot(*snap); err != nil {
			return nil, err
		}
	}
	if err := s.replay(); err != nil {
		return nil, err
	}
	return s, nil
}

// replay applies every valid record in wal.log, truncates any corrupt tail
// and leaves the file open for appending.
func (s *Storage) replay() error {
	path := filepath.Join(s.dir, walFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open log: %w", err)
	}
	valid, records, err := s.scan(f)
	if err != nil {
		f.Close()
		return err
	}
	if info, err := f.Stat(); err == nil && info.Size() > valid {
		s.logger.Warn("wal: truncating torn or corrupt tail",
			"path", path, "validBytes", valid, "discardedBytes", info.Size()-valid)
		if err := f.Truncate(valid); err != nil {
			f.Close()
			return fmt.Errorf("wal: truncate tail: %w", err)
		}
		if err := s.sync(f); err != nil {
			f.Close()
			return err
		}
	}
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("wal: seek: %w", err)
	}
	s.f = f
	s.logger.Debug("wal: recovered", "records", records, "bytes", valid)
	return nil
}

// scan reads records until EOF or the first invalid record and returns the
// byte offset where valid data ends.
func (s *Storage) scan(f *os.File) (valid int64, records int, err error) {
	r := bufio.NewReader(f)
	var header [headerSize]byte
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return valid, records, nil // clean EOF or torn header
		}
		n := binary.LittleEndian.Uint32(header[0:4])
		sum := binary.LittleEndian.Uint32(header[4:8])
		if n == 0 || n > maxRecord {
			return valid, records, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return valid, records, nil // torn payload
		}
		if crc32.Checksum(payload, crcTable) != sum {
			return valid, records, nil
		}
		if err := s.applyRecord(payload); err != nil {
			return 0, 0, fmt.Errorf("wal: record %d at offset %d: %w", records, valid, err)
		}
		valid += int64(headerSize) + int64(n)
		records++
	}
}

func (s *Storage) applyRecord(p []byte) error {
	d := codec.NewDecoder(p[1:])
	switch recordType(p[0]) {
	case recHardState:
		hs, err := decodeHardState(d)
		if err != nil {
			return err
		}
		s.hs = hs
		return s.state.SetHardState(hs)
	case recEntries:
		entries, err := decodeEntries(d)
		if err != nil {
			return err
		}
		return s.state.Append(entries)
	case recTruncate:
		idx := d.Uvarint()
		if err := wrap(d.Err()); err != nil {
			return err
		}
		return s.state.TruncateFrom(idx)
	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, p[0])
	}
}

// InitialState implements raft.Storage.
func (s *Storage) InitialState() (raft.HardState, *raft.Snapshot, []raft.Entry, error) {
	return s.state.InitialState()
}

// SetHardState implements raft.Storage.
func (s *Storage) SetHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeRecord(encodeHardState(hs)); err != nil {
		return err
	}
	s.hs = hs
	return s.state.SetHardState(hs)
}

// Append implements raft.Storage.
func (s *Storage) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeRecord(encodeEntries(entries)); err != nil {
		return err
	}
	return s.state.Append(entries)
}

// TruncateFrom implements raft.Storage.
func (s *Storage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeRecord(encodeTruncate(index)); err != nil {
		return err
	}
	return s.state.TruncateFrom(index)
}

// SaveSnapshot implements raft.Storage. The snapshot file is replaced
// atomically first; the log is then rewritten to hold only the hard state and
// entries after the snapshot. A crash between the two steps is safe: on
// recovery, entries already covered by the snapshot are ignored.
func (s *Storage) SaveSnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeSnapshot(s.dir, snap, !s.opts.NoSync); err != nil {
		return err
	}
	if err := s.state.SaveSnapshot(snap); err != nil {
		return err
	}
	return s.compactLocked()
}

// compactLocked atomically replaces wal.log with a minimal equivalent.
func (s *Storage) compactLocked() error {
	_, _, entries, _ := s.state.InitialState()

	tmpPath := filepath.Join(s.dir, walFile+".tmp")
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: compact: %w", err)
	}
	w := bufio.NewWriter(tmp)
	writeFramed(w, encodeHardState(s.hs))
	if len(entries) > 0 {
		writeFramed(w, encodeEntries(entries))
	}
	if err := w.Flush(); err != nil {
		tmp.Close()
		return fmt.Errorf("wal: compact: %w", err)
	}
	if err := s.sync(tmp); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("wal: compact: %w", err)
	}

	path := filepath.Join(s.dir, walFile)
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("wal: compact rename: %w", err)
	}
	if err := s.syncDir(); err != nil {
		return err
	}
	s.f.Close()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopen after compact: %w", err)
	}
	s.f = f
	return nil
}

// Close releases the log file.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

func (s *Storage) writeRecord(payload []byte) error {
	s.buf = frame(s.buf[:0], payload)
	if _, err := s.f.Write(s.buf); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	return s.sync(s.f)
}

func (s *Storage) sync(f *os.File) error {
	if s.opts.NoSync {
		return nil
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

func (s *Storage) syncDir() error {
	if s.opts.NoSync {
		return nil
	}
	return syncDir(s.dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: fsync dir: %w", err)
	}
	return nil
}

func frame(dst, payload []byte) []byte {
	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.Checksum(payload, crcTable))
	dst = append(dst, header[:]...)
	return append(dst, payload...)
}

func writeFramed(w *bufio.Writer, payload []byte) {
	w.Write(frame(nil, payload))
}

package raft

import (
	"errors"
	"fmt"
	"sync"
)

// HardState is the state Raft must persist before responding to RPCs (Figure 2).
type HardState struct {
	Term     uint64
	VotedFor string
}

// SnapshotMeta identifies the last log entry covered by a snapshot.
type SnapshotMeta struct {
	Index uint64
	Term  uint64
}

// Snapshot is an application state snapshot plus its position in the log.
type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// Storage durably persists Raft state. Every method must be durable on return:
// Raft acknowledges votes and entries to peers only after these calls succeed.
type Storage interface {
	// InitialState returns all persisted state; called once when a Node starts.
	// The returned entries begin immediately after the snapshot index.
	InitialState() (HardState, *Snapshot, []Entry, error)
	// SetHardState persists the current term and vote.
	SetHardState(HardState) error
	// Append persists entries. If entries[0].Index is at or below the last
	// stored index, stored entries from that index onward are replaced.
	Append(entries []Entry) error
	// TruncateFrom deletes all stored entries with index >= index.
	TruncateFrom(index uint64) error
	// SaveSnapshot persists snap and deletes stored entries with index <= snap.Meta.Index.
	SaveSnapshot(snap Snapshot) error
}

// ErrLogGap is returned when appended entries do not follow the stored log.
var ErrLogGap = errors.New("raft: appended entries leave a gap in the log")

// MemoryStorage is an in-memory Storage. It is used in tests, where Clone
// simulates a node's disk surviving a crash.
type MemoryStorage struct {
	mu      sync.Mutex
	hs      HardState
	snap    *Snapshot
	entries []Entry // entries[0].Index == snapIndex()+1
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) snapIndex() uint64 {
	if s.snap == nil {
		return 0
	}
	return s.snap.Meta.Index
}

// InitialState implements Storage.
func (s *MemoryStorage) InitialState() (HardState, *Snapshot, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snap *Snapshot
	if s.snap != nil {
		c := cloneSnapshot(*s.snap)
		snap = &c
	}
	return s.hs, snap, append([]Entry(nil), s.entries...), nil
}

// SetHardState implements Storage.
func (s *MemoryStorage) SetHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs = hs
	return nil
}

// Append implements Storage.
func (s *MemoryStorage) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Ignore any prefix already covered by the snapshot.
	for len(entries) > 0 && entries[0].Index <= s.snapIndex() {
		entries = entries[1:]
	}
	if len(entries) == 0 {
		return nil
	}
	last := s.snapIndex() + uint64(len(s.entries))
	first := entries[0].Index
	if first > last+1 {
		return fmt.Errorf("%w: last=%d first=%d", ErrLogGap, last, first)
	}
	s.entries = append(s.entries[:first-s.snapIndex()-1], entries...)
	return nil
}

// TruncateFrom implements Storage.
func (s *MemoryStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.snapIndex() {
		s.entries = nil
		return nil
	}
	if off := index - s.snapIndex() - 1; off < uint64(len(s.entries)) {
		s.entries = s.entries[:off]
	}
	return nil
}

// SaveSnapshot implements Storage.
func (s *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Meta.Index <= s.snapIndex() {
		return nil
	}
	drop := snap.Meta.Index - s.snapIndex()
	if drop >= uint64(len(s.entries)) {
		s.entries = nil
	} else {
		s.entries = append([]Entry(nil), s.entries[drop:]...)
	}
	c := cloneSnapshot(snap)
	s.snap = &c
	return nil
}

// Clone returns a deep copy, simulating the contents of disk after a crash.
func (s *MemoryStorage) Clone() *MemoryStorage {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := &MemoryStorage{hs: s.hs, entries: append([]Entry(nil), s.entries...)}
	if s.snap != nil {
		snap := cloneSnapshot(*s.snap)
		c.snap = &snap
	}
	return c
}

func cloneSnapshot(s Snapshot) Snapshot {
	return Snapshot{Meta: s.Meta, Data: append([]byte(nil), s.Data...)}
}

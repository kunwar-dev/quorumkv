package kv

import (
	"fmt"

	"github.com/kunwar-dev/quorumkv/internal/codec"
)

// session records the last request applied for a client.
type session struct {
	seq    uint64
	result Result
}

// StateMachine is the deterministic key-value state replicated by Raft.
// It is not safe for concurrent use; Store serialises access.
type StateMachine struct {
	data     map[string][]byte
	sessions map[string]session
}

// NewStateMachine returns an empty state machine.
func NewStateMachine() *StateMachine {
	return &StateMachine{data: make(map[string][]byte), sessions: make(map[string]session)}
}

// Apply executes c and returns its result. Duplicate requests (same client
// and sequence number) return the original result without re-executing.
func (m *StateMachine) Apply(c Command) Result {
	if c.ClientID != "" {
		if s, ok := m.sessions[c.ClientID]; ok && c.Seq <= s.seq {
			// A retry of the latest request returns its cached result. Older
			// requests cannot be outstanding: clients issue one at a time.
			return s.result
		}
	}

	var r Result
	switch c.Op {
	case OpGet:
		r.Value, r.Found = m.data[c.Key]
	case OpPut:
		m.data[c.Key] = c.Value
		r.Found = true
	case OpDelete:
		_, r.Found = m.data[c.Key]
		delete(m.data, c.Key)
	}

	if c.ClientID != "" {
		m.sessions[c.ClientID] = session{seq: c.Seq, result: r}
	}
	return r
}

// Len returns the number of keys.
func (m *StateMachine) Len() int { return len(m.data) }

const snapshotVersion = 1

// Snapshot serialises the full state, including client sessions, so that
// deduplication survives log compaction.
func (m *StateMachine) Snapshot() []byte {
	var e codec.Encoder
	e.Byte(snapshotVersion)
	e.Uvarint(uint64(len(m.data)))
	for k, v := range m.data {
		e.String(k)
		e.Bytes(v)
	}
	e.Uvarint(uint64(len(m.sessions)))
	for id, s := range m.sessions {
		e.String(id)
		e.Uvarint(s.seq)
		e.Bytes(s.result.Value)
		if s.result.Found {
			e.Byte(1)
		} else {
			e.Byte(0)
		}
	}
	return e.Buf
}

// Restore replaces the state with a snapshot produced by Snapshot.
func (m *StateMachine) Restore(b []byte) error {
	d := codec.NewDecoder(b)
	if v := d.Byte(); d.Err() == nil && v != snapshotVersion {
		return fmt.Errorf("kv: unsupported snapshot version %d", v)
	}
	n := d.Uvarint()
	data := make(map[string][]byte, min(n, uint64(d.Len())))
	for range n {
		k := d.String()
		data[k] = d.Bytes()
		if d.Err() != nil {
			break
		}
	}
	ns := d.Uvarint()
	sessions := make(map[string]session, min(ns, uint64(d.Len())))
	for range ns {
		id := d.String()
		s := session{seq: d.Uvarint()}
		s.result.Value = d.Bytes()
		s.result.Found = d.Byte() == 1
		if d.Err() != nil {
			break
		}
		sessions[id] = s
	}
	if err := d.Err(); err != nil {
		return fmt.Errorf("kv: restore snapshot: %w", err)
	}
	m.data, m.sessions = data, sessions
	return nil
}

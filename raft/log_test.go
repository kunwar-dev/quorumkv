package raft

import (
	"errors"
	"testing"
)

func mkLog(snapIndex, snapTerm uint64, terms ...uint64) raftLog {
	l := raftLog{snapIndex: snapIndex, snapTerm: snapTerm}
	for i, t := range terms {
		l.entries = append(l.entries, Entry{Index: snapIndex + uint64(i) + 1, Term: t})
	}
	return l
}

func TestRaftLogIndexing(t *testing.T) {
	l := mkLog(10, 2, 2, 3, 3, 5) // indexes 11..14
	if l.lastIndex() != 14 || l.lastTerm() != 5 {
		t.Fatalf("last = %d/%d", l.lastIndex(), l.lastTerm())
	}
	for _, tc := range []struct {
		i    uint64
		want uint64
		ok   bool
	}{{9, 0, false}, {10, 2, true}, {11, 2, true}, {13, 3, true}, {14, 5, true}, {15, 0, false}} {
		if got, ok := l.term(tc.i); got != tc.want || ok != tc.ok {
			t.Errorf("term(%d) = %d,%v want %d,%v", tc.i, got, ok, tc.want, tc.ok)
		}
	}
	if s := l.slice(12, 14); len(s) != 2 || s[0].Index != 12 || s[1].Index != 13 {
		t.Fatalf("slice(12,14) = %v", s)
	}
	if got := l.firstIndexOfTerm(3, 13); got != 12 {
		t.Fatalf("firstIndexOfTerm(3) = %d, want 12", got)
	}
	if got, ok := l.lastIndexOfTerm(3); !ok || got != 13 {
		t.Fatalf("lastIndexOfTerm(3) = %d,%v", got, ok)
	}
	if _, ok := l.lastIndexOfTerm(4); ok {
		t.Fatal("found nonexistent term 4")
	}
}

func TestRaftLogCompactAndTruncate(t *testing.T) {
	l := mkLog(0, 0, 1, 1, 2, 2, 3)
	l.compact(3, 2)
	if l.snapIndex != 3 || l.snapTerm != 2 || l.lastIndex() != 5 || len(l.entries) != 2 {
		t.Fatalf("after compact: %+v", l)
	}
	l.truncateFrom(5)
	if l.lastIndex() != 4 {
		t.Fatalf("after truncate: last=%d", l.lastIndex())
	}
	l.compact(10, 3) // beyond the log: everything goes
	if l.lastIndex() != 10 || len(l.entries) != 0 {
		t.Fatalf("compact past end: %+v", l)
	}
}

func TestMemoryStorageSemantics(t *testing.T) {
	s := NewMemoryStorage()
	es := func(from, to, term uint64) []Entry {
		var out []Entry
		for i := from; i <= to; i++ {
			out = append(out, Entry{Index: i, Term: term})
		}
		return out
	}
	if err := s.Append(es(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(es(4, 6, 2)); err != nil { // overwrite 4..5
		t.Fatal(err)
	}
	if err := s.Append(es(9, 9, 2)); !errors.Is(err, ErrLogGap) {
		t.Fatalf("expected ErrLogGap, got %v", err)
	}
	if err := s.SaveSnapshot(Snapshot{Meta: SnapshotMeta{Index: 3, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	_, snap, got, _ := s.InitialState()
	if snap.Meta.Index != 3 || len(got) != 3 || got[0].Index != 4 || got[0].Term != 2 {
		t.Fatalf("state after snapshot: snap=%+v entries=%v", snap.Meta, got)
	}
	if err := s.TruncateFrom(5); err != nil {
		t.Fatal(err)
	}
	if _, _, got, _ := s.InitialState(); len(got) != 1 {
		t.Fatalf("after truncate: %v", got)
	}
}

func TestConfigValidation(t *testing.T) {
	base := Config{ID: "a", Peers: []string{"a", "b"}, Storage: NewMemoryStorage(), Transport: nopTransport{}}
	cases := map[string]func(*Config){
		"missing id":       func(c *Config) { c.ID = "" },
		"id not in peers":  func(c *Config) { c.Peers = []string{"b", "c"} },
		"no storage":       func(c *Config) { c.Storage = nil },
		"heartbeat >= eto": func(c *Config) { c.ElectionTimeout = 100; c.HeartbeatInterval = 100 },
	}
	for name, mutate := range cases {
		c := base
		mutate(&c)
		if err := c.validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	c := base
	if err := c.validate(); err != nil || c.HeartbeatInterval == 0 || c.MaxEntriesPerAppend == 0 {
		t.Fatalf("defaults not applied: %v %+v", err, c)
	}
}

type nopTransport struct{ Transport }

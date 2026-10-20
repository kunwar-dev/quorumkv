package wal

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kunwar-dev/quorumkv/raft"
)

var quiet = Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

func open(t *testing.T, dir string) *Storage {
	t.Helper()
	s, err := Open(dir, quiet)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func entries(from, to, term uint64) []raft.Entry {
	var es []raft.Entry
	for i := from; i <= to; i++ {
		es = append(es, raft.Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("cmd-%d-%d", term, i))})
	}
	return es
}

func mustState(t *testing.T, s *Storage) (raft.HardState, *raft.Snapshot, []raft.Entry) {
	t.Helper()
	hs, snap, es, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	return hs, snap, es
}

func TestRecoverHardStateAndEntries(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	must(t, s.SetHardState(raft.HardState{Term: 3, VotedFor: "n2"}))
	must(t, s.Append(entries(1, 10, 1)))
	must(t, s.Append(entries(8, 12, 2))) // overwrites 8..10
	must(t, s.TruncateFrom(12))
	s.Close()

	r := open(t, dir)
	hs, snap, got := mustState(t, r)
	if hs != (raft.HardState{Term: 3, VotedFor: "n2"}) {
		t.Fatalf("hard state = %+v", hs)
	}
	if snap != nil {
		t.Fatalf("unexpected snapshot %+v", snap.Meta)
	}
	want := append(entries(1, 7, 1), entries(8, 11, 2)...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entries mismatch:\n got %v\nwant %v", got, want)
	}
}

func TestSnapshotCompactsLog(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	must(t, s.SetHardState(raft.HardState{Term: 2, VotedFor: "n1"}))
	must(t, s.Append(entries(1, 100, 2)))
	before := fileSize(t, filepath.Join(dir, walFile))

	must(t, s.SaveSnapshot(raft.Snapshot{Meta: raft.SnapshotMeta{Index: 90, Term: 2}, Data: []byte("state@90")}))
	if after := fileSize(t, filepath.Join(dir, walFile)); after >= before {
		t.Fatalf("log not compacted: %d -> %d bytes", before, after)
	}
	must(t, s.Append(entries(101, 105, 3)))
	s.Close()

	r := open(t, dir)
	hs, snap, got := mustState(t, r)
	if hs.Term != 2 || hs.VotedFor != "n1" {
		t.Fatalf("hard state lost across compaction: %+v", hs)
	}
	if snap == nil || snap.Meta != (raft.SnapshotMeta{Index: 90, Term: 2}) || string(snap.Data) != "state@90" {
		t.Fatalf("snapshot = %+v", snap)
	}
	want := append(entries(91, 100, 2), entries(101, 105, 3)...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entries after snapshot mismatch: got %d entries", len(got))
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	must(t, s.Append(entries(1, 5, 1)))
	must(t, s.Append(entries(6, 6, 1)))
	s.Close()

	// Simulate a crash mid-write: chop the final record in half.
	path := filepath.Join(dir, walFile)
	size := fileSize(t, path)
	must(t, os.Truncate(path, size-5))

	r := open(t, dir)
	_, _, got := mustState(t, r)
	if !reflect.DeepEqual(got, entries(1, 5, 1)) {
		t.Fatalf("expected entries 1..5 after torn write, got %d entries", len(got))
	}
	// The log must accept new writes cleanly after recovery.
	must(t, r.Append(entries(6, 7, 2)))
	r.Close()

	r2 := open(t, dir)
	if _, _, got := mustState(t, r2); len(got) != 7 {
		t.Fatalf("expected 7 entries after re-append, got %d", len(got))
	}
}

func TestCorruptRecordStopsReplay(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	must(t, s.Append(entries(1, 3, 1)))
	must(t, s.Append(entries(4, 6, 1)))
	s.Close()

	// Flip a byte inside the last record's payload.
	path := filepath.Join(dir, walFile)
	raw, err := os.ReadFile(path)
	must(t, err)
	raw[len(raw)-2] ^= 0xFF
	must(t, os.WriteFile(path, raw, 0o644))

	r := open(t, dir)
	if _, _, got := mustState(t, r); !reflect.DeepEqual(got, entries(1, 3, 1)) {
		t.Fatalf("expected only the intact prefix, got %d entries", len(got))
	}
}

// TestCrashBetweenSnapshotAndCompaction covers a crash after the snapshot
// file is installed but before the log is rewritten.
func TestCrashBetweenSnapshotAndCompaction(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	must(t, s.Append(entries(1, 50, 1)))
	s.Close()

	must(t, writeSnapshot(dir, raft.Snapshot{Meta: raft.SnapshotMeta{Index: 40, Term: 1}, Data: []byte("s")}, true))

	r := open(t, dir)
	_, snap, got := mustState(t, r)
	if snap == nil || snap.Meta.Index != 40 {
		t.Fatalf("snapshot not recovered")
	}
	if !reflect.DeepEqual(got, entries(41, 50, 1)) {
		t.Fatalf("expected entries 41..50, got %d entries", len(got))
	}
}

func TestCorruptSnapshotIsReported(t *testing.T) {
	dir := t.TempDir()
	must(t, writeSnapshot(dir, raft.Snapshot{Meta: raft.SnapshotMeta{Index: 5, Term: 1}, Data: []byte("abc")}, true))
	path := filepath.Join(dir, snapshotFile)
	raw, err := os.ReadFile(path)
	must(t, err)
	raw[len(raw)-1] ^= 0xFF
	must(t, os.WriteFile(path, raw, 0o644))

	if _, err := Open(dir, quiet); err == nil {
		t.Fatal("expected an error for a corrupt snapshot")
	}
}

func BenchmarkAppendFsync(b *testing.B)  { benchmarkAppend(b, false) }
func BenchmarkAppendNoSync(b *testing.B) { benchmarkAppend(b, true) }

func benchmarkAppend(b *testing.B, noSync bool) {
	s, err := Open(b.TempDir(), Options{NoSync: noSync, Logger: quiet.Logger})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	data := make([]byte, 128)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := range b.N {
		if err := s.Append([]raft.Entry{{Index: uint64(i + 1), Term: 1, Data: data}}); err != nil {
			b.Fatal(err)
		}
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	must(t, err)
	return info.Size()
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

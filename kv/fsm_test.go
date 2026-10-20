package kv

import (
	"bytes"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	in := Command{Op: OpPut, Key: "k/1", Value: []byte("v"), ClientID: "c1", Seq: 42}
	out, err := DecodeCommand(in.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if out.Op != in.Op || out.Key != in.Key || !bytes.Equal(out.Value, in.Value) || out.ClientID != in.ClientID || out.Seq != in.Seq {
		t.Fatalf("round trip mismatch: %+v vs %+v", out, in)
	}
	if _, err := DecodeCommand(in.Encode()[:5]); err == nil {
		t.Fatal("expected error on truncated command")
	}
}

func TestApplySemantics(t *testing.T) {
	m := NewStateMachine()
	if r := m.Apply(Command{Op: OpGet, Key: "a"}); r.Found {
		t.Fatal("missing key reported as found")
	}
	m.Apply(Command{Op: OpPut, Key: "a", Value: []byte("1")})
	if r := m.Apply(Command{Op: OpGet, Key: "a"}); !r.Found || string(r.Value) != "1" {
		t.Fatalf("get after put = %+v", r)
	}
	if r := m.Apply(Command{Op: OpDelete, Key: "a"}); !r.Found {
		t.Fatal("delete of existing key reported not found")
	}
	if r := m.Apply(Command{Op: OpDelete, Key: "a"}); r.Found {
		t.Fatal("second delete reported found")
	}
}

func TestDuplicateRequestsExecuteOnce(t *testing.T) {
	m := NewStateMachine()
	m.Apply(Command{Op: OpPut, Key: "x", Value: []byte("1"), ClientID: "c", Seq: 1})
	first := m.Apply(Command{Op: OpDelete, Key: "x", ClientID: "c", Seq: 2})
	// Meanwhile another client writes x.
	m.Apply(Command{Op: OpPut, Key: "x", Value: []byte("2"), ClientID: "other", Seq: 1})
	// The retried delete must not run again and must return the original result.
	retry := m.Apply(Command{Op: OpDelete, Key: "x", ClientID: "c", Seq: 2})
	if retry.Found != first.Found {
		t.Fatalf("retry returned %+v, original %+v", retry, first)
	}
	if r := m.Apply(Command{Op: OpGet, Key: "x"}); string(r.Value) != "2" {
		t.Fatalf("duplicate delete re-executed: x=%q", r.Value)
	}
}

func TestSnapshotRestorePreservesSessions(t *testing.T) {
	m := NewStateMachine()
	m.Apply(Command{Op: OpPut, Key: "a", Value: []byte("1"), ClientID: "c", Seq: 7})
	m.Apply(Command{Op: OpPut, Key: "b", Value: []byte("2")})

	r := NewStateMachine()
	if err := r.Restore(m.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got := r.Apply(Command{Op: OpGet, Key: "b"}); string(got.Value) != "2" {
		t.Fatalf("b = %q after restore", got.Value)
	}
	// Seq 7 is a duplicate: it must not overwrite a.
	r.Apply(Command{Op: OpPut, Key: "a", Value: []byte("overwritten"), ClientID: "c", Seq: 7})
	if got := r.Apply(Command{Op: OpGet, Key: "a"}); string(got.Value) != "1" {
		t.Fatalf("session lost across snapshot: a=%q", got.Value)
	}
	if err := r.Restore([]byte{snapshotVersion, 5}); err == nil {
		t.Fatal("expected error on truncated snapshot")
	}
}

package wal_test

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kunwar-dev/quorumkv/internal/memnet"
	"github.com/kunwar-dev/quorumkv/raft"
	"github.com/kunwar-dev/quorumkv/wal"
)

// TestClusterRecoversFromDisk runs a 3-node Raft cluster on real WAL
// storage, compacts, stops every node, reopens the data directories and
// checks that committed state survives and the cluster keeps working.
func TestClusterRecoversFromDisk(t *testing.T) {
	ids := []string{"a", "b", "c"}
	root := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	type member struct {
		node    *raft.Node
		store   *wal.Storage
		stop    chan struct{}
		mu      sync.Mutex
		applied []string // index-ordered commands, "" for no-ops
	}

	boot := func(net *memnet.Network) []*member {
		ms := make([]*member, len(ids))
		for i, id := range ids {
			store, err := wal.Open(filepath.Join(root, id), wal.Options{Logger: logger})
			if err != nil {
				t.Fatal(err)
			}
			applyCh := make(chan raft.ApplyMsg)
			node, err := raft.NewNode(raft.Config{
				ID: id, Peers: ids, Storage: store, Transport: net.Transport(id),
				ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond, Logger: logger,
			}, applyCh)
			if err != nil {
				t.Fatal(err)
			}
			m := &member{node: node, store: store, stop: make(chan struct{})}
			go func() {
				for {
					select {
					case <-m.stop:
						return
					case msg := <-applyCh:
						m.mu.Lock()
						if msg.Snapshot != nil {
							m.applied = decode(msg.Snapshot.Data)
						} else {
							m.applied = append(m.applied, string(msg.Data))
							if msg.Index%25 == 0 {
								snap := encode(m.applied)
								m.mu.Unlock()
								if err := node.Snapshot(msg.Index, snap); err != nil {
									t.Error(err)
								}
								continue
							}
						}
						m.mu.Unlock()
					}
				}
			}()
			net.Register(id, node)
			node.Start()
			ms[i] = m
		}
		return ms
	}
	shutdown := func(ms []*member) {
		for _, m := range ms {
			m.node.Stop()
			close(m.stop)
			m.store.Close()
		}
	}
	propose := func(ms []*member, cmd string) {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for _, m := range ms {
				if _, _, err := m.node.Propose([]byte(cmd)); err == nil {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("no leader accepted %q", cmd)
	}
	waitFor := func(ms []*member, cmd string) []string {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			done := 0
			var state []string
			for _, m := range ms {
				m.mu.Lock()
				if n := len(m.applied); n > 0 && m.applied[n-1] == cmd {
					done++
					state = append([]string(nil), m.applied...)
				}
				m.mu.Unlock()
			}
			if done == len(ms) {
				return state
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%q not applied on all nodes", cmd)
		return nil
	}

	ms := boot(memnet.New())
	for i := range 100 {
		propose(ms, fmt.Sprintf("v%d", i))
	}
	before := commands(waitFor(ms, "v99"))
	shutdown(ms)

	ms = boot(memnet.New())
	defer shutdown(ms)
	propose(ms, "after-restart")
	after := commands(waitFor(ms, "after-restart"))

	if len(after) != len(before)+1 {
		t.Fatalf("expected %d commands after restart, got %d", len(before)+1, len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("command %d changed across restart: %q -> %q", i, before[i], after[i])
		}
	}
	for _, m := range ms {
		if m.node.Status().SnapshotIndex == 0 {
			t.Fatalf("node %s did not recover a snapshot", m.node.ID())
		}
	}
}

// commands drops no-op markers so states can be compared across leader changes.
func commands(applied []string) []string {
	var out []string
	for _, c := range applied {
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

func encode(cmds []string) []byte {
	var out []byte
	for _, c := range cmds {
		out = append(out, byte(len(c)))
		out = append(out, c...)
	}
	return out
}

func decode(b []byte) []string {
	var out []string
	for len(b) > 0 {
		n := int(b[0])
		out = append(out, string(b[1:1+n]))
		b = b[1+n:]
	}
	return out
}

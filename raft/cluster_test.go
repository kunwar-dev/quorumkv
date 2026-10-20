package raft_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kunwar-dev/quorumkv/internal/memnet"
	"github.com/kunwar-dev/quorumkv/raft"
)

const (
	electionTimeout = 150 * time.Millisecond
	heartbeat       = 30 * time.Millisecond
)

// cluster is a test harness that runs n Raft nodes on an in-memory network,
// records what each node applies and continuously checks that no two nodes
// ever apply different commands at the same index (State Machine Safety).
type cluster struct {
	t             *testing.T
	net           *memnet.Network
	ids           []string
	snapshotEvery uint64

	mu        sync.Mutex
	nodes     []*raft.Node
	storages  []*raft.MemoryStorage
	stopApply []chan struct{}
	applied   []map[uint64][]byte // per node: index -> command
	lastIndex []uint64
	connected []bool
	failure   string
}

func newCluster(t *testing.T, n int, reliable bool, snapshotEvery uint64) *cluster {
	t.Helper()
	c := &cluster{
		t:             t,
		net:           memnet.New(),
		snapshotEvery: snapshotEvery,
		nodes:         make([]*raft.Node, n),
		storages:      make([]*raft.MemoryStorage, n),
		stopApply:     make([]chan struct{}, n),
		applied:       make([]map[uint64][]byte, n),
		lastIndex:     make([]uint64, n),
		connected:     make([]bool, n),
	}
	c.net.SetReliable(reliable)
	for i := range n {
		c.ids = append(c.ids, "n"+strconv.Itoa(i))
	}
	for i := range n {
		c.storages[i] = raft.NewMemoryStorage()
		c.start(i)
	}
	t.Cleanup(c.shutdown)
	return c
}

// start boots node i from whatever is in its storage.
func (c *cluster) start(i int) {
	c.t.Helper()
	applyCh := make(chan raft.ApplyMsg)
	node, err := raft.NewNode(raft.Config{
		ID:                c.ids[i],
		Peers:             c.ids,
		Storage:           c.storages[i],
		Transport:         c.net.Transport(c.ids[i]),
		ElectionTimeout:   electionTimeout,
		HeartbeatInterval: heartbeat,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, applyCh)
	if err != nil {
		c.t.Fatalf("start %d: %v", i, err)
	}
	stop := make(chan struct{})
	c.mu.Lock()
	c.nodes[i] = node
	c.stopApply[i] = stop
	c.applied[i] = make(map[uint64][]byte)
	c.lastIndex[i] = 0
	c.connected[i] = true
	c.mu.Unlock()

	c.net.Register(c.ids[i], node)
	go c.consume(i, node, applyCh, stop)
	node.Start()
}

func (c *cluster) consume(i int, node *raft.Node, applyCh <-chan raft.ApplyMsg, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case msg := <-applyCh:
			if snapIdx, ok := c.apply(i, msg); ok {
				if err := node.Snapshot(snapIdx, c.encodeSnapshot(i, snapIdx)); err != nil {
					c.fail("node %d: snapshot at %d: %v", i, snapIdx, err)
				}
			}
		}
	}
}

// apply records msg for node i and checks ordering and cross-node agreement.
// It reports whether the node should now take a snapshot.
func (c *cluster) apply(i int, msg raft.ApplyMsg) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if msg.Snapshot != nil {
		var state map[uint64][]byte
		if err := gob.NewDecoder(bytes.NewReader(msg.Snapshot.Data)).Decode(&state); err != nil {
			c.failLocked("node %d: decode snapshot: %v", i, err)
			return 0, false
		}
		c.applied[i] = state
		c.lastIndex[i] = msg.Index
		return 0, false
	}
	if msg.Index != c.lastIndex[i]+1 {
		c.failLocked("node %d applied index %d out of order (last %d)", i, msg.Index, c.lastIndex[i])
		return 0, false
	}
	for j, log := range c.applied {
		if prev, ok := log[msg.Index]; ok && !bytes.Equal(prev, msg.Data) {
			c.failLocked("index %d: node %d applied %q but node %d applied %q", msg.Index, i, msg.Data, j, prev)
		}
	}
	c.applied[i][msg.Index] = msg.Data
	c.lastIndex[i] = msg.Index
	return msg.Index, c.snapshotEvery > 0 && msg.Index%c.snapshotEvery == 0
}

func (c *cluster) encodeSnapshot(i int, upTo uint64) []byte {
	c.mu.Lock()
	state := make(map[uint64][]byte, upTo)
	for idx, v := range c.applied[i] {
		if idx <= upTo {
			state[idx] = v
		}
	}
	c.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		c.fail("encode snapshot: %v", err)
	}
	return buf.Bytes()
}

func (c *cluster) fail(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failLocked(format, args...)
}

func (c *cluster) failLocked(format string, args ...any) {
	if c.failure == "" {
		c.failure = fmt.Sprintf(format, args...)
	}
}

func (c *cluster) checkFailure() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != "" {
		c.t.Fatal(c.failure)
	}
}

func (c *cluster) disconnect(i int) {
	c.net.Disconnect(c.ids[i])
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
}

func (c *cluster) connect(i int) {
	c.net.Connect(c.ids[i])
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
}

// crash stops node i; its storage survives, as a disk would.
func (c *cluster) crash(i int) {
	c.disconnect(i)
	c.mu.Lock()
	node, stop := c.nodes[i], c.stopApply[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if node == nil {
		return
	}
	node.Stop()
	close(stop)
	c.mu.Lock()
	c.storages[i] = c.storages[i].Clone()
	c.mu.Unlock()
}

func (c *cluster) restart(i int) {
	c.crash(i)
	c.start(i)
}

func (c *cluster) shutdown() {
	for i := range c.nodes {
		c.crash(i)
	}
}

func (c *cluster) node(i int) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected[i] && c.nodes[i] != nil
}

// checkOneLeader waits for exactly one leader among connected nodes and returns it.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for range 10 {
		time.Sleep(electionTimeout * 3)
		leaders := map[uint64][]int{}
		for i := range c.ids {
			if !c.isConnected(i) {
				continue
			}
			if term, ok := c.node(i).State(); ok {
				leaders[term] = append(leaders[term], i)
			}
		}
		var lastTerm uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			lastTerm = max(lastTerm, term)
		}
		if len(leaders) > 0 {
			return leaders[lastTerm][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := range c.ids {
		if c.isConnected(i) {
			if _, ok := c.node(i).State(); ok {
				c.t.Fatalf("node %d claims leadership but should be in a minority", i)
			}
		}
	}
}

// checkTerms asserts all connected nodes agree on the term and returns it.
func (c *cluster) checkTerms() uint64 {
	c.t.Helper()
	var term uint64
	for i := range c.ids {
		if c.isConnected(i) {
			t, _ := c.node(i).State()
			if term == 0 {
				term = t
			} else if t != term {
				c.t.Fatalf("nodes disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted returns how many nodes have applied index, and the command there.
func (c *cluster) nCommitted(index uint64) (int, []byte) {
	c.checkFailure()
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd []byte
	for i := range c.applied {
		if v, ok := c.applied[i][index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one proposes cmd until it is committed on at least `expected` nodes and
// returns its index. With retry, a failed attempt is resubmitted, as a
// client would after a leader change.
func (c *cluster) one(cmd []byte, expected int, retry bool) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	start := 0
	for time.Now().Before(deadline) {
		var index uint64
		for range c.ids {
			start = (start + 1) % len(c.ids)
			if !c.isConnected(start) {
				continue
			}
			if idx, _, err := c.node(start).Propose(cmd); err == nil {
				index = idx
				break
			}
		}
		if index != 0 {
			waitUntil := time.Now().Add(2 * time.Second)
			for time.Now().Before(waitUntil) {
				if n, got := c.nCommitted(index); n >= expected && bytes.Equal(got, cmd) {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%q) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%q) failed to reach agreement before deadline", cmd)
	return 0
}

// wait blocks until index is applied on at least n nodes.
func (c *cluster) wait(index uint64, n int) []byte {
	c.t.Helper()
	sleep := 10 * time.Millisecond
	for range 30 {
		if got, cmd := c.nCommitted(index); got >= n {
			return cmd
		}
		time.Sleep(sleep)
		sleep = min(sleep*2, time.Second)
	}
	c.t.Fatalf("index %d not committed on %d nodes", index, n)
	return nil
}

func cmd(s string) []byte { return []byte(s) }

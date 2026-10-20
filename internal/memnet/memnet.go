// Package memnet is an in-process network for testing Raft clusters under
// deterministic fault injection: node partitions, message loss, reordering
// and long delays, without real sockets.
package memnet

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kunwar-dev/quorumkv/raft"
)

// ErrUnreachable is returned when the destination is disconnected or a message is dropped.
var ErrUnreachable = errors.New("memnet: destination unreachable")

// Network routes RPCs between registered handlers.
type Network struct {
	mu        sync.Mutex
	handlers  map[string]raft.Handler
	connected map[string]bool
	reliable  bool
	rng       *rand.Rand
	rpcs      atomic.Int64
}

// New returns a reliable network with no registered nodes.
func New() *Network {
	return &Network{
		handlers:  make(map[string]raft.Handler),
		connected: make(map[string]bool),
		reliable:  true,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Register attaches a node's RPC handler and connects it.
func (n *Network) Register(id string, h raft.Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.connected[id] = true
}

// Connect restores a node's connectivity to the rest of the network.
func (n *Network) Connect(id string) { n.setConnected(id, true) }

// Disconnect isolates a node: it can neither send nor receive.
func (n *Network) Disconnect(id string) { n.setConnected(id, false) }

func (n *Network) setConnected(id string, v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = v
}

// SetReliable toggles message loss (10%) and random delay (up to 25ms).
func (n *Network) SetReliable(v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = v
}

// RPCCount returns the total number of RPCs attempted.
func (n *Network) RPCCount() int64 { return n.rpcs.Load() }

// Transport returns a raft.Transport that sends as node `from`.
func (n *Network) Transport(from string) raft.Transport { return &transport{net: n, from: from} }

func (n *Network) random() (delay time.Duration, drop bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.reliable {
		return 0, false
	}
	return time.Duration(n.rng.Int63n(int64(25 * time.Millisecond))), n.rng.Intn(10) == 0
}

func (n *Network) route(from, to string) (raft.Handler, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h := n.handlers[to]
	return h, h != nil && n.connected[from] && n.connected[to]
}

func call[Resp any](ctx context.Context, n *Network, from, to string, invoke func(raft.Handler) *Resp) (*Resp, error) {
	n.rpcs.Add(1)
	delay, drop := n.random()
	if !sleep(ctx, delay) {
		return nil, ctx.Err()
	}
	h, ok := n.route(from, to)
	if !ok || drop {
		// Model a timeout rather than an instant failure.
		sleep(ctx, 20*time.Millisecond)
		return nil, ErrUnreachable
	}

	done := make(chan *Resp, 1)
	go func() { done <- invoke(h) }()
	select {
	case resp := <-done:
		// The reply can be lost too, after the receiver has acted on the request.
		if _, ok := n.route(to, from); !ok {
			return nil, ErrUnreachable
		}
		if _, drop := n.random(); drop {
			return nil, ErrUnreachable
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type transport struct {
	net  *Network
	from string
}

func (t *transport) RequestVote(ctx context.Context, peer string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return call(ctx, t.net, t.from, peer, func(h raft.Handler) *raft.RequestVoteResponse { return h.HandleRequestVote(req) })
}

func (t *transport) AppendEntries(ctx context.Context, peer string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return call(ctx, t.net, t.from, peer, func(h raft.Handler) *raft.AppendEntriesResponse { return h.HandleAppendEntries(req) })
}

func (t *transport) InstallSnapshot(ctx context.Context, peer string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	return call(ctx, t.net, t.from, peer, func(h raft.Handler) *raft.InstallSnapshotResponse { return h.HandleInstallSnapshot(req) })
}

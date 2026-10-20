package kv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kunwar-dev/quorumkv/raft"
)

var (
	// ErrLeadershipLost means the request's outcome is unknown because this
	// node stopped being leader before the request committed. Retrying with
	// the same client ID and sequence number is safe.
	ErrLeadershipLost = errors.New("kv: leadership lost; outcome unknown, retry")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("kv: store closed")
)

// NotLeaderError is returned when a request reaches a follower.
type NotLeaderError struct {
	// Leader is the ID of the last known leader, or "" if unknown.
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "kv: not the leader; leader unknown"
	}
	return fmt.Sprintf("kv: not the leader; try %s", e.Leader)
}

// Options configures a Store.
type Options struct {
	// SnapshotThreshold triggers a snapshot after this many applied entries.
	// Zero disables snapshotting.
	SnapshotThreshold uint64
	Logger            *slog.Logger
}

type outcome struct {
	result Result
	err    error
}

type waiter struct {
	term uint64
	ch   chan outcome
}

// Store is a replicated key-value store backed by a Raft node.
type Store struct {
	node   *raft.Node
	opts   Options
	logger *slog.Logger

	mu           sync.Mutex
	fsm          *StateMachine
	waiters      map[uint64]waiter // log index -> pending request
	appliedIndex uint64
	lastSnapshot uint64

	done chan struct{}
	wg   sync.WaitGroup
}

// Open creates the Raft node described by rc, restores state from its
// storage and starts serving.
func Open(rc raft.Config, opts Options) (*Store, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if rc.Logger == nil {
		rc.Logger = opts.Logger
	}
	applyCh := make(chan raft.ApplyMsg, 256)
	node, err := raft.NewNode(rc, applyCh)
	if err != nil {
		return nil, err
	}
	s := &Store{
		node:    node,
		opts:    opts,
		logger:  opts.Logger.With("node", rc.ID),
		fsm:     NewStateMachine(),
		waiters: make(map[uint64]waiter),
		done:    make(chan struct{}),
	}
	s.wg.Add(1)
	go s.run(applyCh)
	node.Start()
	return s, nil
}

// Node exposes the underlying Raft node, e.g. to register it with a transport.
func (s *Store) Node() *raft.Node { return s.node }

// Close stops the node and the apply loop.
func (s *Store) Close() {
	s.node.Stop()
	select {
	case <-s.done:
		return
	default:
	}
	close(s.done)
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	for idx, w := range s.waiters {
		w.ch <- outcome{err: ErrClosed}
		delete(s.waiters, idx)
	}
}

// Stats reports apply-side counters.
type Stats struct {
	AppliedIndex uint64
	Keys         int
}

// Stats returns current apply-side counters.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{AppliedIndex: s.appliedIndex, Keys: s.fsm.Len()}
}

// Execute replicates cmd and returns its result once it has been applied.
func (s *Store) Execute(ctx context.Context, cmd Command) (Result, error) {
	s.mu.Lock()
	// Propose and register under s.mu so the apply loop cannot deliver the
	// result before the waiter exists.
	index, term, err := s.node.Propose(cmd.Encode())
	if err != nil {
		s.mu.Unlock()
		if errors.Is(err, raft.ErrNotLeader) {
			return Result{}, &NotLeaderError{Leader: s.node.Leader()}
		}
		return Result{}, err
	}
	ch := make(chan outcome, 1)
	s.waiters[index] = waiter{term: term, ch: ch}
	s.mu.Unlock()

	check := time.NewTicker(50 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case o := <-ch:
			return o.result, o.err
		case <-ctx.Done():
			s.dropWaiter(index, ch)
			return Result{}, ctx.Err()
		case <-check.C:
			if t, leader := s.node.State(); t != term || !leader {
				s.dropWaiter(index, ch)
				select {
				case o := <-ch: // applied just before we noticed
					return o.result, o.err
				default:
					return Result{}, ErrLeadershipLost
				}
			}
		}
	}
}

func (s *Store) dropWaiter(index uint64, ch chan outcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.waiters[index]; ok && w.ch == ch {
		delete(s.waiters, index)
	}
}

func (s *Store) run(applyCh <-chan raft.ApplyMsg) {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case msg := <-applyCh:
			s.apply(msg)
		}
	}
}

func (s *Store) apply(msg raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if msg.Snapshot != nil {
		if err := s.fsm.Restore(msg.Snapshot.Data); err != nil {
			// Diverging from the replicated state would be worse than stopping.
			panic(fmt.Sprintf("kv: cannot restore snapshot at %d: %v", msg.Index, err))
		}
		s.appliedIndex, s.lastSnapshot = msg.Index, msg.Index
		for idx, w := range s.waiters {
			if idx <= msg.Index {
				w.ch <- outcome{err: ErrLeadershipLost}
				delete(s.waiters, idx)
			}
		}
		return
	}

	s.appliedIndex = msg.Index
	var res Result
	if msg.Data != nil {
		cmd, err := DecodeCommand(msg.Data)
		if err != nil {
			panic(fmt.Sprintf("kv: undecodable entry at %d: %v", msg.Index, err))
		}
		res = s.fsm.Apply(cmd)
	}
	if w, ok := s.waiters[msg.Index]; ok {
		delete(s.waiters, msg.Index)
		if w.term == msg.Term {
			w.ch <- outcome{result: res}
		} else {
			// A different leader's entry landed at our index.
			w.ch <- outcome{err: ErrLeadershipLost}
		}
	}

	if t := s.opts.SnapshotThreshold; t > 0 && msg.Index-s.lastSnapshot >= t {
		if err := s.node.Snapshot(msg.Index, s.fsm.Snapshot()); err != nil {
			s.logger.Warn("snapshot failed", "index", msg.Index, "err", err)
			return
		}
		s.lastSnapshot = msg.Index
	}
}

package raft

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"slices"
	"sync"
	"time"
)

// Role is a node's current role in the cluster.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return "leader"
	}
}

var (
	// ErrNotLeader is returned by Propose on a node that is not the leader.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrStopped is returned after Stop has been called.
	ErrStopped = errors.New("raft: node stopped")
	// ErrSnapshotOutOfRange is returned when a snapshot index is not yet committed.
	ErrSnapshotOutOfRange = errors.New("raft: snapshot index beyond commit index")
)

// Config configures a Node.
type Config struct {
	// ID uniquely identifies this node. It must appear in Peers.
	ID string
	// Peers lists every cluster member, including this node.
	Peers []string
	// Storage persists hard state, log entries and snapshots.
	Storage Storage
	// Transport delivers RPCs to peers.
	Transport Transport
	// ElectionTimeout is the base election timeout; each node waits a random
	// duration in [ElectionTimeout, 2*ElectionTimeout) before campaigning.
	ElectionTimeout time.Duration
	// HeartbeatInterval is how often an idle leader sends AppendEntries.
	// It must be well below ElectionTimeout.
	HeartbeatInterval time.Duration
	// MaxEntriesPerAppend caps the batch size of a single AppendEntries RPC.
	MaxEntriesPerAppend int
	// Logger receives structured logs. Defaults to slog.Default().
	Logger *slog.Logger
}

func (c *Config) validate() error {
	if c.ID == "" {
		return errors.New("raft: config: ID is required")
	}
	if !slices.Contains(c.Peers, c.ID) {
		return fmt.Errorf("raft: config: Peers must include ID %q", c.ID)
	}
	if c.Storage == nil || c.Transport == nil {
		return errors.New("raft: config: Storage and Transport are required")
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = c.ElectionTimeout / 6
	}
	if c.HeartbeatInterval >= c.ElectionTimeout {
		return errors.New("raft: config: HeartbeatInterval must be less than ElectionTimeout")
	}
	if c.MaxEntriesPerAppend == 0 {
		c.MaxEntriesPerAppend = 1024
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// ApplyMsg delivers a committed entry, or a snapshot, to the application.
type ApplyMsg struct {
	Index uint64
	Term  uint64
	// Data is the proposed command; nil for internal no-op entries.
	Data []byte
	// Snapshot, when non-nil, means the application must replace its entire
	// state with Snapshot.Data. Index and Term then refer to the snapshot point.
	Snapshot *Snapshot
}

// Status is a point-in-time view of a node, for observability.
type Status struct {
	ID            string
	Role          Role
	Term          uint64
	Leader        string
	CommitIndex   uint64
	LastApplied   uint64
	LastLogIndex  uint64
	SnapshotIndex uint64
}

// Node is a single member of a Raft cluster.
type Node struct {
	cfg       Config
	id        string
	peers     []string // excludes self
	storage   Storage
	transport Transport
	logger    *slog.Logger
	applyCh   chan<- ApplyMsg

	mu        sync.Mutex
	applyCond *sync.Cond
	rng       *rand.Rand

	// Persistent state (mirrored in Storage).
	role     Role
	term     uint64
	votedFor string
	log      raftLog
	snapshot []byte

	// Volatile state.
	leaderID string
	// persistedIndex is the last log index known to be durable locally. The
	// leader appends to its in-memory log and persists asynchronously, in
	// batches, in parallel with replication (Raft paper §10.2.1).
	persistedIndex   uint64
	persistInflight  bool
	persistCond      *sync.Cond
	persistNotify    chan struct{}
	commitIndex      uint64
	lastApplied      uint64
	pendingSnapshot  bool
	electionDeadline time.Time

	// Leader state, reinitialised on every election win.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	replNotify map[string]chan struct{}
	leaderStop chan struct{}

	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup
}

// NewNode restores a node from cfg.Storage. Committed entries are delivered
// on applyCh, in log order, after Start is called. If a snapshot was
// persisted it is delivered first so the application can restore its state.
func NewNode(cfg Config, applyCh chan<- ApplyMsg) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	hs, snap, entries, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}

	h := fnv.New64a()
	h.Write([]byte(cfg.ID))
	n := &Node{
		cfg:           cfg,
		id:            cfg.ID,
		storage:       cfg.Storage,
		transport:     cfg.Transport,
		logger:        cfg.Logger.With("node", cfg.ID),
		applyCh:       applyCh,
		rng:           rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(h.Sum64()))),
		term:          hs.Term,
		votedFor:      hs.VotedFor,
		stopCh:        make(chan struct{}),
		persistNotify: make(chan struct{}, 1),
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.persistCond = sync.NewCond(&n.mu)
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.peers = append(n.peers, p)
		}
	}
	if snap != nil {
		n.log.snapIndex, n.log.snapTerm = snap.Meta.Index, snap.Meta.Term
		n.snapshot = snap.Data
		n.commitIndex = snap.Meta.Index
		n.lastApplied = snap.Meta.Index
		n.pendingSnapshot = true
	}
	n.log.entries = entries
	n.persistedIndex = n.log.lastIndex()
	n.resetElectionTimerLocked()
	return n, nil
}

// Start launches the node's background goroutines.
func (n *Node) Start() {
	n.wg.Add(3)
	go n.runTicker()
	go n.runApplier()
	go n.runPersister()
}

// Stop halts the node and waits for its goroutines to exit. It is idempotent.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	if n.leaderStop != nil {
		close(n.leaderStop)
		n.leaderStop = nil
	}
	n.applyCond.Broadcast()
	n.persistCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// ID returns the node's ID.
func (n *Node) ID() string { return n.id }

// State returns the current term and whether this node believes it is leader.
func (n *Node) State() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term, n.role == Leader
}

// Leader returns the ID of the current known leader, or "" if unknown.
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.id, Role: n.role, Term: n.term, Leader: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastLogIndex: n.log.lastIndex(), SnapshotIndex: n.log.snapIndex,
	}
}

// Propose appends data to the replicated log. It returns as soon as the
// entry is durable locally; the entry is committed once it appears on the
// apply channel at the returned index with the returned term.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, 0, ErrStopped
	}
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: EntryNormal, Data: data}
	n.appendLeaderLocked(e)
	return e.Index, e.Term, nil
}

// Snapshot informs Raft that the application has captured its state up to
// and including index, allowing the log prefix to be discarded.
func (n *Node) Snapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.flushLocked()
	if index <= n.log.snapIndex {
		return nil
	}
	if index > n.commitIndex {
		return ErrSnapshotOutOfRange
	}
	t, _ := n.log.term(index)
	n.log.compact(index, t)
	n.snapshot = data
	n.must(n.storage.SaveSnapshot(Snapshot{Meta: SnapshotMeta{Index: index, Term: t}, Data: data}))
	n.persistedIndex = max(n.persistedIndex, index)
	n.logger.Debug("log compacted", "index", index, "term", t)
	return nil
}

// ---------------------------------------------------------------------------
// Elections

func (n *Node) runTicker() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval / 2)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		if n.role != Leader && time.Now().After(n.electionDeadline) {
			n.startElectionLocked()
		}
		n.mu.Unlock()
	}
}

func (n *Node) resetElectionTimerLocked() {
	base := n.cfg.ElectionTimeout
	n.electionDeadline = time.Now().Add(base + time.Duration(n.rng.Int63n(int64(base))))
}

func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.persistHardStateLocked()
	n.resetElectionTimerLocked()
	n.logger.Debug("starting election", "term", n.term)

	if len(n.peers) == 0 {
		n.becomeLeaderLocked()
		return
	}

	term := n.term
	req := &RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
	votes := 1
	for _, peer := range n.peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
			defer cancel()
			resp, err := n.transport.RequestVote(ctx, peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.term {
				n.becomeFollowerLocked(resp.Term, "")
				return
			}
			if n.role != Candidate || n.term != term || !resp.VoteGranted {
				return
			}
			votes++
			if n.isQuorum(votes) {
				n.becomeLeaderLocked()
			}
		}(peer)
	}
}

func (n *Node) isQuorum(count int) bool { return count*2 > len(n.peers)+1 }

func (n *Node) becomeFollowerLocked(term uint64, leader string) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.persistHardStateLocked()
	}
	if n.role == Leader {
		close(n.leaderStop)
		n.leaderStop = nil
		n.resetElectionTimerLocked()
		n.logger.Info("stepped down", "term", n.term)
	}
	n.role = Follower
	n.leaderID = leader
}

func (n *Node) becomeLeaderLocked() {
	if n.stopped {
		return
	}
	n.role = Leader
	n.leaderID = n.id
	n.leaderStop = make(chan struct{})
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	n.replNotify = make(map[string]chan struct{}, len(n.peers))

	next := n.log.lastIndex() + 1
	// A no-op in the new term lets entries from earlier terms commit (§5.4.2)
	// and gives the leader an up-to-date commit index quickly.
	noop := Entry{Index: next, Term: n.term, Type: EntryNoop}
	n.log.append(noop)

	for _, peer := range n.peers {
		n.nextIndex[peer] = next
		n.matchIndex[peer] = 0
		notify := make(chan struct{}, 1)
		n.replNotify[peer] = notify
		n.wg.Add(1)
		go n.runReplicator(peer, n.term, notify, n.leaderStop)
	}
	n.logger.Info("became leader", "term", n.term, "lastIndex", n.log.lastIndex())
	n.notifyPersisterLocked()
	n.notifyReplicatorsLocked()
}

// HandleRequestVote implements Handler.
func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term > n.term {
		n.becomeFollowerLocked(req.Term, "")
	}
	resp := &RequestVoteResponse{Term: n.term}
	if req.Term < n.term {
		return resp
	}
	upToDate := req.LastLogTerm > n.log.lastTerm() ||
		(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		n.persistHardStateLocked()
		n.resetElectionTimerLocked()
		resp.VoteGranted = true
	}
	return resp
}

// ---------------------------------------------------------------------------
// Replication (leader side)

func (n *Node) notifyReplicatorsLocked() {
	for _, ch := range n.replNotify {
		select {
		case ch <- struct{}{}:
		default: // a wake-up is already pending
		}
	}
}

// runReplicator keeps one follower up to date for the duration of a term.
// There is at most one RPC in flight per follower; entries proposed while it
// is outstanding are batched into the next AppendEntries.
func (n *Node) runReplicator(peer string, term uint64, notify <-chan struct{}, stop <-chan struct{}) {
	defer n.wg.Done()
	heartbeat := time.NewTicker(n.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	for {
		more := n.replicateOnce(peer, term)
		if more {
			select {
			case <-stop:
				return
			default:
				continue
			}
		}
		select {
		case <-stop:
			return
		case <-notify:
		case <-heartbeat.C:
		}
	}
}

// replicateOnce sends a single AppendEntries or InstallSnapshot RPC and
// reports whether the follower still has entries to catch up on.
func (n *Node) replicateOnce(peer string, term uint64) bool {
	n.mu.Lock()
	if n.role != Leader || n.term != term {
		n.mu.Unlock()
		return false
	}
	next := n.nextIndex[peer]
	if next <= n.log.snapIndex {
		req := &InstallSnapshotRequest{
			Term:              term,
			LeaderID:          n.id,
			LastIncludedIndex: n.log.snapIndex,
			LastIncludedTerm:  n.log.snapTerm,
			Data:              n.snapshot,
		}
		n.mu.Unlock()
		return n.sendSnapshot(peer, term, req)
	}

	prev := next - 1
	prevTerm, _ := n.log.term(prev)
	hi := min(n.log.lastIndex()+1, next+uint64(n.cfg.MaxEntriesPerAppend))
	req := &AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.slice(next, hi),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
	resp, err := n.transport.AppendEntries(ctx, peer, req)
	cancel()
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollowerLocked(resp.Term, "")
		return false
	}
	if n.role != Leader || n.term != term {
		return false
	}
	if resp.Success {
		match := prev + uint64(len(req.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
			n.maybeCommitLocked()
		}
		n.nextIndex[peer] = max(n.nextIndex[peer], match+1)
	} else {
		n.nextIndex[peer] = n.backtrackLocked(next, resp)
	}
	return n.nextIndex[peer] <= n.log.lastIndex()
}

// backtrackLocked computes the follower's next index after a rejection,
// skipping an entire conflicting term per round trip.
func (n *Node) backtrackLocked(next uint64, resp *AppendEntriesResponse) uint64 {
	ni := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		if last, ok := n.log.lastIndexOfTerm(resp.ConflictTerm); ok {
			ni = last + 1
		}
	}
	if ni == 0 || ni >= next {
		ni = next - 1
	}
	return max(ni, 1)
}

func (n *Node) sendSnapshot(peer string, term uint64, req *InstallSnapshotRequest) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 4*n.cfg.ElectionTimeout)
	resp, err := n.transport.InstallSnapshot(ctx, peer, req)
	cancel()
	if err != nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollowerLocked(resp.Term, "")
		return false
	}
	if n.role != Leader || n.term != term {
		return false
	}
	if req.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = req.LastIncludedIndex
		n.maybeCommitLocked()
	}
	n.nextIndex[peer] = max(n.nextIndex[peer], req.LastIncludedIndex+1)
	return n.nextIndex[peer] <= n.log.lastIndex()
}

// maybeCommitLocked advances commitIndex to the highest index replicated on
// a majority, provided that entry is from the current term (§5.4.2).
func (n *Node) maybeCommitLocked() {
	if n.role != Leader {
		return
	}
	matches := make([]uint64, 0, len(n.peers)+1)
	matches = append(matches, n.persistedIndex) // the leader counts itself only once durable
	for _, p := range n.peers {
		matches = append(matches, n.matchIndex[p])
	}
	slices.Sort(matches)
	// With matches sorted ascending, the entry at this position is held by a majority.
	candidate := matches[(len(matches)-1)/2]
	if candidate <= n.commitIndex {
		return
	}
	if t, ok := n.log.term(candidate); ok && t == n.term {
		n.commitIndex = candidate
		n.applyCond.Broadcast()
		n.notifyReplicatorsLocked() // propagate the new commit index promptly
	}
}

// ---------------------------------------------------------------------------
// Replication (follower side)

// HandleAppendEntries implements Handler.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.flushLocked()
	if req.Term < n.term {
		return &AppendEntriesResponse{Term: n.term}
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leaderID = req.LeaderID
	n.resetElectionTimerLocked()
	resp := &AppendEntriesResponse{Term: n.term}

	prev, entries := req.PrevLogIndex, req.Entries
	if prev < n.log.snapIndex {
		// Everything up to snapIndex is committed and compacted here; skip it.
		skip := n.log.snapIndex - prev
		if skip >= uint64(len(entries)) {
			resp.Success = true
			return resp
		}
		prev, entries = n.log.snapIndex, entries[skip:]
	} else if prev > n.log.lastIndex() {
		resp.ConflictIndex = n.log.lastIndex() + 1
		return resp
	} else if t, _ := n.log.term(prev); t != req.PrevLogTerm {
		resp.ConflictTerm = t
		resp.ConflictIndex = n.log.firstIndexOfTerm(t, prev)
		return resp
	}

	for i, e := range entries {
		if e.Index > n.log.lastIndex() {
			n.appendLocalLocked(entries[i:]...)
			break
		}
		if t, _ := n.log.term(e.Index); t != e.Term {
			n.log.truncateFrom(e.Index)
			n.appendLocalLocked(entries[i:]...)
			break
		}
	}

	if req.LeaderCommit > n.commitIndex {
		lastNew := prev + uint64(len(entries))
		if c := min(req.LeaderCommit, lastNew); c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Broadcast()
		}
	}
	resp.Success = true
	return resp
}

// HandleInstallSnapshot implements Handler.
func (n *Node) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.flushLocked()
	if req.Term < n.term {
		return &InstallSnapshotResponse{Term: n.term}
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leaderID = req.LeaderID
	n.resetElectionTimerLocked()
	resp := &InstallSnapshotResponse{Term: n.term}

	if req.LastIncludedIndex <= n.commitIndex {
		return resp // we already have everything the snapshot covers
	}
	snap := Snapshot{Meta: SnapshotMeta{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm}, Data: req.Data}
	if t, ok := n.log.term(req.LastIncludedIndex); ok && t == req.LastIncludedTerm {
		// Our log extends past the snapshot consistently: keep the suffix.
		n.log.compact(req.LastIncludedIndex, req.LastIncludedTerm)
		n.must(n.storage.SaveSnapshot(snap))
	} else {
		n.log = raftLog{snapIndex: req.LastIncludedIndex, snapTerm: req.LastIncludedTerm}
		n.must(n.storage.SaveSnapshot(snap))
		n.must(n.storage.TruncateFrom(req.LastIncludedIndex + 1))
	}
	n.snapshot = req.Data
	n.persistedIndex = n.log.lastIndex()
	n.commitIndex = req.LastIncludedIndex
	n.pendingSnapshot = true
	n.applyCond.Broadcast()
	n.logger.Info("installed snapshot", "index", req.LastIncludedIndex, "term", req.LastIncludedTerm)
	return resp
}

// ---------------------------------------------------------------------------
// Applying committed entries

func (n *Node) runApplier() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		for !n.stopped && !n.pendingSnapshot && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if n.pendingSnapshot {
			msg := ApplyMsg{
				Index: n.log.snapIndex,
				Term:  n.log.snapTerm,
				Snapshot: &Snapshot{
					Meta: SnapshotMeta{Index: n.log.snapIndex, Term: n.log.snapTerm},
					Data: n.snapshot,
				},
			}
			n.pendingSnapshot = false
			n.lastApplied = max(n.lastApplied, n.log.snapIndex)
			n.mu.Unlock()
			if !n.deliver(msg) {
				return
			}
			continue
		}
		lo, hi := max(n.lastApplied, n.log.snapIndex)+1, n.commitIndex
		entries := n.log.slice(lo, hi+1)
		n.mu.Unlock()

		for _, e := range entries {
			msg := ApplyMsg{Index: e.Index, Term: e.Term}
			if e.Type == EntryNormal {
				msg.Data = e.Data
			}
			if !n.deliver(msg) {
				return
			}
		}
		n.mu.Lock()
		n.lastApplied = max(n.lastApplied, hi)
		n.mu.Unlock()
	}
}

func (n *Node) deliver(msg ApplyMsg) bool {
	select {
	case n.applyCh <- msg:
		return true
	case <-n.stopCh:
		return false
	}
}

// ---------------------------------------------------------------------------
// Persistence helpers

// appendLocalLocked appends and synchronously persists entries. Followers
// use it because they must not acknowledge entries until they are durable.
// Callers must have waited for any in-flight leader batch (waitPersistLocked)
// so storage writes stay ordered.
func (n *Node) appendLocalLocked(entries ...Entry) {
	n.log.append(entries...)
	n.must(n.storage.Append(entries))
	n.persistedIndex = n.log.lastIndex()
}

// appendLeaderLocked appends to the in-memory log and lets replication start
// immediately; the persister makes the entry durable in the background,
// batching concurrent proposals into one storage write (group commit).
func (n *Node) appendLeaderLocked(e Entry) {
	n.log.append(e)
	n.notifyPersisterLocked()
	n.notifyReplicatorsLocked()
}

func (n *Node) notifyPersisterLocked() {
	select {
	case n.persistNotify <- struct{}{}:
	default:
	}
}

// waitPersistLocked blocks until no leader batch is being written, so that
// truncations and snapshots are ordered after it in storage.
func (n *Node) waitPersistLocked() {
	for n.persistInflight && !n.stopped {
		n.persistCond.Wait()
	}
}

// flushLocked makes the whole in-memory log durable before the caller acts
// on it. A deposed leader may still hold entries the persister has not
// written; a follower must never acknowledge entries that are not on disk,
// and storage truncations must be ordered after earlier appends.
func (n *Node) flushLocked() {
	n.waitPersistLocked()
	if n.stopped || n.persistedIndex >= n.log.lastIndex() {
		return
	}
	lo := max(n.persistedIndex, n.log.snapIndex) + 1
	n.must(n.storage.Append(n.log.slice(lo, n.log.lastIndex()+1)))
	n.persistedIndex = n.log.lastIndex()
}

// runPersister writes the leader's not-yet-durable entries in batches.
func (n *Node) runPersister() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.persistNotify:
		}
		n.mu.Lock()
		for !n.stopped && n.persistedIndex < n.log.lastIndex() {
			lo := max(n.persistedIndex, n.log.snapIndex) + 1
			hi := n.log.lastIndex()
			batch := n.log.slice(lo, hi+1)
			n.persistInflight = true
			n.mu.Unlock()

			err := n.storage.Append(batch)

			n.mu.Lock()
			n.persistInflight = false
			n.persistCond.Broadcast()
			n.must(err)
			// While the write was in flight the log could only grow or be
			// compacted, never truncated, so [lo, hi] is now durable.
			if t, ok := n.log.term(hi); (!ok && hi <= n.log.snapIndex) || t == batch[len(batch)-1].Term {
				n.persistedIndex = max(n.persistedIndex, hi)
			}
			n.maybeCommitLocked()
		}
		n.mu.Unlock()
	}
}

func (n *Node) persistHardStateLocked() {
	n.must(n.storage.SetHardState(HardState{Term: n.term, VotedFor: n.votedFor}))
}

// must enforces fail-stop semantics: if durable storage fails, continuing
// could violate Raft's safety guarantees, so the node halts.
func (n *Node) must(err error) {
	if err != nil {
		n.logger.Error("storage failure; halting node", "err", err)
		panic(fmt.Sprintf("raft: storage failure on %s: %v", n.id, err))
	}
}

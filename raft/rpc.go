package raft

import "context"

// EntryType distinguishes application commands from internal entries.
type EntryType uint8

const (
	// EntryNormal carries an application command.
	EntryNormal EntryType = iota
	// EntryNoop is appended by a new leader to commit entries from prior terms.
	EntryNoop
)

// Entry is a single replicated log entry.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// RequestVoteRequest is sent by candidates to gather votes (§5.2).
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse is the reply to RequestVoteRequest.
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesRequest replicates log entries and doubles as a heartbeat (§5.3).
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesResponse is the reply to AppendEntriesRequest. On rejection,
// ConflictTerm and ConflictIndex let the leader skip a whole term per round
// trip instead of decrementing nextIndex one entry at a time.
type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotRequest ships a snapshot to a follower whose required log
// entries have already been compacted away on the leader (§7).
type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotResponse is the reply to InstallSnapshotRequest.
type InstallSnapshotResponse struct {
	Term uint64
}

// Handler is implemented by Node and invoked by a Transport when a peer's RPC arrives.
type Handler interface {
	HandleRequestVote(*RequestVoteRequest) *RequestVoteResponse
	HandleAppendEntries(*AppendEntriesRequest) *AppendEntriesResponse
	HandleInstallSnapshot(*InstallSnapshotRequest) *InstallSnapshotResponse
}

// Transport sends RPCs to peers. Implementations must honour ctx cancellation.
type Transport interface {
	RequestVote(ctx context.Context, peer string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, peer string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout).
//
// A Node replicates an ordered log of opaque commands across a cluster and
// delivers committed entries, in order, on an apply channel. The package is
// transport- and storage-agnostic: callers supply a Transport for RPCs and a
// Storage for durable state, which keeps the consensus logic testable under
// deterministic fault injection.
//
// Implemented:
//   - leader election with randomized timeouts
//   - log replication with per-peer pipelined replicators and batching
//   - accelerated log backtracking using conflict term/index hints
//   - leader no-op on election so prior-term entries commit safely (§5.4.2)
//   - log compaction via application snapshots and InstallSnapshot
//   - crash recovery from persisted hard state, log and snapshot
package raft

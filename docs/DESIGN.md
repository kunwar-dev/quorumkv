# quorumkv design

This document explains how quorumkv works and why it is built this way. It assumes some familiarity with the [Raft paper](https://raft.github.io/raft.pdf); section numbers (§) refer to it.

## 1. Request lifecycle

A `PUT /v1/kv/user/42` sent to the leader goes through these steps:

1. **`server`** parses the request into a `kv.Command{Op, Key, Value, ClientID, Seq}`.
2. **`kv.Store.Execute`** encodes the command and calls `raft.Node.Propose`. It registers a waiter for the returned log index *under the same lock*, so the result cannot be applied before anyone is waiting for it.
3. **`raft.Node`** appends the entry to its in-memory log. It then wakes the **persister**, which writes it to the WAL, and the per-follower **replicators**, which send it to followers. Both happen in parallel.
4. **Followers** append the entry, `fsync` it, and acknowledge.
5. The leader advances `commitIndex` to the highest index stored durably on a majority, provided that index is from the current term (§5.4.2). The leader counts itself only up to its own durable index.
6. Every node's **applier** delivers committed entries, in order, to `kv.Store`. The store applies each one to the state machine and wakes the matching waiter.
7. The waiter checks that the entry at its index has the term it proposed in. If not, a different leader's entry occupies that slot, and the client gets "outcome unknown, retry".

## 2. Raft implementation notes

**Concurrency model.** Each node has one mutex guarding all consensus state, and four kinds of goroutine:

| Goroutine | Job |
|---|---|
| Ticker | Starts an election when the randomized timeout expires. |
| Applier | Delivers committed entries without holding the lock. |
| Persister | Makes the leader's log durable in batches. |
| Replicators | One per follower, alive only while leader. |

No goroutine waits on the application channel while holding the lock. The only I/O done under the lock is the disk writes that must finish before a reply goes out, such as recording a vote or a follower persisting entries before it acknowledges them.

**Replication.** Each follower has a dedicated replicator with at most one RPC in flight. New proposals only signal a buffered channel, so a burst of proposals collapses into one `AppendEntries` of up to `MaxEntriesPerAppend` entries. This keeps network round trips, and follower `fsync`s, roughly constant per batch instead of per write.

**Fast backtracking.** On a log mismatch the follower returns `ConflictTerm` and the first index of that term. The leader jumps `nextIndex` past the whole conflicting term in one round trip instead of decrementing by one per RPC. `TestFastBackup` asserts the RPC count stays bounded after repairing 50-entry divergent logs.

**Leader no-op.** A new leader appends a no-op entry in its term. Entries from earlier terms are only committed indirectly (§5.4.2). Without the no-op, a leader with no new client traffic could never commit them.

**Parallel leader persistence (group commit).** Raft allows the leader to write to its disk in parallel with replicating to followers (§10.2.1). Proposals append only to the in-memory log. The persister then writes everything not yet durable as one `Append` with a single `fsync`. Two invariants keep this safe:

- **The leader counts itself in the commit quorum only up to `persistedIndex`.** Commitment therefore still means "durable on a majority".
- **Before acting as a follower, a node flushes any unwritten tail** (`flushLocked`). A deposed leader may still hold entries the persister has not written. Without the flush it could acknowledge entries that are not on disk, or a later truncation could reach storage before an earlier append. Storage operations wait for any in-flight batch so they stay ordered.

**Snapshots.** The application decides when to snapshot by calling `Node.Snapshot(index, data)`. Raft then discards the log prefix. A follower whose `nextIndex` falls inside the compacted prefix receives `InstallSnapshot`. If its log extends consistently past the snapshot, it keeps the suffix; otherwise it discards its log.

**Fail-stop on storage errors.** If a durable write fails, continuing could violate safety, for example by voting twice in a term after losing the vote record. The node panics instead of risking that.

## 3. Write-ahead log format

`wal.Storage` keeps two files per node:

```
snapshot   QKVSNAP1 | crc32c | uvarint index | uvarint term | state bytes
wal.log    record* where record = u32 length | u32 crc32c(payload) | payload
           payload = type(1) | body
             1 hard state   uvarint term, string votedFor
             2 entries      uvarint n, n × (index, term, type, bytes data)
             3 truncate     uvarint index
```

- **Durability.** Every mutation is written and `fsync`ed before it returns.
- **Recovery.** Records are replayed into an in-memory mirror. Replay stops at the first short or checksum-failing record, which is how a torn write at crash time shows up, and the file is truncated there.
- **Snapshots.**
  1. The snapshot is written to a temp file, `fsync`ed, renamed, and the directory is `fsync`ed.
  2. The log is then atomically rewritten to hold only the hard state and the entries after the snapshot.

  A crash between steps 1 and 2 is harmless, because replay ignores entries the snapshot already covers.
- **Corruption.** A corrupt snapshot is reported, not skipped. Because snapshots are installed by atomic rename, a bad snapshot file means real damage rather than a torn write.

## 4. Linearizability and exactly-once semantics

- **Linearizable reads.** A read executes when its log entry applies, which falls between the request's start and its response. Any read therefore sees every write that completed before it started. A node that has lost leadership cannot return stale data: it cannot commit new entries, and the waiter detects the term change.
- **Exactly-once retries.** Clients number their requests. The state machine stores `(lastSeq, lastResult)` per client and returns the cached result for a duplicate instead of executing it again. For example, a `DELETE` retried after a leader crash does not delete a key that another client has since recreated. Sessions are part of the snapshot, so deduplication survives log compaction.
- **How it is tested.** `kv/linearizability_test.go` records the start time, end time and result of every client operation while a nemesis isolates nodes and the network drops messages. Operations whose outcome is unknown are recorded as possibly taking effect at any later time. Porcupine then searches for a valid linearization, partitioned by key.

## 5. Failure behaviour

| Failure | Behaviour |
|---|---|
| Leader crashes | Followers time out after 300-600 ms (the default), elect a new leader, and clients follow the redirect. Writes that were not acknowledged may or may not have committed, and retries are deduplicated. |
| Minority partitioned | The majority side keeps serving. An isolated leader cannot commit and returns `503`/`ErrLeadershipLost`. |
| Majority lost | No writes or reads succeed (CP behaviour). They resume automatically when a quorum returns. |
| Node crashes mid-write | The WAL truncates the torn record on restart. The node rejoins and catches up through `AppendEntries` or `InstallSnapshot`. |
| Slow follower | It is not on the commit path. The leader sends it a snapshot if it falls behind the compacted log. |

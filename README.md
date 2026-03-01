# Distributed Systems (CS 6450)

Labs from CS 6450 — Distributed Systems at the University of Utah. Implemented in Go.

## Labs

### Lab 1 — MapReduce
A MapReduce framework with a coordinator and multiple workers communicating over RPC. Workers fetch tasks, execute map or reduce functions, and handle failures through coordinator-side timeout detection.

### Lab 2 — Key-Value Server
A linearizable single-server key-value store with at-most-once semantics. Uses client-side unique identifiers and server-side deduplication to make retries safe under network failures.

### Lab 3 — Raft Consensus
Full implementation of the Raft consensus protocol across three parts: leader election and heartbeats (3a), log replication with consistency guarantees under failures (3b), and persistence with crash recovery and log compaction via snapshotting (3c).

### Lab 4 — Fault-Tolerant Key-Value Service
A replicated key-value service built on top of Raft. Clients interact with a leader-backed cluster that maintains linearizable semantics even under server crashes and network partitions. Snapshotting is integrated to bound log growth.

## Structure

```
cs6450-labs/src/
├── mr/          # MapReduce coordinator and worker
├── kvsrv/       # Single-server KV store
├── raft/        # Raft consensus implementation
├── kvraft/      # Fault-tolerant KV service on Raft
├── shardkv/     # Sharded KV service (bonus)
└── shardctrler/ # Shard controller
```

## Language

Go 1.21+

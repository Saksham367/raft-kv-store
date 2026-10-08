# raft-kv-store

[![CI](https://github.com/Saksham367/raft-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/Saksham367/raft-kv-store/actions/workflows/ci.yml)

This is a fault-tolerant, **linearizable** key-value store written in Go. It replicates every write with my own implementation of the **Raft** consensus algorithm, built from the paper. It uses only the Go standard library.

A cluster of 2f+1 nodes keeps serving reads and writes while up to f nodes are crashed or partitioned away. No acknowledged write is ever lost, and every client retry is applied **exactly once**.

## Results

The benchmark ran a real cluster: each node had its own HTTP server, Raft RPCs went over HTTP on localhost, and every node **fsync'd its state to disk** before acknowledging. The whole run was on one 2-vCPU VM, with 64 concurrent clients and 128-byte values. Run `go run ./cmd/kvbench` to reproduce it.

| Cluster | Committed writes/sec | p50 | p99 | Leader failover (kill leader → next write commits) |
| --- | --- | --- | --- | --- |
| 3 nodes | **~2,000** | 27 ms | 102 ms | **mean 425 ms**, worst 612 ms (10 trials) |
| 5 nodes | ~1,400 | 41 ms | 130 ms | mean 409 ms, worst 514 ms (10 trials) |

Group commit (below) raised 3-node throughput **7–8×**, from 274 to about 2,000 writes/sec.

**Correctness testing.** There are 13 Raft tests and 3 KV tests, all run under Go's **race detector**. They inject partitions, crashes and restarts, dropped and delayed messages (10% drop rate), and long message reordering, including the Figure 8 scenario from the paper. The harness cross-checks every applied entry on every replica. The KV tests check linearizability: each client's reads must equal exactly its acknowledged writes, and the final state must contain every acknowledged append exactly once.

## Architecture

```
 clients ──HTTP──► kvnode ──────────────────────────────────────────────┐
                    │  kv.Server  (state machine + dedup table)         │
                    │     │ Start(op)               ▲ ApplyMsg (in order)│
                    │     ▼                         │                    │
                    │  raft.Raft ── election, replication, snapshots     │
                    │     │  Transport (HTTP/JSON or simulated network)  │
                    │     ▼                                              │
                    │  Persister (fsync + atomic rename)                 │
                    └────────────────────────────────────────────────────┘
                              ▲               ▲
                     AppendEntries / RequestVote / InstallSnapshot
                              ▼               ▼
                         other replicas (same stack)
```

### Raft (`raft/`)

- **Leader election** uses randomized timeouts and the election restriction (§5.4.1), so a leader always holds every committed entry.
- **Log replication** runs one replicator goroutine per follower. Fast log backtracking uses conflict term and index hints, so the leader skips a whole conflicting term per round trip.
- **Commitment** follows the Figure 8 rule: a leader only commits entries from its own term by counting replicas.
- **Persistence** saves term, vote and log before replying to any RPC. The file persister writes to a temp file, fsyncs it, then renames, so a crash mid-write leaves the previous state intact.
- **Group commit.** `Start()` appends to memory and wakes a flusher, which fsyncs the whole batch once, outside the lock and in parallel with replication. The leader counts itself toward a majority only for entries it has flushed (§10.2.1 of the dissertation). Persisted states are versioned, so an older batch can never overwrite newer state.
- **Snapshots and InstallSnapshot** let the service compact the log. A follower that falls behind the compaction point catches up from a snapshot.
- **Ordered apply.** A single applier goroutine delivers snapshots and commands strictly in order.

### Key-value service (`kv/`)

- `Get`, `Put`, `Append` and `Delete` all go through the Raft log, so **reads are linearizable** too. There are no stale reads from a deposed leader.
- **Exactly-once semantics.** Each clerk tags requests with `(ClientID, Seq)`, and the state machine keeps a dedup table that is included in snapshots. A write retried after a leader crash takes effect once.
- **Leader loss detection.** If a different entry commits at the index a request was waiting on, the leader lost office and the client is told to retry elsewhere.
- **Log compaction.** When the persisted Raft state passes a threshold, the server snapshots its map and dedup table and Raft discards the log prefix.

### Simulated network (`transport/memnet.go`)

The test network can partition any node, drop requests and replies, add random delays, reorder messages by up to 2 s, and swap in a restarted server. Each RPC gets copied arguments, as a real network would.

## Run it

```bash
go test -race ./...                       # full suite (~90 s)
go run ./cmd/kvbench -nodes 3 -clients 64 # throughput, latency, failover

./scripts/local-cluster.sh                # 3 nodes on :9001-9003
curl -L -X PUT --data hello localhost:9001/kv/greeting
curl -L localhost:9002/kv/greeting        # any node; followers redirect to the leader
curl -L -X POST --data ' world' localhost:9003/kv/greeting/append
curl localhost:9001/status                # role, term, commit index, log bounds

docker compose up -d                      # same cluster in containers
docker compose stop node0                 # kill a replica, keep writing
```

HTTP clients can send `X-Client-ID` and `X-Seq` headers so their retries become idempotent.

## Layout

```
raft/          consensus: raft.go, log.go (snapshot-aware log), persister.go, tests
kv/            replicated state machine, clerk (client), linearizability tests
transport/     HTTP/JSON transport and fault-injecting in-memory network
cmd/kvnode     server binary (client API + Raft RPC endpoints)
cmd/kvbench    multi-node benchmark: throughput, latency percentiles, failover time
```

## Ideas for further work

- Append-only write-ahead log instead of rewriting the full state on each flush
- Pipelined AppendEntries (several RPCs in flight per follower)
- Leader leases or ReadIndex for reads that skip the log
- Membership changes (joint consensus)
- Sharding across multiple Raft groups

#  Kronos

> A distributed key-value store built from scratch with a hand-crafted Raft consensus engine —  
> leader election, log replication, fault tolerance, and snapshots. No libraries. No shortcuts.

---

## What Is This?

CommitOrDie is a **production-grade distributed key-value store** implemented entirely from scratch in Java. It is the kind of system that sits underneath etcd, CockroachDB, and TiKV — the layer that makes distributed databases safe and consistent even when machines crash, networks partition, and clocks drift.

At its core is a hand-written implementation of the **Raft consensus algorithm** — the mechanism that allows a cluster of independent nodes to behave as a single, coherent system. Every design decision in this project mirrors what real distributed systems engineers face: how do you ensure no two nodes ever disagree on committed state? How do you elect a new leader in milliseconds without data loss? How do you keep the log from growing forever?

This is not a tutorial project. It is a from-scratch engineering exercise targeting the internals that most engineers only read about in papers.

---

## Why Raft? Why Not Paxos?

Raft was designed explicitly to be **understandable**. Leslie Lamport's Paxos (1989) is famously correct and famously incomprehensible — there are entire PhD theses dedicated to translating Paxos into something implementable. Raft (Ongaro & Ousterhout, 2014) achieves the same safety guarantees by decomposing the consensus problem into three mostly-independent sub-problems: **leader election**, **log replication**, and **safety**.

Both algorithms guarantee the same thing: if a value is committed, every non-faulty node will eventually agree on it. Raft just makes the path to implementing that guarantee dramatically more tractable.

---

## The Distributed Systems Problem This Solves

Imagine you have a key-value store on a single server. It is simple and fast. Now you need it to survive server crashes — so you add two more servers as replicas. Immediately, three unsolvable problems appear:

**1. Who is the source of truth?**  
If all three nodes accept writes independently, they will diverge. You need one authoritative node — a leader — at all times.

**2. How do you elect a leader safely?**  
If two nodes both think they are the leader at the same time (split-brain), you get conflicting commits and data corruption. The algorithm must make this mathematically impossible.

**3. How do you replicate atomically?**  
A write is not safe until it is on a majority of nodes. If the leader crashes mid-replication, the new leader must know exactly which writes were committed and which were not — and roll back or re-apply accordingly.

Raft solves all three, provably.

---

## System Architecture

```mermaid
graph TB
    Client["🖥️ Client<br/>(SDK / HTTP / CLI)"]

    subgraph Cluster ["Raft Cluster (3 or 5 nodes)"]
        direction TB

        subgraph Leader ["Node 1 — Leader"]
            LE["Raft Engine"]
            LL["Raft Log"]
            LSM["State Machine<br/>(KV HashMap)"]
            LE --> LL
            LL --> LSM
        end

        subgraph Follower2 ["Node 2 — Follower"]
            FE2["Raft Engine"]
            FL2["Raft Log"]
            FSM2["State Machine"]
            FE2 --> FL2
            FL2 --> FSM2
        end

        subgraph Follower3 ["Node 3 — Follower"]
            FE3["Raft Engine"]
            FL3["Raft Log"]
            FSM3["State Machine"]
            FE3 --> FL3
            FL3 --> FSM3
        end

        LE -- "AppendEntries RPC" --> FE2
        LE -- "AppendEntries RPC" --> FE3
        FE2 -- "ACK" --> LE
        FE3 -- "ACK" --> LE
    end

    subgraph Storage ["Persistent Storage (per node)"]
        WAL["WAL / RocksDB<br/>Append-only log"]
        SNAP["Snapshot store<br/>Compacted state"]
    end

    Client -- "PUT / GET / DELETE" --> Leader
    Leader -- "200 OK (after commit)" --> Client
    LSM --> WAL
    LSM --> SNAP
```

---

## Raft In Detail

### The Three Roles

Every node in a Raft cluster is always in exactly one of three states:

```mermaid
stateDiagram-v2
    [*] --> Follower : Node starts up

    Follower --> Candidate : Election timeout fires\n(150–300ms, randomized)
    Candidate --> Follower : Discovers node with higher term\nor another leader
    Candidate --> Candidate : Election timeout fires\n(split vote — retry)
    Candidate --> Leader : Receives votes from majority
    Leader --> Follower : Discovers higher term\n(deposed by new election)
```

| Role | What It Does |
|---|---|
| **Follower** | Passive. Accepts log entries from the leader. Redirects client writes to leader. If it doesn't hear from the leader within its timeout, it becomes a candidate. |
| **Candidate** | Actively soliciting votes. Sends `RequestVote` RPCs to all peers. If it gets a majority, it becomes the new leader. If it discovers a legitimate leader or higher term, it reverts to follower. |
| **Leader** | The single authority. Accepts all writes, replicates to followers, commits when majority ACKs. Sends heartbeats every 50ms to prevent new elections. |

---

### Leader Election — How a New Leader Is Chosen

This is the most delicate part. The algorithm must ensure that **exactly one leader exists per term**, and that the elected leader has the most up-to-date log of all candidates.

```mermaid
sequenceDiagram
    participant N1 as Node 1 (Candidate)
    participant N2 as Node 2 (Follower)
    participant N3 as Node 3 (Follower)

    Note over N1: Election timeout fires.<br/>Increments term to 4.<br/>Votes for itself.

    N1->>N2: RequestVote(term=4, lastLogIndex=47, lastLogTerm=3)
    N1->>N3: RequestVote(term=4, lastLogIndex=47, lastLogTerm=3)

    Note over N2: Checks: term 4 > my term 3 ✓<br/>Haven't voted in term 4 ✓<br/>Candidate log ≥ my log ✓<br/>Grants vote.

    N2-->>N1: VoteGranted(term=4)

    Note over N1: Has votes from self + N2 = majority (2/3).<br/>Immediately becomes Leader for term 4.

    N1->>N2: AppendEntries(term=4, entries=[]) — first heartbeat
    N1->>N3: AppendEntries(term=4, entries=[]) — first heartbeat

    Note over N3: VoteGranted reply arrives late.<br/>N1 is already leader — doesn't matter.
    N3-->>N1: VoteGranted(term=4)
```

**Vote safety rules** — a node will only grant a vote if ALL of the following are true:
1. The candidate's term is ≥ the voter's current term
2. The voter has not already voted in this term
3. The candidate's log is at least as up-to-date as the voter's log  
   (compared by `lastLogTerm` first, then `lastLogIndex`)

Rule 3 is what prevents a stale node from becoming leader. A node that crashed and missed 100 entries cannot win an election because any voter with those entries will reject it.

---

### Log Replication — The Write Path

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader (Node 1)
    participant F2 as Follower (Node 2)
    participant F3 as Follower (Node 3)

    C->>L: PUT username=alice

    Note over L: Step 1: Append to local log<br/>entry={term:3, index:47, cmd:"PUT username=alice"}<br/>Persist to WAL. Do NOT apply yet.

    par Replicate in parallel
        L->>F2: AppendEntries(prevIndex=46, prevTerm=3, entries=[{47,"PUT username=alice"}], leaderCommit=46)
        L->>F3: AppendEntries(prevIndex=46, prevTerm=3, entries=[{47,"PUT username=alice"}], leaderCommit=46)
    end

    Note over F2: Validates: log[46].term == 3 ✓<br/>Appends entry 47. Persists.
    Note over F3: Validates: log[46].term == 3 ✓<br/>Appends entry 47. Persists.

    F2-->>L: Success(matchIndex=47)
    F3-->>L: Success(matchIndex=47)

    Note over L: matchIndex[N2]=47, matchIndex[N3]=47<br/>Majority (self + N2) have index 47<br/>commitIndex = 47<br/>Apply to state machine: kv.put("username","alice")

    L-->>C: 200 OK {committed: true, index: 47}

    Note over F2,F3: Next heartbeat carries leaderCommit=47<br/>Followers advance their commitIndex<br/>and apply to their own state machines.
```

The key insight: **the client only hears success after the entry is committed on a majority**. If the leader crashes between replicating and responding, the new leader will already have the entry (it was on a majority), and the client's retry will be a no-op.

---

### The Raft Log — Structure and Invariants

The log is an ordered sequence of `LogEntry` objects. It is the single source of truth. The KV store is simply the result of replaying the log from the beginning.

```
Index:  1        2        3        4        5        6        7
       ┌────────┬────────┬────────┬────────┬────────┬────────┬────────┐
Term:  │ t=1    │ t=1    │ t=2    │ t=2    │ t=3    │ t=3    │ t=3    │
       │ PUT    │ PUT    │ PUT    │ DEL    │ PUT    │ CAS    │ PUT    │
       │ x=1    │ y=2    │ x=5    │ y      │ z=99   │ x=5→7  │ w=hello│
       └────────┴────────┴────────┴────────┴────────┴────────┴────────┘
                                   ▲
                            commitIndex=4
                            (entries 1-4 are safe,
                             5-7 not yet committed)
```

**Core log invariants (guaranteed by Raft):**
- If two log entries at the same index have the same term, they contain the same command
- If two logs agree at index `i`, they agree on every entry before `i`
- A committed entry will never be overwritten by any future leader

---

### Fault Tolerance — What Happens When Things Break

#### Scenario 1: Leader crashes mid-replication

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader (crashes)
    participant F2 as Follower 2
    participant F3 as Follower 3

    L->>F2: AppendEntries(index=47)
    L->>F3: AppendEntries(index=47)
    F2-->>L: ACK

    Note over L: CRASH — never receives F3's ACK.<br/>Never commits index 47.<br/>Never replies to client.

    Note over F2,F3: No heartbeat for 200ms.<br/>Election timeout fires on F3 (fires first — randomized).

    F3->>F2: RequestVote(term=4, lastLogIndex=46, lastLogTerm=3)
    Note over F2: F3's log matches. Grants vote.
    F2-->>F3: VoteGranted

    Note over F3: F3 becomes leader (term 4).<br/>Entry 47 is on F2 but NOT committed.<br/>F3 will overwrite it during log repair<br/>OR recommit it if F2's entry matches.

    C->>F3: PUT username=alice (retry after timeout)
    Note over F3: Processes as new write.<br/>Commits safely.
    F3-->>C: 200 OK
```

#### Scenario 2: Network partition (split-brain attempt)

```mermaid
graph LR
    subgraph PartitionA ["Partition A · minority"]
        N1["Node 1<br/>Old leader<br/>Term 3"]
        N2["Node 2<br/>Follower"]
    end

    subgraph PartitionB ["Partition B · majority"]
        N3["Node 3<br/>New leader<br/>Term 4"]
        N4["Node 4<br/>Follower"]
        N5["Node 5<br/>Follower"]
    end

    N1 -. "network cut" .- N3
    N1 -. "network cut" .- N4
    N2 -. "network cut" .- N3

    ClientA["Client A"] --> N1
    ClientB["Client B"] --> N3

    style N1 fill:#FCEBEB,stroke:#A32D2D,color:#501313
    style N3 fill:#E1F5EE,stroke:#0F6E56,color:#04342C
```

**What happens:**

- Node 1 (old leader, Term 3) is isolated with only Node 2 — a minority. It **cannot commit any writes** because it can never get majority ACK. Client A's writes are silently rejected or time out.
- Nodes 3, 4, 5 elect a new leader (Node 3, Term 4) among themselves — a majority. Client B's writes commit normally.
- When the partition heals, Node 1 receives a message with Term 4. It **immediately steps down** as leader and reverts to follower. Its un-committed entries (if any) are overwritten by Node 3's log.
- **Zero data loss** — nothing that was committed on the majority partition is ever lost. Nothing that failed to reach majority is ever falsely committed.

---

### Log Compaction and Snapshots

Without intervention, the Raft log grows forever. After 10,000 entries, replaying the entire log to rebuild state on a restarted node would take seconds. Snapshots solve this.

```mermaid
graph LR
    subgraph Before compaction
        direction LR
        E1["idx:1\nPUT x=1"]
        E2["idx:2\nPUT y=2"]
        E3["idx:3\nDEL x"]
        E4["idx:4\nPUT z=9"]
        E5["idx:5\nPUT x=7"]
        E6["idx:6\n..."]
        E1 --> E2 --> E3 --> E4 --> E5 --> E6
    end

    subgraph After compaction
        direction LR
        SNAP["SNAPSHOT\nlastIndex=5\nlastTerm=3\n─────────\ny=2\nz=9\nx=7"]
        E6b["idx:6\n..."]
        SNAP --> E6b
    end
```

**When a snapshot is taken:**
1. The state machine serializes the current KV map to disk along with the `lastIncludedIndex` and `lastIncludedTerm`
2. All log entries up to `lastIncludedIndex` are deleted
3. If a follower is so far behind that the leader no longer has the log entries it needs, the leader sends the entire snapshot via `InstallSnapshot` RPC

---

### Linearizable Reads — The Subtle Problem

A naive `GET` implementation reads directly from the leader's in-memory KV map. This seems safe but has a subtle bug: a leader that has been network-partitioned may not know it has been deposed. It will happily serve stale reads from its (now-outdated) state machine.

Two correct approaches:

**Read-index (implemented here):**
```
1. Leader records its current commitIndex as readIndex
2. Leader sends a round of heartbeats to confirm it's still leader (majority responds)
3. Leader waits until its state machine applies up to readIndex
4. Leader serves the read
```

**Lease-based reads (optimization):**
```
Leader maintains a time-bounded lease. If it received a majority heartbeat ACK
within the last `electionTimeout / clockDriftBound` milliseconds, it is
guaranteed to still be the only leader — serve the read immediately without
a heartbeat round.
```

---

## Data Flow — Complete PUT Request Lifecycle

```mermaid
flowchart TD
    A["Client sends PUT k=v"] --> B{"Request hits\nwhich node?"}
    B -- "Hits follower" --> C["Follower redirects\nto leader address"]
    C --> D
    B -- "Hits leader" --> D["Leader appends entry\nto local log\n{term, index, cmd}"]
    D --> E["Leader persists entry\nto WAL on disk"]
    E --> F["Fan out AppendEntries\nRPC to all followers\nin parallel"]
    F --> G{"Majority\nACK received?"}
    G -- "No — timeout or\nnode unavailable" --> H["Retry with\nexponential backoff"]
    H --> G
    G -- "Yes — majority\nappended" --> I["Advance commitIndex\nApply to state machine\nkv.put(k, v)"]
    I --> J["Return 200 OK\nto client"]
    J --> K["Next heartbeat carries\nnew leaderCommit to followers"]
    K --> L["Followers apply\nentry to their own\nstate machines"]
```

---

## The Raft Log vs The State Machine

This distinction is the most important concept in the entire system.

| | Raft Log | State Machine (KV Store) |
|---|---|---|
| **What it stores** | Every command ever issued, in order | The current value of every key |
| **Size** | Grows with every write (until compacted) | Fixed to number of live keys |
| **Source of truth?** | Yes — it is the truth | No — it is derived from the log |
| **Survives crashes?** | Yes — persisted to WAL | Rebuilt by replaying the log (or restored from snapshot) |
| **Shared across nodes?** | Yes — identical on all committed nodes | Yes — identical after applying same log entries |
| **What "committed" means** | Entry safely on majority of logs | Entry applied and visible to reads |

---

## Concurrency Model

The Raft engine is intentionally single-threaded per node for state management. This eliminates an entire class of race conditions. A single `RaftStateMachine` thread owns:

- `currentTerm`
- `votedFor`
- `log[]`
- `commitIndex`
- `lastApplied`
- `nextIndex[]` (per peer)
- `matchIndex[]` (per peer)

All incoming RPCs (`AppendEntries`, `RequestVote`, `InstallSnapshot`) are queued and processed sequentially on this thread. The heartbeat timer and election timer post events to this queue — they do not modify state directly.

Network I/O (gRPC) runs on a separate Netty thread pool. The HTTP API runs on Spring Boot's thread pool. Both communicate with the Raft state machine via a thread-safe event queue.

```mermaid
graph LR
    subgraph IO Threads — Netty thread pool
        GS["gRPC server\n(incoming RPCs)"]
        GC["gRPC client stubs\n(outgoing RPCs)"]
    end

    subgraph Timer Threads
        HB["Heartbeat timer\n50ms"]
        ET["Election timer\n150–300ms"]
    end

    subgraph Raft Thread — single threaded
        Q["Event queue"]
        RSM["RaftStateMachine\nprocessEvent()"]
        Q --> RSM
    end

    subgraph Application Threads — Spring Boot
        HTTP["HTTP API\nPUT / GET / DELETE"]
    end

    GS -- "enqueue RPC event" --> Q
    HB -- "enqueue HeartbeatTick" --> Q
    ET -- "enqueue ElectionTimeout" --> Q
    HTTP -- "enqueue ClientWrite\n(blocks until committed)" --> Q
    RSM -- "schedule sends" --> GC
```

---

## Consistency Guarantees

| Property | Guarantee |
|---|---|
| **Linearizability** | Every read reflects the result of all writes that completed before it, as if the system were a single machine |
| **Durability** | A write acknowledged to the client is on disk on a majority of nodes and will survive any single-node failure |
| **No split-brain** | Two nodes can never simultaneously be leader in the same term — mathematically impossible with Raft's vote rules |
| **Log matching** | If any two nodes have a committed entry at index `i`, their entire logs match up to `i` |
| **Leader completeness** | A newly elected leader always has every committed entry from all previous terms |

---

## Failure Tolerance

For a cluster of `n` nodes, Raft can tolerate up to `⌊(n-1)/2⌋` simultaneous failures:

| Cluster size | Majority needed | Failures tolerated |
|:---:|:---:|:---:|
| 1 | 1 | 0 |
| 3 | 2 | **1** |
| 5 | 3 | **2** |
| 7 | 4 | **3** |

This is why production deployments run 3 or 5 nodes — never 2 or 4 (even-numbered clusters have worse failure properties relative to their cost).

---

## Key Design Decisions

### Why hand-rolled Raft and not a library?

Libraries like `ratis` or `copycat` abstract the consensus layer away. The point of this project is to implement exactly what those libraries implement — the `nextIndex` repair loop, the vote term tracking, the log truncation on conflict. Using a library would reduce this to a CRUD app with a fancy name.

### Why Java?

Java's explicit threading model, strong type system, and gRPC/Protobuf ecosystem make the concurrency architecture visible and debuggable. The verbosity that makes Java unfashionable in other contexts is an asset here — every state transition is explicit and traceable.

### Why RocksDB for persistence?

RocksDB's LSM-tree architecture is already optimized for the exact access pattern of a Raft log: sequential appends with occasional range reads. Its Java bindings (`rocksdbjni`) are mature, and its snapshot mechanism maps naturally onto Raft's log compaction.

### Why gRPC over raw TCP/Netty?

Defining `AppendEntries`, `RequestVote`, and `InstallSnapshot` as `.proto` services gives typed, versioned, bidirectional RPCs for free. Raw TCP would mean hand-rolling framing, serialization, and backpressure — all solved problems. The one place where gRPC is non-ideal is streaming large snapshots, which uses gRPC server-side streaming.

---

## Observability

Every meaningful event in the consensus engine emits a metric via Micrometer, exportable to Prometheus and visualized in Grafana.

| Metric | Type | What it signals |
|---|---|---|
| `raft_current_term` | Gauge | How many elections have occurred |
| `raft_leader_changes_total` | Counter | Cluster instability detector |
| `raft_log_entries_total` | Counter | Write throughput |
| `raft_commit_latency_ms` | Histogram | End-to-end write latency |
| `raft_append_entries_rpc_duration_ms` | Histogram | Network replication cost |
| `raft_log_size_bytes` | Gauge | When to trigger compaction |
| `raft_snapshot_install_total` | Counter | How often lagging nodes need a full snapshot |

---

## References

- [In Search of an Understandable Consensus Algorithm (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf) — the original Raft paper
- [Designing Data-Intensive Applications — Martin Kleppmann](https://dataintensive.net/) — Chapter 9 covers consistency and consensus at depth
- [etcd internals](https://github.com/etcd-io/etcd) — production Raft in Go, good reference for edge cases
- [The Log: What every software engineer should know about real-time data's unifying abstraction — Jay Kreps](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — essential reading on why the log is the right abstraction

---

<div align="center">
  Built to understand what sits underneath the databases everyone uses but nobody opens.
</div>

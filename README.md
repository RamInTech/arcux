# arcux

> A from-scratch, range-sharded, Raft-replicated transactional database with **per-table tunable consistency**, written in Rust.

A table is declared `CP` or `AP` at creation time, and that declaration selects a genuinely different write path underneath:

```sql
CREATE TABLE ledger     (...) WITH (consistency = 'CP');   -- Percolator 2PC + Raft + TSO → Snapshot Isolation
CREATE TABLE post_likes (...) WITH (consistency = 'AP');   -- leaderless W=1 + HLC + LWW → always available
```

One storage engine, one cluster, two consistency regimes — chosen by the schema, not toggled per request. The consensus, storage engine, and transaction protocol are **hand-rolled** (no `raft-rs`, no RocksDB) — building them is the point.

## Why per-table consistency

Most databases force one consistency model on every table. But not all data needs the same guarantee: an account ledger needs strict serializable correctness even under contention; a "likes" counter or activity feed just needs to always accept writes, even if replicas are briefly out of sync. arcux lets both live in the same cluster, on the same storage engine, distinguished only by how the table was declared — so you get strong consistency where correctness matters and availability where it doesn't, without running two separate systems.

## How it works

### The keyspace is sharded into regions

The whole keyspace (every key, across every table) is cut into contiguous **regions** — `[start, end)` byte ranges, split at each table's key-prefix boundary. Every region carries a **regime**: `CP` or `AP`. A table's own region gets the regime it was declared with; the gaps between/around tables default to `CP` (strong-by-default). This is range-sharding, not hash-sharding — the same approach used by CockroachDB/TiKV — so a scan over a table stays a contiguous read.

### The catalog declares the regime

The **catalog** (`server/src/catalog.rs`) is the `table name → regime` map, populated by `create_table(name, CP|AP)`. It resolves a key's regime by **longest matching prefix**, and drives the region tiling above — the declaration is the only place consistency is chosen; nothing is negotiated per request.

### CP regions: Raft groups with a leader

Each `CP` region is replicated as its own **independent Raft group** across the region's replica set. A node that hosts multiple CP regions runs multiple Raft groups in parallel, each with its own leader, its own log, and its own majority — completely independent of the other groups', even though they share the same physical nodes. A write commits only once it's durably replicated to a **majority** of that region's voters, giving Snapshot Isolation and zero acknowledged-write loss across a leader failure. Multi-key transactions replicate too: each `prewrite`/`commit` is itself a Raft-committed command, so every replica reaches the same conflict decision deterministically (full Percolator-style 2PC, not just single-key writes).

A client that hits a non-leader replica gets redirected (`NotLeader`) and retries against the current leader — this redirection is transparent to application code, and is handled automatically by the client SDK's cluster mode.

### AP regions: leaderless, always available

Each `AP` region has **no leader and no Raft log**. Whichever node a client's write reaches becomes the coordinator: it stamps the write with its local **Hybrid Logical Clock**, applies it locally, acknowledges the client immediately (W=1), and fans the write out best-effort to the region's other replicas. If a peer is unreachable, the write still succeeds — that's the availability trade. Conflicting or re-delivered writes resolve by **Last-Writer-Wins** on the HLC timestamp, so convergence needs no coordination.

### One engine, one cluster

CP and AP regions coexist on the same nodes, writing into the **same underlying storage engine** — the regime only decides how a write gets *ordered and replicated* before it lands, not where it's stored. This means adding an `AP` table next to a `CP` table is just another `create_table` call; no second cluster, no separate infrastructure.

## Architecture at a glance

```
Client SDK  ──▶  routes each key to its owning region (and, for CP, to that region's leader)
                     │
                     ▼
        ┌─────────────────────────────┐
        │   Node (tonic gRPC server)  │
        │                             │
        │  Catalog: table → regime    │
        │  Region tiling: keyspace    │
        │  split into [start,end)     │
        │  ranges, one regime each    │
        │                             │
        │  CP region → Raft group     │──▶ replicated log, majority commit, leader election
        │  AP region → leaderless set │──▶ local write + best-effort fan-out, HLC + LWW
        │                             │
        │  Shared storage engine      │──▶ WAL, MVCC, LSM/SSTables — every region writes here
        └─────────────────────────────┘
```

## What's in each crate

- [`engine/`](engine/) — the storage engine (`arcux-engine`): write-ahead log, MVCC over an LSM tree, crash recovery, and single-node Percolator transactions.
- [`rpc/`](rpc/) — the gRPC wire contract (`kv`/`raft`/`pd` protobufs) and generated code.
- [`pd/`](pd/) — the Placement Driver (`arcux-pd`): cluster timestamp oracle, region registry, per-node membership and failure detection.
- [`raft/`](raft/) — the hand-rolled Raft consensus core (`arcux-raft`): election, replication, commit safety, persistence, snapshotting, membership changes — built transport-free and proven deterministically.
- [`server/`](server/) — the `tonic` server (`arcux-server`): binds Raft groups to regions, runs the AP leaderless path, hosts the consistency catalog, and serves the KV/PD RPCs.
  - `raft_group.rs` — per-region Raft group driver.
  - `multiraft.rs` — many region groups multiplexed over one transport, keyed by `group_id`.
  - `hlc.rs` / `ap.rs` — the AP write path (HLC timestamps + leaderless fan-out).
  - `catalog.rs` — `create_table(name, CP|AP)` and the region tiling it drives.
- [`client/`](client/) — the async client SDK (`arcux-client`): region-aware routing, transparent retry on stale routes or leader changes, and (for a static cluster with no PD) automatic leader-following.

## Build & test

Requires the Rust toolchain ([rustup](https://rustup.rs)); the version is pinned in `rust-toolchain.toml`.

```bash
cargo build              # build the workspace
cargo test               # run the full suite
cargo test --features proptests   # include property tests
```

## Try it: CP and AP tables side by side

Start a node with one CP table and one AP table:

```bash
cargo run -p arcux-server -- --listen 127.0.0.1:50071 --data ./arcux-cat \
  --table ledger=cp --table likes=ap
```

In another terminal, use the interactive shell to write to both:

```bash
ARCUX_ADDR=http://127.0.0.1:50071 cargo run -p arcux-client --bin arcux
```

```
arcux> put ledger/acct1 100      # CP table — strongly consistent, via Raft
arcux> put likes/post1 5         # AP table — leaderless HLC/LWW
arcux> get ledger/acct1
arcux> get likes/post1
```

For a real multi-node cluster (so you can see per-region leader election and AP fan-out across nodes), run three servers with `--voters`/`--cluster`, and point the shell at all three endpoints — it auto-discovers and follows the current leader:

```bash
ARCUX_CLUSTER=3 cargo run -p arcux-client --bin arcux
```

## License

Dual-licensed under MIT or Apache-2.0.

## Author

Ramkumar M

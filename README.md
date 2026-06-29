# arcux

> A from-scratch, range-sharded, Raft-replicated transactional database with **per-table tunable consistency**, written in Rust.

A table is declared `CP` or `AP` at creation time, and that declaration selects a genuinely different write path underneath:

```sql
CREATE TABLE ledger     (...) WITH (consistency = 'CP');   -- Percolator 2PC + Raft + TSO → Snapshot Isolation
CREATE TABLE post_likes (...) WITH (consistency = 'AP');   -- leaderless W=1 + HLC + LWW → always available
```

One storage engine, one cluster, two consistency regimes — chosen by the schema, not toggled per request. The consensus, storage engine, and transaction protocol are **hand-rolled** (no `raft-rs`, no RocksDB) — building them is the point.

## Status

| Phase | Scope | State |
|---|---|---|
| **1** | Single-node storage engine — WAL, MVCC, SSTables, crash recovery, single-node Percolator | ✅ **implemented & tested** |
| 1b | Storage hardening — leveled compaction, bloom filters, block cache, version-set | ⏳ non-blocking backlog |
| **2** | gRPC/`tonic` network layer + async client SDK (correctness slice) | ✅ **implemented & tested** |
| 2b | RPC hardening — idempotency dedup, backpressure/`Overloaded`, blocking client, soak test | ⏳ non-blocking backlog |
| **3** | Range-sharding foundations (single-node slice) — regions + epochs, the Placement Driver (cluster TSO + routing), region-aware client with `RegionStale` retry | ✅ **implemented & tested** |
| **3b** | Multi-node distribution — regions placed across nodes (per-node routing) · region merge · PD membership/failure detector · HLC TSO | ✅ **implemented & tested** |
| **4** | Consensus core — hand-rolled Raft: leader election · log replication · commit safety · persistence, proven on a deterministic in-process cluster | ✅ **core implemented & tested** |
| 4b | Raft integration — bind regions to groups · leader routing/`NotLeader` · WAL-backed log · snapshots · membership · MultiRaft · PD-on-Raft | ⏳ pending (remainder of P4 DoD) |
| 5–6 | distributed Percolator + AP HLC/LWW · rebalance · anti-entropy · chaos | 📐 designed |

A region-aware client routes through a Placement Driver to the data node that owns each
key — the keyspace is now distributed **across** nodes, with epoch-versioned regions, a
restart-safe **HLC** cluster timestamp oracle, PD-driven placement + a membership/failure
detector, and transparent `RegionStale` (and `NotLeader`) refresh-and-retry on split or
merge. The Phase-4 **Raft core** — leader election, log replication, and the commit-safety
rules — is implemented as a standalone, deterministically-tested crate. Binding each region
to a Raft group (the **4b** integration step, now unblocked by 3b) is the next move; the
cross-region transaction layer (5–6) is designed.

## What's implemented (Phase 1)

A durable, multi-version, transactional key-value engine in the [`engine/`](engine/) crate:

- **Write-ahead log** with group-commit `fsync` and CRC32C-framed records; torn tails are discarded on replay, so no acknowledged write is lost.
- **LSM storage** — concurrent skiplist memtable → immutable SSTables (CRC'd data blocks → index → footer); minimal atomic manifest.
- **MVCC** over Lock/Default/Write column families with descending-timestamp encoding (one forward seek finds the latest visible version).
- **Single-node Percolator** — prewrite/commit with snapshot-isolated conflict checks and self-healing lock resolution.
- **Crash recovery** — reload manifest → replay WAL past the flushed watermark → reclaim orphans.

## What's implemented (Phase 2)

A `tonic` gRPC server wrapping the engine, plus an async client SDK — the
[`rpc/`](rpc/), [`server/`](server/), and [`client/`](client/) crates:

- **Frozen, versioned wire contract** — `kv` (Get/Put/Delete/Begin/Prewrite/Commit, plus a defined-but-stubbed `Scan`) and stubbed `raft`/`pd` services, so the schema is fixed before the distributed phases land. `pd.GetTimestamp` is served from the node's TSO.
- **Thin async server** — handlers reuse the Phase-1 `Transaction`/`mvcc_get` verbatim and bridge the synchronous engine via `spawn_blocking`, so an `fsync` never stalls the reactor.
- **Async client** — one multiplexed HTTP/2 channel; `begin/prewrite/commit/get/put/delete` plus a `transact()` convenience. Conflicts/locks surface as typed errors.
- **Hermetic build** — protobufs compile via a vendored `protoc` (no system install needed).

## What's implemented (Phase 3)

Range-sharding's foundations — the [`pd/`](pd/) crate (Placement Driver) plus region
awareness in the server and client:

- **Cluster TSO** — one authoritative, **restart-safe** timestamp oracle: it reserves timestamps in persisted windows, so a crash never re-issues one. Data nodes pull `start_ts`/`commit_ts` from it (client-side batched) over `pd.GetTimestamp`.
- **Regions & epochs** — the keyspace is partitioned into `[start, end)` regions, each carrying a monotonic `epoch`. A node splits a region locally and bumps the epoch; the node is **authoritative** for its epochs (no propagation window), mirroring TiKV — Phase 4 just makes the split a Raft command.
- **Routing** — PD aggregates the regions nodes report via heartbeat and serves `GetRegion`/`ListRegions`. The region-aware client caches routes, stamps a `Context` (region id + epoch) on every request, and on a `RegionStale` reply re-resolves from PD and retries — so a split is invisible to application code.
- **Compatibility** — all additive on the frozen wire contract (`VERSION` 2): a `Context` on the KV requests, a node `SplitRegion`, and `pd.Region`/`ListRegions`. The Phase-2 direct path (no `Context`) is unchanged, so single-node callers need no PD.

## What's implemented (Phase 3b)

The single-node slice, distributed into a real cluster — regions placed **across** nodes,
with PD as the placement authority:

- **Multi-node placement + per-node routing** — the region descriptor grows `{node_id, address}`; PD aggregates heartbeats **per node** (no more global clobber) and tells the client which node owns each key. The client opens one channel **per node**, **binary-searches** its sorted route cache, and dispatches each request to the owner. PD is the placement authority: a fresh node starts empty and **adopts** its assignment from the two-way heartbeat (seeded partition, or the whole keyspace for the first node). Region ids are node-namespaced (`node_id` in the high bits) so independent splits never collide.
- **Membership + failure detector** — PD tracks each node's `address`/`last_seen` and marks a node **down** when it goes silent past a timeout (a background sweep against an injectable clock); a down node's regions drop out of routing until it returns.
- **HLC TSO** — timestamps now pack physical-ms high bits + a logical low counter, tracking wall-clock while staying strictly monotonic (a backwards clock never regresses one) and restart-safe via the persisted high-water mark.
- **Region merge** — the inverse of split (`MergeRegion`): two adjacent halves fold back into one, epoch bumped, with the range-coverage invariant preserved.
- **`NotLeader` + binary-search routing in the client** — `NotLeader{leader_hint}` is handled alongside `RegionStale` on the same refresh-and-retry path (wired now; meaningful once Phase 4 adds per-region leaders).
- **Compatibility** — additive on the wire (`VERSION` 3): node addressing on `pd.Region`/`GetRegionResponse`, `address` + assigned-regions on `Heartbeat`, and a `kv.MergeRegion` RPC. The Phase-2 direct path and the Phase-3 single-node tests are unchanged.

*Deferred (with the Phase-1b range iterator / Phase-4 Raft):* live cross-node region **move** with data migration and per-region engine isolation (split/merge stay data-in-place on one node), and **PD-on-Raft** HA.

## What's implemented (Phase 4)

The hand-rolled consensus core — the [`raft/`](raft/) crate (`arcux-raft`) — built
transport-free so the whole protocol can be proven deterministically before it is wired
into regions:

- **Raft state machine (Figure 2)** — follower/candidate/leader roles, randomized election timeouts, `RequestVote` with the up-to-date-log restriction, and `AppendEntries` enforcing the **Log Matching** property on every append.
- **Commit safety** — `commit_index` advances only to an entry of the leader's *current* term backed by a majority `match_index` (the Figure-8 rule), so a stale leader can never commit a divergent entry; **State Machine Safety** is asserted continuously by the harness.
- **Persistence & restart** — term, vote, and log are written through a `Storage` trait before the node acts on them; a node rebuilt from persisted state recovers term/vote/log and never double-votes.
- **Two clean integration seams** — a `Storage` trait (an engine/WAL-backed impl drops in later) and a `Message`/`Entry` model that maps 1:1 onto the frozen [`raft.proto`](rpc/proto/raft.proto) RPCs, so binding a region to a group, routing to the leader, and applying committed entries into the region's engine is wiring, not a rewrite.

Snapshots / log compaction (`InstallSnapshot`) and single-server membership changes are
the next Phase-4 milestone; the wire contract already reserves room for both.

### Tested

```
cargo test                       # 84 tests (39 engine + 3 rpc schema + 21 PD + 10 raft + 11 server e2e)
cargo test --features proptests  # + property tests
```

Phase 1 highlights: a process-`SIGKILL` recovery oracle (zero acknowledged-write loss across random kill points), and a concurrent **bank-transfer conserved-sum** test proving Snapshot Isolation under contention (which surfaced — and the code now guards against — two subtle concurrency bugs: cross-CF read atomicity and conditional lock ownership). Phase 2 adds in-process gRPC end-to-end tests: full-transaction visibility, cross-network prewrite conflict, snapshot-`commit_ts` reads, and a frozen-but-`Unimplemented` `Scan`. Phase 3 stands up an in-process cluster (PD + node + routed client) and proves the routing path: a TSO restart-safety check, a region split bumping the epoch, and a stale client transparently recovering via `RegionStale` → refresh → retry. Phase 3b extends this to **two** data nodes: keys route to their owning node (proven by stopping one node and watching only its keys go unreachable), a split **and** a merge keep traffic flowing across the change, PD's failure detector marks a stopped node down within a bounded time, and the HLC TSO stays strictly monotonic across a PD restart. Phase 4 drives a deterministic in-process Raft cluster through election, replication, isolated-follower catch-up, a minority-partition liveness/safety split, restart-persistence, and a randomized partition/heal fuzz (24 seeds) that asserts State Machine Safety after every step.

## Build & test

Requires the Rust toolchain ([rustup](https://rustup.rs)); the version is pinned in `rust-toolchain.toml`.

```bash
cargo build              # build the workspace
cargo test               # run the full suite
cargo test --features proptests   # include property tests
```

## Repository layout

```
Cargo.toml               # workspace
rust-toolchain.toml
engine/                  # Phase 1: the storage engine crate (arcux-engine)
  src/
    wal.rs               # write-ahead log: framing + replay
    memtable.rs          # per-CF skiplist memtable
    sstable/             # SSTable writer/reader + block codec
    manifest.rs          # live-SSTable set + flushed watermark
    db.rs                # engine core: group-commit committer, flush, recovery
    keys.rs / encoding.rs# MVCC key/value codecs
    mvcc.rs              # snapshot reads + lock resolution
    percolator.rs        # single-node 2PC transactions
    clock.rs             # monotonic timestamp source (TSO stand-in)
  tests/                 # WAL, crash recovery, MVCC/SI, Percolator
rpc/                     # Phase 2: frozen gRPC wire contract (kv/raft/pd) + generated code
  proto/                 # kv.proto · raft.proto · pd.proto
  build.rs               # tonic codegen via vendored protoc
pd/                      # Phase 3/3b: Placement Driver (arcux-pd) — cluster TSO + region router + membership
  src/tso.rs             # restart-safe HLC timestamp oracle (physical-ms + logical)
  src/region.rs          # regions + epoch-versioned registry (route/split/merge/persist)
  src/cluster.rs         # per-node membership + placement + failure detector (3b)
  src/service.rs         # the pd.PdService gRPC impl
raft/                    # Phase 4: hand-rolled Raft consensus core (arcux-raft)
  src/node.rs            # the RaftNode state machine (Figure 2): election + replication + commit
  src/storage.rs         # Storage trait + in-memory impl (term/vote/log persistence)
  src/message.rs         # wire-agnostic Message/Entry (maps onto raft.proto)
  tests/cluster.rs       # deterministic in-process cluster: partitions, restart, safety fuzz
server/                  # Phase 2/3/3b: tonic server (arcux-server) over the engine + multi-node region routing
client/                  # Phase 2/3/3b: async client SDK (arcux-client), per-node routing via PD
```

Later phases add `txn/` and friends.

## Roadmap

`P1 ✅ → P2 ✅ (RPC) → P3 ✅ (regions + PD/TSO, single-node slice) → P3b ✅ (multi-node distribution) → P4 ✅ (Raft consensus core, in isolation) → P4b (bind regions to groups · WAL-backed log · snapshots · membership · MultiRaft) → P5 (distributed Percolator CP + HLC/LWW AP) → P6 (rebalance · anti-entropy · chaos · security)`, with **P1b** (compaction · bloom · cache · version-set) and **P2b** (RPC hardening) as non-blocking tracks.

## License

Dual-licensed under MIT or Apache-2.0.

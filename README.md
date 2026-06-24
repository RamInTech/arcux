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
| 3–6 | Regions + PD/TSO · per-region Raft · distributed Percolator + AP HLC/LWW · rebalance/anti-entropy/chaos | 📐 designed |

A remote client now drives the full single-node transactional API over gRPC; the
distributed layers (3–6) are designed and in progress.

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

### Tested

```
cargo test                       # 46 tests (39 engine + 2 rpc schema + 5 gRPC e2e)
cargo test --features proptests  # + property tests
```

Phase 1 highlights: a process-`SIGKILL` recovery oracle (zero acknowledged-write loss across random kill points), and a concurrent **bank-transfer conserved-sum** test proving Snapshot Isolation under contention (which surfaced — and the code now guards against — two subtle concurrency bugs: cross-CF read atomicity and conditional lock ownership). Phase 2 adds in-process gRPC end-to-end tests: full-transaction visibility, cross-network prewrite conflict, snapshot-`commit_ts` reads, and a frozen-but-`Unimplemented` `Scan`.

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
server/                  # Phase 2: tonic server (arcux-server) over the engine
client/                  # Phase 2: async client SDK (arcux-client)
```

Later phases add `raft/`, `region/`, `pd/`, and friends.

## Roadmap

`P1 ✅ → P2 ✅ (RPC) → P3 (regions + PD/TSO) → P4 (per-region Raft) → P5 (distributed Percolator CP + HLC/LWW AP) → P6 (rebalance · anti-entropy · chaos · security)`, with **P1b** (compaction · bloom · cache · version-set) and **P2b** (RPC hardening) as non-blocking tracks.

## License

Dual-licensed under MIT or Apache-2.0.

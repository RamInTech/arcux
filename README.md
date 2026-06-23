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
| 2 | gRPC/`tonic` network layer + client SDK | 🔜 next |
| 3–6 | Regions + PD/TSO · per-region Raft · distributed Percolator + AP HLC/LWW · rebalance/anti-entropy/chaos | 📐 designed |

The single-node transactional engine is **correct and durable today**; the distributed layers are designed and in progress.

## What's implemented (Phase 1)

A durable, multi-version, transactional key-value engine in the [`engine/`](engine/) crate:

- **Write-ahead log** with group-commit `fsync` and CRC32C-framed records; torn tails are discarded on replay, so no acknowledged write is lost.
- **LSM storage** — concurrent skiplist memtable → immutable SSTables (CRC'd data blocks → index → footer); minimal atomic manifest.
- **MVCC** over Lock/Default/Write column families with descending-timestamp encoding (one forward seek finds the latest visible version).
- **Single-node Percolator** — prewrite/commit with snapshot-isolated conflict checks and self-healing lock resolution.
- **Crash recovery** — reload manifest → replay WAL past the flushed watermark → reclaim orphans.

### Tested

```
cargo test                       # 39 tests
cargo test --features proptests  # + property tests
```

Highlights: a process-`SIGKILL` recovery oracle (zero acknowledged-write loss across random kill points), and a concurrent **bank-transfer conserved-sum** test proving Snapshot Isolation under contention (which surfaced — and the code now guards against — two subtle concurrency bugs: cross-CF read atomicity and conditional lock ownership).

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
```

Later phases add `raft/`, `region/`, `pd/`, `rpc/`, `client/`, `server/`, and friends.

## Roadmap

`P1 ✅ → P2 (RPC) → P3 (regions + PD/TSO) → P4 (per-region Raft) → P5 (distributed Percolator CP + HLC/LWW AP) → P6 (rebalance · anti-entropy · chaos · security)`, with **P1b** (compaction · bloom · cache · version-set) as a non-blocking hardening track.

## License

Dual-licensed under MIT or Apache-2.0.

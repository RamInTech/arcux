//! Cross-region distributed Percolator — deterministic core.
//!
//! Each region is an independent [`Engine`] (its own committed state machine), so a
//! transaction here genuinely spans engines the way it would span nodes. These tests drive
//! the multi-region 2PC coordinator and the cross-region reader through the cases that make
//! the protocol correct:
//!
//! * a transaction spanning three regions commits atomically and is visible at `commit_ts`
//!   (and invisible to a snapshot taken before it);
//! * a coordinator that dies **after** the primary commit but **before** finalizing
//!   secondaries is recovered by a later reader (roll **forward**);
//! * a coordinator that dies **before** the primary commit, once its lock TTL expires, is
//!   rolled **back** by a later reader;
//! * a cross-region write-write conflict aborts and cleans up its own partial locks;
//! * a reader that meets a still-alive primary lock **waits**.

use arcux_engine::{Engine, Error, Mutation, Options};
use arcux_server::cross_txn::{cross_get, CrossTxn, Regions};

/// Range-sharded regions: disjoint `[start, end)` byte ranges, each backed by its own engine.
struct RangeRegions {
    // (id, start, end, engine); `end` empty means "unbounded".
    shards: Vec<(u64, Vec<u8>, Vec<u8>, Engine)>,
    _dirs: Vec<tempfile::TempDir>,
}

impl RangeRegions {
    /// Build regions from `[start, end)` bounds (ids assigned 1..). Ranges must tile the
    /// keyspace so every key lands in exactly one region.
    fn new(bounds: &[(&[u8], &[u8])]) -> RangeRegions {
        let mut shards = Vec::new();
        let mut dirs = Vec::new();
        for (i, (start, end)) in bounds.iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let eng = Engine::open(Options::new(dir.path())).unwrap();
            shards.push((i as u64 + 1, start.to_vec(), end.to_vec(), eng));
            dirs.push(dir);
        }
        RangeRegions { shards, _dirs: dirs }
    }
}

impl Regions for RangeRegions {
    fn region_of(&self, key: &[u8]) -> u64 {
        for (id, start, end, _) in &self.shards {
            let above_start = key >= start.as_slice();
            let below_end = end.is_empty() || key < end.as_slice();
            if above_start && below_end {
                return *id;
            }
        }
        panic!("no region owns key {key:?}");
    }
    fn engine(&self, id: u64) -> &Engine {
        &self.shards.iter().find(|s| s.0 == id).expect("region id").3
    }
}

const TTL: u64 = 1_000_000; // generous lease: locks never look expired mid-transaction

/// Three regions: A `[, g)`, B `[g, n)`, C `[n, )`.
fn three_regions() -> RangeRegions {
    RangeRegions::new(&[(b"", b"g"), (b"g", b"n"), (b"n", b"")])
}

#[test]
fn commit_across_regions_is_atomic_and_snapshot_isolated() {
    let r = three_regions();
    // Keys land in three different regions; the primary "acct" is in region A.
    let txn = CrossTxn::new(
        10,
        vec![
            Mutation::put(b"acct".to_vec(), b"A".to_vec()), // region A (primary)
            Mutation::put(b"item".to_vec(), b"B".to_vec()), // region B
            Mutation::put(b"post".to_vec(), b"C".to_vec()), // region C
        ],
    )
    .unwrap();

    txn.prewrite(&r, TTL).unwrap();
    txn.commit(&r, 15).unwrap();

    // Visible at a snapshot after the commit...
    assert_eq!(cross_get(&r, b"acct", 20).unwrap(), Some(b"A".to_vec()));
    assert_eq!(cross_get(&r, b"item", 20).unwrap(), Some(b"B".to_vec()));
    assert_eq!(cross_get(&r, b"post", 20).unwrap(), Some(b"C".to_vec()));

    // ...and invisible to a snapshot taken before it committed (snapshot isolation).
    assert_eq!(cross_get(&r, b"item", 12).unwrap(), None);
}

#[test]
fn reader_rolls_forward_after_coordinator_dies_post_primary_commit() {
    let r = three_regions();
    let txn = CrossTxn::new(
        10,
        vec![
            Mutation::put(b"acct".to_vec(), b"A".to_vec()), // primary, region A
            Mutation::put(b"item".to_vec(), b"B".to_vec()), // secondary, region B
        ],
    )
    .unwrap();

    txn.prewrite(&r, TTL).unwrap();
    // Commit the primary (the transaction is now committed) then *crash* — the secondary in
    // region B is left locked, never finalized.
    txn.commit_primary(&r, 15).unwrap();

    // A reader of the secondary follows its lock to the primary's region, sees the commit,
    // and rolls the secondary forward — returning the committed value.
    assert_eq!(cross_get(&r, b"item", 20).unwrap(), Some(b"B".to_vec()));
    // The lock is gone: a plain unresolved read now sees it too.
    assert_eq!(r.engine(r.region_of(b"item")).mvcc_get_unresolved(b"item", 20).unwrap(), Some(b"B".to_vec()));
}

#[test]
fn reader_rolls_back_after_coordinator_dies_pre_commit_and_ttl_expires() {
    let r = three_regions();
    // Seed a prior committed value for the secondary so we can see the rollback reveal it.
    let seed = CrossTxn::new(1, vec![Mutation::put(b"item".to_vec(), b"old".to_vec())]).unwrap();
    seed.prewrite(&r, TTL).unwrap();
    seed.commit(&r, 2).unwrap();

    // A second txn prewrites both keys with a SHORT ttl, then crashes before committing.
    let short_ttl = 5;
    let txn = CrossTxn::new(
        10,
        vec![
            Mutation::put(b"acct".to_vec(), b"A".to_vec()), // primary, region A
            Mutation::put(b"item".to_vec(), b"B".to_vec()), // secondary, region B
        ],
    )
    .unwrap();
    txn.prewrite(&r, short_ttl).unwrap();

    // A reader arrives well past the lease (read_ts 100 > lock ttl 15): the primary's lock is
    // expired, so the txn is declared dead and the secondary rolls back to the prior value.
    assert_eq!(cross_get(&r, b"item", 100).unwrap(), Some(b"old".to_vec()));
    // The primary was also killed, so it shows no committed value either.
    assert_eq!(cross_get(&r, b"acct", 100).unwrap(), None);
}

#[test]
fn cross_region_write_write_conflict_aborts_and_cleans_up() {
    let r = three_regions();
    // A committed write to the region-B key at commit_ts 50.
    let winner = CrossTxn::new(40, vec![Mutation::put(b"item".to_vec(), b"win".to_vec())]).unwrap();
    winner.prewrite(&r, TTL).unwrap();
    winner.commit(&r, 50).unwrap();

    // A txn with start_ts 45 (< the commit at 50) prewrites its primary in A, then hits the
    // conflicting committed write on the region-B key → abort.
    let loser = CrossTxn::new(
        45,
        vec![
            Mutation::put(b"acct".to_vec(), b"A".to_vec()), // primary, region A
            Mutation::put(b"item".to_vec(), b"lose".to_vec()), // conflicts in region B
        ],
    )
    .unwrap();
    let err = loser.prewrite(&r, TTL).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "expected a write-write conflict, got {err:?}");

    // The primary's partial lock in region A was cleaned up, so it doesn't block a later txn.
    assert_eq!(
        r.engine(r.region_of(b"acct")).get_cf_raw(arcux_engine::Cf::Lock, b"acct").unwrap(),
        None,
        "aborted txn must drop its own primary lock"
    );
    // The winner's value is intact and readable.
    assert_eq!(cross_get(&r, b"item", 60).unwrap(), Some(b"win".to_vec()));
}

#[test]
fn reader_waits_on_a_still_alive_primary_lock() {
    let r = three_regions();
    let txn = CrossTxn::new(
        10,
        vec![
            Mutation::put(b"acct".to_vec(), b"A".to_vec()), // primary, region A
            Mutation::put(b"item".to_vec(), b"B".to_vec()), // secondary, region B
        ],
    )
    .unwrap();
    txn.prewrite(&r, TTL).unwrap(); // generous ttl → the primary lock is alive

    // A reader at read_ts 20 (< the huge ttl) meets the secondary lock, finds the primary
    // still alive, and must wait rather than guess.
    match cross_get(&r, b"item", 20) {
        Err(Error::KeyIsLocked(_)) => {}
        other => panic!("expected KeyIsLocked (wait), got {other:?}"),
    }
}

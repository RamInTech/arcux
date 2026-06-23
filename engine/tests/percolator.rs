//! Single-node Percolator tests: commit visibility, prewrite conflict aborts, and
//! the three lock-resolution outcomes (roll forward / roll back / wait).

use arcux_engine::keys::encode_data_key;
use arcux_engine::{Cf, Engine, Error, Mutation, Options, Transaction, Tso, Value};

const MAX_TTL: u64 = u64::MAX;

fn open() -> (tempfile::TempDir, Engine, Tso) {
    let dir = tempfile::tempdir().unwrap();
    let eng = Engine::open(Options::new(dir.path())).unwrap();
    (dir, eng, Tso::new())
}

fn commit_put(eng: &Engine, tso: &Tso, key: &[u8], val: &[u8]) -> u64 {
    let start_ts = tso.now();
    let txn = Transaction::new(eng, start_ts, vec![Mutation::put(key.to_vec(), val.to_vec())]).unwrap();
    txn.prewrite(MAX_TTL).unwrap();
    let commit_ts = tso.now();
    txn.commit(commit_ts).unwrap();
    commit_ts
}

#[test]
fn commit_makes_value_visible_at_and_after_commit_ts() {
    let (_d, eng, tso) = open();
    let start_ts = tso.now();
    let txn = Transaction::new(&eng, start_ts, vec![Mutation::put(b"acct".to_vec(), b"100".to_vec())]).unwrap();
    txn.prewrite(MAX_TTL).unwrap();
    let commit_ts = tso.now();
    txn.commit(commit_ts).unwrap();

    // Visible after commit.
    let rt = tso.now();
    assert_eq!(eng.snapshot(rt).get(b"acct").unwrap(), Some(b"100".to_vec()));
    // A snapshot taken at our own start_ts must NOT see it (committed later).
    assert_eq!(eng.snapshot(start_ts).get(b"acct").unwrap(), None);
}

#[test]
fn prewrite_aborts_on_existing_lock() {
    let (_d, eng, tso) = open();
    let t1 = tso.now();
    eng.prewrite_one(b"k", &Value::Put(b"a".to_vec()), b"k", t1, MAX_TTL).unwrap();

    let t2 = tso.now();
    let res = eng.prewrite_one(b"k", &Value::Put(b"b".to_vec()), b"k", t2, MAX_TTL);
    assert!(matches!(res, Err(Error::Conflict(_))), "second prewrite must conflict on the lock");
}

#[test]
fn prewrite_aborts_on_write_after_snapshot() {
    let (_d, eng, tso) = open();
    // Allocate a *stale* start_ts first.
    let stale_start = tso.now();
    // Another transaction commits the same key at a later commit_ts.
    commit_put(&eng, &tso, b"k", b"newer");
    // Prewriting with the stale start_ts now sees a commit newer than our snapshot.
    let res = eng.prewrite_one(b"k", &Value::Put(b"x".to_vec()), b"k", stale_start, MAX_TTL);
    assert!(matches!(res, Err(Error::Conflict(_))), "write-after-snapshot must conflict");
}

#[test]
fn reader_rolls_committed_secondary_forward() {
    let (_d, eng, tso) = open();
    // Two-key txn: primary p, secondary s.
    let start_ts = tso.now();
    let muts = vec![
        Mutation::put(b"p".to_vec(), b"vp".to_vec()),
        Mutation::put(b"s".to_vec(), b"vs".to_vec()),
    ];
    let txn = Transaction::new(&eng, start_ts, muts).unwrap();
    txn.prewrite(MAX_TTL).unwrap();
    let commit_ts = tso.now();
    // Simulate a crash after the primary commit but before secondaries are finalized.
    txn.commit_primary(commit_ts).unwrap();
    assert!(eng.get_cf_raw(Cf::Lock, b"s").unwrap().is_some(), "secondary still locked");

    // A reader resolves the leftover lock by rolling the secondary forward.
    let rt = tso.now();
    assert_eq!(eng.snapshot(rt).get(b"s").unwrap(), Some(b"vs".to_vec()));
    assert_eq!(eng.snapshot(rt).get(b"p").unwrap(), Some(b"vp".to_vec()));
    // The lock is gone and a Write record now exists for s.
    assert_eq!(eng.get_cf_raw(Cf::Lock, b"s").unwrap(), None);
    assert!(eng.get_cf_raw(Cf::Write, &encode_data_key(b"s", rt)).is_ok());
}

#[test]
fn reader_rolls_back_expired_lock_to_prior_value() {
    let (_d, eng, tso) = open();
    // Prior committed value.
    commit_put(&eng, &tso, b"p", b"old");

    // A new txn prewrites p="new" with a TTL that is already in the past for any
    // later reader (ttl == its own start_ts), and never commits.
    let s1 = tso.now();
    eng.prewrite_one(b"p", &Value::Put(b"new".to_vec()), b"p", s1, /*ttl*/ s1).unwrap();

    let rt = tso.now();
    assert!(rt > s1, "reader timestamp must exceed the expired ttl");
    // Reader rolls the dead lock back and sees the prior committed value.
    assert_eq!(eng.snapshot(rt).get(b"p").unwrap(), Some(b"old".to_vec()));
    assert_eq!(eng.get_cf_raw(Cf::Lock, b"p").unwrap(), None, "expired lock removed");
    assert_eq!(
        eng.get_cf_raw(Cf::Default, &encode_data_key(b"p", s1)).unwrap(),
        None,
        "uncommitted value removed"
    );
}

#[test]
fn reader_waits_on_live_lock() {
    let (_d, eng, tso) = open();
    let s = tso.now();
    eng.prewrite_one(b"q", &Value::Put(b"pending".to_vec()), b"q", s, MAX_TTL).unwrap();

    let rt = tso.now();
    let res = eng.snapshot(rt).get(b"q");
    assert!(matches!(res, Err(Error::KeyIsLocked(_))), "a live lock must block the read, not expose the value");
}

//! Snapshot-Isolation invariants:
//!  * snapshot stability — a read at `read_ts` is unaffected by later commits;
//!  * no dirty reads — an uncommitted write is never exposed;
//!  * conserved sum — concurrent Percolator bank transfers never create/destroy money.

use std::sync::Arc;

use arcux_engine::{Engine, Error, Mutation, Options, Transaction, Tso};

const MAX_TTL: u64 = u64::MAX;

fn commit_put(eng: &Engine, tso: &Tso, key: &[u8], val: &[u8]) {
    let start_ts = tso.now();
    let txn = Transaction::new(eng, start_ts, vec![Mutation::put(key.to_vec(), val.to_vec())]).unwrap();
    txn.prewrite(MAX_TTL).unwrap();
    let commit_ts = tso.now();
    txn.commit(commit_ts).unwrap();
}

#[test]
fn snapshot_is_stable_across_later_commits() {
    let dir = tempfile::tempdir().unwrap();
    let eng = Engine::open(Options::new(dir.path())).unwrap();
    let tso = Tso::new();

    commit_put(&eng, &tso, b"x", b"v1");
    let rt = tso.now(); // snapshot after v1, before v2
    commit_put(&eng, &tso, b"x", b"v2");

    // The old snapshot still sees v1; a fresh snapshot sees v2.
    assert_eq!(eng.snapshot(rt).get(b"x").unwrap(), Some(b"v1".to_vec()));
    let rt2 = tso.now();
    assert_eq!(eng.snapshot(rt2).get(b"x").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn no_dirty_reads() {
    let dir = tempfile::tempdir().unwrap();
    let eng = Engine::open(Options::new(dir.path())).unwrap();
    let tso = Tso::new();

    commit_put(&eng, &tso, b"p", b"base");
    let rt_mid = tso.now(); // after base commit

    // An uncommitted prewrite at a *later* start_ts than our snapshot.
    let sd = tso.now();
    eng.prewrite_one(b"p", &arcux_engine::Value::Put(b"dirty".to_vec()), b"p", sd, MAX_TTL).unwrap();

    // The snapshot taken before the dirty txn started sees committed base, never dirty.
    assert_eq!(eng.snapshot(rt_mid).get(b"p").unwrap(), Some(b"base".to_vec()));
}

#[test]
fn concurrent_transfers_conserve_sum() {
    let dir = tempfile::tempdir().unwrap();
    let eng = Arc::new(Engine::open(Options::new(dir.path())).unwrap());
    let tso = Arc::new(Tso::new());

    const N_ACCTS: usize = 5;
    const START: i64 = 100;
    let total: i64 = START * N_ACCTS as i64;

    for i in 0..N_ACCTS {
        commit_put(&eng, &tso, acct(i).as_bytes(), START.to_string().as_bytes());
    }

    let mut handles = Vec::new();
    for w in 0..4u64 {
        let eng = eng.clone();
        let tso = tso.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = w.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x1234_5678;
            for _ in 0..60 {
                rng = next_rand(rng);
                let a = (rng as usize) % N_ACCTS;
                rng = next_rand(rng);
                let mut b = (rng as usize) % N_ACCTS;
                if a == b {
                    b = (b + 1) % N_ACCTS;
                }
                rng = next_rand(rng);
                let amt = (rng % 20 + 1) as i64;
                try_transfer(&eng, &tso, a, b, amt);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Sum is conserved and no balance went negative.
    let rt = tso.now();
    let snap = eng.snapshot(rt);
    let mut sum = 0i64;
    for i in 0..N_ACCTS {
        let bal = read_balance(&snap, i).expect("final read");
        assert!(bal >= 0, "account {i} went negative: {bal}");
        sum += bal;
    }
    assert_eq!(sum, total, "money was created or destroyed");
}

fn acct(i: usize) -> String {
    format!("acct{i:02}")
}

fn read_balance(snap: &arcux_engine::Snapshot, i: usize) -> Result<i64, Error> {
    Ok(snap
        .get(acct(i).as_bytes())?
        .map(|b| String::from_utf8(b).unwrap().parse::<i64>().unwrap())
        .unwrap_or(0))
}

/// Read both balances at a snapshot, debit/credit, and commit via Percolator,
/// retrying on conflict/lock. Returns once the transfer succeeds or is skipped.
fn try_transfer(eng: &Engine, tso: &Tso, a: usize, b: usize, amt: i64) {
    for _ in 0..5000 {
        let start_ts = tso.now();
        let snap = eng.snapshot(start_ts);
        let (ba, bb) = match (read_balance(&snap, a), read_balance(&snap, b)) {
            (Ok(x), Ok(y)) => (x, y),
            _ => continue, // a key was locked by a concurrent txn → retry
        };
        if ba < amt {
            return; // insufficient funds → no-op (still conserves the sum)
        }
        // Canonical key order makes the primary deterministic, curbing livelock.
        let mut muts = vec![
            Mutation::put(acct(a).into_bytes(), (ba - amt).to_string().into_bytes()),
            Mutation::put(acct(b).into_bytes(), (bb + amt).to_string().into_bytes()),
        ];
        muts.sort_by(|m, n| m.key.cmp(&n.key));
        let txn = Transaction::new(eng, start_ts, muts).unwrap();
        match txn.prewrite(MAX_TTL) {
            Ok(()) => {
                let commit_ts = tso.now();
                txn.commit(commit_ts).unwrap();
                return;
            }
            Err(Error::Conflict(_)) | Err(Error::KeyIsLocked(_)) => continue,
            Err(e) => panic!("unexpected transfer error: {e}"),
        }
    }
}

fn next_rand(mut x: u64) -> u64 {
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

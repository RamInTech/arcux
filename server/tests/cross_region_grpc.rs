//! End-to-end Phase-5 test: **cross-region distributed Percolator over gRPC + Raft**.
//!
//! Three data nodes each host three region Raft groups (`[-inf,"g")`, `["g","n")`,
//! `["n",+inf)`, all RF=3) multiplexed over one `RaftService`. A **client-side coordinator**
//! (the [`Coordinator`] here — it holds no durable server state) drives a transaction whose
//! keys span all three regions: it splits the mutations by region and prewrites each region's
//! slice through *that region's* Raft log (primary region first), commits the **primary** in
//! its region as the single linearization point, then finalizes the secondaries. Every step is
//! a Raft proposal routed by key; the primary's fate is learned via the `CheckTxnStatus` RPC.
//!
//! It reproduces the deterministic core's cases (`cross_region_txn.rs`) over the real
//! transport:
//!
//! * a three-region transaction commits atomically and is snapshot-isolated;
//! * a coordinator that dies **after** the primary commit is recovered by a reader (roll
//!   **forward**) — the secondary's lock is followed cross-region to the committed primary;
//! * a coordinator that dies **before** commit, once its lock TTL expires, is rolled **back**
//!   by a reader (the expired primary lock is GC'd through Raft first);
//! * a cross-region write-write conflict aborts and cleans up its own partial locks;
//! * a reader that meets a still-alive primary lock **waits**.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::Options;
use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self, TxnFate};
use arcux_server::multiraft::{Regime, RegionPlacement};
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

/// A generous absolute lock lease so a healthy transaction's locks never look expired.
const BIG_TTL_ADD: u64 = 1 << 32;

/// Region boundaries (split at "g" and "n"): region A `[,"g")`, B `["g","n")`, C `["n",)`.
fn region_of(key: &[u8]) -> usize {
    if key < b"g".as_slice() {
        0
    } else if key < b"n".as_slice() {
        1
    } else {
        2
    }
}

fn put(key: &[u8], value: &[u8]) -> kv::Mutation {
    kv::Mutation { op: kv::Op::Put as i32, key: key.to_vec(), value: value.to_vec() }
}

/// What a cross-region read settled to.
#[derive(Debug, PartialEq)]
enum ReadOutcome {
    Value(Option<Vec<u8>>),
    /// The key is locked by a transaction whose primary is still alive — the reader waits.
    Locked,
}

/// One raw `Get`, resolved against whichever node answers.
enum GetOnce {
    Value(Option<Vec<u8>>),
    Locked { primary: Vec<u8>, lock_ts: u64 },
}

struct Node {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

struct Cluster {
    nodes: Vec<Node>,
    clients: Vec<KvServiceClient<Channel>>,
    _dir: tempfile::TempDir,
    _clock: Arc<dyn TimestampSource>,
}

impl Cluster {
    /// Three nodes, each hosting three regions split at "g" and "n" (all RF=3).
    async fn start() -> Cluster {
        let dir = tempfile::tempdir().expect("tempdir");
        let ids = [1u64, 2, 3];

        let mut listeners = Vec::new();
        let mut eps = HashMap::new();
        for &id in &ids {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            eps.insert(id, format!("http://{}", l.local_addr().unwrap()));
            listeners.push((id, l));
        }
        // One shared oracle so commit_ts is globally monotonic across region leaders.
        let clock: Arc<dyn TimestampSource> = Arc::new(LocalClock::new());

        let bounds: [(Vec<u8>, Vec<u8>); 3] = [
            (vec![], b"g".to_vec()),
            (b"g".to_vec(), b"n".to_vec()),
            (b"n".to_vec(), vec![]),
        ];

        let mut nodes = Vec::new();
        for (id, listener) in listeners {
            let peers: HashMap<u64, String> =
                eps.iter().filter(|(p, _)| **p != id).map(|(p, ep)| (*p, ep.clone())).collect();
            let placements: Vec<RegionPlacement> = bounds
                .iter()
                .enumerate()
                .map(|(i, (start, end))| RegionPlacement {
                    region_id: i as u64 + 1,
                    start: start.clone(),
                    end: end.clone(),
                    epoch: 1,
                    regime: Regime::Cp,
                    voters: ids.to_vec(),
                    peers: peers.clone(),
                })
                .collect();
            let state = AppState::open_multiraft(
                Options::new(dir.path().join(format!("node{id}"))),
                id,
                placements,
                clock.clone(),
            )
            .expect("open multiraft node");
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let handle = tokio::spawn(async move {
                let _ = serve_on(state, listener, async {
                    let _ = rx.await;
                })
                .await;
            });
            nodes.push(Node { shutdown: Some(tx), handle: Some(handle) });
        }

        let mut clients = Vec::new();
        for id in ids {
            clients.push(connect_retry(&eps[&id]).await);
        }
        Cluster { nodes, clients, _dir: dir, _clock: clock }
    }

    fn coordinator(&self) -> Coordinator<'_> {
        Coordinator { clients: &self.clients }
    }

    async fn stop(mut self) {
        for n in &mut self.nodes {
            if let Some(tx) = n.shutdown.take() {
                let _ = tx.send(());
            }
        }
        for n in &mut self.nodes {
            if let Some(h) = n.handle.take() {
                let _ = h.await;
            }
        }
    }
}

/// A client-side cross-region coordinator over the raw KV RPCs. It holds **no durable state**:
/// the primary lock is the source of truth, so a "crash" is just dropping this struct — any
/// later reader completes the transaction via lock resolution.
struct Coordinator<'a> {
    clients: &'a [KvServiceClient<Channel>],
}

impl Coordinator<'_> {
    /// Allocate a `start_ts` from whichever node answers (timestamps are global).
    async fn begin(&self) -> u64 {
        for _ in 0..80 {
            for c in self.clients {
                if let Ok(r) = c.clone().begin(kv::BeginRequest {}).await {
                    return r.into_inner().start_ts;
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no node served begin");
    }

    /// Autocommit a single key (used to seed prior committed state).
    async fn seed(&self, key: &[u8], value: &[u8]) {
        for _ in 0..100 {
            for c in self.clients {
                let req = kv::PutRequest {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    context: None,
                    table: String::new(),
                };
                match c.clone().put(req).await {
                    Ok(resp) => match resp.into_inner().error.and_then(|e| e.kind) {
                        None => return,
                        Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_)) => continue,
                        Some(other) => panic!("unexpected seed error: {other:?}"),
                    },
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader accepted the seed write");
    }

    /// Prewrite one region's slice (all `mutations` in the same region) through its Raft log,
    /// recording the transaction's **global** `primary`. `Err(kind)` on a real conflict.
    async fn prewrite_region(
        &self,
        start_ts: u64,
        primary: &[u8],
        mutations: &[kv::Mutation],
        ttl: u64,
    ) -> Result<(), Kind> {
        for _ in 0..80 {
            for c in self.clients {
                let req = kv::PrewriteRequest {
                    start_ts,
                    primary: primary.to_vec(),
                    mutations: mutations.to_vec(),
                    ttl,
                    context: None,
                };
                match c.clone().prewrite(req).await {
                    Ok(resp) => match resp.into_inner().errors.into_iter().next().and_then(|e| e.kind) {
                        None => return Ok(()),
                        Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_)) => continue,
                        Some(other) => return Err(other),
                    },
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader accepted prewrite");
    }

    /// Phase 1 across regions: prewrite the primary's region first, then each other region. On
    /// a conflict, roll back the regions already prewritten (their own partial locks are
    /// cleaned by the failing region's apply) and return the error.
    async fn prewrite_all(
        &self,
        start_ts: u64,
        primary: &[u8],
        mutations: &[kv::Mutation],
        ttl: u64,
    ) -> Result<(), Kind> {
        let mut by_region: BTreeMap<usize, Vec<kv::Mutation>> = BTreeMap::new();
        for m in mutations {
            by_region.entry(region_of(&m.key)).or_default().push(m.clone());
        }
        let primary_region = region_of(primary);
        let mut order: Vec<usize> = vec![primary_region];
        order.extend(by_region.keys().copied().filter(|r| *r != primary_region));

        let mut done: Vec<Vec<u8>> = Vec::new();
        for r in order {
            let muts = &by_region[&r];
            match self.prewrite_region(start_ts, primary, muts, ttl).await {
                Ok(()) => done.extend(muts.iter().map(|m| m.key.clone())),
                Err(kind) => {
                    // Undo the transaction's other regions (this region cleaned itself up).
                    let mut by: BTreeMap<usize, Vec<Vec<u8>>> = BTreeMap::new();
                    for k in done {
                        by.entry(region_of(&k)).or_default().push(k);
                    }
                    for (_, keys) in by {
                        self.resolve(keys, start_ts, 0).await;
                    }
                    return Err(kind);
                }
            }
        }
        Ok(())
    }

    /// Phase 2a: commit the **primary** in its region — the linearization point. Returns the
    /// server-allocated `commit_ts` (which every secondary is finalized at).
    async fn commit_primary(&self, start_ts: u64, primary: &[u8]) -> u64 {
        for _ in 0..80 {
            for c in self.clients {
                let req = kv::CommitRequest {
                    start_ts,
                    primary: primary.to_vec(),
                    keys: vec![primary.to_vec()],
                    context: None,
                };
                match c.clone().commit(req).await {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        match r.error.and_then(|e| e.kind) {
                            None => return r.commit_ts,
                            Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_)) => continue,
                            Some(other) => panic!("unexpected commit error: {other:?}"),
                        }
                    }
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader accepted the primary commit");
    }

    /// Roll `keys` (all one region) forward to `commit_ts` (> 0) or back (== 0), through the
    /// key region's Raft log. Drives secondary finalization and reader-driven resolution.
    async fn resolve(&self, keys: Vec<Vec<u8>>, start_ts: u64, commit_ts: u64) {
        if keys.is_empty() {
            return;
        }
        for _ in 0..80 {
            for c in self.clients {
                let req = kv::ResolveLockRequest {
                    keys: keys.clone(),
                    start_ts,
                    commit_ts,
                    context: None,
                };
                match c.clone().resolve_lock(req).await {
                    Ok(resp) => match resp.into_inner().error.and_then(|e| e.kind) {
                        None => return,
                        Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_)) => continue,
                        Some(other) => panic!("unexpected resolve error: {other:?}"),
                    },
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader accepted resolve_lock");
    }

    /// Finalize the secondaries at `commit_ts`, each in its own region.
    async fn finalize(&self, primary: &[u8], mutations: &[kv::Mutation], start_ts: u64, commit_ts: u64) {
        let mut by_region: BTreeMap<usize, Vec<Vec<u8>>> = BTreeMap::new();
        for m in mutations {
            if m.key != primary {
                by_region.entry(region_of(&m.key)).or_default().push(m.key.clone());
            }
        }
        for (_, keys) in by_region {
            self.resolve(keys, start_ts, commit_ts).await;
        }
    }

    /// Ask the primary's region for a transaction's fate. Returns `(fate, commit_ts)`.
    async fn check_txn_status(&self, primary: &[u8], start_ts: u64, now_ts: u64) -> (i32, u64) {
        for _ in 0..80 {
            for c in self.clients {
                let req = kv::CheckTxnStatusRequest {
                    primary: primary.to_vec(),
                    start_ts,
                    now_ts,
                    context: None,
                };
                match c.clone().check_txn_status(req).await {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        match r.error.as_ref().and_then(|e| e.kind.as_ref()) {
                            Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_)) => continue,
                            Some(other) => panic!("unexpected check_txn_status error: {other:?}"),
                            None => return (r.fate, r.commit_ts),
                        }
                    }
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader answered check_txn_status");
    }

    /// One raw read, retrying transient redirects until a node gives a definitive answer.
    async fn get_once(&self, key: &[u8], read_ts: u64) -> GetOnce {
        for _ in 0..80 {
            for c in self.clients {
                let req = kv::GetRequest { key: key.to_vec(), read_ts, context: None, table: String::new() };
                match c.clone().get(req).await {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        match r.error.and_then(|e| e.kind) {
                            None => return GetOnce::Value(if r.found { Some(r.value) } else { None }),
                            Some(Kind::Locked(l)) => {
                                return GetOnce::Locked { primary: l.primary, lock_ts: l.lock_ts }
                            }
                            Some(Kind::NotLeader(_))
                            | Some(Kind::RegionStale(_))
                            | Some(Kind::Retryable(_)) => continue,
                            Some(other) => panic!("unexpected get error: {other:?}"),
                        }
                    }
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader served the read");
    }

    /// A cross-region snapshot read at `read_ts`: on a leftover lock, follow it to the
    /// primary's region for the fate, resolve the local key through *its* region, then re-read.
    async fn cross_get(&self, key: &[u8], read_ts: u64) -> ReadOutcome {
        for _ in 0..40 {
            match self.get_once(key, read_ts).await {
                GetOnce::Value(v) => return ReadOutcome::Value(v),
                GetOnce::Locked { primary, lock_ts } => {
                    // "At my read timestamp, is this transaction's primary lock expired?"
                    let (fate, commit_ts) = self.check_txn_status(&primary, lock_ts, read_ts).await;
                    if fate == TxnFate::Committed as i32 {
                        self.resolve(vec![key.to_vec()], lock_ts, commit_ts).await;
                    } else if fate == TxnFate::RolledBack as i32 {
                        self.resolve(vec![key.to_vec()], lock_ts, 0).await;
                    } else {
                        return ReadOutcome::Locked; // primary alive → wait
                    }
                }
            }
        }
        panic!("cross_get did not settle");
    }
}

async fn connect_retry(ep: &str) -> KvServiceClient<Channel> {
    for _ in 0..100 {
        if let Ok(c) = KvServiceClient::connect(ep.to_string()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    panic!("could not connect to {ep}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn commit_across_regions_is_atomic_and_snapshot_isolated() {
    let cluster = Cluster::start().await;
    let co = cluster.coordinator();

    // Keys land in three different regions; the primary "acct" is in region A.
    let muts = vec![put(b"acct", b"A"), put(b"item", b"B"), put(b"post", b"C")];
    let start_ts = co.begin().await;
    co.prewrite_all(start_ts, b"acct", &muts, start_ts + BIG_TTL_ADD).await.expect("prewrite ok");
    let commit_ts = co.commit_primary(start_ts, b"acct").await;
    co.finalize(b"acct", &muts, start_ts, commit_ts).await;

    // Visible at a snapshot after the commit...
    assert_eq!(co.cross_get(b"acct", commit_ts + 1).await, ReadOutcome::Value(Some(b"A".to_vec())));
    assert_eq!(co.cross_get(b"item", commit_ts + 1).await, ReadOutcome::Value(Some(b"B".to_vec())));
    assert_eq!(co.cross_get(b"post", commit_ts + 1).await, ReadOutcome::Value(Some(b"C".to_vec())));

    // ...and invisible to a snapshot taken before it committed (snapshot isolation).
    assert_eq!(co.cross_get(b"item", start_ts).await, ReadOutcome::Value(None));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reader_rolls_forward_after_coordinator_dies_post_primary_commit() {
    let cluster = Cluster::start().await;
    let co = cluster.coordinator();

    // primary "acct" (region A), secondary "item" (region B).
    let muts = vec![put(b"acct", b"A"), put(b"item", b"B")];
    let start_ts = co.begin().await;
    co.prewrite_all(start_ts, b"acct", &muts, start_ts + BIG_TTL_ADD).await.expect("prewrite ok");
    // Commit the primary (the txn is now committed) then *crash* — the secondary in region B
    // is left locked, never finalized.
    let commit_ts = co.commit_primary(start_ts, b"acct").await;

    // A reader of the secondary follows its lock cross-region to the committed primary and
    // rolls the secondary forward — returning the committed value.
    assert_eq!(co.cross_get(b"item", commit_ts + 1).await, ReadOutcome::Value(Some(b"B".to_vec())));
    // The lock is gone now: a plain read sees the value too.
    match co.get_once(b"item", commit_ts + 1).await {
        GetOnce::Value(v) => assert_eq!(v, Some(b"B".to_vec())),
        GetOnce::Locked { .. } => panic!("secondary should be resolved"),
    }

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reader_rolls_back_after_coordinator_dies_pre_commit_and_ttl_expires() {
    let cluster = Cluster::start().await;
    let co = cluster.coordinator();

    // Seed a prior committed value for the secondary so the rollback reveals it.
    co.seed(b"item", b"old").await;

    // A second txn prewrites both keys with a SHORT ttl, then crashes before committing.
    let muts = vec![put(b"acct", b"A"), put(b"item", b"B")];
    let start_ts = co.begin().await;
    let short_ttl = start_ts + 5;
    co.prewrite_all(start_ts, b"acct", &muts, short_ttl).await.expect("prewrite ok");

    // A reader arrives past the lease (read_ts > ttl, and ≥ the lock's start_ts so the lock is
    // visible): the primary's lock is expired, so the txn is declared dead (its primary lock
    // GC'd through Raft) and the secondary rolls back to the prior value.
    let far_future = start_ts + 10_000;
    assert_eq!(co.cross_get(b"item", far_future).await, ReadOutcome::Value(Some(b"old".to_vec())));
    // The primary was killed too, so it shows no committed value.
    assert_eq!(co.cross_get(b"acct", far_future).await, ReadOutcome::Value(None));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_region_write_write_conflict_aborts_and_cleans_up() {
    let cluster = Cluster::start().await;
    let co = cluster.coordinator();

    // The loser takes its snapshot first…
    let loser_start = co.begin().await;

    // …then a winner fully commits "item" at a later commit_ts (single-region, primary "item").
    let win = vec![put(b"item", b"win")];
    let ws = co.begin().await;
    co.prewrite_all(ws, b"item", &win, ws + BIG_TTL_ADD).await.expect("winner prewrite");
    let wc = co.commit_primary(ws, b"item").await;

    // The loser (stale snapshot) prewrites its primary in A, then conflicts on "item" in B.
    let lose = vec![put(b"acct", b"la"), put(b"item", b"lose")];
    let err = co
        .prewrite_all(loser_start, b"acct", &lose, loser_start + BIG_TTL_ADD)
        .await
        .expect_err("must conflict");
    assert!(
        matches!(err, Kind::Conflict(_) | Kind::Retryable(_)),
        "expected a write-write conflict, got {err:?}",
    );

    // The aborted txn cleaned up its primary lock in region A, so a fresh txn can take it.
    let ns = co.begin().await;
    co.prewrite_all(ns, b"acct", &[put(b"acct", b"new")], ns + BIG_TTL_ADD)
        .await
        .expect("aborted loser must have freed its primary lock");
    // The winner's value is intact.
    assert_eq!(co.cross_get(b"item", wc + 1).await, ReadOutcome::Value(Some(b"win".to_vec())));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reader_waits_on_a_still_alive_primary_lock() {
    let cluster = Cluster::start().await;
    let co = cluster.coordinator();

    // primary "acct" (A), secondary "item" (B); a generous ttl keeps the primary lock alive.
    let muts = vec![put(b"acct", b"A"), put(b"item", b"B")];
    let start_ts = co.begin().await;
    co.prewrite_all(start_ts, b"acct", &muts, start_ts + BIG_TTL_ADD).await.expect("prewrite ok");
    // No commit.

    // A reader within the lease meets the secondary lock, finds the primary still alive, and
    // must wait rather than guess.
    assert_eq!(co.cross_get(b"item", start_ts + 10).await, ReadOutcome::Locked);

    cluster.stop().await;
}

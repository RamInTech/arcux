//! End-to-end Phase-4b+ tests: a full **multi-key transaction** (begin → prewrite →
//! commit) replicated through Raft across three data nodes. They prove a committed
//! transaction is readable, **survives a leader kill** (both keys intact — zero loss), and
//! that a **write-write conflict** is detected at apply time. Each Percolator step is a Raft
//! command whose conflict-check runs deterministically on every replica.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::Options;
use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

const LEASE: u64 = 1 << 32;

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
        let clock: Arc<dyn TimestampSource> = Arc::new(LocalClock::new());

        let mut nodes = Vec::new();
        for (id, listener) in listeners {
            let peers: HashMap<u64, String> =
                eps.iter().filter(|(p, _)| **p != id).map(|(p, ep)| (*p, ep.clone())).collect();
            let state = AppState::open_replicated(
                Options::new(dir.path().join(format!("node{id}"))),
                id,
                ids.to_vec(),
                peers,
                clock.clone(),
            )
            .expect("open replicated node");
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

    /// Allocate a `start_ts` from whichever node answers (timestamps are global).
    async fn begin(&self) -> u64 {
        for _ in 0..80 {
            for c in &self.clients {
                if let Ok(r) = c.clone().begin(kv::BeginRequest {}).await {
                    return r.into_inner().start_ts;
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no node served begin");
    }

    /// Prewrite all mutations against the leader (others reply `NotLeader`). `Ok(leader_idx)`
    /// on success; `Err(kind)` on a real per-key error (conflict / lock).
    async fn prewrite(
        &self,
        start_ts: u64,
        primary: &[u8],
        mutations: &[kv::Mutation],
    ) -> Result<usize, Kind> {
        for _ in 0..80 {
            for (i, c) in self.clients.iter().enumerate() {
                let req = kv::PrewriteRequest {
                    start_ts,
                    primary: primary.to_vec(),
                    mutations: mutations.to_vec(),
                    ttl: start_ts.saturating_add(LEASE),
                    context: None,
                };
                match c.clone().prewrite(req).await {
                    Ok(resp) => match resp.into_inner().errors.into_iter().next().and_then(|e| e.kind) {
                        None => return Ok(i),
                        Some(Kind::NotLeader(_)) => continue,
                        Some(other) => return Err(other),
                    },
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader accepted prewrite");
    }

    /// Commit against the leader; returns the leader index. Panics on an unexpected error.
    async fn commit(&self, start_ts: u64, primary: &[u8], keys: &[Vec<u8>]) -> usize {
        for _ in 0..80 {
            for (i, c) in self.clients.iter().enumerate() {
                let req = kv::CommitRequest {
                    start_ts,
                    primary: primary.to_vec(),
                    keys: keys.to_vec(),
                    context: None,
                };
                match c.clone().commit(req).await {
                    Ok(resp) => match resp.into_inner().error.and_then(|e| e.kind) {
                        None => return i,
                        Some(Kind::NotLeader(_)) => continue,
                        Some(other) => panic!("unexpected commit error: {other:?}"),
                    },
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader accepted commit");
    }

    /// Read `key` from the leader (followers redirect; a transient lock is retried).
    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        for _ in 0..80 {
            for c in &self.clients {
                let req = kv::GetRequest { key: key.to_vec(), read_ts: 0, context: None };
                match c.clone().get(req).await {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        match r.error.and_then(|e| e.kind) {
                            None => return if r.found { Some(r.value) } else { None },
                            Some(Kind::NotLeader(_)) | Some(Kind::Retryable(_)) => continue,
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

    /// Run a full transaction over `(key, value)` pairs (first is the primary). Returns the
    /// leader index that committed it.
    async fn transact(&self, pairs: &[(&[u8], &[u8])]) -> usize {
        let primary = pairs[0].0.to_vec();
        let mutations: Vec<kv::Mutation> = pairs
            .iter()
            .map(|(k, v)| kv::Mutation { op: kv::Op::Put as i32, key: k.to_vec(), value: v.to_vec() })
            .collect();
        let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.to_vec()).collect();
        let start_ts = self.begin().await;
        self.prewrite(start_ts, &primary, &mutations).await.expect("prewrite ok");
        self.commit(start_ts, &primary, &keys).await
    }

    async fn stop_node(&mut self, idx: usize) {
        if let Some(tx) = self.nodes[idx].shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.nodes[idx].handle.take() {
            let _ = h.await;
        }
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

async fn connect_retry(ep: &str) -> KvServiceClient<Channel> {
    for _ in 0..100 {
        if let Ok(c) = KvServiceClient::connect(ep.to_string()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    panic!("could not connect to {ep}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replicated_transaction_survives_a_leader_kill() {
    let mut cluster = Cluster::start().await;

    // A two-key transaction (primary "acct/a") replicated through the log.
    let leader = cluster.transact(&[(b"acct/a", b"100"), (b"acct/b", b"200")]).await;
    assert_eq!(cluster.get(b"acct/a").await, Some(b"100".to_vec()));
    assert_eq!(cluster.get(b"acct/b").await, Some(b"200".to_vec()));

    // Kill the leader; the surviving majority elects a new one.
    cluster.stop_node(leader).await;

    // Both keys of the committed transaction survive — zero acknowledged loss.
    assert_eq!(cluster.get(b"acct/a").await, Some(b"100".to_vec()));
    assert_eq!(cluster.get(b"acct/b").await, Some(b"200".to_vec()));

    // The cluster keeps serving transactions under the new leader.
    let new_leader = cluster.transact(&[(b"acct/c", b"300")]).await;
    assert_ne!(new_leader, leader, "a different node leads after the kill");
    assert_eq!(cluster.get(b"acct/c").await, Some(b"300".to_vec()));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn write_write_conflict_is_detected_at_apply() {
    let cluster = Cluster::start().await;

    // T2 takes its snapshot first…
    let t2_start = cluster.begin().await;

    // …then T1 fully commits "x" at a later commit_ts.
    cluster.transact(&[(b"x", b"t1")]).await;

    // T2 now prewrites "x" at its stale snapshot: a committed version newer than its
    // start_ts exists, so the prewrite must conflict (detected at apply on the leader).
    let muts = vec![kv::Mutation { op: kv::Op::Put as i32, key: b"x".to_vec(), value: b"t2".to_vec() }];
    let err = cluster.prewrite(t2_start, b"x", &muts).await.expect_err("must conflict");
    assert!(
        matches!(err, Kind::Conflict(_) | Kind::Retryable(_)),
        "expected a write conflict, got {err:?}",
    );
    // T1's value stands.
    assert_eq!(cluster.get(b"x").await, Some(b"t1".to_vec()));

    cluster.stop().await;
}

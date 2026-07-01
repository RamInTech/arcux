//! End-to-end Phase-5b test: **one cluster serving two consistency regimes, chosen by a
//! `create_table` declaration.** A CP table and an AP table are declared in the catalog; the
//! server routes each key to its region's regime — a CP key gets the transactional Raft path,
//! an AP key gets the leaderless always-available path — proving the regime comes from the
//! **declaration**, not from hand-wired placement.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::Options;
use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_server::catalog::Catalog;
use arcux_server::multiraft::Regime;
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

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
    /// Three nodes hosting two regions whose regimes are **derived from `catalog`**: an
    /// `acct/` region (CP) and a `feed/` region (AP), split at `"feed/"`.
    async fn start(catalog: &Catalog) -> Cluster {
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
            // Regimes come straight from the catalog: region 1 = [-inf,"feed/") (CP, holds
            // acct/*), region 2 = ["feed/",+inf) (AP, holds feed/*).
            let placements = vec![
                catalog.place(1, vec![], b"feed/".to_vec(), ids.to_vec(), peers.clone()),
                catalog.place(2, b"feed/".to_vec(), vec![], ids.to_vec(), peers.clone()),
            ];
            let state = AppState::open_multiraft(
                Options::new(dir.path().join(format!("node{id}"))),
                id,
                placements,
                clock.clone(),
            )
            .expect("open node");
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

    /// Put by trying each node until one accepts (the CP leader, or any AP replica); others
    /// reply `NotLeader`. Retries across rounds to ride out a CP election.
    async fn put(&self, key: &[u8], value: &[u8]) {
        for _ in 0..100 {
            for c in &self.clients {
                let req =
                    kv::PutRequest { key: key.to_vec(), value: value.to_vec(), context: None };
                match c.clone().put(req).await {
                    Ok(resp) => match resp.into_inner().error.and_then(|e| e.kind) {
                        None => return,
                        Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_)) => continue,
                        Some(other) => panic!("unexpected put error: {other:?}"),
                    },
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no node accepted the write for {key:?}");
    }

    /// Read, retrying across nodes/rounds until `expected` shows up.
    async fn get_until(&self, key: &[u8], expected: &[u8]) {
        for _ in 0..100 {
            for c in &self.clients {
                let req = kv::GetRequest { key: key.to_vec(), read_ts: 0, context: None };
                if let Ok(resp) = c.clone().get(req).await {
                    let r = resp.into_inner();
                    match r.error.and_then(|e| e.kind) {
                        None if r.found && r.value == expected => return,
                        _ => continue,
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("never observed {expected:?} for {key:?}");
    }

    /// Put to a specific node (used to show the AP write succeeds on a lone survivor).
    async fn put_on(&self, node: usize, key: &[u8], value: &[u8]) {
        let req = kv::PutRequest { key: key.to_vec(), value: value.to_vec(), context: None };
        let resp = self.clients[node].clone().put(req).await.expect("put rpc").into_inner();
        assert!(resp.error.is_none(), "AP put on node {node} should succeed: {:?}", resp.error);
    }

    async fn get_on(&self, node: usize, key: &[u8]) -> Option<Vec<u8>> {
        let req = kv::GetRequest { key: key.to_vec(), read_ts: 0, context: None };
        let r = self.clients[node].clone().get(req).await.expect("get rpc").into_inner();
        r.found.then_some(r.value)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn create_table_selects_the_consistency_regime() {
    // Declare two tables — the whole point is that this declaration picks the path.
    let mut cat = Catalog::new();
    cat.create_table("acct", Regime::Cp); // strong: transactional, leader-based
    cat.create_table("feed", Regime::Ap); // available: leaderless, LWW

    let mut cluster = Cluster::start(&cat).await;

    // `acct/*` → CP path: a replicated write, readable (leader-based, Snapshot Isolation).
    cluster.put(b"acct/alice", b"100").await;
    cluster.get_until(b"acct/alice", b"100").await;

    // `feed/*` → AP path: a leaderless write, readable from any replica.
    cluster.put_on(0, b"feed/post1", b"liked").await;
    cluster.get_until(b"feed/post1", b"liked").await;

    // The AP declaration's payoff: kill a majority; the `feed/` (AP) table stays
    // write-available on the lone survivor — where the `acct/` (CP) table could not commit.
    cluster.stop_node(1).await;
    cluster.stop_node(2).await;
    cluster.put_on(0, b"feed/post2", b"still-liked").await; // succeeds despite 2/3 down
    assert_eq!(cluster.get_on(0, b"feed/post2").await, Some(b"still-liked".to_vec()));
    assert_eq!(cluster.get_on(0, b"feed/post1").await, Some(b"liked".to_vec()));

    cluster.stop().await;
}

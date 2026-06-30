//! End-to-end Phase-5 tests for the **AP** (always-available) write path: a leaderless
//! region replicated by HLC-stamped, Last-Writer-Wins writes. They prove a write is accepted
//! by **any** node and propagates to the others (LWW), and — the headline AP property — that
//! a write **still succeeds when a majority of replicas is down**, where the CP (Raft) path
//! would refuse for lack of a quorum.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::Options;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_server::multiraft::{Regime, RegionPlacement};
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

struct Node {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

/// Three nodes hosting one **AP** region covering the whole keyspace (RF=3).
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
        // AP needs no shared TSO, but open_* wants a clock for the (here unused) CP path.
        let clock: Arc<dyn TimestampSource> = Arc::new(LocalClock::new());

        let mut nodes = Vec::new();
        for (id, listener) in listeners {
            let peers: HashMap<u64, String> =
                eps.iter().filter(|(p, _)| **p != id).map(|(p, ep)| (*p, ep.clone())).collect();
            let placement = RegionPlacement {
                region_id: 1,
                start: vec![],
                end: vec![],
                epoch: 1,
                regime: Regime::Ap,
                voters: ids.to_vec(),
                peers,
            };
            let state = AppState::open_multiraft(
                Options::new(dir.path().join(format!("node{id}"))),
                id,
                vec![placement],
                clock.clone(),
            )
            .expect("open AP node");
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

    /// Put on a specific node (the AP coordinator). Returns the HLC stamp (commit_ts).
    async fn put(&self, node: usize, key: &[u8], value: &[u8]) -> u64 {
        let req = kv::PutRequest { key: key.to_vec(), value: value.to_vec(), context: None };
        let resp = self.clients[node].clone().put(req).await.expect("put rpc").into_inner();
        assert!(resp.error.is_none(), "AP put should never error: {:?}", resp.error);
        resp.commit_ts
    }

    /// Read on a specific node (any AP replica serves).
    async fn get(&self, node: usize, key: &[u8]) -> Option<Vec<u8>> {
        let req = kv::GetRequest { key: key.to_vec(), read_ts: 0, context: None };
        let r = self.clients[node].clone().get(req).await.expect("get rpc").into_inner();
        match r.error.and_then(|e| e.kind) {
            None => r.found.then_some(r.value),
            Some(other) => panic!("unexpected get error: {other:?}"),
        }
    }

    /// Read on `node`, retrying until it returns `expected` (AP propagation is async).
    async fn get_until(&self, node: usize, key: &[u8], expected: &[u8]) {
        for _ in 0..100 {
            if self.get(node, key).await.as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("node {node} never converged on {expected:?} for {key:?}");
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
async fn ap_writes_are_leaderless_and_resolve_last_writer_wins() {
    let cluster = Cluster::start().await;

    // A write to node 0 (no leader needed) is readable there immediately and propagates.
    cluster.put(0, b"k", b"v1").await;
    assert_eq!(cluster.get(0, b"k").await, Some(b"v1".to_vec()));
    cluster.get_until(1, b"k", b"v1").await; // reached node 1 via fan-out (it observed the HLC)

    // A *later* write to node 1 (its HLC has observed v1, so its stamp is higher) wins
    // everywhere — Last-Writer-Wins by HLC.
    cluster.put(1, b"k", b"v2").await;
    cluster.get_until(0, b"k", b"v2").await;
    cluster.get_until(2, b"k", b"v2").await;

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ap_stays_available_when_a_majority_is_down() {
    let mut cluster = Cluster::start().await;

    cluster.put(0, b"x", b"1").await;

    // Kill the other two replicas — node 0 is now a lone minority. A CP (Raft) region could
    // not commit here (no quorum); the AP path accepts the write anyway (W=1, fan-out to the
    // dead peers is best-effort and ignored).
    cluster.stop_node(1).await;
    cluster.stop_node(2).await;

    cluster.put(0, b"y", b"2").await; // succeeds despite 2/3 replicas down — the AP property
    assert_eq!(cluster.get(0, b"y").await, Some(b"2".to_vec()));
    assert_eq!(cluster.get(0, b"x").await, Some(b"1".to_vec()));

    cluster.stop().await;
}

//! End-to-end Phase-5b tests: **AP convergence** — anti-entropy + read-repair.
//!
//! The Phase-5 AP path is *available* during a partition (W=1, best-effort fan-out) but a write
//! that misses a partitioned/crashed replica is never re-delivered, so replicas can drift
//! apart permanently. These tests prove the two mechanisms that close that gap converge them:
//!
//! * **anti-entropy** — a background Merkle-digest reconcile between replicas heals *any* key,
//!   read or not;
//! * **read-repair** — an AP read returns the Last-Writer-Wins winner and heals the stale
//!   replicas inline.
//!
//! To create divergence deterministically (no flaky real partition), a version is injected into
//! **one** replica via the internal `ReplicateAp` RPC — exactly the state a healed partition
//! leaves: one node holds a write the others lack. Each mechanism is then isolated by whether
//! the **background** anti-entropy pass is enabled (`set_anti_entropy_interval_ms`).

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

const REGION_ID: u64 = 1;
/// A distinctive HLC stamp for injected versions (nothing else writes these keys, so any
/// value wins LWW; a fixed one keeps the tests deterministic).
const INJECT_HLC: u64 = 42_000_000;

struct Node {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

struct Cluster {
    nodes: Vec<Node>,
    states: Vec<Arc<AppState>>,
    clients: Vec<KvServiceClient<Channel>>,
    _dir: tempfile::TempDir,
    _clock: Arc<dyn TimestampSource>,
}

impl Cluster {
    /// Three nodes hosting one AP region over the whole keyspace (RF=3).
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
        let mut states = Vec::new();
        for (id, listener) in listeners {
            let peers: HashMap<u64, String> =
                eps.iter().filter(|(p, _)| **p != id).map(|(p, ep)| (*p, ep.clone())).collect();
            let placement = RegionPlacement {
                region_id: REGION_ID,
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
            let served = state.clone();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let handle = tokio::spawn(async move {
                let _ = serve_on(served, listener, async {
                    let _ = rx.await;
                })
                .await;
            });
            nodes.push(Node { shutdown: Some(tx), handle: Some(handle) });
            states.push(state);
        }

        let mut clients = Vec::new();
        for id in ids {
            clients.push(connect_retry(&eps[&id]).await);
        }
        Cluster { nodes, states, clients, _dir: dir, _clock: clock }
    }

    /// Turn the background anti-entropy pass up/down on every node.
    fn set_anti_entropy_ms(&self, ms: u64) {
        for s in &self.states {
            s.set_anti_entropy_interval_ms(ms);
        }
    }

    /// Inject a version into **one** node only (via ReplicateAp) — the divergence a healed
    /// partition leaves: node `node` holds `key=value@hlc`, the others don't.
    async fn inject(&self, node: usize, key: &[u8], value: &[u8], hlc: u64) {
        let req = kv::ReplicateApRequest {
            region_id: REGION_ID,
            key: key.to_vec(),
            value: value.to_vec(),
            is_delete: false,
            hlc_ts: hlc,
        };
        self.clients[node].clone().replicate_ap(req).await.expect("inject via replicate_ap");
    }

    async fn get(&self, node: usize, key: &[u8]) -> Option<Vec<u8>> {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn anti_entropy_converges_a_divergent_replica_in_the_background() {
    let mut cluster = Cluster::start().await;
    // Background anti-entropy on and brisk.
    cluster.set_anti_entropy_ms(80);

    // A write reached only node 0 (as if the fan-out to 1 & 2 was dropped by a partition).
    cluster.inject(0, b"apple", b"red", INJECT_HLC).await;

    // Give the background reconcile time to pull it to nodes 1 and 2 (no reads involved).
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Isolate node 1: kill node 0 (the origin) AND node 2, so a read on node 1 has **no live
    // peer** — read-repair can fetch nothing, so what node 1 returns is purely its *own*
    // engine state. If it's the value, background anti-entropy delivered it.
    cluster.stop_node(0).await;
    cluster.stop_node(2).await;
    assert_eq!(
        cluster.get(1, b"apple").await,
        Some(b"red".to_vec()),
        "background anti-entropy must have converged node 1 without any read",
    );

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn read_repair_returns_and_heals_the_winner() {
    let mut cluster = Cluster::start().await;
    // Disable background anti-entropy so ONLY read-repair can move data.
    cluster.set_anti_entropy_ms(3_600_000);

    // The write reached only node 0.
    cluster.inject(0, b"banana", b"yellow", INJECT_HLC).await;

    // A read on node 1 (which lacks the key) must return the winner — read-repair fetches it
    // from node 0 inline. (Anti-entropy is off, so this can only be read-repair.)
    assert_eq!(
        cluster.get(1, b"banana").await,
        Some(b"yellow".to_vec()),
        "read-repair must return the LWW winner from a peer",
    );

    // Read-repair also *heals*: node 1 adopted it locally and pushed it to node 2. Kill node 0
    // (the only original holder); the value must still be readable, proving it was persisted on
    // the survivors, not merely proxied.
    cluster.stop_node(0).await;
    // node 1 adopted it inline; give the async push to node 2 a moment, then verify both.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(cluster.get(1, b"banana").await, Some(b"yellow".to_vec()), "node 1 kept the repair");
    assert_eq!(cluster.get(2, b"banana").await, Some(b"yellow".to_vec()), "node 2 was healed too");

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn anti_entropy_reconciles_both_directions() {
    // Different keys diverged on different nodes; after reconciliation each node must hold
    // *every* key — convergence flows both ways because each node pulls from each peer. We
    // isolate each node in turn (kill the other two) and confirm it holds all three keys from
    // its own engine alone (no read-repair possible), so a fresh cluster is built per target.
    for target in [0usize, 1, 2] {
        let mut c = Cluster::start().await;
        c.set_anti_entropy_ms(80);
        c.inject(0, b"k-a", b"A", INJECT_HLC).await; // only on node 0
        c.inject(1, b"k-b", b"B", INJECT_HLC).await; // only on node 1
        c.inject(2, b"k-c", b"C", INJECT_HLC).await; // only on node 2

        // Let a few reconcile rounds carry every key to every node.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        for other in [0usize, 1, 2] {
            if other != target {
                c.stop_node(other).await;
            }
        }
        assert_eq!(c.get(target, b"k-a").await, Some(b"A".to_vec()), "node {target} missing k-a");
        assert_eq!(c.get(target, b"k-b").await, Some(b"B".to_vec()), "node {target} missing k-b");
        assert_eq!(c.get(target, b"k-c").await, Some(b"C".to_vec()), "node {target} missing k-c");
        c.stop().await;
    }
}

//! `create table` on a real cluster — the case that has never worked.
//!
//! Until now the node carved its own routing table, told nobody, and (because a catalog tiling
//! numbers regions positionally) would have left the same region id naming a different key range
//! on each node. That was rejected outright. Here PD owns the region table: a declaration is
//! carved once, through Raft, with a stable id, and pushed to every voter — so a table created
//! against one node is immediately usable through another.

use std::collections::HashMap;
use std::sync::Arc;

use arcux_client::Client;
use arcux_engine::Options;
use arcux_pd::raft_server::{self, DEFAULT_FD_INTERVAL_MS, DEFAULT_FD_TIMEOUT_MS};
use arcux_rpc::kv::Regime;
use arcux_server::multiraft::Regime as ServerRegime;
use arcux_server::{open_catalog_node, serve_on, AppState};
use tokio::net::TcpListener;

const IDS: [u64; 3] = [1, 2, 3];

struct Cluster {
    endpoints: HashMap<u64, String>,
    states: HashMap<u64, Arc<AppState>>,
    shutdowns: Vec<Option<tokio::sync::oneshot::Sender<()>>>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Cluster {
    /// A single-node **replicated** PD (the single-process one keeps no region table, so it
    /// cannot carve) plus three catalog nodes, each attached to it and each declaring nothing —
    /// every table in these tests arrives through PD.
    async fn start() -> Cluster {
        let pd_dir = tempfile::tempdir().expect("tempdir");
        let pd_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind pd");
        let pd_addr = format!("http://{}", pd_listener.local_addr().unwrap());
        let pd_group =
            raft_server::start_group(1, HashMap::from([(1, pd_addr.clone())]), pd_dir.path())
                .expect("start pd");
        let (pd_tx, pd_rx) = tokio::sync::oneshot::channel::<()>();
        {
            let g = pd_group.clone();
            tokio::spawn(async move {
                let _ = raft_server::serve_on(
                    g,
                    pd_listener,
                    DEFAULT_FD_TIMEOUT_MS,
                    DEFAULT_FD_INTERVAL_MS,
                    async {
                        let _ = pd_rx.await;
                    },
                )
                .await;
            });
        }
        for _ in 0..100 {
            if pd_group.is_leader() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(pd_group.is_leader(), "PD never elected a leader");

        // Bind every node first so each learns the others' real ephemeral addresses.
        let mut bound = Vec::new();
        for id in IDS {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind node");
            let ep = format!("http://{}", l.local_addr().unwrap());
            bound.push((id, l, ep));
        }
        let endpoints: HashMap<u64, String> =
            bound.iter().map(|(id, _, ep)| (*id, ep.clone())).collect();

        let mut shutdowns = vec![Some(pd_tx)];
        let mut dirs = vec![pd_dir];
        let mut states = HashMap::new();
        for (id, listener, ep) in bound {
            let dir = tempfile::tempdir().expect("tempdir");
            let peers: HashMap<u64, String> =
                endpoints.iter().filter(|(p, _)| **p != id).map(|(p, a)| (*p, a.clone())).collect();
            let state = open_catalog_node(
                Options::new(dir.path()),
                id,
                IDS.to_vec(),
                peers,
                Vec::new(),
            )
            .expect("open_catalog_node");
            state.attach_pd(pd_addr.clone(), ep.clone()).await.expect("attach_pd");

            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let serving = state.clone();
            tokio::spawn(async move {
                let _ = serve_on(serving, listener, async {
                    let _ = rx.await;
                })
                .await;
            });
            states.insert(id, state);
            shutdowns.push(Some(tx));
            dirs.push(dir);
        }

        Cluster { endpoints, states, shutdowns, _dirs: dirs }
    }

    /// A client pinned to one node — used to prove *which* node a declaration was made against.
    fn client(&self, id: u64) -> Client {
        Client::connect(self.endpoints[&id].clone()).expect("connect")
    }

    /// A leader-following client that starts at `id`. A CP region's writes must reach its Raft
    /// leader, which is a routing concern independent of this pass; what matters here is that the
    /// region exists and is reachable *at all* from a node that did not create it.
    fn cluster_client_from(&self, id: u64) -> Client {
        let mut eps = vec![self.endpoints[&id].clone()];
        eps.extend(self.endpoints.iter().filter(|(k, _)| **k != id).map(|(_, v)| v.clone()));
        Client::connect_cluster(eps).expect("connect cluster")
    }

    async fn stop(mut self) {
        for sd in self.shutdowns.iter_mut() {
            if let Some(tx) = sd.take() {
                let _ = tx.send(());
            }
        }
    }
}

/// Retries a write past the window where a brand-new region's replicas are still converging.
async fn put_until_ready(c: &mut Client, table: &str, key: &[u8], value: &[u8]) -> bool {
    for _ in 0..60 {
        if c.put(table, key.to_vec(), value.to_vec()).await.is_ok() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    false
}

/// The payoff: declare against one node, use it through another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_created_on_one_node_is_usable_through_another() {
    let cluster = Cluster::start().await;

    let mut creator = cluster.client(1);
    let (region_id, start, end) = creator.create_table("orders", Regime::Cp).await.unwrap();
    assert!(region_id > 0);
    assert_eq!((start, end), (b"orders/".to_vec(), b"orders0".to_vec()));

    // The case that was impossible: a client that never spoke to the creating node can use it.
    let mut other = cluster.cluster_client_from(3);
    assert!(
        put_until_ready(&mut other, "orders", b"o1", b"100").await,
        "node 3 never became able to serve a table created on node 1"
    );
    assert_eq!(other.get("orders", b"o1".to_vec()).await.unwrap(), Some(b"100".to_vec()));

    // And the write is visible from the node that created it — one region, not three.
    assert_eq!(creator.get("orders", b"o1".to_vec()).await.unwrap(), Some(b"100".to_vec()));

    // Every node agrees on the region id, which is the thing positional tiling could not give.
    for id in IDS {
        let hosted: Vec<u64> = cluster.states[&id].hosted_region_ids();
        assert!(hosted.contains(&region_id), "node {id} does not host region {region_id}");
    }

    cluster.stop().await;
}

/// An AP table converges the same way, without a quorum gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ap_table_created_on_one_node_is_usable_through_another() {
    let cluster = Cluster::start().await;

    cluster.client(1).create_table("clicks", Regime::Ap).await.unwrap();

    let mut other = cluster.cluster_client_from(2);
    assert!(put_until_ready(&mut other, "clicks", b"c1", b"tap").await);
    assert_eq!(other.get("clicks", b"c1".to_vec()).await.unwrap(), Some(b"tap".to_vec()));

    cluster.stop().await;
}

/// Reconciling twice must change nothing. A blind re-found would open a second WAL and spawn a
/// second actor thread per region, every tick — the trap `MultiRaft::insert`'s map-level
/// idempotence hides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciling_again_is_a_no_op() {
    let cluster = Cluster::start().await;
    cluster.client(1).create_table("orders", Regime::Cp).await.unwrap();

    let node = cluster.states[&2].clone();
    let before = node.hosted_region_ids();
    let version = node.reconcile().await.expect("reconcile");
    let again = node.reconcile().await.expect("reconcile");

    assert_eq!(version, again, "an already-applied version is ignored, not re-applied");
    assert_eq!(node.hosted_region_ids(), before, "membership unchanged");

    cluster.stop().await;
}

/// Split and merge are refused while PD owns the region table — two authorities mutating one
/// table is how a cluster diverges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_is_rejected_while_pd_owns_the_region_table() {
    let cluster = Cluster::start().await;

    let err = cluster.client(1).split_region(b"m".to_vec()).await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("PD owns the region table"), "unexpected error: {msg}");

    cluster.stop().await;
}

/// `--table` flags still declare tables for a non-PD node; this pins that the PD path did not
/// change the single-node contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_without_pd_still_carves_locally() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = open_catalog_node(
        Options::new(dir.path()),
        1,
        vec![1],
        HashMap::new(),
        vec![("ledger".to_string(), ServerRegime::Cp)],
    )
    .expect("open_catalog_node");
    assert_eq!(state.declared_tables().len(), 1);
    assert_eq!(state.reconcile().await.expect("no-op without PD"), 0);
}

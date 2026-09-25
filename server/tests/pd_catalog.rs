//! A node attached to the **single-process** PD.
//!
//! That PD keeps no region table and no catalog — only the replicated one can carve, because
//! allocating a table id and splitting a region have to go through a log every replica applies.
//! It answers with catalog version 0, which a node reads as "nothing to adopt", so a node
//! attached to it keeps serving exactly the regions it opened with.
//!
//! What PD does still learn here is **placement**: each region a node reports, how it is served,
//! and the replica set holding it. `pd_cluster_tables.rs` and `create_table.rs` cover the
//! replicated PD that owns the catalog.

use std::net::SocketAddr;
use std::sync::Arc;

use arcux_client::Client;
use arcux_engine::Options;
use arcux_pd::server::serve_on as pd_serve_on;
use arcux_pd::{Pd, Regime as PdRegime};
use arcux_rpc::kv::Regime;
use arcux_server::multiraft::Regime as ServerRegime;
use arcux_server::{open_catalog_node, serve_on, AppState};
use tokio::net::TcpListener;

struct Harness {
    pd: Arc<Pd>,
    pd_endpoint: String,
    nodes: Vec<Node>,
    shutdowns: Vec<Option<tokio::sync::oneshot::Sender<()>>>,
    _dirs: Vec<tempfile::TempDir>,
}

struct Node {
    addr: SocketAddr,
    _state: Arc<AppState>,
}

impl Harness {
    async fn start() -> Harness {
        let pd = Arc::new(Pd::seeded(Vec::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind pd");
        let pd_addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let pd = pd.clone();
            tokio::spawn(async move {
                let _ = pd_serve_on(pd, listener, async {
                    let _ = rx.await;
                })
                .await;
            });
        }
        Harness {
            pd,
            nodes: Vec::new(),
            shutdowns: vec![Some(tx)],
            _dirs: Vec::new(),
            pd_endpoint: format!("http://{pd_addr}"),
        }
    }

    /// A catalog node with `tables` declared, attached to PD before it starts serving.
    async fn add_node(&mut self, node_id: u64, tables: Vec<(String, ServerRegime)>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = open_catalog_node(Options::new(dir.path()), node_id, vec![node_id], Default::default(), tables)
            .expect("open_catalog_node");
        // Fast heartbeats, so PD hears a region's voters soon after its group elects a leader.
        state.set_heartbeat_interval_ms(50);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind node");
        let addr = listener.local_addr().unwrap();
        state
            .attach_pd(self.pd_endpoint.clone(), format!("http://{addr}"))
            .await
            .expect("attach_pd");

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let serving = state.clone();
        tokio::spawn(async move {
            let _ = serve_on(serving, listener, async {
                let _ = rx.await;
            })
            .await;
        });

        self.nodes.push(Node { addr, _state: state });
        self.shutdowns.push(Some(tx));
        self._dirs.push(dir);
    }

    fn client(&self, i: usize) -> Client {
        Client::connect(format!("http://{}", self.nodes[i].addr)).expect("connect")
    }

    async fn stop(mut self) {
        for sd in self.shutdowns.iter_mut() {
            if let Some(tx) = sd.take() {
                let _ = tx.send(());
            }
        }
    }
}

/// PD learns where each region is and how it is served. A region's regime and its voter set are
/// the two things `node_id` alone could never describe.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pd_learns_each_regions_regime_and_replica_set() {
    let mut h = Harness::start().await;
    h.add_node(1, vec![("ledger".into(), ServerRegime::Cp), ("events".into(), ServerRegime::Ap)])
        .await;

    // Tables are ids now: `ledger` is 1 (CP) and `events` is 2 (AP), each owning its id range.
    let ledger = 1u32.to_be_bytes();
    let events = 2u32.to_be_bytes();
    let placed = h.pd.members.route(&events).expect("PD routes the AP range");
    assert_eq!(placed.regime, PdRegime::Ap);
    // An AP region has no Raft group, so no leader and no voters to report.
    assert!(placed.voters.is_empty());

    // A CP region's voters come from its leader — the group's committed membership, never a
    // follower's possibly-uncommitted view. The single-voter group has to elect itself first.
    let mut voters = Vec::new();
    for _ in 0..100 {
        let ledger_now = h.pd.members.route(&ledger).expect("PD routes the CP range");
        assert_eq!(ledger_now.regime, PdRegime::Cp);
        voters = ledger_now.voters;
        if !voters.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(voters, vec![1], "reported by the region's leader");

    // And PD can say where that node is, which is what it hands out in place of `--peer` flags.
    assert_eq!(h.pd.members.node_addrs().len(), 1);

    h.stop().await;
}

/// The single-process PD holds no catalog, so its reply carries version 0 and the node adopts
/// nothing. Attaching must therefore leave what the node serves exactly as it was — otherwise a
/// node would repoint routing at regions whose Raft groups were never founded, and the failure
/// would be silent until a read missed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attaching_to_a_catalogless_pd_does_not_change_what_the_node_serves() {
    let mut h = Harness::start().await;
    h.add_node(1, vec![("ledger".into(), ServerRegime::Cp), ("events".into(), ServerRegime::Ap)])
        .await;
    let mut c = h.client(0);

    // The node's own catalog is untouched by the exchange — including the built-in `default`.
    assert_eq!(
        c.list_tables().await.unwrap(),
        vec![
            (0, "default".to_string(), Regime::Cp),
            (2, "events".to_string(), Regime::Ap),
            (1, "ledger".to_string(), Regime::Cp),
        ]
    );

    // And both regimes still route and serve.
    c.put("events", b"e1".to_vec(), b"tap".to_vec()).await.unwrap();
    assert_eq!(c.get("events", b"e1".to_vec()).await.unwrap(), Some(b"tap".to_vec()));
    for _ in 0..50 {
        if c.put("ledger", b"l1".to_vec(), b"100".to_vec()).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    }
    assert_eq!(c.get("ledger", b"l1".to_vec()).await.unwrap(), Some(b"100".to_vec()));

    h.stop().await;
}

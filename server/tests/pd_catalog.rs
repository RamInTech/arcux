//! A catalog node reporting to PD.
//!
//! Until now `--table` and `--pd` were mutually exclusive and `open_multiraft` hardcoded
//! `pd: None`, so PD had never seen a node that has tables. A catalog node can now attach to PD
//! and report what it has declared, which is what lets PD hold a cluster-wide catalog and name
//! nodes that disagree about a table's regime — a misconfiguration nothing detected before, and
//! one that silently tiles and routes the same keys differently on each node.
//!
//! The attachment is **report-only**: PD records what it hears and this node ignores the reply.
//! Adopting PD's view would rewrite the routing table to regions whose Raft groups were never
//! founded, so the tests below check the node keeps serving its own tiling afterwards.

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_catalog_node_reports_its_tables_and_regimes_to_pd() {
    let mut h = Harness::start().await;
    h.add_node(1, vec![("ledger".into(), ServerRegime::Cp), ("events".into(), ServerRegime::Ap)])
        .await;

    // PD now holds the catalog — which it had no way to learn before.
    assert_eq!(
        h.pd.members.tables(),
        vec![("events".to_string(), PdRegime::Ap), ("ledger".to_string(), PdRegime::Cp)]
    );
    assert!(h.pd.members.table_conflicts().is_empty(), "one node cannot disagree with itself");

    // And each region carries how it is served plus the replica set holding it — `node_id`
    // alone could not have described either.
    let placed = h.pd.members.route(b"events/e1").expect("PD routes the AP range");
    assert_eq!(placed.regime, PdRegime::Ap);
    assert_eq!(placed.voters, vec![1]);
    assert_eq!(h.pd.members.route(b"ledger/l1").map(|p| p.regime), Some(PdRegime::Cp));

    h.stop().await;
}

/// The report-only contract: attaching to PD must not disturb what the node serves. Adopting
/// PD's assignment here would repoint the routing table at regions with no Raft group behind
/// them, and the failure would be silent until a read missed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attaching_to_pd_does_not_change_what_the_node_serves() {
    let mut h = Harness::start().await;
    h.add_node(1, vec![("ledger".into(), ServerRegime::Cp), ("events".into(), ServerRegime::Ap)])
        .await;
    let mut c = h.client(0);

    // The node's own catalog is untouched by the exchange.
    assert_eq!(
        c.list_tables().await.unwrap(),
        vec![("events".to_string(), Regime::Ap), ("ledger".to_string(), Regime::Cp)]
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

/// The payoff: nothing else checks that a cluster's `--table` flags agree, and a mismatch tiles
/// and routes the same keys differently on each node with no error anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pd_flags_two_nodes_that_disagree_about_a_table() {
    let mut h = Harness::start().await;
    h.add_node(1, vec![("ledger".into(), ServerRegime::Cp), ("events".into(), ServerRegime::Ap)])
        .await;
    h.add_node(2, vec![("ledger".into(), ServerRegime::Cp), ("events".into(), ServerRegime::Cp)])
        .await;

    let conflicts = h.pd.members.table_conflicts();
    assert_eq!(conflicts.len(), 1, "only `events` disagrees, not `ledger`");
    let c = &conflicts[0];
    assert_eq!(c.name, "events");
    assert_eq!((c.node_id, c.regime), (1, PdRegime::Ap));
    assert_eq!((c.other_node_id, c.other_regime), (2, PdRegime::Cp));

    h.stop().await;
}

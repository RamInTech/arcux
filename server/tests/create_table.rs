//! `create table` against PD, and the invariant the whole numeric-id scheme exists for: **one
//! region per table, and nothing in between.**
//!
//! A table used to be a key prefix (`orders/o1`), so tiling n tables produced 2n+1 regions —
//! the tables plus an untabled "gap" between and around each one, every gap a CP region with
//! its own Raft group, elections and heartbeats, holding no data. A table is now a PD-allocated
//! id that owns exactly `[be32(id), be32(id+1))`, so a table's region ends precisely where the
//! next begins and a gap has nowhere to exist.
//!
//! One node and one replicated PD (the single-process PD keeps no region table, so it cannot
//! carve). The cluster case — declare against one node, use it through another — is
//! `pd_cluster_tables.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arcux_client::{Client, ClientError};
use arcux_engine::Options;
use arcux_pd::raft_server::{self, DEFAULT_FD_INTERVAL_MS, DEFAULT_FD_TIMEOUT_MS};
use arcux_rpc::kv::Regime;
use arcux_server::{open_catalog_node, serve_on, AppState};
use tokio::net::TcpListener;

struct TestServer {
    endpoint: String,
    state: Arc<AppState>,
    shutdowns: Vec<Option<tokio::sync::oneshot::Sender<()>>>,
    _dirs: Vec<tempfile::TempDir>,
}

impl TestServer {
    async fn start() -> TestServer {
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
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(pd_group.is_leader(), "PD never elected a leader");

        let dir = tempfile::tempdir().expect("tempdir");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind node");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        // No tables and no peers: everything this node serves beyond `default` arrives from PD.
        let state = open_catalog_node(Options::new(dir.path()), 1, Vec::new(), HashMap::new(), Vec::new())
            .expect("open_catalog_node");
        state.attach_pd(pd_addr, endpoint.clone()).await.expect("attach_pd");

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let serving = state.clone();
        tokio::spawn(async move {
            let _ = serve_on(serving, listener, async {
                let _ = rx.await;
            })
            .await;
        });

        TestServer {
            endpoint,
            state,
            shutdowns: vec![Some(pd_tx), Some(tx)],
            _dirs: vec![pd_dir, dir],
        }
    }

    fn client(&self) -> Client {
        Client::connect(self.endpoint.clone()).expect("connect")
    }

    async fn stop(mut self) {
        for sd in self.shutdowns.iter_mut() {
            if let Some(tx) = sd.take() {
                let _ = tx.send(());
            }
        }
    }
}

/// A freshly-founded CP group still needs to win an election (tens to hundreds of ms) before it
/// can serve, and `Client::put` burns its retries in microseconds without waiting.
async fn put_until_ready(c: &mut Client, table: &str, key: &[u8], value: &[u8]) -> bool {
    for _ in 0..60 {
        if c.put(table, key.to_vec(), value.to_vec()).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// **The headline invariant.** Region count is exactly 1 + tables, the regions are contiguous
/// from the start of the keyspace to +inf, and none of them is an untabled gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regions_are_one_per_table_with_no_gaps() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    for (name, regime) in [("orders", Regime::Cp), ("clicks", Regime::Ap), ("ledger", Regime::Cp)] {
        c.create_table(name, regime).await.expect("create table");
    }

    // Read the node's own routing table: what it actually serves, not what PD intended.
    let regions: Vec<_> = srv.state.hosted_region_ids();
    assert_eq!(regions.len(), 4, "default + 3 tables, and not one gap region between them");

    let tables = c.list_tables().await.expect("list tables");
    assert_eq!(
        tables.iter().map(|(id, n, _)| (*id, n.as_str())).collect::<Vec<_>>(),
        vec![(2, "clicks"), (0, "default"), (3, "ledger"), (1, "orders")],
        "ids are allocated in creation order; the listing is name-sorted"
    );

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_created_cp_table_works_immediately_with_no_restart() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    let (table_id, _region, start, end) =
        c.create_table("orders", Regime::Cp).await.expect("create table");
    assert_eq!(table_id, 1, "the first user table; 0 is the built-in default");
    assert_eq!(start, 1u32.to_be_bytes().to_vec(), "the table owns its own id range");
    assert!(end.is_empty(), "the newest table runs to +inf");

    assert!(put_until_ready(&mut c, "orders", b"o1", b"100").await, "table never became writable");
    assert_eq!(c.get("orders", b"o1".to_vec()).await.unwrap(), Some(b"100".to_vec()));

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_created_ap_table_works_immediately_with_no_restart() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    c.create_table("clicks", Regime::Ap).await.expect("create table");
    // AP is leaderless: no election to wait out, so the very first write must land.
    c.put("clicks", b"c1".to_vec(), b"tap".to_vec()).await.expect("AP write");
    assert_eq!(c.get("clicks", b"c1".to_vec()).await.unwrap(), Some(b"tap".to_vec()));

    srv.stop().await;
}

/// Keys are stored under a 4-byte id prefix. A read must give back the key the caller wrote —
/// tested on a **named** table, where a leaked prefix would be four non-zero bytes rather than
/// zeros that a partial strip could hide.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_returns_bare_user_keys() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    c.create_table("orders", Regime::Ap).await.expect("create table");
    for key in [b"o1", b"o2"] {
        c.put("orders", key.to_vec(), b"v".to_vec()).await.expect("put");
    }

    let mut rows = c.scan("orders", vec![], vec![], 0).await.expect("whole-table scan");
    rows.sort();
    assert_eq!(
        rows,
        vec![(b"o1".to_vec(), b"v".to_vec()), (b"o2".to_vec(), b"v".to_vec())],
        "no id prefix leaks into what the client sees"
    );

    srv.stop().await;
}

/// The whole `default` table is one contiguous range now, so scanning it works. It could not
/// before: the untabled namespace was scattered across the gaps between other tables, and the
/// server rejected the scan outright.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_whole_default_table_can_be_scanned() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    assert!(put_until_ready(&mut c, "", b"a", b"1").await, "default table never became writable");
    c.put("", b"b".to_vec(), b"2".to_vec()).await.expect("put");
    // A declared table alongside it, to prove the scan is bounded to `default` and not the
    // whole keyspace.
    c.create_table("orders", Regime::Ap).await.expect("create table");
    c.put("orders", b"o1".to_vec(), b"x".to_vec()).await.expect("put");

    let mut rows = c.scan("", vec![], vec![], 0).await.expect("whole default-table scan");
    rows.sort();
    assert_eq!(rows, vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())]);

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reserved_default_name_and_a_different_regime_are_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    // `default` always exists and is what an omitted table name resolves to.
    assert!(c.create_table("default", Regime::Cp).await.is_err());
    assert!(c.create_table("", Regime::Cp).await.is_err());

    c.create_table("orders", Regime::Cp).await.expect("create table");
    match c.create_table("orders", Regime::Ap).await {
        Err(e) => assert!(
            e.to_string().contains("already exists as CP"),
            "the refusal names the regime the table really has: {e}"
        ),
        Ok(r) => panic!("a CP table must not be reported as created AP: {r:?}"),
    }

    // The rejections cost nothing: `orders` is still the only user table, still id 1.
    let tables = c.list_tables().await.expect("list tables");
    assert_eq!(tables.len(), 2, "default + orders");
    assert_eq!(tables.iter().find(|(_, n, _)| n == "orders").map(|(id, _, _)| *id), Some(1));

    srv.stop().await;
}

/// Re-creating a table with the regime it already has is **not** an error. A client whose
/// create succeeded but whose reply was lost must be able to retry and get the same answer —
/// otherwise the retry reports failure for a table that exists exactly as asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn re_creating_with_the_same_regime_is_idempotent() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    let (first, ..) = c.create_table("orders", Regime::Cp).await.expect("create table");
    let (again, ..) = c.create_table("orders", Regime::Cp).await.expect("same regime is fine");
    assert_eq!(first, again, "the same table, not a second one");
    assert_eq!(c.list_tables().await.unwrap().len(), 2, "default + orders, no duplicate");

    srv.stop().await;
}

/// **The race.** Two creates of one name with different regimes, in flight at once, both got
/// past the node's pre-check — its catalog copy did not have the name yet — and PD answered the
/// loser with the winner's table as if it were a success. One caller was told it had the regime
/// it asked for when it had the other: a CP table silently reported as AP, or the reverse.
///
/// Every `OK` must describe the table that actually exists, and exactly one regime may win.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_never_report_a_regime_the_table_does_not_have() {
    let srv = TestServer::start().await;

    for i in 0..20 {
        let name = format!("race{i}");
        let (mut a, mut b) = (srv.client(), srv.client());
        let (na, nb) = (name.clone(), name.clone());
        let cp = tokio::spawn(async move { a.create_table(na, Regime::Cp).await.is_ok() });
        let ap = tokio::spawn(async move { b.create_table(nb, Regime::Ap).await.is_ok() });
        let (cp_ok, ap_ok) = (cp.await.unwrap(), ap.await.unwrap());

        let actual = srv
            .client()
            .list_tables()
            .await
            .expect("list tables")
            .into_iter()
            .find(|(_, n, _)| *n == name)
            .map(|(_, _, r)| r)
            .expect("one of the two creates made the table");
        assert!(!(cp_ok && ap_ok), "{name}: both regimes reported created");
        if cp_ok {
            assert_eq!(actual, Regime::Cp, "{name}: reported CP but the table is {actual:?}");
        }
        if ap_ok {
            assert_eq!(actual, Regime::Ap, "{name}: reported AP but the table is {actual:?}");
        }
    }

    srv.stop().await;
}

/// A leader-following client whose first endpoint is dead must still resolve and create
/// tables. Both used to make exactly one attempt at the current endpoint, so a fresh client
/// could not use any named table while node 1 was down — on a cluster that was otherwise fine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_lookup_and_creation_fail_over_past_a_dead_endpoint() {
    let srv = TestServer::start().await;
    // A port nothing listens on, first in the list — the endpoint the client starts on.
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("http://{}", l.local_addr().unwrap())
    };
    let mut c = Client::connect_cluster(vec![dead, srv.endpoint.clone()]).expect("connect");

    c.create_table("orders", Regime::Ap).await.expect("create fails over to the live node");
    let fresh = Client::connect_cluster(vec![
        {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://{}", l.local_addr().unwrap())
        },
        srv.endpoint.clone(),
    ])
    .expect("connect");
    let mut fresh = fresh;
    fresh.put("orders", b"k".to_vec(), b"v".to_vec()).await.expect("a fresh client resolves the name");
    assert_eq!(fresh.get("orders", b"k".to_vec()).await.unwrap(), Some(b"v".to_vec()));

    srv.stop().await;
}

/// An undeclared name used to be invented on the spot as a CP range. A key is now stored under
/// its table's id, so a name nobody has created cannot be routed at all — and the client says so
/// rather than writing somewhere surprising.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writing_to_a_table_that_does_not_exist_is_an_error() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    match c.put("nope", b"k".to_vec(), b"v".to_vec()).await {
        Err(ClientError::UnknownTable(t)) => assert_eq!(t, "nope"),
        other => panic!("expected UnknownTable, got {other:?}"),
    }
    match c.get("nope", b"k".to_vec()).await {
        Err(ClientError::UnknownTable(_)) => {}
        other => panic!("expected UnknownTable, got {other:?}"),
    }

    srv.stop().await;
}

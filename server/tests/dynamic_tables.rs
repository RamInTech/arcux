//! Dynamic table creation (single-node): a client declares a table live against an
//! already-running server — `create table <name> <cp|ap>` — and its region is usable
//! immediately, no restart. Mirrors `grpc_e2e.rs`'s `TestServer` pattern, but the node starts
//! in catalog/multiraft mode with **zero** declared tables (what `--dynamic-tables` gives a
//! real binary), since `CreateTable` needs `AppState::raft`/`AppState::ap` to attach to.

use std::net::SocketAddr;

use arcux_client::{Client, ClientError};
use arcux_engine::Options;
use arcux_rpc::kv::Regime;
use arcux_server::multiraft::Regime as ServerRegime;
use arcux_server::serve_on;
use tokio::net::TcpListener;

struct TestServer {
    addr: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
    /// `None` when the caller owns the data directory, which is how a restart test keeps the
    /// same directory alive across two successive nodes.
    _dir: Option<tempfile::TempDir>,
}

impl TestServer {
    /// Catalog/multiraft mode with zero declared tables — `Catalog::placements()` still
    /// tiles the whole keyspace into one CP region (the "undeclared keys default to CP" gap),
    /// so `CreateTable` always has an enclosing region to carve from, exactly like a real
    /// `--dynamic-tables` node.
    async fn start() -> TestServer {
        TestServer::start_with_tables(vec![]).await
    }

    /// The same node, but with tables declared up front — what `--table name=cp|ap` gives a real
    /// binary.
    async fn start_with_tables(tables: Vec<(String, ServerRegime)>) -> TestServer {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut srv = TestServer::start_at(dir.path(), tables, vec![1]).await.expect("start");
        srv._dir = Some(dir);
        srv
    }

    /// A node on a caller-owned directory, through the **real** startup path
    /// (`open_catalog_node`: load the persisted catalog, merge the `--table` flags, tile, save).
    /// Restart tests depend on that being the same code `serve_catalog` runs, not a copy.
    async fn start_at(
        data_dir: &std::path::Path,
        tables: Vec<(String, ServerRegime)>,
        voters: Vec<u64>,
    ) -> Result<TestServer, Box<dyn std::error::Error + Send + Sync>> {
        let state =
            arcux_server::open_catalog_node(Options::new(data_dir), 1, voters, Default::default(), tables)?;
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = serve_on(state, listener, async {
                let _ = rx.await;
            })
            .await;
        });
        Ok(TestServer { addr, shutdown: Some(tx), handle, _dir: None })
    }

    fn client(&self) -> Client {
        Client::connect(format!("http://{}", self.addr)).expect("connect")
    }

    async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }
}

/// A freshly-founded single-node CP group still needs to self-elect (election timeout, tens to
/// hundreds of ms) before it can serve — `Client::put`/`get` don't wait that out themselves
/// (the shell's `run_command` does, with the same kind of backoff; see
/// `client/src/bin/arcux.rs`). Retries the first write to a just-created CP table.
async fn put_until_ready(c: &mut Client, table: &str, key: &[u8], value: &[u8]) {
    for _ in 0..50 {
        if c.put(table, key.to_vec(), value.to_vec()).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    }
    panic!("put_until_ready: {table:?} never became writable");
}

/// The read-side counterpart. A CP region restored at startup is a fresh Raft group that has to
/// win an election before it can serve, so the first read after a restart races that election.
async fn get_until_ready(c: &mut Client, table: &str, key: &[u8]) -> Option<Vec<u8>> {
    for _ in 0..50 {
        if let Ok(v) = c.get(table, key.to_vec()).await {
            return v;
        }
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    }
    panic!("get_until_ready: {table:?} never became readable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn created_cp_table_works_immediately_no_restart() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    let (id, start, end) = c.create_table("orders", Regime::Cp).await.unwrap();
    assert!(id > 0);
    assert_eq!(start, b"orders/".to_vec());
    assert_eq!(end, b"orders0".to_vec());

    put_until_ready(&mut c, "orders", b"o1", b"100").await;
    assert_eq!(c.get("orders", b"o1".to_vec()).await.unwrap(), Some(b"100".to_vec()));

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn created_ap_table_works_immediately_no_restart() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    c.create_table("clicks", Regime::Ap).await.unwrap();
    c.put("clicks", b"c1".to_vec(), b"tap".to_vec()).await.unwrap();
    let pairs = c.scan("clicks", vec![], vec![], 0).await.unwrap();
    assert_eq!(pairs, vec![(b"clicks/c1".to_vec(), b"tap".to_vec())]);

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creating_a_table_over_existing_data_is_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    // Untabled write that happens to fall under what "orders" would claim.
    put_until_ready(&mut c, "", b"orders/o1", b"100").await;

    let err = c.create_table("orders", Regime::Cp).await.unwrap_err();
    match err {
        ClientError::Rpc(status) => assert!(
            status.message().contains("already has data"),
            "unexpected message: {}",
            status.message()
        ),
        other => panic!("expected an Rpc error, got {other:?}"),
    }

    srv.stop().await;
}

/// The `--table` startup path: what a node was declared with is discoverable over the wire,
/// name-sorted regardless of the order the flags came in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_tables_are_listed_with_their_regimes() {
    let srv = TestServer::start_with_tables(vec![
        ("ledger".to_string(), ServerRegime::Cp),
        ("events".to_string(), ServerRegime::Ap),
    ])
    .await;
    let mut c = srv.client();

    assert_eq!(
        c.list_tables().await.unwrap(),
        vec![("events".to_string(), Regime::Ap), ("ledger".to_string(), Regime::Cp)]
    );

    srv.stop().await;
}

/// The live path agrees with the startup one: a table created with no restart shows up in the
/// listing, carrying the regime it was created with.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_created_table_appears_in_the_listing() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    assert!(c.list_tables().await.unwrap().is_empty(), "a zero-table node declares nothing");

    c.create_table("orders", Regime::Cp).await.unwrap();
    c.create_table("clicks", Regime::Ap).await.unwrap();

    assert_eq!(
        c.list_tables().await.unwrap(),
        vec![("clicks".to_string(), Regime::Ap), ("orders".to_string(), Regime::Cp)]
    );

    srv.stop().await;
}

/// The regression test for the whole persistence gap: before the catalog was durable, a restart
/// re-tiled from the (empty) startup flags, so both tables vanished from `tables` and the AP one
/// silently came back CP — a consistency guarantee changing with no error — while its data was
/// still readable and its range could no longer be re-declared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_created_table_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    let srv = TestServer::start_at(dir.path(), vec![], vec![1]).await.expect("start");
    let mut c = srv.client();
    c.create_table("orders", Regime::Cp).await.unwrap();
    c.create_table("clicks", Regime::Ap).await.unwrap();
    put_until_ready(&mut c, "orders", b"o1", b"100").await;
    c.put("clicks", b"c1".to_vec(), b"tap".to_vec()).await.unwrap();
    srv.stop().await;

    // Restart on the same directory with no flags at all — what a plain restart looks like.
    let srv = TestServer::start_at(dir.path(), vec![], vec![1]).await.expect("restart");
    let mut c = srv.client();

    assert_eq!(
        c.list_tables().await.unwrap(),
        vec![("clicks".to_string(), Regime::Ap), ("orders".to_string(), Regime::Cp)],
        "declarations and regimes must survive; clicks must not come back CP"
    );
    assert_eq!(get_until_ready(&mut c, "orders", b"o1").await, Some(b"100".to_vec()));
    assert_eq!(get_until_ready(&mut c, "clicks", b"c1").await, Some(b"tap".to_vec()));

    // And the range is still owned by its table, so nothing is stranded needing a re-declare.
    let err = c.create_table("clicks", Regime::Ap).await.unwrap_err();
    match err {
        ClientError::Rpc(status) => assert!(
            status.message().contains("already declared"),
            "expected an already-declared error, got: {}",
            status.message()
        ),
        other => panic!("expected an Rpc error, got {other:?}"),
    }

    srv.stop().await;
}

/// A table first introduced by a `--table` flag is recorded too, so it keeps its regime on a
/// later restart that omits the flag — the same durability a live-created table gets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flag_declared_table_is_persisted_and_survives_without_the_flag() {
    let dir = tempfile::tempdir().expect("tempdir");

    let srv = TestServer::start_at(dir.path(), vec![("events".to_string(), ServerRegime::Ap)], vec![1])
        .await
        .expect("start");
    srv.stop().await;

    let srv = TestServer::start_at(dir.path(), vec![], vec![1]).await.expect("restart");
    let mut c = srv.client();
    assert_eq!(c.list_tables().await.unwrap(), vec![("events".to_string(), Regime::Ap)]);
    srv.stop().await;
}

/// A regime cannot be changed after creation, so a flag disagreeing with what is on disk is an
/// operator error. Refusing to start beats silently picking a side — taking the flag is exactly
/// the silent AP-to-CP downgrade this work removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flag_contradicting_the_persisted_regime_refuses_to_start() {
    let dir = tempfile::tempdir().expect("tempdir");

    let srv = TestServer::start_at(dir.path(), vec![], vec![1]).await.expect("start");
    let mut c = srv.client();
    c.create_table("clicks", Regime::Ap).await.unwrap();
    srv.stop().await;

    let started =
        TestServer::start_at(dir.path(), vec![("clicks".to_string(), ServerRegime::Cp)], vec![1]).await;
    let msg = match started {
        Ok(_) => panic!("a contradicting --table flag must abort startup"),
        Err(e) => e.to_string(),
    };
    assert!(msg.contains("clicks"), "message should name the table: {msg}");
    assert!(msg.contains("ap") && msg.contains("cp"), "should name both regimes: {msg}");
}

/// Live creation is single-node only. On a cluster the other nodes would never learn about the
/// table, and because a catalog tiling numbers regions by position, the same region id would
/// name a different key range on each node. Reject it rather than diverge silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_table_is_rejected_on_a_multi_node_cluster() {
    let dir = tempfile::tempdir().expect("tempdir");
    let srv = TestServer::start_at(dir.path(), vec![], vec![1, 2, 3]).await.expect("start");
    let mut c = srv.client();

    let err = c.create_table("orders", Regime::Cp).await.unwrap_err();
    match err {
        ClientError::Rpc(status) => assert!(
            status.message().contains("multi-node cluster"),
            "unexpected message: {}",
            status.message()
        ),
        other => panic!("expected an Rpc error, got {other:?}"),
    }

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_table_name_is_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    c.create_table("orders", Regime::Cp).await.unwrap();
    let err = c.create_table("orders", Regime::Ap).await.unwrap_err();
    match err {
        ClientError::Rpc(status) => assert!(
            status.message().contains("already declared"),
            "unexpected message: {}",
            status.message()
        ),
        other => panic!("expected an Rpc error, got {other:?}"),
    }

    srv.stop().await;
}

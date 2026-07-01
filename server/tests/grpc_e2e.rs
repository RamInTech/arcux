//! End-to-end gRPC tests: a real `arcux-client` driving a real `arcux-server` (engine on
//! a tempdir) over a loopback HTTP/2 connection on an ephemeral port. Proves the full
//! transactional API survives the network boundary, that SI conflict detection still
//! fires, that snapshot reads honour `commit_ts`, and that `Scan` reports `Unimplemented`.

use std::net::SocketAddr;

use arcux_client::{put_mutation, Client, ClientError};
use arcux_engine::Options;
use arcux_server::{serve_on, AppState};
use tokio::net::TcpListener;

/// A running in-process server bound to an ephemeral port, shut down on `stop()`.
struct TestServer {
    addr: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> TestServer {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = AppState::open(Options::new(dir.path())).expect("open engine");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = serve_on(state, listener, async {
                let _ = rx.await;
            })
            .await;
        });
        TestServer { addr, shutdown: Some(tx), handle, _dir: dir }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_then_get_roundtrips() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    c.put(b"alpha".to_vec(), b"one".to_vec()).await.unwrap();
    assert_eq!(c.get(b"alpha".to_vec()).await.unwrap(), Some(b"one".to_vec()));
    assert_eq!(c.get(b"missing".to_vec()).await.unwrap(), None);

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_key_transaction_is_visible() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    let commit_ts = c
        .transact(vec![
            put_mutation(b"acct:a".to_vec(), b"100".to_vec()),
            put_mutation(b"acct:b".to_vec(), b"50".to_vec()),
        ])
        .await
        .unwrap();
    assert!(commit_ts > 0);

    assert_eq!(c.get(b"acct:a".to_vec()).await.unwrap(), Some(b"100".to_vec()));
    assert_eq!(c.get(b"acct:b".to_vec()).await.unwrap(), Some(b"50".to_vec()));

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_prewrite_conflicts() {
    let srv = TestServer::start().await;
    let mut a = srv.client();
    let mut b = srv.client();

    // A prewrites key K and holds the lock (never commits).
    let a_start = a.begin().await.unwrap();
    a.prewrite(
        a_start,
        b"K".to_vec(),
        vec![put_mutation(b"K".to_vec(), b"av".to_vec())],
        a_start + 1_000_000,
    )
    .await
    .unwrap();

    // B prewrites the same key → must fail on A's lock (a per-key protocol error).
    let b_start = b.begin().await.unwrap();
    let res = b
        .prewrite(
            b_start,
            b"K".to_vec(),
            vec![put_mutation(b"K".to_vec(), b"bv".to_vec())],
            b_start + 1_000_000,
        )
        .await;
    match res {
        Err(ClientError::Key(_)) => {}
        other => panic!("expected a conflict/lock key-error, got {other:?}"),
    }

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_read_honours_commit_ts() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    let commit_ts = c.put(b"k".to_vec(), b"v1".to_vec()).await.unwrap();
    // Strictly before the commit: invisible.
    assert_eq!(c.get_at(b"k".to_vec(), commit_ts - 1).await.unwrap(), None);
    // At the commit timestamp: visible.
    assert_eq!(c.get_at(b"k".to_vec(), commit_ts).await.unwrap(), Some(b"v1".to_vec()));

    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_returns_an_ordered_range() {
    let srv = TestServer::start().await;
    let mut c = srv.client();

    for (k, v) in [("k/a", "1"), ("k/b", "2"), ("k/c", "3"), ("k/d", "4"), ("other", "x")] {
        c.put(k.as_bytes().to_vec(), v.as_bytes().to_vec()).await.unwrap();
    }

    // A prefix range comes back in key order, half-open [start, end).
    let pairs = c.scan(b"k/".to_vec(), b"k0".to_vec(), 0).await.unwrap();
    let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"k/a".to_vec(), b"k/b".to_vec(), b"k/c".to_vec(), b"k/d".to_vec()]);
    assert_eq!(pairs[1].1, b"2".to_vec());

    // `limit` caps the batch.
    let two = c.scan(b"k/".to_vec(), b"k0".to_vec(), 2).await.unwrap();
    assert_eq!(two.len(), 2);
    assert_eq!(two[0].0, b"k/a".to_vec());

    srv.stop().await;
}

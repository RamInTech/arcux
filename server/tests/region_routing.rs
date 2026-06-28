//! End-to-end region-routing tests for Phase 3: a real PD + a real data node + a
//! region-aware client, all in-process over loopback. They prove a routed client reaches
//! its data through PD, that a region split bumps the epoch, and that a client holding a
//! now-stale route is told `RegionStale`, re-resolves from PD, and retries transparently.

use std::net::SocketAddr;
use std::sync::Arc;

use arcux_client::Client;
use arcux_engine::Options;
use arcux_pd::server::{serve_on as pd_serve_on, Pd};
use arcux_rpc::kv;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_server::{serve_on, AppState};
use tokio::net::TcpListener;

/// A running in-process cluster: one PD and one data node, both on ephemeral ports.
struct Cluster {
    node_ep: String,
    pd_ep: String,
    shutdowns: Vec<tokio::sync::oneshot::Sender<()>>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn start() -> Cluster {
        let dir = tempfile::tempdir().expect("tempdir");

        // PD first, so the node can register with it on startup.
        let pd = Arc::new(Pd::ephemeral());
        let pd_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind pd");
        let pd_addr: SocketAddr = pd_listener.local_addr().unwrap();
        let (pd_tx, pd_rx) = tokio::sync::oneshot::channel::<()>();
        let pd_handle = tokio::spawn(async move {
            let _ = pd_serve_on(pd, pd_listener, async {
                let _ = pd_rx.await;
            })
            .await;
        });
        let pd_ep = format!("http://{pd_addr}");

        // The node connects to PD, registers its (single, whole-keyspace) region, and
        // serves KV. open_with_pd performs the initial heartbeat synchronously.
        let state = AppState::open_with_pd(Options::new(dir.path()), pd_ep.clone(), 1)
            .await
            .expect("open node with pd");
        let node_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind node");
        let node_addr: SocketAddr = node_listener.local_addr().unwrap();
        let (node_tx, node_rx) = tokio::sync::oneshot::channel::<()>();
        let node_handle = tokio::spawn(async move {
            let _ = serve_on(state, node_listener, async {
                let _ = node_rx.await;
            })
            .await;
        });
        let node_ep = format!("http://{node_addr}");

        Cluster {
            node_ep,
            pd_ep,
            shutdowns: vec![pd_tx, node_tx],
            handles: vec![pd_handle, node_handle],
            _dir: dir,
        }
    }

    /// A region-aware client (routes via PD).
    fn client(&self) -> Client {
        Client::connect_with_pd(self.node_ep.clone(), self.pd_ep.clone()).expect("connect")
    }

    async fn raw_kv(&self) -> KvServiceClient<tonic::transport::Channel> {
        KvServiceClient::connect(self.node_ep.clone()).await.expect("raw connect")
    }

    async fn stop(mut self) {
        for tx in self.shutdowns.drain(..) {
            let _ = tx.send(());
        }
        for h in self.handles.drain(..) {
            let _ = h.await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routed_client_reads_and_writes_through_pd() {
    let cluster = Cluster::start().await;
    let mut c = cluster.client();

    // The client resolves the whole-keyspace region from PD and routes to the node.
    c.put(b"alpha".to_vec(), b"one".to_vec()).await.unwrap();
    assert_eq!(c.get(b"alpha".to_vec()).await.unwrap(), Some(b"one".to_vec()));
    assert_eq!(c.get(b"missing".to_vec()).await.unwrap(), None);

    // Timestamps come from PD's oracle and advance.
    let t1 = c.begin().await.unwrap();
    let t2 = c.begin().await.unwrap();
    assert!(t2 > t1, "PD-issued timestamps must advance");

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_makes_stale_route_refresh_and_retry() {
    let cluster = Cluster::start().await;
    let mut c = cluster.client();

    // Warm the cache with the whole-keyspace region (epoch 1) and write a key.
    c.put(b"m".to_vec(), b"v1".to_vec()).await.unwrap();
    assert_eq!(c.get(b"m".to_vec()).await.unwrap(), Some(b"v1".to_vec()));

    // Split the keyspace at "m": left [-inf, "m") and right ["m", +inf), both epoch 2.
    let (left, right) = c.split_region(b"m".to_vec()).await.unwrap();
    assert_ne!(left, right, "split yields two distinct regions");

    // The client still caches the pre-split region (epoch 1). Writing "z" routes with
    // the stale epoch → the node replies RegionStale → the client re-resolves from PD
    // (now the right region) and retries, all transparently.
    c.put(b"z".to_vec(), b"v2".to_vec()).await.unwrap();
    assert_eq!(c.get(b"z".to_vec()).await.unwrap(), Some(b"v2".to_vec()));

    // "a" lands in the left region (also epoch 2) after a refresh.
    c.put(b"a".to_vec(), b"v3".to_vec()).await.unwrap();
    assert_eq!(c.get(b"a".to_vec()).await.unwrap(), Some(b"v3".to_vec()));

    // The key written before the split is still readable (storage is not partitioned in
    // Phase 3 — only routing is).
    assert_eq!(c.get(b"m".to_vec()).await.unwrap(), Some(b"v1".to_vec()));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_epoch_is_rejected_with_region_stale() {
    let cluster = Cluster::start().await;

    // Split so region 1's epoch moves from 1 to 2.
    cluster.client().split_region(b"m".to_vec()).await.unwrap();

    // A raw request claiming the old epoch (1) for region 1 must be rejected, and the
    // error must carry the current epoch so a client knows where to refresh to.
    let mut raw = cluster.raw_kv().await;
    let resp = raw
        .put(kv::PutRequest {
            key: b"a".to_vec(),
            value: b"x".to_vec(),
            context: Some(kv::Context { region_id: 1, region_epoch: 1 }),
        })
        .await
        .unwrap()
        .into_inner();

    match resp.error.and_then(|e| e.kind) {
        Some(kv::key_error::Kind::RegionStale(rs)) => assert_eq!(rs.new_epoch, 2),
        other => panic!("expected RegionStale with epoch 2, got {other:?}"),
    }

    cluster.stop().await;
}

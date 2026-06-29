//! End-to-end multi-node tests for Phase 3b: a real PD + **two** data nodes + a
//! region-aware client, all in-process over loopback. They prove the keyspace is
//! distributed across nodes (each key reaches the node that owns it), that a split and a
//! merge keep traffic flowing via transparent `RegionStale` retries, that PD's failure
//! detector marks a stopped node down within a bounded time, and that the TSO stays
//! strictly monotonic across a PD restart.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arcux_client::Client;
use arcux_engine::Options;
use arcux_pd::server::serve_on as pd_serve_on;
use arcux_pd::{region_id, Pd, Region};
use arcux_rpc::pd;
use arcux_rpc::pd::pd_service_client::PdServiceClient;
use arcux_server::{serve_on, AppState};
use tokio::net::TcpListener;

/// Fast failure-detector + heartbeat timings so the tests don't dawdle (heartbeat must be
/// well under the failure-detector timeout, or a live node would be marked down).
const FD_TIMEOUT_MS: u64 = 1_200;
const FD_INTERVAL_MS: u64 = 150;
const HEARTBEAT_MS: u64 = 150;

/// A running in-process cluster: one PD and two data nodes on ephemeral ports, with the
/// PD handle retained so tests can assert on membership (e.g. node-down detection).
struct Cluster {
    pd: Arc<Pd>,
    pd_ep: String,
    node_eps: Vec<String>,
    shutdowns: Vec<Option<tokio::sync::oneshot::Sender<()>>>,
    handles: Vec<Option<tokio::task::JoinHandle<()>>>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    /// Stand up a PD seeded with a two-way split — node 1 owns `[-inf, "m")`, node 2 owns
    /// `["m", +inf)` — plus the two data nodes that adopt those assignments.
    async fn start() -> Cluster {
        let dir = tempfile::tempdir().expect("tempdir");

        // Seed PD so the keyspace is pre-partitioned across the two nodes. Ids are
        // node-namespaced (see `region_id`) so later splits never collide.
        let seed = vec![
            (Region { id: region_id(1, 1), start: vec![], end: b"m".to_vec(), epoch: 1 }, 1),
            (Region { id: region_id(2, 1), start: b"m".to_vec(), end: vec![], epoch: 1 }, 2),
        ];
        let pd = Arc::new(Pd::seeded(seed).with_failure_detector(FD_TIMEOUT_MS, FD_INTERVAL_MS));
        let pd_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind pd");
        let pd_addr = pd_listener.local_addr().unwrap();
        let pd_ep = format!("http://{pd_addr}");
        let (pd_tx, pd_rx) = tokio::sync::oneshot::channel::<()>();
        let pd_handle = {
            let pd = pd.clone();
            tokio::spawn(async move {
                let _ = pd_serve_on(pd, pd_listener, async {
                    let _ = pd_rx.await;
                })
                .await;
            })
        };

        let mut node_eps = Vec::new();
        let mut shutdowns = vec![Some(pd_tx)];
        let mut handles = vec![Some(pd_handle)];

        for node_id in [1u64, 2] {
            // Bind first so the node advertises its real ephemeral address to PD.
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind node");
            let addr = listener.local_addr().unwrap();
            let ep = format!("http://{addr}");
            let state = AppState::open_with_pd(
                Options::new(dir.path().join(format!("node{node_id}"))),
                pd_ep.clone(),
                node_id,
                ep.clone(),
            )
            .await
            .expect("open node with pd");
            state.set_heartbeat_interval_ms(HEARTBEAT_MS);

            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let handle = tokio::spawn(async move {
                let _ = serve_on(state, listener, async {
                    let _ = rx.await;
                })
                .await;
            });
            node_eps.push(ep);
            shutdowns.push(Some(tx));
            handles.push(Some(handle));
        }

        Cluster { pd, pd_ep, node_eps, shutdowns, handles, _dir: dir }
    }

    /// A region-aware client (routes per node via PD). The first node is only a fallback.
    fn client(&self) -> Client {
        Client::connect_with_pd(self.node_eps[0].clone(), self.pd_ep.clone()).expect("connect")
    }

    /// Stop data node `node_id` (1-based) and wait for its task to finish, so it stops
    /// heartbeating and PD's failure detector can mark it down.
    async fn stop_node(&mut self, node_id: u64) {
        let slot = node_id as usize; // index 0 is PD
        if let Some(tx) = self.shutdowns[slot].take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handles[slot].take() {
            let _ = h.await;
        }
    }

    async fn stop(mut self) {
        for tx in self.shutdowns.iter_mut().filter_map(|s| s.take()) {
            let _ = tx.send(());
        }
        for h in self.handles.iter_mut().filter_map(|h| h.take()) {
            let _ = h.await;
        }
    }
}

/// Poll `cond` until it holds or `within` elapses.
async fn wait_until(mut cond: impl FnMut() -> bool, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_route_to_their_owning_node_and_failure_is_detected() {
    let mut cluster = Cluster::start().await;
    let mut c = cluster.client();

    // "alpha" lives in node 1's range, "zebra" in node 2's. Both are reachable, proving
    // the client dispatches each key to a *different* owning node.
    c.put(b"alpha".to_vec(), b"1".to_vec()).await.unwrap();
    c.put(b"zebra".to_vec(), b"2".to_vec()).await.unwrap();
    assert_eq!(c.get(b"alpha".to_vec()).await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(c.get(b"zebra".to_vec()).await.unwrap(), Some(b"2".to_vec()));

    // Stop node 2; PD must mark it down within the detector's bound.
    cluster.stop_node(2).await;
    let members = cluster.pd.members.clone();
    assert!(
        wait_until(|| members.is_down(2), Duration::from_secs(4)).await,
        "PD should mark a stopped node down within the failure-detector timeout",
    );

    // Node 1's data is still served; node 2's range is now unroutable — concrete proof the
    // two keys really lived on two different nodes.
    assert_eq!(c.get(b"alpha".to_vec()).await.unwrap(), Some(b"1".to_vec()));
    assert!(c.get(b"zebra".to_vec()).await.is_err(), "the stopped node's keys are unreachable");
    // Node 1 is still alive.
    assert!(!cluster.pd.members.is_down(1));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_then_merge_keep_traffic_flowing() {
    let cluster = Cluster::start().await;
    let mut c = cluster.client();

    // Warm the cache and write into node 1's region.
    c.put(b"g".to_vec(), b"v1".to_vec()).await.unwrap();

    // Split node 1's region at "f": [-inf,"f") | ["f","m"). The client still caches the
    // pre-split route (epoch 1); the next write to "g" gets RegionStale, re-resolves from
    // PD, and retries — transparently.
    let (left, right) = c.split_region(b"f".to_vec()).await.unwrap();
    assert_ne!(left, right, "split yields two distinct regions");
    c.put(b"g".to_vec(), b"v2".to_vec()).await.unwrap();
    assert_eq!(c.get(b"g".to_vec()).await.unwrap(), Some(b"v2".to_vec()));

    // Keys on both sides of the split, and node 2's range, all still serve.
    c.put(b"alpha".to_vec(), b"a".to_vec()).await.unwrap();
    assert_eq!(c.get(b"alpha".to_vec()).await.unwrap(), Some(b"a".to_vec()));
    c.put(b"zebra".to_vec(), b"z".to_vec()).await.unwrap();
    assert_eq!(c.get(b"zebra".to_vec()).await.unwrap(), Some(b"z".to_vec()));

    // Merge ["f","m") back into [-inf,"f"); traffic to "g" keeps flowing across the change.
    c.merge_region(b"f".to_vec()).await.unwrap();
    c.put(b"g".to_vec(), b"v3".to_vec()).await.unwrap();
    assert_eq!(c.get(b"g".to_vec()).await.unwrap(), Some(b"v3".to_vec()));
    // The pre-split/merge data is intact.
    assert_eq!(c.get(b"alpha".to_vec()).await.unwrap(), Some(b"a".to_vec()));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tso_is_monotonic_across_pd_restart() {
    let dir = tempfile::tempdir().unwrap();

    async fn run_pd(pd: Arc<Pd>) -> (String, tokio::sync::oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ep = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = pd_serve_on(pd, listener, async {
                let _ = rx.await;
            })
            .await;
        });
        (ep, tx, handle)
    }

    // First incarnation: issue a batch of timestamps, then shut down.
    let highest = {
        let pd = Arc::new(Pd::open(dir.path()).unwrap());
        let (ep, tx, handle) = run_pd(pd).await;
        let mut c = PdServiceClient::connect(ep).await.unwrap();
        let mut last = 0;
        for _ in 0..5 {
            last = c.get_timestamp(pd::GetTimestampRequest { count: 8 }).await.unwrap().into_inner().timestamp;
        }
        let _ = tx.send(());
        let _ = handle.await;
        last
    };

    // Restart on the same data dir: every newly issued timestamp must still exceed
    // everything issued before the restart (no regression across a forced failover).
    let pd = Arc::new(Pd::open(dir.path()).unwrap());
    let (ep, tx, handle) = run_pd(pd).await;
    let mut c = PdServiceClient::connect(ep).await.unwrap();
    let after = c.get_timestamp(pd::GetTimestampRequest { count: 1 }).await.unwrap().into_inner().timestamp;
    assert!(after > highest, "TSO regressed across restart: {after} <= {highest}");

    let _ = tx.send(());
    let _ = handle.await;
}

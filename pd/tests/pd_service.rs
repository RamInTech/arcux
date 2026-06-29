//! gRPC-level tests for the PD service: the TSO hands out monotonic, batched
//! timestamps, and a node's heartbeat populates the routing PD serves to clients.

use std::net::SocketAddr;
use std::sync::Arc;

use arcux_pd::convert::to_proto;
use arcux_pd::server::{serve_on, Pd};
use arcux_pd::Region;
use arcux_rpc::pd;
use arcux_rpc::pd::pd_service_client::PdServiceClient;
use tokio::net::TcpListener;

/// An in-process PD bound to an ephemeral port, shut down on `stop()`.
struct TestPd {
    addr: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
}

impl TestPd {
    async fn start() -> TestPd {
        let pd = Arc::new(Pd::ephemeral());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = serve_on(pd, listener, async {
                let _ = rx.await;
            })
            .await;
        });
        TestPd { addr, shutdown: Some(tx), handle }
    }

    async fn client(&self) -> PdServiceClient<tonic::transport::Channel> {
        PdServiceClient::connect(format!("http://{}", self.addr)).await.expect("connect")
    }

    async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tso_is_monotonic_and_batches() {
    let pd = TestPd::start().await;
    let mut c = pd.client().await;

    let a = c.get_timestamp(pd::GetTimestampRequest { count: 1 }).await.unwrap().into_inner();
    let b = c.get_timestamp(pd::GetTimestampRequest { count: 10 }).await.unwrap().into_inner();
    let d = c.get_timestamp(pd::GetTimestampRequest { count: 1 }).await.unwrap().into_inner();

    assert!(a.timestamp < b.timestamp, "timestamps strictly increase");
    assert_eq!(b.count, 10, "the batch size is echoed back");
    assert!(d.timestamp >= b.timestamp + 10, "a batch of 10 reserves a contiguous block");

    pd.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_populates_routing() {
    let pd = TestPd::start().await;
    let mut c = pd.client().await;

    // Before any node reports in, PD cannot route.
    assert!(c.get_region(pd::GetRegionRequest { key: b"k".to_vec() }).await.is_err());

    // A node reports the two regions it owns, along with its serving address.
    let regions = vec![
        to_proto(&Region { id: 1, start: vec![], end: b"m".to_vec(), epoch: 2 }),
        to_proto(&Region { id: 2, start: b"m".to_vec(), end: vec![], epoch: 2 }),
    ];
    c.heartbeat(pd::HeartbeatRequest { node_id: 1, regions, address: "http://node1".into() })
        .await
        .unwrap();

    // Now routing reflects the reported topology, tagged with the owning node.
    let r = c.get_region(pd::GetRegionRequest { key: b"z".to_vec() }).await.unwrap().into_inner();
    assert_eq!(r.region_id, 2);
    assert_eq!(r.epoch, 2);
    assert_eq!(r.node_id, 1, "PD attributes the region to the reporting node");
    assert_eq!(r.address, "http://node1", "and carries its serving address for routing");

    let all = c.list_regions(pd::ListRegionsRequest {}).await.unwrap().into_inner();
    assert_eq!(all.regions.len(), 2);

    pd.stop().await;
}

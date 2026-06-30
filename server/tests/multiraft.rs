//! End-to-end Phase-4b++ MultiRaft test: three data nodes each host **two** region Raft
//! groups (`[-inf,"m")` and `["m",+inf)`, RF=3) multiplexed over one `RaftService`. It
//! proves keys route to the right region, the two regions run as **independent** groups,
//! and that killing one region's leader is survived per-region (each region still has a
//! majority) — the foundation cross-region transactions (Phase 5) build on.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::Options;
use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_server::multiraft::{Regime, RegionPlacement};
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

struct Node {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

struct Cluster {
    nodes: Vec<Node>,
    clients: Vec<KvServiceClient<Channel>>,
    _dir: tempfile::TempDir,
    _clock: Arc<dyn TimestampSource>,
}

impl Cluster {
    /// Three nodes, each hosting two regions split at "m" (both RF=3 across all three).
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
        for (id, listener) in listeners {
            let peers: HashMap<u64, String> =
                eps.iter().filter(|(p, _)| **p != id).map(|(p, ep)| (*p, ep.clone())).collect();
            // Region 1 = [-inf,"m"), region 2 = ["m",+inf); both replicated on all three nodes.
            let placements = vec![
                RegionPlacement {
                    region_id: 1,
                    start: vec![],
                    end: b"m".to_vec(),
                    epoch: 1,
                    regime: Regime::Cp,
                    voters: ids.to_vec(),
                    peers: peers.clone(),
                },
                RegionPlacement {
                    region_id: 2,
                    start: b"m".to_vec(),
                    end: vec![],
                    epoch: 1,
                    regime: Regime::Cp,
                    voters: ids.to_vec(),
                    peers: peers.clone(),
                },
            ];
            let state = AppState::open_multiraft(
                Options::new(dir.path().join(format!("node{id}"))),
                id,
                placements,
                clock.clone(),
            )
            .expect("open multiraft node");
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let handle = tokio::spawn(async move {
                let _ = serve_on(state, listener, async {
                    let _ = rx.await;
                })
                .await;
            });
            nodes.push(Node { shutdown: Some(tx), handle: Some(handle) });
        }

        let mut clients = Vec::new();
        for id in ids {
            clients.push(connect_retry(&eps[&id]).await);
        }
        Cluster { nodes, clients, _dir: dir, _clock: clock }
    }

    /// Put `key=value` against whichever node leads the key's region (others reply
    /// `NotLeader`); returns the index of the serving node.
    async fn leader_put(&self, key: &[u8], value: &[u8]) -> usize {
        for _ in 0..100 {
            for (i, c) in self.clients.iter().enumerate() {
                let req =
                    kv::PutRequest { key: key.to_vec(), value: value.to_vec(), context: None };
                match c.clone().put(req).await {
                    Ok(resp) => match resp.into_inner().error.and_then(|e| e.kind) {
                        None => return i,
                        Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_)) => continue,
                        Some(other) => panic!("unexpected put error: {other:?}"),
                    },
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no region leader accepted the write");
    }

    async fn leader_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        for _ in 0..100 {
            for c in &self.clients {
                let req = kv::GetRequest { key: key.to_vec(), read_ts: 0, context: None };
                match c.clone().get(req).await {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        match r.error.and_then(|e| e.kind) {
                            None => return if r.found { Some(r.value) } else { None },
                            Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_))
                            | Some(Kind::Retryable(_)) => continue,
                            Some(other) => panic!("unexpected get error: {other:?}"),
                        }
                    }
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no region leader served the read");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn keys_route_to_their_region_group_and_survive_per_region_failover() {
    let mut cluster = Cluster::start().await;

    // "alpha" (< "m") lands in region 1; "zebra" (>= "m") in region 2 — two distinct groups.
    let r1_leader = cluster.leader_put(b"alpha", b"1").await;
    cluster.leader_put(b"zebra", b"2").await;
    assert_eq!(cluster.leader_get(b"alpha").await, Some(b"1".to_vec()));
    assert_eq!(cluster.leader_get(b"zebra").await, Some(b"2".to_vec()));

    // Kill the node leading region 1. Both groups still have a 2/3 majority, so region 1
    // re-elects and region 2 is unaffected — the groups are independent.
    cluster.stop_node(r1_leader).await;

    assert_eq!(cluster.leader_get(b"alpha").await, Some(b"1".to_vec()), "region 1 recovered");
    assert_eq!(cluster.leader_get(b"zebra").await, Some(b"2".to_vec()), "region 2 unaffected");

    // Both regions keep accepting writes under their (possibly new) leaders.
    cluster.leader_put(b"beta", b"3").await; // region 1
    cluster.leader_put(b"yak", b"4").await; // region 2
    assert_eq!(cluster.leader_get(b"beta").await, Some(b"3".to_vec()));
    assert_eq!(cluster.leader_get(b"yak").await, Some(b"4".to_vec()));

    cluster.stop().await;
}

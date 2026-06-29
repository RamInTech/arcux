//! End-to-end Phase-4b replication tests: **three** data nodes form one whole-keyspace
//! Raft group over the real `tonic` transport + durable `WalStorage`, in-process on
//! loopback. They prove the Phase-4 Definition of Done — a write replicates to a majority,
//! a follower redirects with `NotLeader`, and **killing the leader** triggers a re-election
//! after which service resumes with **every acknowledged write still present** (zero loss).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::Options;
use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

struct Node {
    ep: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

/// A 3-node replicated cluster (one Raft group spanning the whole keyspace).
struct Cluster {
    nodes: Vec<Node>,
    clients: Vec<KvServiceClient<Channel>>,
    _dir: tempfile::TempDir,
    // Keep the shared clock alive for the cluster's lifetime so `commit_ts` stays globally
    // monotonic across a leader change (in production this is PD's TSO).
    _clock: Arc<dyn TimestampSource>,
}

impl Cluster {
    async fn start() -> Cluster {
        let dir = tempfile::tempdir().expect("tempdir");
        let ids = [1u64, 2, 3];

        // Bind all three listeners first so every node can be told its peers' addresses.
        let mut listeners = Vec::new();
        let mut eps = HashMap::new();
        for &id in &ids {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let ep = format!("http://{}", l.local_addr().unwrap());
            eps.insert(id, ep);
            listeners.push((id, l));
        }

        // One shared in-process oracle → globally-monotonic timestamps across the group.
        let clock: Arc<dyn TimestampSource> = Arc::new(LocalClock::new());

        let mut nodes = Vec::new();
        for (id, listener) in listeners {
            let peers: HashMap<u64, String> =
                eps.iter().filter(|(p, _)| **p != id).map(|(p, ep)| (*p, ep.clone())).collect();
            let state = AppState::open_replicated(
                Options::new(dir.path().join(format!("node{id}"))),
                id,
                ids.to_vec(),
                peers,
                clock.clone(),
            )
            .expect("open replicated node");
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let handle = tokio::spawn(async move {
                let _ = serve_on(state, listener, async {
                    let _ = rx.await;
                })
                .await;
            });
            nodes.push(Node { ep: eps[&id].clone(), shutdown: Some(tx), handle: Some(handle) });
        }

        // Pre-connect a KV client to each node (retry until the server is accepting).
        let mut clients = Vec::new();
        for n in &nodes {
            clients.push(connect_retry(&n.ep).await);
        }

        Cluster { nodes, clients, _dir: dir, _clock: clock }
    }

    /// Put `key=value` by trying each node until the leader accepts it (others reply
    /// `NotLeader`); retries across rounds to ride out an in-progress election. Returns the
    /// `commit_ts` and the index of the node that served as leader.
    async fn leader_put(&self, key: Vec<u8>, value: Vec<u8>) -> (u64, usize) {
        for _ in 0..80 {
            for (i, c) in self.clients.iter().enumerate() {
                let mut c = c.clone();
                let req = kv::PutRequest { key: key.clone(), value: value.clone(), context: None };
                match c.put(req).await {
                    Ok(resp) => match resp.into_inner().error.and_then(|e| e.kind) {
                        None => return (0, i), // committed (commit_ts not asserted here)
                        Some(Kind::NotLeader(_)) => continue,
                        Some(other) => panic!("unexpected put error: {other:?}"),
                    },
                    Err(_) => continue, // node down or not ready
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader accepted the write within the election window");
    }

    /// Read `key` from whichever node is leader (followers redirect with `NotLeader`).
    async fn leader_get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        for _ in 0..80 {
            for c in &self.clients {
                let mut c = c.clone();
                let req = kv::GetRequest { key: key.clone(), read_ts: 0, context: None };
                match c.get(req).await {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        match r.error.and_then(|e| e.kind) {
                            None => return if r.found { Some(r.value) } else { None },
                            Some(Kind::NotLeader(_)) => continue,
                            Some(other) => panic!("unexpected get error: {other:?}"),
                        }
                    }
                    Err(_) => continue,
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        panic!("no leader served the read within the election window");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn writes_replicate_and_survive_a_leader_kill() {
    let mut cluster = Cluster::start().await;

    // Acknowledge five writes — each one committed by a majority before its ack.
    let mut leader = 0;
    for i in 0..5 {
        let (_ts, l) = cluster.leader_put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes()).await;
        leader = l;
    }
    for i in 0..5 {
        assert_eq!(
            cluster.leader_get(format!("k{i}").into_bytes()).await,
            Some(format!("v{i}").into_bytes()),
            "key k{i} readable before failover",
        );
    }

    // Kill the leader. The remaining two are a majority: they elect a new leader.
    cluster.stop_node(leader).await;

    // Every acknowledged write is still present after the failover — zero loss.
    for i in 0..5 {
        assert_eq!(
            cluster.leader_get(format!("k{i}").into_bytes()).await,
            Some(format!("v{i}").into_bytes()),
            "acknowledged key k{i} survived the leader kill",
        );
    }

    // And the cluster keeps serving: a new write commits under the new leader.
    let (_ts, new_leader) = cluster.leader_put(b"k5".to_vec(), b"v5".to_vec()).await;
    assert_ne!(new_leader, leader, "a different node leads after the kill");
    assert_eq!(cluster.leader_get(b"k5".to_vec()).await, Some(b"v5".to_vec()));

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_follower_redirects_with_not_leader() {
    let cluster = Cluster::start().await;

    // Establish a leader.
    let (_ts, leader) = cluster.leader_put(b"a".to_vec(), b"1".to_vec()).await;

    // A direct write to a follower must come back as NotLeader (the client's redirect cue).
    let follower = (0..cluster.clients.len()).find(|i| *i != leader).expect("a follower exists");
    let mut c = cluster.clients[follower].clone();
    let resp = c
        .put(kv::PutRequest { key: b"b".to_vec(), value: b"2".to_vec(), context: None })
        .await
        .expect("rpc ok")
        .into_inner();
    match resp.error.and_then(|e| e.kind) {
        Some(Kind::NotLeader(_)) => {}
        other => panic!("expected NotLeader from a follower, got {other:?}"),
    }

    cluster.stop().await;
}

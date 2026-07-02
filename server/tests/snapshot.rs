//! End-to-end Phase-4b++ snapshot / log-compaction test.
//!
//! A single CP region (RF=3) is brought up with only **two** of its three replicas. The two
//! live replicas form a majority and commit a burst of writes large enough to cross the
//! log-compaction threshold, so each **compacts** its log — discarding the very entries the
//! third replica is missing. When the third replica finally starts, the leader can no longer
//! back-fill it with `AppendEntries` (its `next_index` is below the log's start), so it ships
//! an **`InstallSnapshot`** instead. Afterwards we assert, on disk:
//!
//! - both live replicas compacted (a `raft.snapshot` exists and the log no longer starts at 1);
//! - the late replica installed a snapshot (its own `raft.snapshot` was written); and
//! - the late replica's engine holds **every** key — the ones inside the snapshot plus the
//!   tail replicated by append on top of it — i.e. its data matches the leader.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::{Engine, Options};
use arcux_raft::Storage as _;
use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_server::multiraft::{Regime, RegionPlacement};
use arcux_server::wal_storage::WalStorage;
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

const REGION_ID: u64 = 1;
/// Comfortably above the driver's compaction threshold (64) so the leader compacts.
const KEYS: usize = 80;

struct Node {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    async fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}
fn val(i: usize) -> Vec<u8> {
    format!("v{i:05}").into_bytes()
}

/// Spawn one data node on `listener` for a single RF=3 CP region spanning all keys.
fn start_node(
    id: u64,
    listener: TcpListener,
    voters: Vec<u64>,
    peers: HashMap<u64, String>,
    data_dir: PathBuf,
    clock: Arc<dyn TimestampSource>,
) -> Node {
    let placements = vec![RegionPlacement {
        region_id: REGION_ID,
        start: vec![],
        end: vec![],
        epoch: 1,
        regime: Regime::Cp,
        voters,
        peers,
    }];
    let state = AppState::open_multiraft(Options::new(data_dir), id, placements, clock)
        .expect("open multiraft node");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let _ = serve_on(state, listener, async {
            let _ = rx.await;
        })
        .await;
    });
    Node { shutdown: Some(tx), handle: Some(handle) }
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

/// Put `key=value` against whichever live node leads the region (others reply `NotLeader`).
async fn leader_put(clients: &[KvServiceClient<Channel>], key: &[u8], value: &[u8]) {
    for _ in 0..150 {
        for c in clients {
            let req = kv::PutRequest { key: key.to_vec(), value: value.to_vec(), context: None };
            match c.clone().put(req).await {
                Ok(resp) => match resp.into_inner().error.and_then(|e| e.kind) {
                    None => return,
                    Some(Kind::NotLeader(_)) | Some(Kind::RegionStale(_)) => continue,
                    Some(other) => panic!("unexpected put error: {other:?}"),
                },
                Err(_) => continue,
            }
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("no region leader accepted the write");
}

/// Wait (up to ~15s) for `path` to appear — used to observe that a replica has installed a
/// snapshot (the file is written durably inside `apply_snapshot`).
async fn wait_for_file(path: &std::path::Path) -> bool {
    for _ in 0..300 {
        if path.exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn raft_dir(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("raft").join(REGION_ID.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn far_behind_replica_catches_up_via_install_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ids = [1u64, 2, 3];
    let clock: Arc<dyn TimestampSource> = Arc::new(LocalClock::new());

    // Bind all three endpoints up front so every node knows every peer's address, but only
    // start nodes 1 and 2 — node 3 joins late.
    let mut listeners = HashMap::new();
    let mut eps = HashMap::new();
    for &id in &ids {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        eps.insert(id, format!("http://{}", l.local_addr().unwrap()));
        listeners.insert(id, l);
    }
    let data_dir = |id: u64| dir.path().join(format!("node{id}"));
    let peers_of = |id: u64| -> HashMap<u64, String> {
        eps.iter().filter(|(p, _)| **p != id).map(|(p, ep)| (*p, ep.clone())).collect()
    };

    let mut node1 = start_node(1, listeners.remove(&1).unwrap(), ids.to_vec(), peers_of(1), data_dir(1), clock.clone());
    let mut node2 = start_node(2, listeners.remove(&2).unwrap(), ids.to_vec(), peers_of(2), data_dir(2), clock.clone());

    let clients = vec![connect_retry(&eps[&1]).await, connect_retry(&eps[&2]).await];

    // The two-node majority commits a burst big enough to trigger compaction on both.
    for i in 0..KEYS {
        leader_put(&clients, &key(i), &val(i)).await;
    }

    // Both live replicas should have compacted their log (snapshot on disk, log start > 1).
    for &id in &[1u64, 2] {
        let rdir = raft_dir(&data_dir(id));
        assert!(
            wait_for_file(&rdir.join("raft.snapshot")).await,
            "node {id} should have compacted its log to a snapshot",
        );
    }

    // Node 3 joins, far behind: its whole catch-up must come from an InstallSnapshot.
    let node3_dir = data_dir(3);
    let mut node3 = start_node(3, listeners.remove(&3).unwrap(), ids.to_vec(), peers_of(3), node3_dir.clone(), clock.clone());

    // Observe the snapshot land on node 3, then let the post-snapshot tail replicate.
    assert!(
        wait_for_file(&raft_dir(&node3_dir).join("raft.snapshot")).await,
        "node 3 should have installed a snapshot from the leader",
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Shut everything down so the on-disk state is quiescent and reopenable.
    node3.stop().await;
    node1.stop().await;
    node2.stop().await;

    // The two live replicas compacted: their log no longer starts at index 1.
    for &id in &[1u64, 2] {
        let s = WalStorage::open(raft_dir(&data_dir(id))).expect("reopen leader storage");
        assert!(s.first_index() > 1, "node {id} log should have been truncated by compaction");
    }

    // Node 3 caught up: reopen its engine and confirm every key/value is present — the ones
    // delivered inside the snapshot plus the tail appended on top of it.
    let engine = Engine::open(Options::new(node3_dir)).expect("reopen node 3 engine");
    let pairs = engine.scan(b"", b"", u64::MAX, 0, true).expect("scan node 3");
    let got: HashMap<Vec<u8>, Vec<u8>> = pairs.into_iter().collect();
    assert_eq!(got.len(), KEYS, "node 3 should hold every committed key");
    for i in 0..KEYS {
        assert_eq!(got.get(&key(i)), Some(&val(i)), "node 3 value for key {i} matches the leader");
    }
}

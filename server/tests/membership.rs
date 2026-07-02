//! End-to-end Phase-4b++ (rest) single-server membership change test.
//!
//! A live RF=3 CP region (nodes 1–3) **grows to RF=4** and then **shrinks back to 3** while
//! serving writes, over the real `tonic` transport:
//!
//! - node 4 starts as a **learner** (empty bootstrap config, so it never campaigns) and is
//!   added with `AddNode(4)`; it catches up by replication and adopts the 4-member config;
//! - a follower is then removed with `RemoveNode`, and the surviving 3 keep committing.
//!
//! Afterwards we reopen node 4's engine on disk and confirm it holds **every** committed key —
//! the ones from before it joined plus the ones written after — i.e. the newcomer became a
//! full replica.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::{Engine, Options};
use arcux_raft::ConfChange;
use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_server::multiraft::{Regime, RegionPlacement};
use arcux_server::raft_group::{ProposeResult, RaftGroup};
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

const REGION_ID: u64 = 1;

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

/// Spawn one data node for a single CP region spanning all keys. Returns the node handle plus
/// its `AppState` so the test can drive membership on its group.
fn start_node(
    id: u64,
    listener: TcpListener,
    voters: Vec<u64>,
    peers: HashMap<u64, String>,
    data_dir: PathBuf,
    clock: Arc<dyn TimestampSource>,
) -> (Node, Arc<AppState>) {
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
    let served = state.clone();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let _ = serve_on(served, listener, async {
            let _ = rx.await;
        })
        .await;
    });
    (Node { shutdown: Some(tx), handle: Some(handle) }, state)
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

/// The region's leader group among `states`, if one has emerged.
fn leader_group(states: &[Arc<AppState>]) -> Option<RaftGroup> {
    states
        .iter()
        .filter_map(|s| s.raft_group(REGION_ID))
        .find(|g| g.is_leader())
        .cloned()
}

/// Drive a membership change on the current leader, retrying across a re-election until it
/// commits and applies.
async fn conf_change(states: &[Arc<AppState>], cc: ConfChange) {
    for _ in 0..200 {
        if let Some(g) = leader_group(states) {
            match g.propose_conf_change(cc).await {
                ProposeResult::Applied(Ok(())) => return,
                ProposeResult::Applied(Err(e)) => panic!("membership change failed to apply: {e}"),
                ProposeResult::NotLeader { .. } => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("no leader accepted the membership change {cc:?}");
}

/// Wait (up to ~15s) for a node's group to observe exactly `expected` voters.
async fn wait_for_voters(state: &Arc<AppState>, mut expected: Vec<u64>) -> bool {
    expected.sort_unstable();
    for _ in 0..300 {
        if let Some(g) = state.raft_group(REGION_ID) {
            let mut v = g.voters();
            v.sort_unstable();
            if v == expected {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_grows_and_shrinks_via_membership_changes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ids = [1u64, 2, 3, 4];
    let clock: Arc<dyn TimestampSource> = Arc::new(LocalClock::new());

    // Bind all four endpoints up front so every node knows every (present or future) peer's
    // address — this stands in for PD handing out replica addresses.
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

    // Start the initial 3-member group.
    let (mut n1, s1) = start_node(1, listeners.remove(&1).unwrap(), vec![1, 2, 3], peers_of(1), data_dir(1), clock.clone());
    let (mut n2, s2) = start_node(2, listeners.remove(&2).unwrap(), vec![1, 2, 3], peers_of(2), data_dir(2), clock.clone());
    let (mut n3, s3) = start_node(3, listeners.remove(&3).unwrap(), vec![1, 2, 3], peers_of(3), data_dir(3), clock.clone());
    let mut states = vec![s1.clone(), s2.clone(), s3.clone()];

    let clients = vec![
        connect_retry(&eps[&1]).await,
        connect_retry(&eps[&2]).await,
        connect_retry(&eps[&3]).await,
    ];

    // Write an initial batch under RF=3.
    for i in 0..10 {
        leader_put(&clients, &key(i), &val(i)).await;
    }

    // --- GROW: add node 4 as a learner, then AddNode(4) --------------------
    let node4_dir = data_dir(4);
    let (mut n4, s4) = start_node(4, listeners.remove(&4).unwrap(), vec![], peers_of(4), node4_dir.clone(), clock.clone());
    states.push(s4.clone());

    conf_change(&states, ConfChange::AddNode(4)).await;

    // Every replica — including the newcomer — converges on the 4-member config.
    for s in &states {
        assert!(
            wait_for_voters(s, vec![1, 2, 3, 4]).await,
            "a node never adopted the 4-member config",
        );
    }

    // Writes continue under RF=4.
    for i in 10..15 {
        leader_put(&clients, &key(i), &val(i)).await;
    }

    // --- SHRINK: remove a follower (not the leader, not the newcomer) ------
    let leader_id = leader_group(&states).expect("a leader exists").leader_id().unwrap();
    let victim = [1u64, 2, 3].into_iter().find(|x| *x != leader_id).unwrap();
    conf_change(&states, ConfChange::RemoveNode(victim)).await;

    // The survivors drop the victim from their configs. We assert on the newcomer (node 4),
    // which is definitely a survivor; the victim itself may never learn of its own removal
    // (the leader stops replicating to it), which is expected.
    let survivors: Vec<u64> = [1, 2, 3, 4].into_iter().filter(|x| *x != victim).collect();
    assert!(
        wait_for_voters(&s4, survivors.clone()).await,
        "the shrunk config didn't propagate to the newcomer",
    );

    // Decommission the removed node, then write again — the shrunk 3-member group stays live.
    let victim_node: &mut Node = match victim {
        1 => &mut n1,
        2 => &mut n2,
        _ => &mut n3,
    };
    victim_node.stop().await;
    for i in 15..18 {
        leader_put(&clients, &key(i), &val(i)).await;
    }

    // Let the tail replicate to the follower (node 4) before we take it down.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Shut everything down so node 4's on-disk state is quiescent and reopenable.
    n4.stop().await;
    n1.stop().await;
    n2.stop().await;
    n3.stop().await;

    // The newcomer became a full replica: reopen its engine and confirm every committed key is
    // present — the batch from before it joined plus everything written after.
    let engine = Engine::open(Options::new(node4_dir)).expect("reopen node 4 engine");
    let pairs = engine.scan(b"", b"", u64::MAX, 0, true).expect("scan node 4");
    let got: HashMap<Vec<u8>, Vec<u8>> = pairs.into_iter().collect();
    for i in 0..18 {
        assert_eq!(got.get(&key(i)), Some(&val(i)), "node 4 missing key {i}");
    }
}

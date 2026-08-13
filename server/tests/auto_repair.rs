//! End-to-end Phase-4b++ (rest) **PD-driven auto re-replication** — the capstone of the
//! Phase-4 Definition of Done: a dead node's region replica is replaced **without operator
//! action**, restoring RF=3.
//!
//! Four real `tonic` data nodes. Nodes 1–3 replicate one CP region (RF=3); node 4 is a
//! **spare hosting nothing** — it doesn't even know the region exists. Each node heartbeats
//! PD's failure detector ([`arcux_pd::Membership`]); a [`Supervisor`] loop sweeps it and
//! repairs any under-replicated watched region:
//!
//! kill a node → detector marks it down → the spare **hosts the region at runtime**
//! (empty bootstrap config) → `AddLearner` (no quorum weight — writes keep flowing while it
//! is cold) → it catches up (append/snapshot) → `AddNode` promotes it → `RemoveNode` drops
//! the dead voter. Voters end `{survivors, 4}`, data intact, writes serving throughout.
//!
//! Covered: a dead **follower**, a dead **leader** (repair rides the re-election), and a
//! graceful **leadership transfer** over the real transport (the decommission primitive).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arcux_engine::{Engine, Options};
use arcux_pd::cluster::now_ms;
use arcux_pd::Membership;
use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_server::multiraft::{Regime, RegionPlacement};
use arcux_server::raft_group::RaftGroup;
use arcux_server::repair::{Supervisor, WatchedRegion};
use arcux_server::{serve_on, AppState, LocalClock, TimestampSource};
use tokio::net::TcpListener;
use tonic::transport::Channel;

const REGION_ID: u64 = 1;
/// Node heartbeat period → the detector's signal.
const HB_MS: u64 = 100;
/// Silence beyond this ⇒ down. Comfortably above the heartbeat, far below test timeouts.
const FD_TIMEOUT_MS: u64 = 800;

fn key(i: usize) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}
fn val(i: usize) -> Vec<u8> {
    format!("v{i:05}").into_bytes()
}

struct Node {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl Node {
    /// Kill the node: stop serving (its Raft actors stop with it) **and** stop heartbeating,
    /// so the failure detector sees it go silent.
    async fn kill(&mut self) {
        self.heartbeat.abort();
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
    }
}

struct Cluster {
    nodes: HashMap<u64, Node>,
    states: HashMap<u64, Arc<AppState>>,
    clients: Vec<KvServiceClient<Channel>>,
    detector: Arc<Membership>,
    supervisor: tokio::task::JoinHandle<()>,
    dir: tempfile::TempDir,
    _clock: Arc<dyn TimestampSource>,
}

impl Cluster {
    /// Nodes 1–3 replicate region 1 (whole keyspace, RF=3); node 4 is a blank spare. The
    /// failure detector + repair supervisor run from the start.
    async fn start() -> Cluster {
        let dir = tempfile::tempdir().expect("tempdir");
        let ids = [1u64, 2, 3, 4];
        let clock: Arc<dyn TimestampSource> = Arc::new(LocalClock::new());
        let detector = Arc::new(Membership::new());

        let mut listeners = HashMap::new();
        let mut eps = HashMap::new();
        for &id in &ids {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            eps.insert(id, format!("http://{}", l.local_addr().unwrap()));
            listeners.insert(id, l);
        }

        let mut nodes = HashMap::new();
        let mut states = HashMap::new();
        for &id in &ids {
            // Only the initial replica set (1–3) knows the region; the spare's transport
            // reachability arrives at repair time via `RaftGroup::add_peer` — deliberately
            // NOT pre-seeded, so the runtime path is what's proven.
            let placements = if id != 4 {
                let peers: HashMap<u64, String> = [1u64, 2, 3]
                    .iter()
                    .filter(|p| **p != id)
                    .map(|p| (*p, eps[p].clone()))
                    .collect();
                vec![RegionPlacement {
                    region_id: REGION_ID,
                    start: vec![],
                    end: vec![],
                    epoch: 1,
                    regime: Regime::Cp,
                    voters: vec![1, 2, 3],
                    peers,
                }]
            } else {
                vec![] // the spare hosts nothing
            };
            let state = AppState::open_multiraft(
                Options::new(dir.path().join(format!("node{id}"))),
                id,
                placements,
                clock.clone(),
            )
            .expect("open multiraft node");

            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let served = state.clone();
            let listener = listeners.remove(&id).unwrap();
            let handle = tokio::spawn(async move {
                let _ = serve_on(served, listener, async {
                    let _ = rx.await;
                })
                .await;
            });
            // The node's liveness signal: heartbeat the detector until the node is killed.
            let hb_detector = detector.clone();
            let hb_addr = eps[&id].clone();
            let heartbeat = tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(HB_MS));
                loop {
                    tick.tick().await;
                    hb_detector.heartbeat(id, hb_addr.clone(), vec![], now_ms());
                }
            });

            nodes.insert(id, Node { shutdown: Some(tx), handle: Some(handle), heartbeat });
            states.insert(id, state);
        }

        // The re-replication supervisor: detect → plan → repair, continuously.
        let supervisor = tokio::spawn(
            Supervisor::new(
                detector.clone(),
                FD_TIMEOUT_MS,
                states.clone(),
                eps.clone(),
                vec![WatchedRegion {
                    region_id: REGION_ID,
                    start: vec![],
                    end: vec![],
                    epoch: 1,
                    target_rf: 3,
                }],
            )
            .run(Duration::from_millis(150)),
        );

        let mut clients = Vec::new();
        for id in ids {
            clients.push(connect_retry(&eps[&id]).await);
        }
        Cluster { nodes, states, clients, detector, supervisor, dir, _clock: clock }
    }

    async fn kill(&mut self, id: u64) {
        self.nodes.get_mut(&id).unwrap().kill().await;
    }

    /// Put against whichever live node leads the region (others redirect).
    async fn leader_put(&self, key: &[u8], value: &[u8]) {
        for _ in 0..200 {
            for c in &self.clients {
                let req =
                    kv::PutRequest {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        context: None,
                        table: String::new(),
                    };
                match c.clone().put(req).await {
                    Ok(resp) => match resp.into_inner().error.and_then(|e| e.kind) {
                        None => return,
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
        for _ in 0..200 {
            for c in &self.clients {
                let req = kv::GetRequest {
                    key: key.to_vec(),
                    read_ts: 0,
                    context: None,
                    table: String::new(),
                };
                match c.clone().get(req).await {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        match r.error.and_then(|e| e.kind) {
                            None => return if r.found { Some(r.value) } else { None },
                            Some(Kind::NotLeader(_))
                            | Some(Kind::RegionStale(_))
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

    /// The region's current leader group among nodes the detector considers live.
    fn leader_group(&self) -> Option<(u64, RaftGroup)> {
        self.states
            .iter()
            .filter(|(id, _)| !self.detector.is_down(**id))
            .filter_map(|(id, s)| s.raft_group(REGION_ID).map(|g| (*id, g)))
            .find(|(_, g)| g.is_leader())
    }

    /// Wait (up to ~30s) until a live node's group reports exactly `expected` voters and no
    /// learners — the repaired steady state.
    async fn wait_for_voters(&self, mut expected: Vec<u64>) {
        expected.sort_unstable();
        for _ in 0..600 {
            if let Some((_, g)) = self.leader_group() {
                let mut v = g.voters();
                v.sort_unstable();
                if v == expected && g.learners().is_empty() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let got = self.leader_group().map(|(_, g)| (g.voters(), g.learners()));
        panic!("repair never converged to voters {expected:?}; current: {got:?}");
    }

    /// Stop everything and return the temp dir (kept alive so engines can be reopened).
    async fn stop(mut self) -> tempfile::TempDir {
        self.supervisor.abort();
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            self.nodes.get_mut(&id).unwrap().kill().await;
        }
        self.dir
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

/// Reopen a stopped node's engine and assert it holds keys `range` — proof the runtime-added
/// replica became a full copy.
fn assert_engine_has(dir: PathBuf, range: std::ops::Range<usize>) {
    let engine = Engine::open(Options::new(dir)).expect("reopen engine");
    let pairs = engine.scan(b"", b"", u64::MAX, 0, true).expect("scan");
    let got: HashMap<Vec<u8>, Vec<u8>> = pairs.into_iter().collect();
    for i in range {
        assert_eq!(got.get(&key(i)), Some(&val(i)), "replica is missing key {i}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dead_follower_is_replaced_automatically() {
    let mut cluster = Cluster::start().await;

    // Committed state under the original RF=3.
    for i in 0..12 {
        cluster.leader_put(&key(i), &val(i)).await;
    }

    // Kill a FOLLOWER. No operator action from here on.
    let (leader_id, _) = cluster.leader_group().expect("a leader exists");
    let victim = [1u64, 2, 3].into_iter().find(|x| *x != leader_id).unwrap();
    cluster.kill(victim).await;

    // Detector notices → spare hosts the region → learner → caught up → promoted → dead
    // voter dropped. RF=3 restored over the live set {survivors, 4}.
    let expected: Vec<u64> = [1u64, 2, 3, 4].into_iter().filter(|x| *x != victim).collect();
    cluster.wait_for_voters(expected).await;

    // The cluster serves throughout and after; old and new data are readable.
    for i in 12..16 {
        cluster.leader_put(&key(i), &val(i)).await;
    }
    assert_eq!(cluster.leader_get(&key(0)).await, Some(val(0)));
    assert_eq!(cluster.leader_get(&key(15)).await, Some(val(15)));

    // Let replication settle, then prove the spare holds EVERYTHING — including the writes
    // from before it ever hosted the region (it caught up, then replicated as a voter).
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let dir = cluster.stop().await;
    assert_engine_has(dir.path().join("node4"), 0..16);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dead_leader_is_replaced_after_reelection() {
    let mut cluster = Cluster::start().await;

    for i in 0..8 {
        cluster.leader_put(&key(i), &val(i)).await;
    }

    // Kill the LEADER: the survivors must first re-elect, then the repair replaces it.
    let (leader_id, _) = cluster.leader_group().expect("a leader exists");
    cluster.kill(leader_id).await;

    let expected: Vec<u64> = [1u64, 2, 3, 4].into_iter().filter(|x| *x != leader_id).collect();
    cluster.wait_for_voters(expected).await;

    // Zero acknowledged loss across the kill + repair, and writes keep working.
    for i in 0..8 {
        assert_eq!(cluster.leader_get(&key(i)).await, Some(val(i)), "key {i} survived");
    }
    for i in 8..10 {
        cluster.leader_put(&key(i), &val(i)).await;
    }

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let dir = cluster.stop().await;
    assert_engine_has(dir.path().join("node4"), 0..10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn leadership_transfers_gracefully_over_grpc() {
    let cluster = Cluster::start().await;

    for i in 0..5 {
        cluster.leader_put(&key(i), &val(i)).await;
    }

    // Hand leadership to another voter — the graceful-decommission primitive: the target
    // gets TimeoutNow over the real transport and wins the election it triggers.
    let (old_leader, group) = cluster.leader_group().expect("a leader exists");
    let target = group.voters().into_iter().find(|v| *v != old_leader).unwrap();
    assert!(group.transfer_leadership(target).await, "the leader accepted the transfer");

    let mut new_leader = None;
    for _ in 0..300 {
        if let Some((id, _)) = cluster.leader_group() {
            if id != old_leader {
                new_leader = Some(id);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(new_leader, Some(target), "leadership moved to the requested target");

    // The handed-off group keeps serving reads and writes.
    assert_eq!(cluster.leader_get(&key(0)).await, Some(val(0)));
    cluster.leader_put(&key(5), &val(5)).await;
    assert_eq!(cluster.leader_get(&key(5)).await, Some(val(5)));

    cluster.stop().await;
}

//! A cluster assembling itself from PD alone — the path a real node takes.
//!
//! Every other multi-node test hands each node its voters and peers explicitly, which is exactly
//! why none of them caught this: three nodes started with only `--pd` each founded region 1 as
//! its sole voter, elected themselves, and accepted writes the others never saw.
//!
//! Here nodes are opened through `open_pd_node` (no regions, no voters, no peers) and attached to
//! a replicated PD one at a time. The first founds `default` alone; PD lists each later node in
//! the region's desired set; the region's leader adds it as a learner and promotes it once it has
//! caught up. The cluster ends with one leader and every node a voter.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arcux_client::{Client, ClientError};
use arcux_engine::Options;
use arcux_pd::raft_server::{self, DEFAULT_FD_INTERVAL_MS, DEFAULT_FD_TIMEOUT_MS};
use arcux_pd::PdGroup;
use arcux_rpc::kv::Regime;
use arcux_server::{open_pd_node, serve_on, AppState};
use tokio::net::TcpListener;

/// Heartbeats this fast make a membership change land in tens of milliseconds, where the real
/// default of a second would make each test take several.
const HEARTBEAT_MS: u64 = 50;

struct Cluster {
    pd: PdGroup,
    pd_addr: String,
    endpoints: HashMap<u64, String>,
    states: HashMap<u64, Arc<AppState>>,
    shutdowns: Vec<Option<tokio::sync::oneshot::Sender<()>>>,
    node_shutdowns: HashMap<u64, tokio::sync::oneshot::Sender<()>>,
    dirs: Vec<tempfile::TempDir>,
}

impl Cluster {
    /// A replicated one-node PD with the given replication target, and no data nodes yet.
    async fn start(replicas: usize) -> Cluster {
        let pd_dir = tempfile::tempdir().expect("tempdir");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind pd");
        let pd_addr = format!("http://{}", listener.local_addr().unwrap());
        let pd = raft_server::start_group(1, HashMap::from([(1, pd_addr.clone())]), pd_dir.path())
            .expect("start pd");
        pd.fsm().set_replicas(replicas);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let g = pd.clone();
            tokio::spawn(async move {
                let _ = raft_server::serve_on(
                    g,
                    listener,
                    DEFAULT_FD_TIMEOUT_MS,
                    DEFAULT_FD_INTERVAL_MS,
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
            });
        }
        for _ in 0..100 {
            if pd.is_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(pd.is_leader(), "PD never elected a leader");
        Cluster {
            pd,
            pd_addr,
            endpoints: HashMap::new(),
            states: HashMap::new(),
            shutdowns: vec![Some(tx)],
            node_shutdowns: HashMap::new(),
            dirs: vec![pd_dir],
        }
    }

    /// Start node `id` the way `arcux-server --pd` does: no regions, no voters, no peers.
    async fn add_node(&mut self, id: u64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind node");
        let ep = format!("http://{}", listener.local_addr().unwrap());
        let state = open_pd_node(Options::new(dir.path()), id).expect("open_pd_node");
        state.set_heartbeat_interval_ms(HEARTBEAT_MS);
        state.attach_pd(self.pd_addr.clone(), ep.clone()).await.expect("attach_pd");

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let serving = state.clone();
        tokio::spawn(async move {
            let _ = serve_on(serving, listener, async {
                let _ = rx.await;
            })
            .await;
        });
        self.endpoints.insert(id, ep);
        self.states.insert(id, state);
        self.node_shutdowns.insert(id, tx);
        self.dirs.push(dir);
    }

    /// Every node's view of `region`: `(node, is_leader, voters)`, for nodes hosting it.
    fn views(&self, region: u64) -> Vec<(u64, bool, Vec<u64>)> {
        let mut out: Vec<_> = self
            .states
            .iter()
            .filter_map(|(id, s)| {
                s.raft_group(region).map(|g| {
                    let mut v = g.voters();
                    v.sort_unstable();
                    (*id, g.is_leader(), v)
                })
            })
            .collect();
        out.sort();
        out
    }

    /// Wait until `region`'s leader reports exactly `want` as its voters — checking, at every
    /// sample along the way, that the region never splits.
    ///
    /// The end state alone is not enough: a node that founded the region by itself would elect
    /// itself and accept writes, and only *later* be pulled into the real group, its writes
    /// discarded. So every sample asserts there is at most one leader, and that no node other
    /// than the founder holds a group whose only voter is itself.
    async fn wait_for_voters(&self, region: u64, founder: u64, want: &[u64]) {
        for _ in 0..1000 {
            let views = self.views(region);
            let leaders = views.iter().filter(|(_, leader, _)| *leader).count();
            assert!(leaders <= 1, "region {region} has {leaders} leaders: {views:?}");
            for (id, _, voters) in &views {
                assert!(
                    *id == founder || voters != &vec![*id],
                    "node {id} founded region {region} on its own — split brain: {views:?}"
                );
            }
            if views.iter().any(|(_, leader, v)| *leader && v == want) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("region {region} never reached voters {want:?}: {:?}", self.views(region));
    }

    /// A client that follows the leader across every node.
    fn client(&self) -> Client {
        let mut ids: Vec<&u64> = self.endpoints.keys().collect();
        ids.sort();
        Client::connect_cluster(ids.into_iter().map(|id| self.endpoints[id].clone()).collect())
            .expect("connect cluster")
    }

    /// The region id PD assigned the `default` table.
    fn default_region(&self) -> u64 {
        self.pd.fsm().assignment()[0].region.id
    }

    /// Stop serving node `id`'s RPCs, so no peer can reach it.
    fn stop_node(&mut self, id: u64) {
        if let Some(tx) = self.node_shutdowns.remove(&id) {
            let _ = tx.send(());
        }
    }

    async fn stop(mut self) {
        for sd in self.shutdowns.iter_mut() {
            if let Some(tx) = sd.take() {
                let _ = tx.send(());
            }
        }
        for (_, tx) in self.node_shutdowns.drain() {
            let _ = tx.send(());
        }
        drop(self.dirs);
    }
}

/// A put that waits out a transient `NoLeader` — a redirect before the write was appended, so
/// retrying it cannot apply it twice — and returns its commit timestamp.
///
/// `None` when the outcome is unknown: on a loaded machine a commit can exceed its deadline, and
/// that write is deliberately **not** retried (it may still apply) nor counted. A test about
/// which timestamps were handed out has nothing to say about a write that was never confirmed.
async fn put_retrying(c: &mut Client, table: &str, key: &[u8]) -> Option<u64> {
    for _ in 0..100 {
        match c.put(table, key.to_vec(), b"v".to_vec()).await {
            Ok(ts) => return Some(ts),
            Err(arcux_client::ClientError::NoLeader) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(arcux_client::ClientError::Undetermined(_)) => return None,
            Err(e) => panic!("put {table:?}: {e}"),
        }
    }
    panic!("put {table:?}: no leader for 2s")
}

async fn put_until_ready(c: &mut Client, table: &str, key: &[u8], value: &[u8]) {
    for _ in 0..100 {
        if c.put(table, key.to_vec(), value.to_vec()).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{table:?} never became writable");
}

/// **The regression test.** Three nodes started with only PD form one group with one leader,
/// and a write through it reaches every replica — rather than three self-elected groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_started_with_only_pd_form_one_group() {
    let mut cluster = Cluster::start(3).await;
    // One at a time, watching the region after each: node 1 founds it alone, and each later node
    // must join node 1's group rather than start one of its own.
    cluster.add_node(1).await;
    let region = cluster.default_region();
    cluster.wait_for_voters(region, 1, &[1]).await;
    cluster.add_node(2).await;
    cluster.wait_for_voters(region, 1, &[1, 2]).await;
    cluster.add_node(3).await;
    cluster.wait_for_voters(region, 1, &[1, 2, 3]).await;
    let views = cluster.views(region);
    assert_eq!(views.iter().filter(|(_, leader, _)| *leader).count(), 1, "one leader: {views:?}");

    let mut c = cluster.client();
    put_until_ready(&mut c, "", b"k1", b"v1").await;

    // The write reaches every replica through the log — which a node founded alone never would.
    for (id, state) in &cluster.states {
        let mut seen = false;
        for _ in 0..100 {
            let group = state.raft_group(region).expect("hosted");
            if group.voters().len() == 3 {
                // Read from the leader-following client, starting at this node.
                let mut eps = vec![cluster.endpoints[id].clone()];
                eps.extend(cluster.endpoints.iter().filter(|(k, _)| *k != id).map(|(_, v)| v.clone()));
                let mut from_here = Client::connect_cluster(eps).expect("connect");
                if from_here.get("", b"k1".to_vec()).await.ok().flatten() == Some(b"v1".to_vec()) {
                    seen = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(seen, "node {id} never saw the write");
    }

    cluster.stop().await;
}

/// Once the cluster has formed, a new table is founded by every node at once — it starts fully
/// replicated rather than growing into it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_created_on_a_formed_cluster_starts_with_every_voter() {
    let mut cluster = Cluster::start(3).await;
    for id in [1, 2, 3] {
        cluster.add_node(id).await;
    }
    cluster.wait_for_voters(cluster.default_region(), 1, &[1, 2, 3]).await;

    let mut c = cluster.client();
    let (_table, region, _, _) = c.create_table("orders", Regime::Cp).await.expect("create");
    cluster.wait_for_voters(region, 1, &[1, 2, 3]).await;
    put_until_ready(&mut c, "orders", b"o1", b"100").await;
    assert_eq!(c.get("orders", b"o1".to_vec()).await.unwrap(), Some(b"100".to_vec()));

    cluster.stop().await;
}

/// The replication target caps growth: with `--replicas 2`, the third node is never added.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn growth_stops_at_the_replication_target() {
    let mut cluster = Cluster::start(2).await;
    for id in [1, 2, 3] {
        cluster.add_node(id).await;
    }
    let region = cluster.default_region();
    cluster.wait_for_voters(region, 1, &[1, 2]).await;

    // Give the driver time it would need to do more, then check it did not.
    tokio::time::sleep(Duration::from_millis(20 * HEARTBEAT_MS)).await;
    let views = cluster.views(region);
    assert!(
        views.iter().all(|(_, _, v)| v == &vec![1, 2]),
        "the third node must not become a voter: {views:?}"
    );
    assert!(cluster.states[&3].raft_group(region).is_none(), "and is not even given the region");

    cluster.stop().await;
}

/// An AP table created while the cluster was one node must fan writes out to the nodes that join
/// later. A peer list fixed when the first node started hosting the region would reach nobody,
/// and the write would spread only when the *other* nodes' anti-entropy happened to pull it.
///
/// Anti-entropy is switched off and the writer is stopped before the read, so the only way node
/// 2 can hold the value is that node 1 fanned it out at write time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ap_table_fans_out_to_nodes_that_joined_after_it_was_created() {
    let mut cluster = Cluster::start(3).await;
    cluster.add_node(1).await;
    let default_region = cluster.default_region();
    cluster.wait_for_voters(default_region, 1, &[1]).await;
    let mut alone = Client::connect(cluster.endpoints[&1].clone()).expect("connect");
    alone.create_table("likes", Regime::Ap).await.expect("create while alone");

    cluster.add_node(2).await;
    cluster.add_node(3).await;
    cluster.wait_for_voters(default_region, 1, &[1, 2, 3]).await;
    // Off *before* waiting: a pass already scheduled at the old interval still runs once, and it
    // must run before the write, or it would pull the value and hide a missing fan-out.
    for state in cluster.states.values() {
        state.set_anti_entropy_interval_ms(3_600_000);
    }
    // Enough beats for node 1 to apply the assignment listing nodes 2 and 3, and for any
    // already-scheduled anti-entropy pass to have fired.
    tokio::time::sleep(Duration::from_millis(20 * HEARTBEAT_MS)).await;

    let mut writer = Client::connect(cluster.endpoints[&1].clone()).expect("connect");
    writer.put("likes", b"post1".to_vec(), b"tap".to_vec()).await.expect("AP write on node 1");
    tokio::time::sleep(Duration::from_millis(200)).await; // the fan-out is asynchronous
    cluster.stop_node(1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut reader = Client::connect(cluster.endpoints[&2].clone()).expect("connect");
    assert_eq!(
        reader.get("likes", b"post1".to_vec()).await.expect("AP read on node 2"),
        Some(b"tap".to_vec()),
        "node 1's write never fanned out to a node that joined after the table was created"
    );

    cluster.stop().await;
}

/// Every CP timestamp comes from PD's one oracle, so two regions led by **different** nodes can
/// never be handed the same one. Each node used to stamp from its own clock: two nodes writing
/// in the same millisecond produced 13 duplicate commit timestamps in 800 writes, live — and
/// Percolator identifies a transaction by its `start_ts`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timestamps_are_unique_across_regions_led_by_different_nodes() {
    let mut cluster = Cluster::start(3).await;
    for id in [1, 2, 3] {
        cluster.add_node(id).await;
    }
    let default = cluster.default_region();
    cluster.wait_for_voters(default, 1, &[1, 2, 3]).await;
    let mut c = cluster.client();
    let (_t, orders, _, _) = c.create_table("orders", Regime::Cp).await.expect("create");
    cluster.wait_for_voters(orders, 1, &[1, 2, 3]).await;
    put_until_ready(&mut c, "orders", b"warm", b"x").await;

    // Hand `default` to node 2, so the two regions stamp their writes on different nodes.
    let g = cluster.states[&1].raft_group(default).expect("hosted");
    assert!(g.transfer_leadership(2).await, "transfer started");
    for _ in 0..200 {
        if cluster.states[&2].raft_group(default).is_some_and(|g| g.is_leader()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(cluster.states[&2].raft_group(default).unwrap().is_leader(), "node 2 leads default");
    assert!(cluster.states[&1].raft_group(orders).unwrap().is_leader(), "node 1 still leads orders");

    let (mut a, mut b) = (cluster.client(), cluster.client());
    put_until_ready(&mut a, "", b"warm", b"x").await;
    let on_default = tokio::spawn(async move {
        let mut ts = Vec::new();
        for i in 0..400u32 {
            ts.extend(put_retrying(&mut a, "", &i.to_be_bytes()).await);
        }
        ts
    });
    let on_orders = tokio::spawn(async move {
        let mut ts = Vec::new();
        for i in 0..400u32 {
            ts.extend(put_retrying(&mut b, "orders", &i.to_be_bytes()).await);
        }
        ts
    });
    let (x, y) = (on_default.await.unwrap(), on_orders.await.unwrap());
    assert!(x.len() + y.len() > 600, "too few confirmed writes to say anything: {} + {}", x.len(), y.len());
    for w in x.windows(2).chain(y.windows(2)) {
        assert!(w[0] < w[1], "one client's writes take increasing timestamps: {} then {}", w[0], w[1]);
    }
    let mut all: Vec<u64> = x.iter().chain(y.iter()).copied().collect();
    all.sort_unstable();
    let before = all.len();
    all.dedup();
    assert_eq!(all.len(), before, "{} commit timestamps were issued twice", before - all.len());

    cluster.stop().await;
}

/// With PD gone, a node keeps serving CP writes from the timestamp window it already reserved,
/// then refuses them — it never falls back to a clock of its own, which is how two nodes came to
/// issue the same timestamp. AP writes, stamped by each node's HLC, carry on regardless.
///
/// Deterministic proof that a node's CP timestamps come from PD: a node stamping from a local
/// clock would never fail here at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cp_writes_stop_once_pd_is_gone_and_the_reserved_window_is_spent() {
    let mut cluster = Cluster::start(3).await;
    for id in [1, 2, 3] {
        cluster.add_node(id).await;
    }
    cluster.wait_for_voters(cluster.default_region(), 1, &[1, 2, 3]).await;
    let mut c = cluster.client();
    c.create_table("hits", Regime::Ap).await.expect("create");
    put_until_ready(&mut c, "", b"before", b"pd-up").await;

    // Stop serving PD.
    if let Some(tx) = cluster.shutdowns[0].take() {
        let _ = tx.send(());
    }

    let mut refused = None;
    for i in 0..2_000u32 {
        match c.put("", i.to_be_bytes().to_vec(), b"v".to_vec()).await {
            Ok(_) => {}
            Err(e) => {
                refused = Some((i, e.to_string()));
                break;
            }
        }
    }
    let (after, msg) = refused.expect("CP writes never stopped without PD — a local clock stamped them");
    assert!(msg.contains("timestamp oracle"), "the refusal names the cause: {msg}");
    assert!(after > 0, "the window already reserved still serves: the first write after PD left worked");

    c.put("hits", b"k".to_vec(), b"v".to_vec()).await.expect("AP writes do not need the oracle");
    assert_eq!(c.get("hits", b"k".to_vec()).await.unwrap(), Some(b"v".to_vec()));

    cluster.stop().await;
}

/// A node id belongs to one data directory. A second process started with `-n 2` used to be
/// accepted: PD took its address over the real node 2's, redirected every peer's Raft traffic
/// for node 2 to a disk holding none of its log, and elections churned (term 1 → 53, live). A
/// directory reused under another id started just as silently. Both are refused now — the
/// impostor by PD, the reused directory before anything in it is opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_id_and_its_data_directory_cannot_be_claimed_twice() {
    let mut cluster = Cluster::start(3).await;
    for id in [1, 2] {
        cluster.add_node(id).await;
    }
    cluster.wait_for_voters(cluster.default_region(), 1, &[1, 2]).await;

    // A second process claiming node 2, on its own fresh directory.
    let impostor_dir = tempfile::tempdir().unwrap();
    let impostor = open_pd_node(Options::new(impostor_dir.path()), 2).expect("its own directory opens");
    let err = impostor
        .attach_pd(cluster.pd_addr.clone(), "http://127.0.0.1:1".into())
        .await
        .expect_err("PD must refuse a second node 2");
    assert!(err.to_string().contains("already belongs to another data directory"), "{err}");
    // And the real node 2 is untouched: still a voter, still reachable at its own address.
    let addrs = cluster.pd.fsm().node_addrs();
    assert_eq!(
        addrs.iter().find(|(id, _)| *id == 2).map(|(_, a)| a.clone()),
        Some(cluster.endpoints[&2].clone()),
        "PD never took the impostor's address for node 2"
    );

    // Node 1's directory opened as node 5: refused before its log is touched.
    let node1_dir = cluster.dirs[1].path().to_path_buf(); // dirs[0] is PD's
    cluster.stop_node(1);
    match open_pd_node(Options::new(&node1_dir), 5) {
        Err(e) => assert!(e.to_string().contains("belongs to node 1"), "{e}"),
        Ok(_) => panic!("node 1's directory must not open as node 5"),
    }

    cluster.stop().await;
}

/// **The hang, and the stale read.** A leader cut off from its majority used to keep both jobs:
/// it parked writes forever — a live client waited 2 minutes 48 seconds — and it answered reads
/// from its own state, returning values the rest of the cluster had already replaced.
///
/// It now stops leading within an election timeout (CheckQuorum), so a read is refused rather
/// than answered staled, and a write comes back quickly — as `undetermined` if it was already
/// appended, since it may yet commit under the next leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_that_loses_its_majority_refuses_reads_and_does_not_hang_writes() {
    let mut cluster = Cluster::start(3).await;
    for id in [1, 2, 3] {
        cluster.add_node(id).await;
    }
    let region = cluster.default_region();
    cluster.wait_for_voters(region, 1, &[1, 2, 3]).await;
    let mut c = cluster.client();
    put_until_ready(&mut c, "", b"k", b"v1").await;

    // Talk to node 1 only, so every request lands on the soon-to-be-cut-off leader.
    let mut pinned = Client::connect(cluster.endpoints[&1].clone()).expect("connect");
    assert_eq!(pinned.get("", b"k".to_vec()).await.unwrap(), Some(b"v1".to_vec()));
    assert!(cluster.states[&1].raft_group(region).unwrap().is_leader(), "node 1 leads");

    // Its followers go away: no majority, and no way to know whether another has taken over.
    cluster.stop_node(2);
    cluster.stop_node(3);

    // Within a couple of election timeouts it stops answering reads from local state. The
    // refusal must be a redirect (`NoLeader`) and must arrive well inside the client's own
    // request deadline: a leader that merely *stalled* the read until it timed out would leave a
    // client hanging for seconds on every read, and would still be claiming leadership.
    let mut refused = None;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = std::time::Instant::now();
        if let Err(e) = pinned.get("", b"k".to_vec()).await {
            refused = Some((e, started.elapsed()));
            break;
        }
    }
    let (refused, took) = refused.expect("kept serving reads with no majority — a stale read waiting to happen");
    assert!(
        matches!(refused, ClientError::NoLeader),
        "a read with no confirmed leadership must be redirected, not stalled: {refused:?}"
    );
    assert!(took < Duration::from_secs(3), "the read was stalled for {took:?} rather than refused");

    // And a write returns rather than hanging.
    let started = std::time::Instant::now();
    let err = pinned.put("", b"k".to_vec(), b"v2".to_vec()).await.expect_err("must not commit");
    let waited = started.elapsed();
    assert!(waited < Duration::from_secs(20), "the write hung for {waited:?}");
    match &err {
        // Appended before the step-down: its fate is genuinely unknown.
        ClientError::Undetermined(_) => {}
        // Or rejected before it was ever appended, which is unambiguous.
        ClientError::NoLeader | ClientError::Rpc(_) => {}
        other => panic!("unexpected error for a write with no majority: {other:?}"),
    }

    cluster.stop().await;
}

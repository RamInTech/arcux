//! End-to-end PD-on-Raft cluster test over real gRPC.
//!
//! Stands up three PD nodes on ephemeral ports, each serving both `PdService` (clients) and
//! `RaftService` (peer replication), and drives them through a leader failover with a real
//! `PdServiceClient` — proving the transport slice: a leader is elected, a follower **redirects**
//! clients to it, and after the leader is killed a new one takes over with the placement intact
//! and the TSO strictly ahead of every timestamp already issued.

use std::collections::HashMap;

use arcux_pd::raft_server::{self, DEFAULT_FD_INTERVAL_MS, DEFAULT_FD_TIMEOUT_MS};
use arcux_pd::PdGroup;
use arcux_rpc::pd::pd_service_client::PdServiceClient;
use arcux_rpc::pd::{
    GetRegionRequest, GetTimestampRequest, HeartbeatRequest, ListTablesRequest, Regime, Region,
    TableDecl,
};
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

struct Node {
    id: u64,
    group: PdGroup,
    addr: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// Each replica gets its own durable Raft log; kept alive for the test's duration.
    _dir: tempfile::TempDir,
}

/// Bind three ephemeral listeners, then start + serve a PD node on each (all knowing the full
/// topology up front, so peers can connect).
async fn start_cluster() -> Vec<Node> {
    let mut bound = Vec::new();
    for id in 1..=3u64 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        bound.push((id, l, addr));
    }
    let addrs: HashMap<u64, String> = bound.iter().map(|(id, _, a)| (*id, a.clone())).collect();

    let mut nodes = Vec::new();
    for (id, listener, addr) in bound {
        let dir = tempfile::tempdir().unwrap();
        let group = raft_server::start_group(id, addrs.clone(), dir.path()).unwrap();
        let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
        let g = group.clone();
        tokio::spawn(async move {
            let _ = raft_server::serve_on(
                g,
                listener,
                DEFAULT_FD_TIMEOUT_MS,
                DEFAULT_FD_INTERVAL_MS,
                async {
                    let _ = sd_rx.await;
                },
            )
            .await;
        });
        nodes.push(Node { id, group, addr, shutdown: Some(sd_tx), _dir: dir });
    }
    nodes
}

/// Poll until some live node (not in `dead`) reports itself leader; return its index in `nodes`.
async fn wait_for_leader(nodes: &[Node], dead: &[u64]) -> usize {
    for _ in 0..120 {
        if let Some(i) = nodes
            .iter()
            .position(|n| !dead.contains(&n.id) && n.group.is_leader())
        {
            // Give the fresh leader a beat to commit its no-op and become read-ready.
            sleep(Duration::from_millis(150)).await;
            return i;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("no PD leader elected within the timeout");
}

async fn client(addr: &str) -> PdServiceClient<tonic::transport::Channel> {
    PdServiceClient::connect(addr.to_string()).await.expect("connect to pd node")
}

fn region(id: u64, start: &[u8], end: &[u8], epoch: u64) -> Region {
    Region {
        id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch,
        node_id: 0,
        address: String::new(),
        regime: Regime::Cp as i32,
        voters: vec![],
    }
}

fn decl(name: &str, regime: Regime) -> TableDecl {
    TableDecl { name: name.to_string(), regime: regime as i32 }
}

/// PD holds the cluster-wide catalog and can name nodes that disagree about a table's regime.
/// Nothing else checks that a cluster was started with matching `--table` flags, and a mismatch
/// is otherwise silent: the nodes tile the keyspace differently and route the same keys to
/// different regimes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pd_reports_the_cluster_catalog_and_flags_disagreeing_nodes() {
    let mut nodes = start_cluster().await;
    let leader = wait_for_leader(&nodes, &[]).await;
    let mut lc = client(&nodes[leader].addr).await;

    // Two data nodes agree on `ledger` but disagree about `events`.
    for (node_id, addr, events) in
        [(7u64, "http://node7", Regime::Ap), (8, "http://node8", Regime::Cp)]
    {
        lc.heartbeat(HeartbeatRequest {
            node_id,
            regions: vec![],
            address: addr.into(),
            tables: vec![decl("ledger", Regime::Cp), decl("events", events)],
        })
        .await
        .unwrap();
    }

    let resp = lc.list_tables(ListTablesRequest {}).await.unwrap().into_inner();
    let names: Vec<&str> = resp.tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["events", "ledger"], "the union, name-sorted");

    assert_eq!(resp.conflicts.len(), 1, "only `events` disagrees");
    let c = &resp.conflicts[0];
    assert_eq!(c.name, "events");
    assert_eq!((c.node_id, c.other_node_id), (7, 8), "both nodes are named");
    assert_ne!(c.regime, c.other_regime);

    // A follower redirects rather than answering with a possibly stale view.
    if let Some(f) = nodes.iter().position(|n| !n.group.is_leader()) {
        let mut fc = client(&nodes[f].addr).await;
        let err = fc.list_tables(ListTablesRequest {}).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    for n in nodes.iter_mut() {
        n.group.shutdown();
        if let Some(sd) = n.shutdown.take() {
            let _ = sd.send(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pd_cluster_elects_serves_redirects_and_fails_over() {
    let mut nodes = start_cluster().await;
    let leader = wait_for_leader(&nodes, &[]).await;
    let leader_addr = nodes[leader].addr.clone();

    // A follower redirects rather than serving a timestamp itself.
    if let Some(follower) = nodes.iter().position(|n| !n.group.is_leader()) {
        let mut fc = client(&nodes[follower].addr).await;
        let err = fc.get_timestamp(GetTimestampRequest { count: 1 }).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable, "follower redirects, not serves");
        assert!(err.message().contains("leader"), "redirect names the leader: {}", err.message());
    }

    // The leader serves timestamps (through Raft) and records placement.
    let mut lc = client(&leader_addr).await;
    let ts1 = lc.get_timestamp(GetTimestampRequest { count: 1 }).await.unwrap().into_inner().timestamp;
    let ts2 = lc.get_timestamp(GetTimestampRequest { count: 1 }).await.unwrap().into_inner().timestamp;
    assert!(ts2 > ts1, "timestamps strictly increase: {ts1} -> {ts2}");

    lc.heartbeat(HeartbeatRequest {
        node_id: 7,
        regions: vec![region(1, b"", b"m", 1)],
        address: "http://node7".into(),
        tables: vec![],
    })
    .await
    .unwrap();
    let r = lc.get_region(GetRegionRequest { key: b"k".to_vec() }).await.unwrap().into_inner();
    assert_eq!(r.node_id, 7, "leader routes k to the placed node 7");
    let highest_issued = ts2;

    // Kill the leader: stop its Raft actor and its gRPC server.
    let dead_id = nodes[leader].id;
    nodes[leader].group.shutdown();
    if let Some(sd) = nodes[leader].shutdown.take() {
        let _ = sd.send(());
    }

    // The surviving two elect a new leader.
    let new_leader = wait_for_leader(&nodes, &[dead_id]).await;
    assert_ne!(nodes[new_leader].id, dead_id, "a different node took over");
    let mut nc = client(&nodes[new_leader].addr).await;

    // Placement survived the failover.
    let r = nc.get_region(GetRegionRequest { key: b"k".to_vec() }).await.unwrap().into_inner();
    assert_eq!(r.node_id, 7, "the new leader still routes k to node 7");

    // The TSO never regresses: the resumed stream is strictly above everything already issued.
    let resumed = nc
        .get_timestamp(GetTimestampRequest { count: 1 })
        .await
        .unwrap()
        .into_inner()
        .timestamp;
    assert!(resumed > highest_issued, "resumed ts {resumed} must exceed issued {highest_issued}");

    // Fresh consensus still works under the surviving majority.
    nc.heartbeat(HeartbeatRequest {
        node_id: 8,
        regions: vec![region(2, b"m", b"", 1)],
        address: "http://node8".into(),
        tables: vec![],
    })
    .await
    .unwrap();
    let r = nc.get_region(GetRegionRequest { key: b"z".to_vec() }).await.unwrap().into_inner();
    assert_eq!(r.node_id, 8, "post-failover placement commits and routes");

    // Tidy up the survivors.
    for n in nodes.iter_mut() {
        n.group.shutdown();
        if let Some(sd) = n.shutdown.take() {
            let _ = sd.send(());
        }
    }
}

//! A data node against a **three-node PD**, across a PD leader failover.
//!
//! Every CP timestamp comes from PD's oracle, so a node that could not follow PD's leader would
//! stop every CP write in the cluster the moment PD's leadership moved: its heartbeats and its
//! timestamp refills would all be answered "not pd leader" forever. The node is given all three
//! PD addresses — with a follower first, so even attaching has to follow a redirect — and must
//! keep writing, and creating tables, after the PD leader dies.

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

struct PdNode {
    id: u64,
    group: PdGroup,
    addr: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    _dir: tempfile::TempDir,
}

async fn start_pd_cluster() -> Vec<PdNode> {
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
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let g = group.clone();
        tokio::spawn(async move {
            let _ = raft_server::serve_on(g, listener, DEFAULT_FD_TIMEOUT_MS, DEFAULT_FD_INTERVAL_MS, async {
                let _ = rx.await;
            })
            .await;
        });
        nodes.push(PdNode { id, group, addr, shutdown: Some(tx), _dir: dir });
    }
    nodes
}

async fn wait_for_pd_leader(nodes: &[PdNode], dead: &[u64]) -> usize {
    for _ in 0..150 {
        if let Some(i) = nodes.iter().position(|n| !dead.contains(&n.id) && n.group.is_leader()) {
            tokio::time::sleep(Duration::from_millis(150)).await; // let it commit its no-op
            return i;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no PD leader elected");
}

async fn put_until_ready(c: &mut Client, table: &str, key: &[u8]) -> u64 {
    let mut last = None;
    for _ in 0..100 {
        match c.put(table, key.to_vec(), b"v".to_vec()).await {
            Ok(ts) => return ts,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{table:?} never became writable: {last:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_keeps_writing_and_creating_tables_across_a_pd_leader_failover() {
    let mut pds = start_pd_cluster().await;
    let leader = wait_for_pd_leader(&pds, &[]).await;

    // Every PD address, a follower first: attaching must already follow a redirect.
    let mut order: Vec<usize> = (0..pds.len()).filter(|i| *i != leader).collect();
    order.push(leader);
    let pd_list = order.iter().map(|i| pds[*i].addr.clone()).collect::<Vec<_>>().join(",");

    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let state: Arc<AppState> = open_pd_node(Options::new(dir.path()), 1).expect("open");
    state.set_heartbeat_interval_ms(50);
    state.attach_pd(pd_list, endpoint.clone()).await.expect("attach through a PD follower");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let serving = state.clone();
    tokio::spawn(async move {
        let _ = serve_on(serving, listener, async {
            let _ = rx.await;
        })
        .await;
    });

    let mut c = Client::connect(endpoint).expect("connect");
    let before = put_until_ready(&mut c, "", b"before").await;

    // Kill PD's leader.
    let dead = pds[leader].id;
    pds[leader].group.shutdown();
    if let Some(sd) = pds[leader].shutdown.take() {
        let _ = sd.send(());
    }
    wait_for_pd_leader(&pds, &[dead]).await;

    // Past the node's reserved timestamp window (256 timestamps, two per write), so at least one
    // refill has to find the new PD leader.
    let mut last = before;
    for i in 0..300u32 {
        let ts = loop {
            match c.put("", i.to_be_bytes().to_vec(), b"v".to_vec()).await {
                Ok(ts) => break ts,
                // A refill that lands mid-election is refused, not stalled; the next one finds the
                // new leader.
                Err(ClientError::Rpc(s)) if s.message().contains("timestamp oracle") => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!("write {i} after the PD failover: {e}"),
            }
        };
        assert!(ts > last, "timestamps keep increasing across the failover: {ts} after {last}");
        last = ts;
    }

    // The heartbeat followed too: a table created now is carved by the new leader and reaches
    // this node through its assignment.
    c.create_table("after", Regime::Cp).await.expect("create through the new PD leader");
    put_until_ready(&mut c, "after", b"k").await;

    let _ = tx.send(());
    for n in pds.iter_mut() {
        if let Some(sd) = n.shutdown.take() {
            let _ = sd.send(());
        }
    }
}

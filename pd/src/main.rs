//! arcux-pd — the Placement Driver server: the cluster's TSO, catalog and region router.
//!
//! Default (a one-node replicated group — it owns the catalog, so it can allocate table ids
//! and carve regions):
//!   arcux-pd [--data <dir>] [--listen <addr:port>]
//!
//! Replicated 3-node group (PD-on-Raft, Phase 4b++), one command per terminal:
//!   arcux-pd -n 1 --cluster 3
//!   arcux-pd -n 2 --cluster 3
//!   arcux-pd -n 3 --cluster 3
//! `--cluster N` derives a localhost ids-`1..=N` topology (node i listens on base-port+i-1,
//! base 2379). For real hosts, give the topology explicitly:
//!   arcux-pd -n 1 --listen 10.0.0.1:2379 --peer 2=http://10.0.0.2:2379 --peer 3=http://10.0.0.3:2379

use std::collections::HashMap;
use std::net::SocketAddr;

/// Base port for `--cluster`: node `i` listens on `PD_BASE_PORT + i - 1`.
const PD_BASE_PORT: u16 = 2379;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut data_dir: Option<String> = None;
    let mut listen: Option<String> = None;
    let mut node_id: Option<u64> = None;
    let mut cluster: Option<u64> = None;
    let mut peers: Vec<(u64, String)> = Vec::new();
    let mut single_process = false;
    let mut replicas = arcux_pd::replicated::DEFAULT_REPLICAS;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" | "-d" => data_dir = Some(args.next().ok_or("--data requires a directory")?),
            "--listen" | "-l" => listen = Some(args.next().ok_or("--listen requires an addr:port")?),
            "--node-id" | "-n" => {
                node_id = Some(args.next().ok_or("--node-id requires an id")?.parse()?)
            }
            "--cluster" | "-c" => {
                cluster = Some(args.next().ok_or("--cluster requires a node count")?.parse()?)
            }
            "--peer" => {
                let spec = args.next().ok_or("--peer requires id=address")?;
                let (id, addr) = spec.split_once('=').ok_or("--peer must be id=address")?;
                peers.push((id.parse()?, as_uri(addr)));
            }
            "--single-process" => single_process = true,
            "--replicas" | "-r" => {
                replicas = args.next().ok_or("--replicas requires a count")?.parse()?;
                if replicas == 0 {
                    return Err("--replicas must be at least 1".into());
                }
            }
            "--help" | "-h" => {
                println!("{HELP}");
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    // Single-process mode, explicitly asked for: a bare TSO + router with no replicated state.
    // It keeps no region table, so it cannot carve a table's range — `create_table` is refused
    // there. Kept for the tests that want a PD with no log, not as a way to run a cluster.
    if single_process {
        if cluster.is_some() || !peers.is_empty() {
            return Err("--single-process cannot be combined with --cluster/--peer".into());
        }
        let listen = listen.unwrap_or_else(|| format!("127.0.0.1:{PD_BASE_PORT}"));
        let data_dir = data_dir.unwrap_or_else(|| String::from("./arcux-pd-data"));
        arcux_pd::format::check_or_init(&data_dir, "PD data")?;
        let addr: SocketAddr = listen.parse()?;
        return arcux_pd::server::serve(data_dir, addr).await;
    }

    // Replicated by default, a group of one unless a topology is given. PD holds the catalog
    // now, and allocating a table id has to go through a log every replica applies — so the
    // no-flags PD has to be the one that can do it, or `arcux-pd` followed by `create table`
    // would fail on the most obvious pair of commands there is.
    let id = node_id.unwrap_or(1);
    let (addrs, bind) = topology(id, cluster, listen, peers)?;
    // Per-node by default, so `-c N` on one host doesn't have three replicas sharing a log.
    let data_dir = data_dir.unwrap_or_else(|| format!("./arcux-pd-n{id}"));
    arcux_pd::format::check_or_init(&data_dir, "PD data")?;
    arcux_pd::raft_server::serve(id, addrs, bind, &data_dir, replicas).await
}

/// Build the `{id → PD address}` topology and this node's bind address. `--cluster N` derives a
/// localhost topology; otherwise the explicit `--peer`s plus this node's `--listen` are used.
fn topology(
    id: u64,
    cluster: Option<u64>,
    listen: Option<String>,
    peers: Vec<(u64, String)>,
) -> Result<(HashMap<u64, String>, SocketAddr), Box<dyn std::error::Error + Send + Sync>> {
    let mut addrs: HashMap<u64, String> = HashMap::new();
    let bind: SocketAddr;

    if let Some(n) = cluster {
        if id < 1 || id > n {
            return Err(format!("--node-id {id} must be in 1..={n}").into());
        }
        for i in 1..=n {
            let port = PD_BASE_PORT + (i as u16) - 1;
            addrs.insert(i, format!("http://127.0.0.1:{port}"));
        }
        let port = PD_BASE_PORT + (id as u16) - 1;
        bind = format!("127.0.0.1:{port}").parse()?;
    } else {
        let listen = listen.ok_or("explicit replicated mode needs --listen")?;
        bind = listen.parse()?;
        addrs.insert(id, as_uri(&listen));
        for (pid, addr) in peers {
            addrs.insert(pid, addr);
        }
    }
    Ok((addrs, bind))
}

/// Prepend `http://` to a bare `host:port` (tonic's `Channel` needs a scheme).
fn as_uri(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

const HELP: &str = "\
arcux-pd — Placement Driver (TSO, catalog, region router)

One node (the default — owns the catalog, so `create table` works):
  arcux-pd [--data <dir>] [--listen <addr:port>]

Replicated 3-node group (PD-on-Raft), one command per terminal:
  arcux-pd -n <id> --cluster <N>                 # localhost ids 1..=N, base port 2379
  arcux-pd -n <id> --listen <addr> --peer <id>=<addr> ...   # explicit topology

Flags:
  -d, --data <dir>        data directory for the Raft log.
                          Default ./arcux-pd-n<id> (./arcux-pd-data with --single-process)
  -l, --listen <addr>     serving address (host:port); default 127.0.0.1:2379
  -n, --node-id <id>      this node's id (default 1)
  -c, --cluster <N>       derive a localhost N-node topology
      --peer <id>=<addr>  a peer's id and address (repeatable, explicit mode)
  -r, --replicas <N>      voters to grow each data region to (default 3). The first node to
                          join founds a region alone; each later one is added by membership
                          change until N are voters
      --single-process    a TSO + router with no replicated state. Holds no region table, so
                          it cannot carve a table: `create table` is refused against it
  -h, --help              show this help";

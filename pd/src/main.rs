//! arcux-pd — the Placement Driver server: the cluster's TSO + region router.
//!
//! Single-process (Phase 3):
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
    let mut data_dir = String::from("./arcux-pd-data");
    let mut listen: Option<String> = None;
    let mut node_id: Option<u64> = None;
    let mut cluster: Option<u64> = None;
    let mut peers: Vec<(u64, String)> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" | "-d" => data_dir = args.next().ok_or("--data requires a directory")?,
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
            "--help" | "-h" => {
                println!("{HELP}");
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    // Replicated mode: any of --cluster / --node-id / --peer selects it.
    if cluster.is_some() || node_id.is_some() || !peers.is_empty() {
        let id = node_id.ok_or("replicated mode needs --node-id / -n")?;
        let (addrs, bind) = topology(id, cluster, listen, peers)?;
        return arcux_pd::raft_server::serve(id, addrs, bind).await;
    }

    // Single-process mode (Phase 3).
    let listen = listen.unwrap_or_else(|| format!("127.0.0.1:{PD_BASE_PORT}"));
    std::fs::create_dir_all(&data_dir)?;
    let addr: SocketAddr = listen.parse()?;
    arcux_pd::server::serve(data_dir, addr).await
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
arcux-pd — Placement Driver (TSO + region router)

Single-process:
  arcux-pd [--data <dir>] [--listen <addr:port>]

Replicated 3-node group (PD-on-Raft):
  arcux-pd -n <id> --cluster <N>                 # localhost ids 1..=N, base port 2379
  arcux-pd -n <id> --listen <addr> --peer <id>=<addr> ...   # explicit topology

Flags:
  -d, --data <dir>        single-process TSO watermark dir (default ./arcux-pd-data)
  -l, --listen <addr>     serving address (host:port)
  -n, --node-id <id>      this node's id (required for replicated mode)
  -c, --cluster <N>       derive a localhost N-node topology
      --peer <id>=<addr>  a peer's id and address (repeatable, explicit mode)
  -h, --help              show this help";

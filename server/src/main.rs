//! arcux-server — opens the Phase-1 engine and serves the gRPC API.
//!
//! Usage:
//!   arcux-server [--data <dir>] [--listen <addr:port>] [--node-id <n>]
//!                [--pd <addr:port>] [--address <uri>]
//!                [--cluster <N> [--base-port <p>]]
//!                [--voters <id,id,...> --peer <id>=<addr> ...]
//!
//! Modes, selected by which flags are present:
//!
//! - **direct** (default) — single-node, a local timestamp oracle, no routing/replication.
//! - **PD** (`--pd`) — join a cluster: timestamps from PD's TSO, regions reported to PD for
//!   client routing. `--address` is the endpoint clients reach this node at.
//! - **replicated CP** — a single whole-keyspace Raft group, described two ways. Easy/local:
//!   `--cluster <N>` is a localhost cluster of ids `1..=N` whose listen address, peers, and
//!   data dir are derived from `--node-id` and a base port (default 50060, so node *i* listens
//!   on `base+i`) — e.g. `arcux-server -n 1 -c 3` per terminal. Explicit: `--voters <ids>`
//!   plus a `--peer <id>=<addr>` per other voter, for real hosts. Replicated writes go to the
//!   elected leader (others reply `NotLeader`); kill the leader to watch a re-election.
//!   Mutually exclusive with `--pd`.
//!
//! Defaults: --data ./arcux-data (per-node ./arcux-n<id> under --cluster),
//!           --listen 127.0.0.1:50051, --node-id 1, --base-port 50060.

use std::collections::HashMap;
use std::net::SocketAddr;

use arcux_engine::Options;
use arcux_server::multiraft::Regime;

const HELP: &str = "\
arcux-server [--data <dir>] [--listen <addr:port>] [--node-id <n>]
             [--pd <addr:port>] [--address <uri>]
             [--cluster <N> [--base-port <p>]]
             [--voters <id,id,...> --peer <id>=<addr> ...]

  -d, --data      <dir>        data directory
  -l, --listen    <addr:port>  bind address (default 127.0.0.1:50051)
  -n, --node-id   <n>          this node's id (default 1)
      --pd        <addr:port>  join a cluster via PD (excludes --voters/--cluster)
      --address   <uri>        endpoint clients reach this node at (PD mode)
  -c, --cluster   <N>          replicated CP shortcut: a localhost cluster of ids 1..=N
      --base-port <p>          base port for --cluster (default 50060; node i => base+i)
      --voters    <id,...>     replicated CP (explicit): the full voter set
      --peer      <id>=<addr>  address of another voter (repeatable)
      --table     <name>=cp|ap declare a table's regime (repeatable); undeclared keys are CP

easy local 3-node cluster (one per terminal):
  arcux-server -n 1 -c 3
  arcux-server -n 2 -c 3
  arcux-server -n 3 -c 3
=> nodes listen on 127.0.0.1:50061/50062/50063, data in ./arcux-n1/2/3

single node with a CP table and an AP table:
  arcux-server --table ledger=cp --table likes=ap
=> keys under ledger/ are strongly consistent (Raft); likes/ is leaderless AP";

/// Normalize a bare `addr:port` to a full `http://` URI (leaving an explicit scheme as-is).
fn as_uri(s: &str) -> String {
    if s.contains("://") {
        s.to_string()
    } else {
        format!("http://{s}")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut data_dir: Option<String> = None;
    let mut listen: Option<String> = None;
    let mut pd: Option<String> = None;
    let mut node_id: u64 = 1;
    let mut address: Option<String> = None;
    let mut voters: Vec<u64> = Vec::new();
    let mut peers: HashMap<u64, String> = HashMap::new();
    let mut cluster: Option<u64> = None;
    let mut base_port: u16 = 50060;
    let mut tables: Vec<(String, Regime)> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" | "-d" => data_dir = Some(args.next().ok_or("--data requires a directory")?),
            "--listen" | "-l" => listen = Some(args.next().ok_or("--listen requires an addr:port")?),
            "--pd" => pd = Some(as_uri(&args.next().ok_or("--pd requires a PD addr:port")?)),
            "--node-id" | "-n" => {
                node_id = args
                    .next()
                    .ok_or("--node-id requires a number")?
                    .parse()
                    .map_err(|_| "--node-id must be a u64")?;
            }
            "--address" => address = Some(as_uri(&args.next().ok_or("--address requires a uri")?)),
            "--cluster" | "-c" => {
                cluster = Some(
                    args.next()
                        .ok_or("--cluster requires a node count N")?
                        .parse()
                        .map_err(|_| "--cluster must be a positive integer")?,
                );
            }
            "--base-port" => {
                base_port = args
                    .next()
                    .ok_or("--base-port requires a port")?
                    .parse()
                    .map_err(|_| "--base-port must be a u16")?;
            }
            "--voters" => {
                let list = args.next().ok_or("--voters requires a comma-separated id list")?;
                voters = list
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.parse::<u64>().map_err(|_| format!("--voters: {s:?} is not a u64")))
                    .collect::<Result<_, _>>()?;
            }
            "--peer" => {
                let spec = args.next().ok_or("--peer requires <id>=<addr>")?;
                let (id, addr) = spec
                    .split_once('=')
                    .ok_or_else(|| format!("--peer must be <id>=<addr>, got {spec:?}"))?;
                let id: u64 = id.parse().map_err(|_| format!("--peer id {id:?} is not a u64"))?;
                peers.insert(id, as_uri(addr));
            }
            "--table" => {
                let spec = args.next().ok_or("--table requires <name>=<cp|ap>")?;
                let (name, regime) = spec
                    .split_once('=')
                    .ok_or_else(|| format!("--table must be <name>=<cp|ap>, got {spec:?}"))?;
                let regime = match regime.to_ascii_lowercase().as_str() {
                    "cp" => Regime::Cp,
                    "ap" => Regime::Ap,
                    other => return Err(format!("--table regime must be cp or ap, got {other:?}").into()),
                };
                tables.push((name.to_string(), regime));
            }
            "--help" | "-h" => {
                println!("{HELP}");
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    // `--cluster N` is sugar: derive a localhost ids-1..=N topology from --node-id + base port,
    // filling in voters/peers/listen/data unless they were given explicitly.
    if let Some(n) = cluster {
        if n == 0 {
            return Err("--cluster N must be >= 1".into());
        }
        if !voters.is_empty() || !peers.is_empty() {
            return Err("--cluster can't be combined with --voters/--peer".into());
        }
        voters = (1..=n).collect();
        for j in 1..=n {
            let ep = format!("127.0.0.1:{}", base_port as u64 + j);
            if j == node_id {
                listen.get_or_insert(ep);
            } else {
                peers.insert(j, as_uri(&ep));
            }
        }
        data_dir.get_or_insert_with(|| format!("./arcux-n{node_id}"));
    }

    let listen = listen.unwrap_or_else(|| "127.0.0.1:50051".to_string());
    let addr: SocketAddr = listen.parse()?;
    let opts = Options::new(data_dir.unwrap_or_else(|| "./arcux-data".to_string()));

    // Catalog mode: --table declarations tile the keyspace into per-regime regions (CP tables
    // as Raft groups, AP tables leaderless). Runs single-node unless a replica set was given.
    if !tables.is_empty() {
        if pd.is_some() {
            return Err("--table (catalog mode) and --pd are mutually exclusive".into());
        }
        let voters = if voters.is_empty() { vec![node_id] } else { voters };
        if !voters.contains(&node_id) {
            return Err(format!("--node-id {node_id} must be one of the voters {voters:?}").into());
        }
        for v in voters.iter().filter(|v| **v != node_id) {
            if !peers.contains_key(v) {
                return Err(format!("missing --peer {v}=<addr> for voter {v}").into());
            }
        }
        return arcux_server::serve_catalog(opts, addr, node_id, voters, peers, tables).await;
    }

    // Mode selection: replicated CP (voters set) ⇒ Raft, else --pd ⇒ PD, else direct.
    if !voters.is_empty() {
        if pd.is_some() {
            return Err("replicated CP (--voters/--cluster) and --pd are mutually exclusive".into());
        }
        if !voters.contains(&node_id) {
            return Err(format!("--node-id {node_id} must be one of the voters {voters:?}").into());
        }
        // Every other voter needs an address to replicate to.
        for v in voters.iter().filter(|v| **v != node_id) {
            if !peers.contains_key(v) {
                return Err(format!("missing --peer {v}=<addr> for voter {v}").into());
            }
        }
        return arcux_server::serve_replicated(opts, addr, node_id, voters, peers).await;
    }

    match pd {
        Some(pd_endpoint) => {
            arcux_server::serve_with_pd(opts, addr, pd_endpoint, node_id, address).await
        }
        None => arcux_server::serve(opts, addr).await,
    }
}

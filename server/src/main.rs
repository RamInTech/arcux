//! arcux-server — opens the Phase-1 engine and serves the gRPC API.
//!
//! Usage:
//!   arcux-server --pd <addr:port>[,<addr:port>...] [--data <dir>] [--listen <addr:port>]
//!                [--node-id <n>] [--address <uri>]
//!
//! **PD is required.** It holds the catalog, allocates every table's id, carves the one region
//! each table owns, and tells this node which regions to host, who the other voters are, and
//! where to reach them. A node is therefore configured with its own identity and PD's address —
//! never with a list of its peers, the way a TiKV store is given `--pd-endpoints` alone.
//!
//! Tables are created at runtime through the client (`create table <name> <cp|ap>`). A fresh
//! cluster already has the built-in `default` table, so `put k v` works with no setup at all.
//!
//! Defaults: --data ./arcux-data, --listen 127.0.0.1:50051, --node-id 1.

use std::net::SocketAddr;

use arcux_engine::Options;

const HELP: &str = "\
arcux-server --pd <addr:port> [--data <dir>] [--listen <addr:port>]
             [--node-id <n>] [--address <uri>]

      --pd        <addr:port>  the Placement Driver to join (required); a comma-separated list
                               for a replicated PD — the node follows its leader either way
  -d, --data      <dir>        data directory (default ./arcux-data)
  -l, --listen    <addr:port>  bind address (default 127.0.0.1:50051)
  -n, --node-id   <n>          this node's id (default 1)
      --address   <uri>        endpoint other nodes and clients reach this node at
                                (default: the bound --listen address)

a single node:
  arcux-pd                                     # terminal 1
  arcux-server --pd 127.0.0.1:2379             # terminal 2
  arcux                                        # terminal 3 (the shell)

a 3-node cluster (PD assigns the regions and tells each node about the others):
  arcux-pd
  arcux-server -n 1 --pd 127.0.0.1:2379 --listen 127.0.0.1:50061
  arcux-server -n 2 --pd 127.0.0.1:2379 --listen 127.0.0.1:50062
  arcux-server -n 3 --pd 127.0.0.1:2379 --listen 127.0.0.1:50063

tables are declared from the client, not here:
  arcux> create table ledger cp
  arcux> create table events ap";

/// Normalize a bare `addr:port` to a full `http://` URI (leaving an explicit scheme as-is).
fn as_uri(s: &str) -> String {
    if s.contains("://") {
        s.to_string()
    } else {
        format!("http://{s}")
    }
}

/// Print a failure as the sentence it is. Returning the error from `main` would print its
/// `Debug` form — `Custom { kind: AddrInUse, error: "…" }` — around a message written to be read.
#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("arcux-server: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut data_dir: Option<String> = None;
    let mut listen: Option<String> = None;
    let mut pd: Option<String> = None;
    let mut node_id: u64 = 1;
    let mut address: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" | "-d" => data_dir = Some(args.next().ok_or("--data requires a directory")?),
            "--listen" | "-l" => listen = Some(args.next().ok_or("--listen requires an addr:port")?),
            // One address or a comma-separated list; the node follows PD's leader either way.
            "--pd" => pd = Some(args.next().ok_or("--pd requires a PD addr:port")?),
            "--node-id" | "-n" => {
                node_id = args
                    .next()
                    .ok_or("--node-id requires a number")?
                    .parse()
                    .map_err(|_| "--node-id must be a u64")?;
            }
            "--address" => address = Some(as_uri(&args.next().ok_or("--address requires a uri")?)),
            "--help" | "-h" => {
                println!("{HELP}");
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    let listen = listen.unwrap_or_else(|| "127.0.0.1:50051".to_string());
    let addr: SocketAddr = listen.parse()?;
    let opts = Options::new(data_dir.unwrap_or_else(|| "./arcux-data".to_string()));

    // No PD, no node: it would have no way to learn its regions, and no way to allocate a table
    // id without risking two nodes giving one name two different ids.
    let Some(pd_endpoint) = pd else {
        return Err("--pd is required: PD holds the catalog and assigns this node its regions. \
                    Start one with `arcux-pd`, then pass --pd <addr:port>"
            .into());
    };

    arcux_server::serve_catalog(opts, addr, node_id, pd_endpoint, address).await
}

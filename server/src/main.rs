//! arcux-server — opens the Phase-1 engine and serves the gRPC API.
//!
//! Usage:
//!   arcux-server [--data <dir>] [--listen <addr:port>] [--pd <addr:port>] [--node-id <n>]
//!                [--address <uri>]
//!
//! Without `--pd` the node runs in direct single-node mode (a local timestamp oracle,
//! no routing enforcement). With `--pd` it joins a cluster: timestamps come from PD's
//! TSO and the node reports its regions to PD for client routing. `--address` is the
//! endpoint clients should reach this node at (defaults to the bound `--listen` address);
//! set it when the listen address isn't reachable as-is (e.g. behind NAT / `0.0.0.0`).
//!
//! Defaults: --data ./arcux-data, --listen 127.0.0.1:50051, --node-id 1

use std::net::SocketAddr;

use arcux_engine::Options;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut data_dir = String::from("./arcux-data");
    let mut listen = String::from("127.0.0.1:50051");
    let mut pd: Option<String> = None;
    let mut node_id: u64 = 1;
    let mut address: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" | "-d" => data_dir = args.next().ok_or("--data requires a directory")?,
            "--listen" | "-l" => listen = args.next().ok_or("--listen requires an addr:port")?,
            "--pd" => {
                let ep = args.next().ok_or("--pd requires a PD addr:port")?;
                // Accept a bare addr:port or a full URI.
                pd = Some(if ep.contains("://") { ep } else { format!("http://{ep}") });
            }
            "--node-id" => {
                node_id = args
                    .next()
                    .ok_or("--node-id requires a number")?
                    .parse()
                    .map_err(|_| "--node-id must be a u64")?;
            }
            "--address" => {
                let a = args.next().ok_or("--address requires a uri")?;
                address = Some(if a.contains("://") { a } else { format!("http://{a}") });
            }
            "--help" | "-h" => {
                println!(
                    "arcux-server [--data <dir>] [--listen <addr:port>] [--pd <addr:port>] \
                     [--node-id <n>] [--address <uri>]"
                );
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    let addr: SocketAddr = listen.parse()?;
    let opts = Options::new(data_dir);
    match pd {
        Some(pd_endpoint) => {
            arcux_server::serve_with_pd(opts, addr, pd_endpoint, node_id, address).await
        }
        None => arcux_server::serve(opts, addr).await,
    }
}

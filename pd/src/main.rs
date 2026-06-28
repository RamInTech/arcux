//! arcux-pd — the Placement Driver server: the cluster's TSO + region router.
//!
//! Usage:
//!   arcux-pd [--data <dir>] [--listen <addr:port>]
//!
//! Defaults: --data ./arcux-pd-data, --listen 127.0.0.1:2379

use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut data_dir = String::from("./arcux-pd-data");
    let mut listen = String::from("127.0.0.1:2379");

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" | "-d" => data_dir = args.next().ok_or("--data requires a directory")?,
            "--listen" | "-l" => listen = args.next().ok_or("--listen requires an addr:port")?,
            "--help" | "-h" => {
                println!("arcux-pd [--data <dir>] [--listen <addr:port>]");
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    std::fs::create_dir_all(&data_dir)?;
    let addr: SocketAddr = listen.parse()?;
    arcux_pd::server::serve(data_dir, addr).await
}
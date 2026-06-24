//! arcux-server — opens the Phase-1 engine and serves the gRPC API.
//!
//! Usage:
//!   arcux-server [--data <dir>] [--listen <addr:port>]
//!
//! Defaults: --data ./arcux-data, --listen 127.0.0.1:50051

use std::net::SocketAddr;

use arcux_engine::Options;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut data_dir = String::from("./arcux-data");
    let mut listen = String::from("127.0.0.1:50051");

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" | "-d" => {
                data_dir = args.next().ok_or("--data requires a directory")?;
            }
            "--listen" | "-l" => {
                listen = args.next().ok_or("--listen requires an addr:port")?;
            }
            "--help" | "-h" => {
                println!("arcux-server [--data <dir>] [--listen <addr:port>]");
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    let addr: SocketAddr = listen.parse()?;
    arcux_server::serve(Options::new(data_dir), addr).await
}

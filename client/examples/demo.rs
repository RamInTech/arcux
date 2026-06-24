//! arcux Phase 2 — end-to-end client demo.
//!
//! Drives a running `arcux-server` over gRPC: a single-key autocommit put/get, then a
//! multi-key transaction via `transact()`, then a snapshot read-back. Mirrors what the
//! `grpc_e2e` tests exercise in-process, but against a live node you started yourself.
//!
//! Usage:
//!   # terminal 1
//!   cargo run -p arcux-server -- --data /tmp/arcux --listen 127.0.0.1:50051
//!   # terminal 2
//!   cargo run -p arcux-client --example demo                 # defaults to 127.0.0.1:50051
//!   cargo run -p arcux-client --example demo http://host:port

use arcux_client::{put_mutation, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::args().nth(1).unwrap_or_else(|| "http://127.0.0.1:50051".to_string());
    println!("→ connecting to {uri}");
    let mut c = Client::connect(uri)?;

    // 1. Single-key autocommit put, then snapshot read at "now".
    let commit_ts = c.put(b"greeting".to_vec(), b"hello, arcux".to_vec()).await?;
    println!("put  greeting              committed @ ts {commit_ts}");
    let got = c.get(b"greeting".to_vec()).await?;
    println!("get  greeting           -> {}", render(&got));

    // 2. Multi-key transaction (first mutation is the primary) committed atomically.
    let txn_ts = c
        .transact(vec![
            put_mutation(b"acct:alice".to_vec(), b"100".to_vec()),
            put_mutation(b"acct:bob".to_vec(), b"50".to_vec()),
        ])
        .await?;
    println!("txn  {{alice,bob}}            committed @ ts {txn_ts}");

    // 3. Read both keys back at the post-commit snapshot.
    println!("get  acct:alice         -> {}", render(&c.get(b"acct:alice".to_vec()).await?));
    println!("get  acct:bob           -> {}", render(&c.get(b"acct:bob".to_vec()).await?));

    // 4. A key that was never written reads as absent.
    println!("get  acct:carol         -> {}", render(&c.get(b"acct:carol".to_vec()).await?));

    println!("✓ demo complete");
    Ok(())
}

/// Render an optional value as UTF-8 (falling back to bytes) or `<none>`.
fn render(v: &Option<Vec<u8>>) -> String {
    match v {
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => format!("{s:?}"),
            Err(_) => format!("{bytes:?}"),
        },
        None => "<none>".to_string(),
    }
}

//! arcux — region-aware routing demo.
//!
//! Drives a running PD + node(s) with a **routed** client: every table is its own region, the
//! client asks PD which region (and node) owns each key, and one transaction commits atomically
//! across two of those regions.
//!
//! Usage:
//!   cargo run -p arcux-pd                                  # PD on :2379
//!   cargo run -p arcux-server -- --pd 127.0.0.1:2379       # node on :50051
//!   cargo run -p arcux-client --example cluster            # this demo
//!
//! Endpoints come from $ARCUX_ADDR (a node, default http://127.0.0.1:50051) and $ARCUX_PD (PD,
//! default http://127.0.0.1:2379). Safe to run twice: re-creating a table with the regime it
//! already has is answered with that table.

use std::time::Duration;

use arcux_client::Client;
use arcux_rpc::kv::Regime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = std::env::var("ARCUX_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let pd = std::env::var("ARCUX_PD").unwrap_or_else(|_| "http://127.0.0.1:2379".to_string());
    println!("→ node {node}, pd {pd}");
    let mut c = Client::connect_with_pd(node, pd)?;

    // Each table is one region: `be32(id) ++ key`, so a table's range ends where the next begins.
    for (name, regime) in [("accounts", Regime::Cp), ("audit", Regime::Cp), ("views", Regime::Ap)] {
        let (id, region, ..) = c.create_table(name, regime).await?;
        println!("table {name:<9} id {id}  → region {region}  ({regime:?})");
    }

    // A just-founded CP region needs one election (up to ~600ms) before it can serve.
    put_until_ready(&mut c, "accounts", b"alice", b"100").await?;
    put_until_ready(&mut c, "audit", b"opened", b"alice").await?;
    c.put("views", b"home".to_vec(), b"1".to_vec()).await?;
    println!("put accounts/alice, audit/opened, views/home   (each routed to its own region)");

    // One transaction, two regions: Percolator prewrites both, then commits the primary key as
    // the single linearization point. Either both writes are visible or neither is.
    let muts = vec![
        c.put_mutation("accounts", b"alice".to_vec(), b"90".to_vec()).await?,
        c.put_mutation("audit", b"withdraw".to_vec(), b"alice:10".to_vec()).await?,
    ];
    let ts = c.transact(muts).await?;
    println!("txn accounts/alice + audit/withdraw   committed @ {ts}");

    for (table, key) in [("accounts", "alice"), ("audit", "withdraw"), ("views", "home")] {
        let v = c.get(table, key.as_bytes().to_vec()).await?;
        println!("get {table}/{key:<9} -> {}", render(&v));
    }
    println!("✓ cluster demo complete");
    Ok(())
}

async fn put_until_ready(
    c: &mut Client,
    table: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last = None;
    for _ in 0..40 {
        match c.put(table, key.to_vec(), value.to_vec()).await {
            Ok(_) => return Ok(()),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!("{table} never became writable: {}", last.map(|e| e.to_string()).unwrap_or_default()).into())
}

fn render(v: &Option<Vec<u8>>) -> String {
    match v {
        Some(b) => String::from_utf8_lossy(b).into_owned(),
        None => "<none>".to_string(),
    }
}

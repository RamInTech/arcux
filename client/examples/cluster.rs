//! arcux Phase 3 — region-aware cluster demo.
//!
//! Drives a running PD + data node with a routed client: a couple of writes, a region
//! split, then writes on both sides of the split — the client transparently re-routes.
//!
//! Usage (three terminals):
//!   cargo run -p arcux-pd                                  # PD on :2379
//!   cargo run -p arcux-server -- --pd 127.0.0.1:2379       # node on :50051, joined to PD
//!   cargo run -p arcux-client --example cluster            # this demo
//!
//! Endpoints come from $ARCUX_ADDR (node, default http://127.0.0.1:50051) and
//! $ARCUX_PD (PD, default http://127.0.0.1:2379).

use arcux_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = std::env::var("ARCUX_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let pd = std::env::var("ARCUX_PD").unwrap_or_else(|_| "http://127.0.0.1:2379".to_string());
    println!("→ node {node}, pd {pd}");
    let mut c = Client::connect_with_pd(node, pd)?;

    // One region covers the whole keyspace; the client routes to it via PD.
    c.put(b"apple".to_vec(), b"1".to_vec()).await?;
    c.put(b"mango".to_vec(), b"2".to_vec()).await?;
    println!("put apple, mango         (single whole-keyspace region)");

    // Split at "m": "apple" now lives in the left region, "mango" in the right.
    let (left, right) = c.split_region(b"m".to_vec()).await?;
    println!("split @ \"m\"              -> region {left} [.., \"m\") + region {right} [\"m\", ..)");

    // The client still cached the pre-split route, so these writes hit RegionStale,
    // re-resolve from PD, and retry — transparently to us.
    c.put(b"zebra".to_vec(), b"3".to_vec()).await?;
    c.put(b"acorn".to_vec(), b"4".to_vec()).await?;
    println!("put zebra, acorn         (re-routed across the split)");

    for k in ["acorn", "apple", "mango", "zebra"] {
        let v = c.get(k.as_bytes().to_vec()).await?;
        println!("get {k:<8}            -> {}", render(&v));
    }
    println!("✓ cluster demo complete");
    Ok(())
}

fn render(v: &Option<Vec<u8>>) -> String {
    match v {
        Some(b) => String::from_utf8_lossy(b).into_owned(),
        None => "<none>".to_string(),
    }
}

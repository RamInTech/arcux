//! arcux Phase 2 — ad-hoc CLI client.
//!
//! A one-shot command against a running `arcux-server`, so you can write from one
//! terminal and read from another — the state lives in the server, not the client.
//!
//! Server address comes from `$ARCUX_ADDR` (default `http://127.0.0.1:50051`).
//!
//! Usage:
//!   cargo run -p arcux-client --example cli -- put <key> <value>
//!   cargo run -p arcux-client --example cli -- get <key> [<read_ts>]
//!   cargo run -p arcux-client --example cli -- delete <key>
//!
//! `get <key>` reads the latest version ("now"); `get <key> <read_ts>` reads the MVCC
//! snapshot at that timestamp — the newest version whose `commit_ts <= read_ts` — so an
//! older `read_ts` recovers a value that a later write has since superseded.
//!
//! Example (two terminals, one server):
//!   cargo run -p arcux-client --example cli -- put greeting "hello, arcux"   # terminal A
//!   cargo run -p arcux-client --example cli -- get greeting                  # terminal B
//!   cargo run -p arcux-client --example cli -- get greeting 3                # read at ts 3

use arcux_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::var("ARCUX_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let args: Vec<String> = std::env::args().skip(1).collect();

    let mut c = Client::connect(uri)?;

    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["put", key, value] => {
            let ts = c.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()).await?;
            println!("OK  put {key:?} = {value:?}  (commit_ts {ts})");
        }
        ["get", key] => print_get(c.get(key.as_bytes().to_vec()).await?),
        ["get", key, read_ts] => {
            let ts: u64 = read_ts
                .parse()
                .map_err(|_| format!("read_ts must be a u64, got {read_ts:?}"))?;
            print_get(c.get_at(key.as_bytes().to_vec(), ts).await?);
        }
        ["delete", key] => {
            let ts = c.delete(key.as_bytes().to_vec()).await?;
            println!("OK  delete {key:?}  (commit_ts {ts})");
        }
        _ => {
            eprintln!("usage:");
            eprintln!("  cli -- put <key> <value>");
            eprintln!("  cli -- get <key> [<read_ts>]");
            eprintln!("  cli -- delete <key>");
            eprintln!("\nserver: $ARCUX_ADDR (default http://127.0.0.1:50051)");
            std::process::exit(2);
        }
    }
    Ok(())
}

/// Print a fetched value as UTF-8 (or bytes), or `<none>` and exit 1 if absent.
fn print_get(v: Option<Vec<u8>>) {
    match v {
        Some(bytes) => println!("{}", render(&bytes)),
        None => {
            println!("<none>");
            std::process::exit(1);
        }
    }
}

/// Render a value as UTF-8 if it is valid, otherwise as raw bytes.
fn render(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => format!("{bytes:?}"),
    }
}

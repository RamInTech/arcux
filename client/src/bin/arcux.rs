//! `arcux` — an interactive shell for a running `arcux-server`.
//!
//! Launching it prints the banner and drops you at a prompt; each line is one command
//! (`put`/`get`/`delete`/`scan`/…) sent to the server over the same async [`Client`] SDK
//! the `cli` example uses. State lives in the server, so you can read here what another
//! terminal wrote.
//!
//! Connection: `$ARCUX_ADDR` (default `http://127.0.0.1:50051`). Set `$ARCUX_PD` to a PD
//! endpoint to connect region-aware (routed per key) instead of direct single-node.
//!
//!   cargo run -p arcux-client --bin arcux
//!   # or install it so `arcux` works anywhere:
//!   cargo install --path client && arcux

use std::io::{self, Write};
use std::time::Duration;

use arcux_client::{Client, ClientError};

const BANNER: &str = r"
   █████╗ ██████╗  ██████╗██╗   ██╗██╗  ██╗
  ██╔══██╗██╔══██╗██╔════╝██║   ██║╚██╗██╔╝
  ███████║██████╔╝██║     ██║   ██║ ╚███╔╝
  ██╔══██║██╔══██╗██║     ██║   ██║ ██╔██╗
  ██║  ██║██║  ██║╚██████╗╚██████╔╝██╔╝ ██╗
  ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝";

const CYAN: &str = "\x1b[36m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pd = std::env::var("ARCUX_PD").ok();
    let endpoints = resolve_endpoints();

    // > 1 endpoint (a cluster list or ARCUX_CLUSTER) ⇒ leader-following; else single node
    // (or PD-routed if ARCUX_PD is set).
    let (mut client, mode) = if endpoints.len() > 1 {
        (Client::connect_cluster(endpoints.clone())?, format!("cluster of {} nodes, following the leader", endpoints.len()))
    } else if let Some(pd_uri) = &pd {
        (Client::connect_with_pd(endpoints[0].clone(), pd_uri.clone())?, format!("region-aware via PD {pd_uri}"))
    } else {
        (Client::connect(endpoints[0].clone())?, "direct".to_string())
    };

    print_banner(&endpoints, &mode);

    // In cluster mode, remember which node we last saw lead so we can announce a failover.
    let mut last_leader = client.current_endpoint();
    if let Some(l) = &last_leader {
        println!("  {DIM}leader (assumed): {l}{RESET}\n");
    }

    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        print!("{CYAN}arcux>{RESET} ");
        io::stdout().flush().ok();

        line.clear();
        // EOF (Ctrl-D) returns 0 bytes read — exit the shell cleanly.
        if stdin.read_line(&mut line)? == 0 {
            println!();
            break;
        }

        let args = tokenize(&line);
        let Some(cmd) = args.first().map(String::as_str) else {
            continue; // blank line
        };

        match cmd {
            "quit" | "exit" | "q" => break,
            "help" | "h" | "?" => print_help(),
            "leader" => match client.current_endpoint() {
                Some(l) => println!("{DIM}current leader (assumed): {l}{RESET}"),
                None => println!("{DIM}not in cluster mode (single node / PD){RESET}"),
            },
            "connect" => match args.get(1) {
                Some(uri) => match Client::connect(uri.clone()) {
                    Ok(c) => {
                        client = c;
                        last_leader = None;
                        println!("{DIM}connected to {uri}{RESET}");
                    }
                    Err(e) => eprintln!("error: {e}"),
                },
                None => eprintln!("usage: connect <uri>"),
            },
            _ => {
                run_command(&mut client, &args).await;
                announce_leader_change(&client, &mut last_leader);
            }
        }
    }

    println!("{DIM}bye{RESET}");
    Ok(())
}

/// Run one command, waiting out a mid-election window: in cluster mode a `NoLeader` result is
/// transient (the cluster is picking a new leader), so retry with a short backoff before giving
/// up. Any other error is reported immediately.
async fn run_command(client: &mut Client, args: &[String]) {
    let mut announced = false;
    for _ in 0..40 {
        match dispatch(client, args).await {
            Ok(()) => return,
            Err(e) if is_no_leader(e.as_ref()) => {
                if !announced {
                    eprintln!("{DIM}· no leader right now — waiting for the cluster to elect one…{RESET}");
                    announced = true;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => {
                eprintln!("{DIM}error:{RESET} {e}");
                return;
            }
        }
    }
    eprintln!("{DIM}error:{RESET} still no leader after retrying — is a majority of the cluster up?");
}

/// True if the boxed error is a transient `NoLeader` (cluster mid-election).
fn is_no_leader(e: &(dyn std::error::Error + 'static)) -> bool {
    matches!(e.downcast_ref::<ClientError>(), Some(ClientError::NoLeader))
}

/// After an op, if the presumed leader endpoint changed (a failover), say so.
fn announce_leader_change(client: &Client, last: &mut Option<String>) {
    let now = client.current_endpoint();
    if now.is_some() && now != *last {
        if let Some(l) = &now {
            println!("{DIM}· leader is now {l}{RESET}");
        }
        *last = now;
    }
}

/// The node endpoints to talk to, from the environment:
/// - `ARCUX_CLUSTER=N` (+ optional `ARCUX_BASE_PORT`, default 50060) ⇒ a localhost cluster of
///   ids `1..=N` on `base+i` (mirrors the server's `-c N`);
/// - else `ARCUX_ADDR` split on commas (one or many);
/// - else the single default `http://127.0.0.1:50051`.
fn resolve_endpoints() -> Vec<String> {
    if let Ok(n) = std::env::var("ARCUX_CLUSTER") {
        if let Ok(n) = n.parse::<u64>() {
            let base: u64 = std::env::var("ARCUX_BASE_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(50060);
            return (1..=n).map(|i| format!("http://127.0.0.1:{}", base + i)).collect();
        }
    }
    let addr = std::env::var("ARCUX_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    addr.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

/// Run one KV command; returns an error (printed, not fatal) rather than exiting the shell.
async fn dispatch(client: &mut Client, args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    match a.as_slice() {
        ["put", key, value] => {
            let ts = client.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()).await?;
            println!("OK  {DIM}commit_ts {ts}{RESET}");
        }
        ["get", key] => print_value(client.get(key.as_bytes().to_vec()).await?),
        ["get", key, read_ts] => {
            let ts: u64 = read_ts.parse().map_err(|_| format!("read_ts must be a u64, got {read_ts:?}"))?;
            print_value(client.get_at(key.as_bytes().to_vec(), ts).await?);
        }
        ["del" | "delete", key] => {
            let ts = client.delete(key.as_bytes().to_vec()).await?;
            println!("OK  {DIM}commit_ts {ts}{RESET}");
        }
        ["scan", start, end] => print_pairs(client.scan(start.as_bytes().to_vec(), end.as_bytes().to_vec(), 0).await?),
        ["scan", start, end, limit] => {
            let limit: u32 = limit.parse().map_err(|_| format!("limit must be a u32, got {limit:?}"))?;
            print_pairs(client.scan(start.as_bytes().to_vec(), end.as_bytes().to_vec(), limit).await?);
        }
        ["split", key] => {
            let (l, r) = client.split_region(key.as_bytes().to_vec()).await?;
            println!("OK  {DIM}split into regions {l} | {r}{RESET}");
        }
        ["merge", key] => {
            let id = client.merge_region(key.as_bytes().to_vec()).await?;
            println!("OK  {DIM}merged region {id}{RESET}");
        }
        [other, ..] => return Err(format!("unknown command {other:?} — try 'help'").into()),
        [] => {}
    }
    Ok(())
}

fn print_banner(endpoints: &[String], mode: &str) {
    println!("{CYAN}{BANNER}{RESET}");
    println!("  {DIM}a distributed transactional KV store{RESET}");
    if endpoints.len() == 1 {
        println!("  {BOLD}server{RESET} {}  {DIM}({mode}){RESET}", endpoints[0]);
    } else {
        println!("  {BOLD}servers{RESET} {}  {DIM}({mode}){RESET}", endpoints.join(", "));
    }
    println!("  {DIM}type 'help' for commands, 'quit' to exit{RESET}");
}

fn print_help() {
    println!(
        "\
commands:
  put <key> <value>          write a value (autocommit); prints commit_ts
  get <key> [read_ts]        read latest, or the MVCC snapshot at read_ts
  delete <key>               delete a key (autocommit)
  scan <start> <end> [limit] range scan [start, end) (empty end = to the end)
  split <key>                split the region owning <key> at <key>
  merge <key>                merge the region starting at <key> leftward
  connect <uri>              point the shell at a different server
  leader                     show the node currently assumed to be the leader
  help | quit

notes:
  values with spaces: put greeting \"hello, arcux\"
  a \"table\" is a key-prefix (t/...); CP-vs-AP is a server-side placement, not a command yet
  cluster mode: set ARCUX_CLUSTER=3 (or a comma-separated ARCUX_ADDR) to auto-follow the leader"
    );
}

/// Print a fetched value as UTF-8 (or raw bytes), or `<none>`.
fn print_value(v: Option<Vec<u8>>) {
    match v {
        Some(bytes) => println!("{}", render(&bytes)),
        None => println!("{DIM}<none>{RESET}"),
    }
}

fn print_pairs(pairs: Vec<(Vec<u8>, Vec<u8>)>) {
    if pairs.is_empty() {
        println!("{DIM}<empty>{RESET}");
        return;
    }
    for (k, v) in &pairs {
        println!("  {} = {}", render(k), render(v));
    }
    println!("{DIM}{} row(s){RESET}", pairs.len());
}

/// Render bytes as UTF-8 if valid, else as a byte-array debug string.
fn render(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => format!("{bytes:?}"),
    }
}

/// Split a line into tokens on whitespace, honouring `"double quotes"` so a value can
/// contain spaces (`put k "a b c"` → ["put", "k", "a b c"]).
fn tokenize(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    let mut has_token = false;
    for ch in line.chars() {
        match ch {
            '"' => has_token = true, // toggle handled below without emitting the quote
            c if c.is_whitespace() && !in_quote => {
                if has_token {
                    out.push(std::mem::take(&mut cur));
                    has_token = false;
                }
            }
            c => {
                cur.push(c);
                has_token = true;
            }
        }
        if ch == '"' {
            in_quote = !in_quote;
        }
    }
    if has_token {
        out.push(cur);
    }
    out
}

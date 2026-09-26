//! A node's connection to the Placement Driver — one link, shared by the heartbeat, table
//! creation and the timestamp oracle, that **follows PD's leader**.
//!
//! PD is a Raft group. Only its leader answers: a follower replies `unavailable` with
//! `not pd leader; leader at <addr>` (see `pd/src/raft_server.rs::redirect`). A node that knew one
//! PD address and never followed that hint would, after a PD failover, be told "not leader" by
//! every heartbeat and every timestamp request forever — and since CP transactions take their
//! timestamps from PD, every CP write in the cluster would stop.
//!
//! So a call here: goes to the PD believed to lead; on a redirect naming the leader, goes there
//! next; on a redirect with no leader yet, or a PD that cannot be reached, moves to the next
//! address and waits briefly for an election. The number of attempts is bounded, so a PD outage
//! surfaces as an error rather than a hang, and every channel carries a timeout so a PD that is
//! alive but not answering cannot hang its caller either.

use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use arcux_rpc::pd::pd_service_client::PdServiceClient;
use tonic::transport::{Channel, Endpoint};
use tonic::{Response, Status};

/// How long one PD call may take. `create_table` is the slowest — a Raft round plus a push to
/// every node — and stays well inside this; a PD that exceeds it is treated as unreachable.
const PD_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const PD_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// Pause after a redirect with no leader, or an unreachable PD: long enough for a PD election
/// (hundreds of ms) to finish, short enough that a caller is not kept waiting for nothing.
const PD_RETRY_PAUSE: Duration = Duration::from_millis(100);

pub struct PdLink {
    /// Every PD address known: those given with `--pd`, plus any a redirect has named.
    endpoints: Mutex<Vec<String>>,
    /// Index into `endpoints` of the PD believed to lead.
    current: Mutex<usize>,
}

impl PdLink {
    /// A link to the PD group at `endpoints` (a comma-separated `--pd`, already split). Lazy: no
    /// connection is made until the first call, so a node can be built before PD is up.
    pub fn new(endpoints: Vec<String>) -> Result<PdLink, Box<dyn std::error::Error + Send + Sync>> {
        let endpoints: Vec<String> = endpoints.into_iter().map(|e| as_uri(&e)).collect();
        if endpoints.is_empty() {
            return Err("--pd needs at least one address".into());
        }
        for e in &endpoints {
            Endpoint::from_shared(e.clone()).map_err(|err| format!("invalid PD address {e:?}: {err}"))?;
        }
        Ok(PdLink { endpoints: Mutex::new(endpoints), current: Mutex::new(0) })
    }

    /// Parse `--pd`: one address, or several separated by commas (`a:2379,b:2380,c:2381`).
    pub fn parse(arg: &str) -> Result<PdLink, Box<dyn std::error::Error + Send + Sync>> {
        PdLink::new(arg.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect())
    }

    /// The PD address currently believed to lead — for messages and logs.
    pub fn current_endpoint(&self) -> String {
        let eps = self.endpoints.lock().expect("pd endpoints poisoned");
        eps[*self.current.lock().expect("pd current poisoned") % eps.len()].clone()
    }

    fn client(&self) -> PdServiceClient<Channel> {
        let endpoint = Endpoint::from_shared(self.current_endpoint())
            .expect("validated in PdLink::new")
            .connect_timeout(PD_CONNECT_TIMEOUT)
            .timeout(PD_RPC_TIMEOUT);
        PdServiceClient::new(endpoint.connect_lazy())
    }

    /// Point at `addr` next — the leader a redirect named — adding it if it is new.
    fn follow(&self, addr: &str) {
        let addr = as_uri(addr);
        let mut eps = self.endpoints.lock().expect("pd endpoints poisoned");
        let idx = match eps.iter().position(|e| *e == addr) {
            Some(i) => i,
            None => {
                eps.push(addr);
                eps.len() - 1
            }
        };
        *self.current.lock().expect("pd current poisoned") = idx;
    }

    fn rotate(&self) {
        let n = self.endpoints.lock().expect("pd endpoints poisoned").len();
        let mut cur = self.current.lock().expect("pd current poisoned");
        *cur = (*cur + 1) % n;
    }

    /// Run `call` against PD's leader, following redirects and skipping unreachable addresses.
    /// A status PD itself returned for any other reason is the answer, and is returned as is.
    pub async fn call<T, F, Fut>(&self, mut call: F) -> Result<T, Status>
    where
        F: FnMut(PdServiceClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        // Every address twice over: once to find the leader, once more across an election.
        let attempts = 2 * self.endpoints.lock().expect("pd endpoints poisoned").len() + 1;
        let mut last = Status::unavailable("PD is unreachable");
        for _ in 0..attempts {
            match call(self.client()).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(s) => {
                    match redirect(&s) {
                        Some(Some(leader)) => self.follow(&leader),
                        Some(None) => {
                            self.rotate();
                            tokio::time::sleep(PD_RETRY_PAUSE).await;
                        }
                        // Never reached PD (connection refused, timed out): try the next one.
                        None if std::error::Error::source(&s).is_some()
                            || s.code() == tonic::Code::DeadlineExceeded =>
                        {
                            self.rotate();
                            tokio::time::sleep(PD_RETRY_PAUSE).await;
                        }
                        None => return Err(s),
                    }
                    last = s;
                }
            }
        }
        Err(last)
    }
}

/// Is this PD's "not the leader" answer? `Some(Some(addr))` names the leader, `Some(None)` means
/// no leader is elected yet, `None` is any other status.
fn redirect(s: &Status) -> Option<Option<String>> {
    if s.code() != tonic::Code::Unavailable {
        return None;
    }
    let msg = s.message();
    if !msg.starts_with("not pd leader") {
        return None;
    }
    Some(msg.split_once("leader at ").map(|(_, addr)| addr.trim().to_string()))
}

fn as_uri(s: &str) -> String {
    if s.contains("://") {
        s.to_string()
    } else {
        format!("http://{s}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_redirect_naming_the_leader_is_followed() {
        let s = Status::unavailable("not pd leader; leader at http://127.0.0.1:2380");
        assert_eq!(redirect(&s), Some(Some("http://127.0.0.1:2380".to_string())));
        assert_eq!(redirect(&Status::unavailable("not pd leader; no leader elected yet")), Some(None));
        // Any other refusal is PD's answer, not a redirect.
        assert_eq!(redirect(&Status::unavailable("create_table: only 1 of 3 voters")), None);
        assert_eq!(redirect(&Status::already_exists("table exists")), None);
    }

    #[test]
    fn follow_points_at_the_named_leader_and_learns_new_addresses() {
        let link = PdLink::parse("127.0.0.1:2379, 127.0.0.1:2380").unwrap();
        assert_eq!(link.current_endpoint(), "http://127.0.0.1:2379");
        link.follow("http://127.0.0.1:2380");
        assert_eq!(link.current_endpoint(), "http://127.0.0.1:2380");
        link.follow("http://127.0.0.1:2381"); // a PD member not given with --pd
        assert_eq!(link.current_endpoint(), "http://127.0.0.1:2381");
        link.rotate();
        assert_eq!(link.current_endpoint(), "http://127.0.0.1:2379", "rotation wraps");
    }

    #[test]
    fn an_empty_or_malformed_pd_list_is_refused() {
        assert!(PdLink::parse("").is_err());
        assert!(PdLink::parse(" , ").is_err());
    }
}

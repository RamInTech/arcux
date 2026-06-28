//! arcux Phase 2/3 — async gRPC client SDK.
//!
//! A thin async wrapper over the generated `KvServiceClient` holding one shared HTTP/2
//! [`Channel`] (multiplexed; the connection-pool-per-peer generalization arrives later).
//! It mirrors the KV RPCs and adds a [`Client::transact`] convenience that runs
//! `begin → prewrite → commit`. Application-level conflicts/locks surface as a typed
//! [`ClientError::Key`]; the blocking client is deferred to Phase 2b.
//!
//! ## Phase 3 — region routing
//!
//! Constructed with [`Client::connect_with_pd`], the client becomes region-aware: it
//! resolves each key's region from PD (caching the result), stamps the routing
//! [`kv::Context`] on every request, and — when the server reports `RegionStale`
//! (the region split out from under a cached route) — invalidates the cache, re-resolves
//! from PD, and retries. [`Client::connect`] keeps the Phase-2 direct behaviour (no
//! routing context), so single-node callers need no PD.

use std::fmt;
use std::sync::{Arc, Mutex};

use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use arcux_rpc::pd;
use arcux_rpc::pd::pd_service_client::PdServiceClient;
use tonic::transport::Channel;

pub use arcux_rpc::kv::Mutation;

/// A generous logical lease (TSO ticks) added to `start_ts` to form a lock's expiry in
/// the [`Client::transact`] convenience. The server TSO is a monotonic counter, so this
/// keeps a transaction's locks from ever looking expired mid-flight.
const DEFAULT_LEASE: u64 = 1 << 32;

/// How many times a routed call re-resolves and retries after a `RegionStale` before
/// giving up. A handful is plenty: each retry follows a real topology change.
const MAX_ROUTING_ATTEMPTS: usize = 5;

pub type Result<T> = std::result::Result<T, ClientError>;

/// Errors a client call can surface.
#[derive(Debug)]
pub enum ClientError {
    /// Transport/connection failure (could not reach the server, bad URI, …).
    Transport(tonic::transport::Error),
    /// The RPC itself failed with a gRPC status (e.g. `Unimplemented`, `Internal`).
    Rpc(tonic::Status),
    /// A normal protocol outcome the server reported in-band (conflict, live lock, …).
    Key(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Transport(e) => write!(f, "transport error: {e}"),
            ClientError::Rpc(s) => write!(f, "rpc error: {} ({})", s.message(), s.code()),
            ClientError::Key(m) => write!(f, "key error: {m}"),
        }
    }
}

impl std::error::Error for ClientError {}

fn key_error(ke: kv::KeyError) -> ClientError {
    let msg = match ke.kind {
        Some(Kind::Conflict(c)) => format!("conflict: {}", c.detail),
        Some(Kind::Locked(l)) => format!("locked by primary {:?} (ttl {})", l.primary, l.ttl),
        Some(Kind::Invalid(s)) => format!("invalid: {s}"),
        Some(Kind::Retryable(s)) => format!("retryable: {s}"),
        Some(Kind::NotLeader(_)) => "not leader".to_string(),
        Some(Kind::RegionStale(rs)) => format!("region stale (new epoch {})", rs.new_epoch),
        None => "unspecified key error".to_string(),
    };
    ClientError::Key(msg)
}

/// Whether a key-error is a `RegionStale` (the signal to re-route and retry).
fn is_region_stale(ke: &kv::KeyError) -> bool {
    matches!(ke.kind, Some(Kind::RegionStale(_)))
}

/// Build a PUT mutation for use with [`Client::transact`].
pub fn put_mutation(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Mutation {
    Mutation { op: kv::Op::Put as i32, key: key.into(), value: value.into() }
}

/// Build a DELETE mutation for use with [`Client::transact`].
pub fn delete_mutation(key: impl Into<Vec<u8>>) -> Mutation {
    Mutation { op: kv::Op::Delete as i32, key: key.into(), value: vec![] }
}

// ------------------------------------------------------------------------------------
// Region routing
// ------------------------------------------------------------------------------------

fn region_contains(r: &pd::Region, key: &[u8]) -> bool {
    key >= r.start_key.as_slice() && (r.end_key.is_empty() || key < r.end_key.as_slice())
}

/// Do two half-open key ranges intersect? (Empty `end_key` is +∞.)
fn ranges_overlap(a: &pd::Region, b: &pd::Region) -> bool {
    let a_ends_at_or_before_b = !a.end_key.is_empty() && a.end_key.as_slice() <= b.start_key.as_slice();
    let b_ends_at_or_before_a = !b.end_key.is_empty() && b.end_key.as_slice() <= a.start_key.as_slice();
    !(a_ends_at_or_before_b || b_ends_at_or_before_a)
}

/// A PD-backed routing cache: resolves a key to its owning region and remembers it.
/// Shared across `Client` clones, so all handles benefit from a warm cache.
#[derive(Clone)]
struct Router {
    pd: PdServiceClient<Channel>,
    cache: Arc<Mutex<Vec<pd::Region>>>,
}

impl Router {
    /// The routing context for `key`: from cache, or resolved from PD on a miss.
    async fn context_for(&self, key: &[u8]) -> Result<kv::Context> {
        if let Some(r) = self.lookup(key) {
            return Ok(kv::Context { region_id: r.id, region_epoch: r.epoch });
        }
        let mut pd = self.pd.clone();
        let resp = pd
            .get_region(pd::GetRegionRequest { key: key.to_vec() })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        let region = pd::Region {
            id: resp.region_id,
            start_key: resp.start_key,
            end_key: resp.end_key,
            epoch: resp.epoch,
        };
        let ctx = kv::Context { region_id: region.id, region_epoch: region.epoch };
        self.insert(region);
        Ok(ctx)
    }

    fn lookup(&self, key: &[u8]) -> Option<pd::Region> {
        self.cache.lock().unwrap().iter().find(|r| region_contains(r, key)).cloned()
    }

    fn insert(&self, region: pd::Region) {
        let mut c = self.cache.lock().unwrap();
        // Drop any cached range the fresh one supersedes (e.g. the pre-split parent).
        c.retain(|r| !ranges_overlap(r, &region));
        c.push(region);
    }

    /// Forget every cached region covering `key` (after a `RegionStale`).
    fn invalidate(&self, key: &[u8]) {
        self.cache.lock().unwrap().retain(|r| !region_contains(r, key));
    }
}

/// An async client over one multiplexed HTTP/2 channel. Cheap to `clone` (the channel
/// and routing cache are shared), so concurrent callers each take their own handle.
#[derive(Clone)]
pub struct Client {
    kv: KvServiceClient<Channel>,
    /// `Some` ⇒ region-aware (routes via PD); `None` ⇒ direct single-node access.
    router: Option<Router>,
}

impl Client {
    /// Connect lazily to a single KV node at `uri` (e.g. `"http://127.0.0.1:50051"`),
    /// in **direct** mode — no PD, no routing context. The TCP/HTTP-2 connection is
    /// established on the first request, so there is no startup race with a server that
    /// is still binding.
    pub fn connect(uri: impl Into<String>) -> Result<Client> {
        let channel = lazy_channel(uri)?;
        Ok(Client { kv: KvServiceClient::new(channel), router: None })
    }

    /// Connect lazily to a KV node and a PD, in **region-aware** mode: requests carry a
    /// routing context resolved (and cached) from PD, and `RegionStale` responses are
    /// transparently re-routed and retried.
    pub fn connect_with_pd(kv_uri: impl Into<String>, pd_uri: impl Into<String>) -> Result<Client> {
        let kv = KvServiceClient::new(lazy_channel(kv_uri)?);
        let pd = PdServiceClient::new(lazy_channel(pd_uri)?);
        let router = Router { pd, cache: Arc::new(Mutex::new(Vec::new())) };
        Ok(Client { kv, router: Some(router) })
    }

    /// The routing context to stamp on a request for `key` (`None` in direct mode).
    async fn context_for(&self, key: &[u8]) -> Result<Option<kv::Context>> {
        match &self.router {
            Some(r) => Ok(Some(r.context_for(key).await?)),
            None => Ok(None),
        }
    }

    /// Drop `key`'s cached route after a `RegionStale` (a no-op in direct mode).
    fn invalidate(&self, key: &[u8]) {
        if let Some(r) = &self.router {
            r.invalidate(key);
        }
    }

    /// Allocate a transaction `start_ts`. (Timestamps are global, so begin needs no
    /// routing context.)
    pub async fn begin(&mut self) -> Result<u64> {
        let resp = self
            .kv
            .begin(kv::BeginRequest {})
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        Ok(resp.start_ts)
    }

    /// Prewrite all mutations (primary first). Returns the first per-key error if any.
    /// Routed on the primary key; retried on `RegionStale`.
    pub async fn prewrite(
        &mut self,
        start_ts: u64,
        primary: Vec<u8>,
        mutations: Vec<Mutation>,
        ttl: u64,
    ) -> Result<()> {
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let context = self.context_for(&primary).await?;
            let resp = self
                .kv
                .prewrite(kv::PrewriteRequest {
                    start_ts,
                    primary: primary.clone(),
                    mutations: mutations.clone(),
                    ttl,
                    context,
                })
                .await
                .map_err(ClientError::Rpc)?
                .into_inner();
            match resp.errors.into_iter().next() {
                Some(ke) if is_region_stale(&ke) => {
                    self.invalidate(&primary);
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(()),
            }
        }
        Err(routing_exhausted())
    }

    /// Commit a prewritten transaction; returns the server-assigned `commit_ts`. Routed
    /// on the primary key; retried on `RegionStale`.
    pub async fn commit(
        &mut self,
        start_ts: u64,
        primary: Vec<u8>,
        keys: Vec<Vec<u8>>,
    ) -> Result<u64> {
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let context = self.context_for(&primary).await?;
            let resp = self
                .kv
                .commit(kv::CommitRequest {
                    start_ts,
                    primary: primary.clone(),
                    keys: keys.clone(),
                    context,
                })
                .await
                .map_err(ClientError::Rpc)?
                .into_inner();
            match resp.error {
                Some(ke) if is_region_stale(&ke) => {
                    self.invalidate(&primary);
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(resp.commit_ts),
            }
        }
        Err(routing_exhausted())
    }

    /// Snapshot read at "now" (the server picks a fresh `read_ts`).
    pub async fn get(&mut self, key: impl Into<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        self.get_at(key, 0).await
    }

    /// Snapshot read at an explicit `read_ts` (0 ⇒ server picks "now").
    pub async fn get_at(&mut self, key: impl Into<Vec<u8>>, read_ts: u64) -> Result<Option<Vec<u8>>> {
        let key = key.into();
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let context = self.context_for(&key).await?;
            let resp = self
                .kv
                .get(kv::GetRequest { key: key.clone(), read_ts, context })
                .await
                .map_err(ClientError::Rpc)?
                .into_inner();
            match resp.error {
                Some(ke) if is_region_stale(&ke) => {
                    self.invalidate(&key);
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(if resp.found { Some(resp.value) } else { None }),
            }
        }
        Err(routing_exhausted())
    }

    /// Autocommit single-key put; returns the `commit_ts`. Routed on the key.
    pub async fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<u64> {
        let key = key.into();
        let value = value.into();
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let context = self.context_for(&key).await?;
            let resp = self
                .kv
                .put(kv::PutRequest { key: key.clone(), value: value.clone(), context })
                .await
                .map_err(ClientError::Rpc)?
                .into_inner();
            match resp.error {
                Some(ke) if is_region_stale(&ke) => {
                    self.invalidate(&key);
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(resp.commit_ts),
            }
        }
        Err(routing_exhausted())
    }

    /// Autocommit single-key delete; returns the `commit_ts`. Routed on the key.
    pub async fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<u64> {
        let key = key.into();
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let context = self.context_for(&key).await?;
            let resp = self
                .kv
                .delete(kv::DeleteRequest { key: key.clone(), context })
                .await
                .map_err(ClientError::Rpc)?
                .into_inner();
            match resp.error {
                Some(ke) if is_region_stale(&ke) => {
                    self.invalidate(&key);
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(resp.commit_ts),
            }
        }
        Err(routing_exhausted())
    }

    /// Split the region owning `split_key` at that key (operational). Returns the
    /// `(left, right)` region ids the node created.
    pub async fn split_region(&mut self, split_key: impl Into<Vec<u8>>) -> Result<(u64, u64)> {
        let resp = self
            .kv
            .split_region(kv::SplitRegionRequest { split_key: split_key.into() })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        let left = resp.left.map(|r| r.id).unwrap_or(0);
        let right = resp.right.map(|r| r.id).unwrap_or(0);
        Ok((left, right))
    }

    /// Range scan — frozen in the wire contract, but the server returns `Unimplemented`
    /// until the Phase 1b iterator lands.
    pub async fn scan(
        &mut self,
        start_key: impl Into<Vec<u8>>,
        end_key: impl Into<Vec<u8>>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_key = start_key.into();
        let context = self.context_for(&start_key).await?;
        let resp = self
            .kv
            .scan(kv::ScanRequest { start_key, end_key: end_key.into(), limit, read_ts: 0, context })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        Ok(resp.pairs.into_iter().map(|p| (p.key, p.value)).collect())
    }

    /// Convenience: run a full transaction (`begin → prewrite → commit`) over `mutations`
    /// (the first is the primary). Returns the `commit_ts`.
    pub async fn transact(&mut self, mutations: Vec<Mutation>) -> Result<u64> {
        let primary = match mutations.first() {
            Some(m) => m.key.clone(),
            None => return Err(ClientError::Key("empty transaction".to_string())),
        };
        let keys: Vec<Vec<u8>> = mutations.iter().map(|m| m.key.clone()).collect();
        let start_ts = self.begin().await?;
        let ttl = start_ts.saturating_add(DEFAULT_LEASE);
        self.prewrite(start_ts, primary.clone(), mutations, ttl).await?;
        self.commit(start_ts, primary, keys).await
    }
}

fn lazy_channel(uri: impl Into<String>) -> Result<Channel> {
    Ok(Channel::from_shared(uri.into())
        .map_err(|e| ClientError::Key(format!("invalid endpoint: {e}")))?
        .connect_lazy())
}

fn routing_exhausted() -> ClientError {
    ClientError::Key("routing retries exhausted (region kept moving?)".to_string())
}

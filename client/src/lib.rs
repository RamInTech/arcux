//! arcux Phase 2/3/3b — async gRPC client SDK.
//!
//! A thin async wrapper over the generated `KvServiceClient`. It mirrors the KV RPCs and
//! adds a [`Client::transact`] convenience that runs `begin → prewrite → commit`.
//! Application-level conflicts/locks surface as a typed [`ClientError::Key`]; the blocking
//! client is deferred to Phase 2b.
//!
//! ## Phase 3 / 3b — region routing across nodes
//!
//! Constructed with [`Client::connect_with_pd`], the client is region-aware. It resolves
//! each key's region **and owning node** from PD (caching the result, ordered by start key
//! and **binary-searched**), opens one channel **per node** (a pool keyed by address), and
//! dispatches each request to the region's owner. When the server reports `RegionStale`
//! (the region split/merged out from under a cached route) **or** `NotLeader` (the owning
//! replica is no longer the leader — meaningful once Phase 4 adds per-region Raft), it
//! invalidates the cached route, re-resolves from PD, and retries. [`Client::connect`]
//! keeps the Phase-2 direct behaviour (no routing context, one node), so single-node
//! callers need no PD.

use std::collections::HashMap;
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

/// How many times a routed call re-resolves and retries after a `RegionStale`/`NotLeader`
/// before giving up. A handful is plenty: each retry follows a real topology change.
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

/// Whether a key-error means "your route is wrong — re-resolve and retry": a `RegionStale`
/// (epoch moved under us) or a `NotLeader` (the owning replica isn't the leader anymore).
fn is_reroute(ke: &kv::KeyError) -> bool {
    matches!(ke.kind, Some(Kind::RegionStale(_)) | Some(Kind::NotLeader(_)))
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

/// A cached region descriptor, including the address of the node that owns it.
#[derive(Clone)]
struct CachedRegion {
    id: u64,
    start: Vec<u8>,
    end: Vec<u8>,
    epoch: u64,
    address: String,
}

impl CachedRegion {
    fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && (self.end.is_empty() || key < self.end.as_slice())
    }
}

/// Do two half-open key ranges intersect? (Empty `end` is +∞.)
fn ranges_overlap(a: &CachedRegion, b: &CachedRegion) -> bool {
    let a_before_b = !a.end.is_empty() && a.end.as_slice() <= b.start.as_slice();
    let b_before_a = !b.end.is_empty() && b.end.as_slice() <= a.start.as_slice();
    !(a_before_b || b_before_a)
}

/// A PD-backed routing layer: resolves a key to its owning region + node (caching the
/// result, binary-searched) and pools one KV channel per node. Shared across `Client`
/// clones, so all handles benefit from a warm cache and a shared connection pool.
#[derive(Clone)]
struct Routing {
    pd: PdServiceClient<Channel>,
    /// Cached regions, kept sorted by `start` so lookup is a binary search.
    cache: Arc<Mutex<Vec<CachedRegion>>>,
    /// One KV client per node address (multiplexed HTTP/2 channels).
    pool: Arc<Mutex<HashMap<String, KvServiceClient<Channel>>>>,
    /// A seed KV endpoint used when PD reports a region with no address (legacy/unplaced).
    fallback_kv: Option<String>,
}

impl Routing {
    /// Resolve `key` to `(routing context, the owning node's KV client)`, from cache or —
    /// on a miss — from PD.
    async fn resolve(&self, key: &[u8]) -> Result<(kv::Context, KvServiceClient<Channel>)> {
        if let Some(r) = self.lookup(key) {
            let client = self.client_for(&r.address)?;
            return Ok((kv::Context { region_id: r.id, region_epoch: r.epoch }, client));
        }
        let mut pd = self.pd.clone();
        let resp = pd
            .get_region(pd::GetRegionRequest { key: key.to_vec() })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        let region = CachedRegion {
            id: resp.region_id,
            start: resp.start_key,
            end: resp.end_key,
            epoch: resp.epoch,
            address: resp.address,
        };
        let client = self.client_for(&region.address)?;
        let ctx = kv::Context { region_id: region.id, region_epoch: region.epoch };
        self.insert(region);
        Ok((ctx, client))
    }

    /// Binary-search the cache for the region containing `key`.
    fn lookup(&self, key: &[u8]) -> Option<CachedRegion> {
        let c = self.cache.lock().unwrap();
        // Rightmost region whose start is <= key; it's the only one that can contain key.
        let idx = c.partition_point(|r| r.start.as_slice() <= key);
        if idx == 0 {
            return None;
        }
        let r = &c[idx - 1];
        if r.contains(key) {
            Some(r.clone())
        } else {
            None // a gap (e.g. an evicted/stale neighbour) — force a PD re-resolve
        }
    }

    /// Insert a freshly-resolved region, dropping any range it supersedes and keeping the
    /// cache sorted by start key.
    fn insert(&self, region: CachedRegion) {
        let mut c = self.cache.lock().unwrap();
        c.retain(|r| !ranges_overlap(r, &region));
        let pos = c.partition_point(|r| r.start < region.start);
        c.insert(pos, region);
    }

    /// Forget every cached region covering `key` (after a `RegionStale`/`NotLeader`).
    fn invalidate(&self, key: &[u8]) {
        self.cache.lock().unwrap().retain(|r| !r.contains(key));
    }

    /// The pooled KV client for `address` (lazily connected), or the fallback endpoint
    /// when PD reported no address.
    fn client_for(&self, address: &str) -> Result<KvServiceClient<Channel>> {
        let address = if address.is_empty() {
            self.fallback_kv
                .clone()
                .ok_or_else(|| ClientError::Key("region has no node address and no fallback".into()))?
        } else {
            address.to_string()
        };
        let mut pool = self.pool.lock().unwrap();
        if let Some(c) = pool.get(&address) {
            return Ok(c.clone());
        }
        let client = KvServiceClient::new(lazy_channel(&address)?);
        pool.insert(address, client.clone());
        Ok(client)
    }
}

/// An async, region-aware client. Cheap to `clone` (the channel pool and routing cache are
/// shared), so concurrent callers each take their own handle.
#[derive(Clone)]
pub struct Client {
    /// `Some` ⇒ region-aware (routes per node via PD); `None` ⇒ direct single-node access.
    routing: Option<Routing>,
    /// The single KV client used in direct mode (`None` when region-aware).
    direct: Option<KvServiceClient<Channel>>,
}

impl Client {
    /// Connect lazily to a single KV node at `uri` (e.g. `"http://127.0.0.1:50051"`),
    /// in **direct** mode — no PD, no routing context. The TCP/HTTP-2 connection is
    /// established on the first request, so there is no startup race with a server that
    /// is still binding.
    pub fn connect(uri: impl Into<String>) -> Result<Client> {
        let channel = lazy_channel(&uri.into())?;
        Ok(Client { routing: None, direct: Some(KvServiceClient::new(channel)) })
    }

    /// Connect lazily to PD in **region-aware** mode: requests are routed per key to the
    /// owning node (resolved + cached from PD), carry a routing context, and are
    /// transparently re-routed on `RegionStale`/`NotLeader`. `kv_uri` is kept only as a
    /// fallback for regions PD reports without an address.
    pub fn connect_with_pd(kv_uri: impl Into<String>, pd_uri: impl Into<String>) -> Result<Client> {
        let pd = PdServiceClient::new(lazy_channel(&pd_uri.into())?);
        let routing = Routing {
            pd,
            cache: Arc::new(Mutex::new(Vec::new())),
            pool: Arc::new(Mutex::new(HashMap::new())),
            fallback_kv: Some(kv_uri.into()),
        };
        Ok(Client { routing: Some(routing), direct: None })
    }

    /// Resolve `key` to `(optional routing context, the KV client to send to)`. In direct
    /// mode the context is `None` and the single node is always used.
    async fn prepare(&self, key: &[u8]) -> Result<(Option<kv::Context>, KvServiceClient<Channel>)> {
        match &self.routing {
            Some(r) => {
                let (ctx, client) = r.resolve(key).await?;
                Ok((Some(ctx), client))
            }
            None => Ok((None, self.direct.clone().expect("direct client present"))),
        }
    }

    /// Drop `key`'s cached route after a `RegionStale`/`NotLeader` (a no-op in direct mode).
    fn invalidate(&self, key: &[u8]) {
        if let Some(r) = &self.routing {
            r.invalidate(key);
        }
    }

    /// Allocate a transaction `start_ts`. Timestamps are global; in routed mode this is
    /// served by the node owning the start of the keyspace, in direct mode by the node.
    pub async fn begin(&mut self) -> Result<u64> {
        let (_ctx, mut kv) = self.prepare(b"").await?;
        let resp = kv.begin(kv::BeginRequest {}).await.map_err(ClientError::Rpc)?.into_inner();
        Ok(resp.start_ts)
    }

    /// Prewrite all mutations (primary first). Returns the first per-key error if any.
    /// Routed on the primary key; retried on `RegionStale`/`NotLeader`.
    pub async fn prewrite(
        &mut self,
        start_ts: u64,
        primary: Vec<u8>,
        mutations: Vec<Mutation>,
        ttl: u64,
    ) -> Result<()> {
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let (context, mut kv) = self.prepare(&primary).await?;
            let resp = kv
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
                Some(ke) if is_reroute(&ke) => {
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
    /// on the primary key; retried on `RegionStale`/`NotLeader`.
    pub async fn commit(
        &mut self,
        start_ts: u64,
        primary: Vec<u8>,
        keys: Vec<Vec<u8>>,
    ) -> Result<u64> {
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let (context, mut kv) = self.prepare(&primary).await?;
            let resp = kv
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
                Some(ke) if is_reroute(&ke) => {
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
            let (context, mut kv) = self.prepare(&key).await?;
            let resp = kv
                .get(kv::GetRequest { key: key.clone(), read_ts, context })
                .await
                .map_err(ClientError::Rpc)?
                .into_inner();
            match resp.error {
                Some(ke) if is_reroute(&ke) => {
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
            let (context, mut kv) = self.prepare(&key).await?;
            let resp = kv
                .put(kv::PutRequest { key: key.clone(), value: value.clone(), context })
                .await
                .map_err(ClientError::Rpc)?
                .into_inner();
            match resp.error {
                Some(ke) if is_reroute(&ke) => {
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
            let (context, mut kv) = self.prepare(&key).await?;
            let resp = kv
                .delete(kv::DeleteRequest { key: key.clone(), context })
                .await
                .map_err(ClientError::Rpc)?
                .into_inner();
            match resp.error {
                Some(ke) if is_reroute(&ke) => {
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
        let split_key = split_key.into();
        let (_ctx, mut kv) = self.prepare(&split_key).await?;
        let resp = kv
            .split_region(kv::SplitRegionRequest { split_key: split_key.clone() })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        // The topology changed; drop the now-stale cached route for this range.
        self.invalidate(&split_key);
        let left = resp.left.map(|r| r.id).unwrap_or(0);
        let right = resp.right.map(|r| r.id).unwrap_or(0);
        Ok((left, right))
    }

    /// Merge the region starting at `boundary_key` into its left neighbour (operational,
    /// the inverse of [`split_region`](Self::split_region)). Returns the merged region id.
    pub async fn merge_region(&mut self, boundary_key: impl Into<Vec<u8>>) -> Result<u64> {
        let boundary_key = boundary_key.into();
        let (_ctx, mut kv) = self.prepare(&boundary_key).await?;
        let resp = kv
            .merge_region(kv::MergeRegionRequest { boundary_key: boundary_key.clone() })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        self.invalidate(&boundary_key);
        Ok(resp.merged.map(|r| r.id).unwrap_or(0))
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
        let (context, mut kv) = self.prepare(&start_key).await?;
        let resp = kv
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

fn lazy_channel(uri: &str) -> Result<Channel> {
    Ok(Channel::from_shared(uri.to_string())
        .map_err(|e| ClientError::Key(format!("invalid endpoint: {e}")))?
        .connect_lazy())
}

fn routing_exhausted() -> ClientError {
    ClientError::Key("routing retries exhausted (region kept moving?)".to_string())
}

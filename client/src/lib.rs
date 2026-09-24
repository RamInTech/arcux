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
use std::time::Duration;

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
    /// In cluster mode: a node answered but none is currently the leader (e.g. an election is
    /// in progress after a failover). Transient — retry shortly.
    NoLeader,
    /// In cluster mode: not one configured node could be reached (every attempt was a transport
    /// failure) — the cluster is likely down. Distinct from `NoLeader`, where nodes *are* up but
    /// leaderless: there is no election to wait out here, the servers themselves are unreachable.
    Unreachable,
    /// No such table. A key is stored under its table's id, so a name the cluster does not know
    /// cannot be routed at all — this fails before anything is sent.
    UnknownTable(String),
    /// In direct mode: the node answered, but it holds no replica of the region this key
    /// belongs to. Retrying the same node cannot help; connect to one of the region's replicas,
    /// or let the client follow the cluster (`connect_cluster` / PD routing).
    NotHosted,
    /// The write reached a leader and was appended, but its commit was never confirmed — the
    /// leader stepped down, or the request timed out. It may still apply. **Never retried
    /// automatically**: read the key back and decide.
    Undetermined(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Transport(e) => write!(f, "transport error: {e}"),
            ClientError::Rpc(s) => write!(f, "rpc error: {} ({})", s.message(), s.code()),
            ClientError::Key(m) => write!(f, "key error: {m}"),
            ClientError::NoLeader => write!(f, "no leader available (election in progress?)"),
            ClientError::Unreachable => write!(f, "no arcux server reachable (is the cluster running?)"),
            ClientError::NotHosted => write!(
                f,
                "this node holds no replica of that table's region — connect to one that does, \
                 or follow the cluster (ARCUX_CLUSTER / a comma-separated ARCUX_ADDR)"
            ),
            ClientError::Undetermined(d) => write!(
                f,
                "outcome unknown — the write may or may not have applied; read the key back to \
                 find out ({d})"
            ),
            ClientError::UnknownTable(t) => {
                write!(f, "unknown table {t:?} — create it with `create table {t} <cp|ap>`")
            }
        }
    }
}

impl std::error::Error for ClientError {}

fn key_error(ke: kv::KeyError) -> ClientError {
    if let Some(Kind::Undetermined(detail)) = &ke.kind {
        return ClientError::Undetermined(detail.clone());
    }
    let msg = match ke.kind {
        Some(Kind::Conflict(c)) => format!("conflict: {}", c.detail),
        Some(Kind::Locked(l)) => format!("locked by primary {:?} (ttl {})", l.primary, l.ttl),
        Some(Kind::Invalid(s)) => format!("invalid: {s}"),
        Some(Kind::Retryable(s)) => format!("retryable: {s}"),
        Some(Kind::NotLeader(_)) => "not leader".to_string(),
        Some(Kind::RegionStale(rs)) => format!("region stale (new epoch {})", rs.new_epoch),
        Some(Kind::Undetermined(d)) => d, // handled above; kept exhaustive
        None => "unspecified key error".to_string(),
    };
    ClientError::Key(msg)
}

/// Whether a key-error means "your route is wrong — re-resolve and retry": a `RegionStale`
/// (epoch moved under us) or a `NotLeader` (the owning replica isn't the leader anymore).
fn is_reroute(ke: &kv::KeyError) -> bool {
    matches!(ke.kind, Some(Kind::RegionStale(_)) | Some(Kind::NotLeader(_)))
}

/// Should a call that failed with this status go to the next node? Yes when it never reached a
/// server — connection refused, timed out — or when the server answered with one of the scan
/// path's own "not here" statuses (a scan response has no key-error field, so a non-leader says
/// so with a status).
///
/// Any other status is the server's **answer**, and is returned as is. Retrying it round the
/// cluster only buries it: an unavailable timestamp oracle came back to the caller as "no leader
/// (election in progress?)", which sends an operator looking for the wrong problem.
fn try_next_node(s: &tonic::Status) -> bool {
    if std::error::Error::source(s).is_some() || s.code() == tonic::Code::DeadlineExceeded {
        return true;
    }
    let m = s.message();
    m.starts_with("not the region leader") || m.starts_with("region not hosted") || m.starts_with("region stale")
}

/// A key as it is stored and routed: `be32(table_id) ++ key`.
///
/// Mirrors `arcux_pd::region::table_key`. The client needs this only to pick a region — the wire
/// request carries the bare key and the table *name*, and the server does the same rewrite.
fn stored_key(id: TableId, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len());
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(key);
    out
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

/// Leader-following routing over a **static** set of node endpoints (no PD). Sends to the
/// presumed leader; on a `NotLeader` redirect or an unreachable node the caller rotates to the
/// next endpoint, so the client tracks leadership across elections/failover. Shared across
/// `Client` clones, so a discovered leader is remembered.
#[derive(Clone)]
struct ClusterRouting {
    /// Every node's KV endpoint, in a fixed order.
    endpoints: Vec<String>,
    /// Index into `endpoints` of the node we currently believe is the leader.
    leader: Arc<Mutex<usize>>,
    /// One lazily-connected KV client per endpoint index.
    pool: Arc<Mutex<HashMap<usize, KvServiceClient<Channel>>>>,
}

impl ClusterRouting {
    /// The KV client for the presumed leader (lazily connected).
    fn current(&self) -> Result<KvServiceClient<Channel>> {
        let idx = *self.leader.lock().unwrap();
        let mut pool = self.pool.lock().unwrap();
        if let Some(c) = pool.get(&idx) {
            return Ok(c.clone());
        }
        let client = KvServiceClient::new(lazy_channel(&self.endpoints[idx])?);
        pool.insert(idx, client.clone());
        Ok(client)
    }

    /// The presumed leader wasn't (redirect) or was unreachable — try the next endpoint.
    fn rotate(&self) {
        let mut l = self.leader.lock().unwrap();
        *l = (*l + 1) % self.endpoints.len();
    }

    /// The endpoint we currently believe leads (for status display).
    fn current_endpoint(&self) -> String {
        self.endpoints[*self.leader.lock().unwrap()].clone()
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
    /// `Some` ⇒ leader-following over a static endpoint set (no PD).
    cluster: Option<ClusterRouting>,
    /// Table name → id, so a key can be routed to the region its table owns.
    ///
    /// Filled on a miss from [`list_tables`](Client::list_tables) and never invalidated: PD
    /// allocates ids in order and never reuses one, so the only staleness possible is "did not
    /// exist when we last looked", which the refill covers. Shared across clones, like the
    /// routing cache.
    tables: Arc<Mutex<HashMap<String, TableId>>>,
}

/// A table's id — `arcux_pd::region::TableId`, mirrored so the client needs no dependency on PD.
pub type TableId = u32;

/// The table an omitted name resolves to. Its id is fixed, so a client can route to it before it
/// has spoken to any server.
const DEFAULT_TABLE_ID: TableId = 0;
const DEFAULT_TABLE_NAME: &str = "default";

/// A cache pre-seeded with the one table that always exists.
fn seeded_tables() -> Arc<Mutex<HashMap<String, TableId>>> {
    let mut m = HashMap::new();
    m.insert(String::new(), DEFAULT_TABLE_ID);
    m.insert(DEFAULT_TABLE_NAME.to_string(), DEFAULT_TABLE_ID);
    Arc::new(Mutex::new(m))
}

impl Client {
    /// Connect lazily to a single KV node at `uri` (e.g. `"http://127.0.0.1:50051"`),
    /// in **direct** mode — no PD, no routing context. The TCP/HTTP-2 connection is
    /// established on the first request, so there is no startup race with a server that
    /// is still binding.
    pub fn connect(uri: impl Into<String>) -> Result<Client> {
        let channel = lazy_channel(&uri.into())?;
        Ok(Client {
            routing: None,
            direct: Some(KvServiceClient::new(channel)),
            cluster: None,
            tables: seeded_tables(),
        })
    }

    /// Connect to a **static cluster** of KV nodes (no PD), following the leader: requests go
    /// to the presumed leader and are transparently re-tried against the next node on a
    /// `NotLeader` redirect or an unreachable node — so writes keep working across an election
    /// or failover. `endpoints` is every node's URI (e.g. the three `http://127.0.0.1:5006x`).
    pub fn connect_cluster(endpoints: Vec<String>) -> Result<Client> {
        if endpoints.is_empty() {
            return Err(ClientError::Key("connect_cluster needs at least one endpoint".into()));
        }
        Ok(Client {
            routing: None,
            direct: None,
            cluster: Some(ClusterRouting {
                endpoints,
                leader: Arc::new(Mutex::new(0)),
                pool: Arc::new(Mutex::new(HashMap::new())),
            }),
            tables: seeded_tables(),
        })
    }

    /// In cluster mode, the endpoint currently believed to be the leader (for status display);
    /// `None` in direct/PD mode.
    pub fn current_endpoint(&self) -> Option<String> {
        self.cluster.as_ref().map(|c| c.current_endpoint())
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
        Ok(Client { routing: Some(routing), direct: None, cluster: None, tables: seeded_tables() })
    }

    /// Resolve `key` to `(optional routing context, the KV client to send to)`. In direct
    /// mode the context is `None` and the single node is always used.
    async fn prepare(&self, key: &[u8]) -> Result<(Option<kv::Context>, KvServiceClient<Channel>)> {
        if let Some(c) = &self.cluster {
            return Ok((None, c.current()?));
        }
        match &self.routing {
            Some(r) => {
                let (ctx, client) = r.resolve(key).await?;
                Ok((Some(ctx), client))
            }
            None => Ok((None, self.direct.clone().expect("direct client present"))),
        }
    }

    /// A route was wrong (`RegionStale`/`NotLeader`) or a node was unreachable: in cluster mode
    /// rotate to the next node (leader-following), in PD mode drop `key`'s cached route, in
    /// direct mode a no-op.
    fn invalidate(&self, key: &[u8]) {
        if let Some(c) = &self.cluster {
            c.rotate();
        } else if let Some(r) = &self.routing {
            r.invalidate(key);
        }
    }

    /// Act on a `NotLeader`/`RegionStale` redirect: refresh the route (routed mode) or move to
    /// the next node (cluster mode) so the loop can retry — or, in **direct** mode, fail at once
    /// on `RegionStale`. A direct client has one node and no route to refresh, so a node that
    /// hosts no replica of the key would be retried until the loop ran out and reported "no
    /// leader", which reads as a cluster outage when the node simply is not a replica.
    fn reroute(&self, key: &[u8], ke: &kv::KeyError) -> Result<()> {
        let direct = self.cluster.is_none() && self.routing.is_none();
        if direct && matches!(ke.kind, Some(Kind::RegionStale(_))) {
            return Err(ClientError::NotHosted);
        }
        self.invalidate(key);
        Ok(())
    }

    /// The error to return when the retry loop is exhausted, in **any** mode (direct, PD-routed,
    /// or cluster): if a node answered but kept redirecting (`NotLeader`/`RegionStale`) it's
    /// `NoLeader` — a mid-election window worth retrying (e.g. a CP region a live `create_table`
    /// just founded, still electing its first leader); if *no* node could be reached at all it's
    /// `Unreachable`, with no election to wait out.
    fn exhausted(&self, reached_a_node: bool) -> ClientError {
        if reached_a_node {
            ClientError::NoLeader
        } else {
            ClientError::Unreachable
        }
    }

    /// Run a call that **any** node can answer — listing tables, creating one, allocating a
    /// timestamp — with the failover `get_at` has: in cluster mode a node that cannot be reached
    /// is skipped for the next one. A status a server actually returned is final and is never
    /// retried elsewhere, because it is an answer, not an outage.
    ///
    /// These calls used to make exactly one attempt at the current endpoint. With that node down,
    /// a fresh client could not resolve a single table name on an otherwise healthy cluster.
    async fn on_any_node<T, F, Fut>(&mut self, mut call: F) -> Result<T>
    where
        F: FnMut(KvServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        // One attempt per endpoint: every node gets a chance, and none is tried twice.
        let attempts = self.cluster.as_ref().map_or(1, |c| c.endpoints.len().max(1));
        for _ in 0..attempts {
            let (_ctx, kv) = self.prepare(b"").await?;
            match call(kv).await {
                Ok(v) => return Ok(v),
                // Only a transport failure carries a source error: the request never reached a
                // server, so trying the next one cannot duplicate anything.
                Err(s) if self.cluster.is_some() && try_next_node(&s) => {
                    self.invalidate(b"");
                }
                Err(s) => return Err(ClientError::Rpc(s)),
            }
        }
        Err(ClientError::Unreachable)
    }

    /// Allocate a transaction `start_ts`. Timestamps are global; in routed mode this is
    /// served by the node owning the start of the keyspace, in direct mode by the node.
    pub async fn begin(&mut self) -> Result<u64> {
        self.on_any_node(|mut kv| async move {
            kv.begin(kv::BeginRequest {}).await.map(|r| r.into_inner().start_ts)
        })
        .await
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
                    self.reroute(&primary, &ke)?;
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(()),
            }
        }
        // A transport error returns `Rpc` above, so exhaustion here means a live node kept
        // redirecting us (`NotLeader`/`RegionStale`) — a reachable-but-leaderless window.
        Err(self.exhausted(true))
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
                    self.reroute(&primary, &ke)?;
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(resp.commit_ts),
            }
        }
        // A transport error returns `Rpc` above, so exhaustion here means a live node kept
        // redirecting us (`NotLeader`/`RegionStale`) — a reachable-but-leaderless window.
        Err(self.exhausted(true))
    }

    /// Snapshot read at "now" (the server picks a fresh `read_ts`).
    pub async fn get(&mut self, table: impl Into<String>, key: impl Into<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        self.get_at(table, key, 0).await
    }

    /// Resolve a table name to the id its keys are stored under, refreshing from `ListTables`
    /// once on a miss.
    ///
    /// Resolve **before** entering a retry loop: the id does not change between attempts, and
    /// `prepare` borrows `&self`. A second miss after a refresh is a hard error — the table does
    /// not exist, and no amount of retrying will conjure it.
    pub async fn table_id(&mut self, table: &str) -> Result<TableId> {
        if let Some(id) = self.tables.lock().unwrap().get(table).copied() {
            return Ok(id);
        }
        // Not holding the lock across the await: another task may be refreshing it too, and
        // either refill is as good as the other.
        let listed = self.list_tables().await?;
        {
            let mut cache = self.tables.lock().unwrap();
            for (id, name, _) in &listed {
                cache.insert(name.clone(), *id);
            }
        }
        self.tables
            .lock()
            .unwrap()
            .get(table)
            .copied()
            .ok_or_else(|| ClientError::UnknownTable(table.to_string()))
    }

    /// Build a PUT mutation for `table`, for use with [`transact`](Self::transact). Resolves the
    /// table id (one cached lookup), because a mutation carries the key as it is stored.
    pub async fn put_mutation(
        &mut self,
        table: &str,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Mutation> {
        let id = self.table_id(table).await?;
        Ok(Mutation {
            op: kv::Op::Put as i32,
            key: stored_key(id, &key.into()),
            value: value.into(),
        })
    }

    /// Build a DELETE mutation for `table`, for use with [`transact`](Self::transact).
    pub async fn delete_mutation(
        &mut self,
        table: &str,
        key: impl Into<Vec<u8>>,
    ) -> Result<Mutation> {
        let id = self.table_id(table).await?;
        Ok(Mutation { op: kv::Op::Delete as i32, key: stored_key(id, &key.into()), value: vec![] })
    }

    /// Snapshot read at an explicit `read_ts` (0 ⇒ server picks "now").
    pub async fn get_at(
        &mut self,
        table: impl Into<String>,
        key: impl Into<Vec<u8>>,
        read_ts: u64,
    ) -> Result<Option<Vec<u8>>> {
        let table = table.into();
        let bare_key = key.into();
        let key = stored_key(self.table_id(&table).await?, &bare_key);
        let mut reached_a_node = false;
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let (context, mut kv) = self.prepare(&key).await?;
            let resp = match kv
                .get(kv::GetRequest { key: bare_key.clone(), read_ts, context, table: table.clone() })
                .await
            {
                Ok(r) => {
                    reached_a_node = true; // a live server answered (leader or a redirect)
                    r.into_inner()
                }
                Err(status) if self.cluster.is_some() && try_next_node(&status) => {
                    self.invalidate(&key); // node unreachable (killed leader?) — try the next
                    continue;
                }
                Err(status) => return Err(ClientError::Rpc(status)),
            };
            match resp.error {
                Some(ke) if is_reroute(&ke) => {
                    self.reroute(&key, &ke)?;
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(if resp.found { Some(resp.value) } else { None }),
            }
        }
        Err(self.exhausted(reached_a_node))
    }

    /// Autocommit single-key put; returns the `commit_ts`. Routed on the key.
    pub async fn put(
        &mut self,
        table: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64> {
        let table = table.into();
        let bare_key = key.into();
        let key = stored_key(self.table_id(&table).await?, &bare_key);
        let value = value.into();
        let mut reached_a_node = false;
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let (context, mut kv) = self.prepare(&key).await?;
            let resp = match kv
                .put(kv::PutRequest {
                    key: bare_key.clone(),
                    value: value.clone(),
                    context,
                    table: table.clone(),
                })
                .await
            {
                Ok(r) => {
                    reached_a_node = true; // a live server answered (leader or a redirect)
                    r.into_inner()
                }
                Err(status) if self.cluster.is_some() && try_next_node(&status) => {
                    self.invalidate(&key); // node unreachable (killed leader?) — try the next
                    continue;
                }
                Err(status) => return Err(ClientError::Rpc(status)),
            };
            match resp.error {
                Some(ke) if is_reroute(&ke) => {
                    self.reroute(&key, &ke)?;
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(resp.commit_ts),
            }
        }
        Err(self.exhausted(reached_a_node))
    }

    /// Autocommit single-key delete; returns the `commit_ts`. Routed on the key.
    pub async fn delete(&mut self, table: impl Into<String>, key: impl Into<Vec<u8>>) -> Result<u64> {
        let table = table.into();
        let bare_key = key.into();
        let key = stored_key(self.table_id(&table).await?, &bare_key);
        let mut reached_a_node = false;
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let (context, mut kv) = self.prepare(&key).await?;
            let resp = match kv
                .delete(kv::DeleteRequest { key: bare_key.clone(), context, table: table.clone() })
                .await
            {
                Ok(r) => {
                    reached_a_node = true; // a live server answered (leader or a redirect)
                    r.into_inner()
                }
                Err(status) if self.cluster.is_some() && try_next_node(&status) => {
                    self.invalidate(&key); // node unreachable (killed leader?) — try the next
                    continue;
                }
                Err(status) => return Err(ClientError::Rpc(status)),
            };
            match resp.error {
                Some(ke) if is_reroute(&ke) => {
                    self.reroute(&key, &ke)?;
                    continue;
                }
                Some(ke) => return Err(key_error(ke)),
                None => return Ok(resp.commit_ts),
            }
        }
        Err(self.exhausted(reached_a_node))
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

    /// Declare a new table and stand up its region live — no restart. Single-node only for now
    /// (see `kv.CreateTable`'s doc): not routed by key, so it's sent to whichever node this
    /// call reaches. Returns the new region's `(id, start, end)`.
    pub async fn create_table(
        &mut self,
        name: impl Into<String>,
        regime: kv::Regime,
    ) -> Result<(TableId, u64, Vec<u8>, Vec<u8>)> {
        let name = name.into();
        // Safe to send to the next node when one is unreachable: re-creating a table with the
        // regime it already has is answered with that table, not an error.
        let req = kv::CreateTableRequest { name: name.clone(), regime: regime as i32 };
        let resp = self
            .on_any_node(|mut kv| {
                let req = req.clone();
                async move { kv.create_table(req).await.map(|r| r.into_inner()) }
            })
            .await?;
        let region =
            resp.region.ok_or_else(|| ClientError::Key("create_table: no region in response".into()))?;
        // Cache the id now: the caller almost certainly writes to the table next, and this saves
        // that first write a `ListTables` round trip.
        self.tables.lock().unwrap().insert(name, resp.table_id);
        Ok((resp.table_id, region.id, region.start_key, region.end_key))
    }

    /// Every table with its id and regime, sorted by name — including the built-in `default`
    /// that an omitted table name resolves to. PD owns the catalog, so this is the cluster's
    /// view, as of the last assignment the node answering has applied.
    pub async fn list_tables(&mut self) -> Result<Vec<(TableId, String, kv::Regime)>> {
        let resp = self
            .on_any_node(|mut kv| async move {
                kv.list_tables(kv::ListTablesRequest {}).await.map(|r| r.into_inner())
            })
            .await?;
        Ok(resp
            .tables
            .into_iter()
            .map(|t| (t.id, t.name, kv::Regime::try_from(t.regime).unwrap_or(kv::Regime::Cp)))
            .collect())
    }

    /// Range scan, table-scoped like [`get`](Self::get)/[`put`](Self::put)/[`delete`](Self::delete).
    /// `table` is a table name, or `""` for `default`. An empty `start_key` *and* empty `end_key`
    /// together mean "the whole table" — the server derives its bounds from the catalog, not the
    /// client — and keys come back as the caller wrote them, without the table-id prefix.
    pub async fn scan(
        &mut self,
        table: impl Into<String>,
        start_key: impl Into<Vec<u8>>,
        end_key: impl Into<Vec<u8>>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table = table.into();
        let start_key = start_key.into();
        let end_key = end_key.into();
        // Routing key: for a full-table scan (both bounds empty) route on the table's own prefix
        // rather than an empty key, which would always resolve to the region owning the very
        // start of the keyspace regardless of which table was asked for.
        let id = self.table_id(&table).await?;
        let route_key = stored_key(id, &start_key);
        for _ in 0..MAX_ROUTING_ATTEMPTS {
            let (context, mut kv) = self.prepare(&route_key).await?;
            let req = kv::ScanRequest {
                start_key: start_key.clone(),
                end_key: end_key.clone(),
                limit,
                read_ts: 0,
                context,
                table: table.clone(),
            };
            match kv.scan(req).await {
                Ok(r) => {
                    return Ok(r.into_inner().pairs.into_iter().map(|p| (p.key, p.value)).collect())
                }
                // A non-leader replies `unavailable`; in cluster mode rotate to the next node.
                Err(status) if self.cluster.is_some() && try_next_node(&status) => {
                    self.invalidate(&route_key);
                    continue;
                }
                Err(status) => return Err(ClientError::Rpc(status)),
            }
        }
        // Only transport-failure rotations reach here (an `Ok` returns above), so no node was
        // reachable ⇒ `Unreachable`, never `NoLeader`.
        Err(self.exhausted(false))
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

/// How long one request may take before the client gives up on the node it was sent to. Above
/// any healthy latency — an fsync spike reaches hundreds of ms, an election ~600ms — and far
/// below a person's patience. Without it, a node that is alive but not answering (a stop, a GC
/// pause, a stalled disk) held a caller forever: a live check hung a client for 30s with a
/// healthy majority one endpoint away.
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

fn lazy_channel(uri: &str) -> Result<Channel> {
    Ok(Channel::from_shared(uri.to_string())
        .map_err(|e| ClientError::Key(format!("invalid endpoint: {e}")))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .connect_lazy())
}

//! arcux Phase 2/3 — the data-node gRPC server: a thin async shim over the Phase-1
//! engine, made region-aware in Phase 3.
//!
//! Each KV handler validates its request, dispatches the (blocking) engine work onto a
//! `spawn_blocking` thread so the tokio reactor is never stalled by an fsync, and maps
//! the engine's `Result` back onto the wire types. No transactional logic lives here —
//! prewrite/commit reuse [`arcux_engine::Transaction`] and reads reuse
//! [`arcux_engine::Engine::mvcc_get`] verbatim.
//!
//! ## Phase 3 — regions & PD
//!
//! Timestamps no longer come from a node-local oracle: the node pulls them from the
//! cluster TSO over `pd.GetTimestamp` (see [`TimestampSource`] / [`PdClock`]). The node
//! is **authoritative** for its own region epochs — it holds a [`RegionRegistry`],
//! enforces the routing [`Context`] a client stamps on each request (replying with a
//! `RegionStale` key-error when the epoch is out of date or the key is out of range),
//! splits regions locally on demand, and reports its regions to PD via heartbeat. A
//! request carrying no `Context` (region_id 0) is the Phase-2 direct path and skips
//! validation, so the in-process single-node tests need no PD.
//!
//! Phase 4 will move region ownership under per-region Raft; the `Context`/`RegionStale`
//! contract here is unchanged by that.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arcux_engine::{Engine, Error, Mutation, Options, Transaction};
use arcux_pd::{Region, RegionRegistry, Tso};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_server::{KvService, KvServiceServer};
use arcux_rpc::kv::{self, Context, KeyError, RegionInfo, RegionStale};
use arcux_rpc::pd;
use arcux_rpc::pd::pd_service_client::PdServiceClient;
use arcux_rpc::raft::raft_service_server::{RaftService, RaftServiceServer};
use arcux_rpc::raft;

/// A logical lease (in TSO ticks) added to `start_ts` to form a lock's expiry. The TSO
/// is a monotonic counter (not wall-clock), so a generous lease keeps autocommit locks
/// from ever looking expired during their brief prewrite→commit window.
const AUTOCOMMIT_LEASE: u64 = 1 << 32;

/// How many timestamps the node reserves from PD per refill. Larger ⇒ fewer round-trips
/// to PD on the hot path, at the cost of skipping more timestamps if the node restarts.
const TSO_BATCH: u32 = 256;

// ------------------------------------------------------------------------------------
// Timestamp source — a node-local oracle (tests) or the cluster TSO via PD.
// ------------------------------------------------------------------------------------

/// The node's source of `start_ts`/`commit_ts`/read timestamps. Always called on a
/// blocking thread (inside `spawn_blocking`), so a PD-backed implementation may block.
pub trait TimestampSource: Send + Sync {
    fn now(&self) -> u64;
}

/// An in-process monotonic oracle. Used by the direct single-node path (Phase-2 tests
/// and demos) where there is no PD to defer to.
pub struct LocalClock(Tso);

impl LocalClock {
    pub fn new() -> LocalClock {
        LocalClock(Tso::ephemeral())
    }
}

impl Default for LocalClock {
    fn default() -> Self {
        LocalClock::new()
    }
}

impl TimestampSource for LocalClock {
    fn now(&self) -> u64 {
        // The ephemeral oracle does no I/O, so this never actually fails.
        self.0.now().expect("ephemeral tso never fails")
    }
}

/// The cluster TSO, consumed via PD with client-side batching: the node reserves a
/// window of timestamps per `pd.GetTimestamp` and hands them out locally until it is
/// exhausted, then refills.
struct PdClock {
    pd: PdServiceClient<Channel>,
    /// Runtime handle so the (synchronous) `now()` can drive an async refill. Safe
    /// because `now()` only ever runs on a `spawn_blocking` thread, never a reactor.
    handle: tokio::runtime::Handle,
    /// `(next, end)` — the half-open window of reserved-but-unissued timestamps.
    window: std::sync::Mutex<(u64, u64)>,
}

impl PdClock {
    fn new(pd: PdServiceClient<Channel>) -> PdClock {
        PdClock { pd, handle: tokio::runtime::Handle::current(), window: std::sync::Mutex::new((0, 0)) }
    }
}

impl TimestampSource for PdClock {
    fn now(&self) -> u64 {
        let mut w = self.window.lock().expect("pd clock poisoned");
        if w.0 >= w.1 {
            let mut pd = self.pd.clone();
            let resp = self
                .handle
                .block_on(async move { pd.get_timestamp(pd::GetTimestampRequest { count: TSO_BATCH }).await })
                .expect("pd tso unreachable")
                .into_inner();
            w.0 = resp.timestamp;
            w.1 = resp.timestamp + resp.count as u64;
        }
        let ts = w.0;
        w.0 += 1;
        ts
    }
}

// ------------------------------------------------------------------------------------
// Node state
// ------------------------------------------------------------------------------------

/// PD connection used for heartbeats (reporting this node's regions + serving address,
/// and adopting the regions PD assigns back).
struct PdHandle {
    client: PdServiceClient<Channel>,
    node_id: u64,
    /// This node's advertised serving endpoint, handed to clients via PD for per-node
    /// routing (e.g. `"http://127.0.0.1:50051"`).
    address: String,
}

/// Default period between liveness heartbeats to PD (PD-connected mode).
const DEFAULT_HEARTBEAT_MS: u64 = 1_000;

/// Shared node state behind an `Arc`: the storage engine, the timestamp source, the
/// authoritative region table, and (when PD-connected) a heartbeat handle.
pub struct AppState {
    pub engine: Engine,
    clock: Arc<dyn TimestampSource>,
    regions: Arc<RegionRegistry>,
    pd: Option<PdHandle>,
    /// Period (ms) of the background liveness heartbeat [`serve_on`] runs when
    /// PD-connected. Settable so tests can heartbeat faster than PD's failure-detector
    /// timeout. Ignored in direct mode.
    hb_interval_ms: std::sync::atomic::AtomicU64,
}

impl AppState {
    /// Direct single-node mode: a local timestamp oracle and a bootstrapped region
    /// table, with no PD and no routing enforcement (clients send no `Context`). This
    /// is the Phase-2 path used by the in-process tests and demos.
    pub fn open(opts: Options) -> arcux_engine::Result<Arc<AppState>> {
        let regions = Arc::new(RegionRegistry::open(&opts.data_dir).map_err(arcux_engine::Error::from)?);
        let engine = Engine::open(opts)?;
        Ok(Arc::new(AppState {
            engine,
            clock: Arc::new(LocalClock::new()),
            regions,
            pd: None,
            hb_interval_ms: std::sync::atomic::AtomicU64::new(DEFAULT_HEARTBEAT_MS),
        }))
    }

    /// PD-connected mode: timestamps come from the cluster TSO, and the node registers
    /// with PD (an initial synchronous heartbeat advertising `address`) — PD is the
    /// placement authority, so a fresh node starts with no regions and **adopts** the set
    /// PD assigns it, making them routable before it serves.
    pub async fn open_with_pd(
        opts: Options,
        pd_endpoint: String,
        node_id: u64,
        address: String,
    ) -> Result<Arc<AppState>, Box<dyn std::error::Error + Send + Sync>> {
        let regions = Arc::new(RegionRegistry::open_empty(&opts.data_dir, node_id)?);
        let engine = Engine::open(opts)?;
        let client = PdServiceClient::connect(pd_endpoint).await?;
        let clock: Arc<dyn TimestampSource> = Arc::new(PdClock::new(client.clone()));
        let state = Arc::new(AppState {
            engine,
            clock,
            regions,
            pd: Some(PdHandle { client, node_id, address }),
            hb_interval_ms: std::sync::atomic::AtomicU64::new(DEFAULT_HEARTBEAT_MS),
        });
        state.heartbeat().await?; // register, adopt our assignment, become routable
        Ok(state)
    }

    /// Set the background heartbeat period (ms). Must be shorter than PD's failure-detector
    /// timeout or a live node will be marked down between beats; tests use a small value.
    pub fn set_heartbeat_interval_ms(&self, ms: u64) {
        self.hb_interval_ms.store(ms.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    /// Report this node's current regions + address to PD and adopt the regions PD
    /// assigns back (a no-op when not PD-connected). The two-way exchange seeds a fresh
    /// node's region set and keeps PD's placement view authoritative. Public so the
    /// background heartbeat loop (and tests) can drive it.
    pub async fn heartbeat(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(pd) = &self.pd else { return Ok(()) };
        let regions: Vec<pd::Region> =
            self.regions.list().iter().map(arcux_pd::convert::to_proto).collect();
        let mut client = pd.client.clone();
        let resp = client
            .heartbeat(pd::HeartbeatRequest {
                node_id: pd.node_id,
                regions,
                address: pd.address.clone(),
            })
            .await?
            .into_inner();
        let assigned: Vec<Region> = resp.regions.iter().map(arcux_pd::convert::from_proto).collect();
        self.regions.adopt(assigned)?;
        Ok(())
    }

    /// Validate a request's routing context against this node's authoritative regions.
    /// Returns `Some(RegionStale)` if the client should re-route, or `None` if the
    /// request may proceed (including the direct path, where no context is supplied).
    fn check_context(&self, ctx: &Option<Context>, key: &[u8]) -> Option<KeyError> {
        let ctx = ctx.as_ref()?;
        if ctx.region_id == 0 {
            return None; // direct (no-routing) request
        }
        match self.regions.by_id(ctx.region_id) {
            // Right region, current epoch, key actually in range → good.
            Some(r) if r.epoch == ctx.region_epoch && r.contains(key) => None,
            // Region exists but the client's epoch/range is stale (e.g. post-split):
            // hand back the current epoch so the client can refresh.
            Some(r) => Some(region_stale(r.epoch)),
            // Unknown region id → tell the client to re-route from scratch.
            None => Some(region_stale(0)),
        }
    }
}

/// Build a `RegionStale` key-error carrying the authoritative epoch hint.
fn region_stale(new_epoch: u64) -> KeyError {
    KeyError { kind: Some(Kind::RegionStale(RegionStale { new_epoch })) }
}

fn region_info(r: &Region) -> RegionInfo {
    RegionInfo { id: r.id, start_key: r.start.clone(), end_key: r.end.clone(), epoch: r.epoch }
}

/// One engine error mapped onto the wire: either a normal per-key protocol outcome
/// (returned in the response body as a [`KeyError`]) or an RPC-level failure
/// (returned as a gRPC [`Status`]).
enum Classified {
    Key(KeyError),
    Status(Status),
}

/// Conflicts and live locks are *expected* protocol outcomes → `KeyError`. Bad
/// arguments, I/O, and corruption are RPC failures → `Status`.
fn classify(e: Error) -> Classified {
    match e {
        Error::Conflict(detail) => {
            Classified::Key(KeyError { kind: Some(Kind::Conflict(kv::Conflict { detail })) })
        }
        Error::KeyIsLocked(detail) => Classified::Key(KeyError { kind: Some(Kind::Retryable(detail)) }),
        Error::InvalidArgument(d) => Classified::Status(Status::invalid_argument(d)),
        Error::Io(e) => Classified::Status(Status::internal(format!("io: {e}"))),
        Error::Corruption(d) => Classified::Status(Status::internal(format!("corruption: {d}"))),
    }
}

/// Translate a wire mutation into an engine mutation.
fn to_engine_mutation(m: &kv::Mutation) -> Mutation {
    match kv::Op::try_from(m.op) {
        Ok(kv::Op::Delete) => Mutation::delete(m.key.clone()),
        _ => Mutation::put(m.key.clone(), m.value.clone()),
    }
}

/// Join a `spawn_blocking` result, turning a panic/cancel into an internal `Status`.
async fn run_blocking<T, F>(f: F) -> Result<T, Status>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Status::internal(format!("engine task failed: {e}")))
}

// ------------------------------------------------------------------------------------
// KV service
// ------------------------------------------------------------------------------------

#[derive(Clone)]
pub struct KvApi {
    state: Arc<AppState>,
}

#[tonic::async_trait]
impl KvService for KvApi {
    async fn begin(
        &self,
        _request: Request<kv::BeginRequest>,
    ) -> Result<Response<kv::BeginResponse>, Status> {
        // start_ts comes from the cluster TSO; allocate on a blocking thread because a
        // PD-backed clock may do a blocking refill.
        let state = self.state.clone();
        let start_ts = run_blocking(move || state.clock.now()).await?;
        Ok(Response::new(kv::BeginResponse { start_ts }))
    }

    async fn prewrite(
        &self,
        request: Request<kv::PrewriteRequest>,
    ) -> Result<Response<kv::PrewriteResponse>, Status> {
        let req = request.into_inner();
        if let Some(ke) = self.state.check_context(&req.context, &req.primary) {
            return Ok(Response::new(kv::PrewriteResponse { errors: vec![ke] }));
        }
        let state = self.state.clone();
        let res = run_blocking(move || {
            let muts: Vec<Mutation> = req.mutations.iter().map(to_engine_mutation).collect();
            let txn = Transaction::new(&state.engine, req.start_ts, muts)?;
            txn.prewrite(req.ttl)
        })
        .await?;

        match res {
            Ok(()) => Ok(Response::new(kv::PrewriteResponse { errors: vec![] })),
            Err(e) => match classify(e) {
                Classified::Key(ke) => Ok(Response::new(kv::PrewriteResponse { errors: vec![ke] })),
                Classified::Status(s) => Err(s),
            },
        }
    }

    async fn commit(
        &self,
        request: Request<kv::CommitRequest>,
    ) -> Result<Response<kv::CommitResponse>, Status> {
        let req = request.into_inner();
        if let Some(ke) = self.state.check_context(&req.context, &req.primary) {
            return Ok(Response::new(kv::CommitResponse { commit_ts: 0, error: Some(ke) }));
        }
        let state = self.state.clone();
        let res = run_blocking(move || {
            // commit only reads each mutation's *key*; values are placeholders. The
            // primary must be mutations[0], so prepend it and skip any dup in `keys`.
            let mut muts = vec![Mutation::delete(req.primary.clone())];
            for k in &req.keys {
                if k != &req.primary {
                    muts.push(Mutation::delete(k.clone()));
                }
            }
            let txn = Transaction::new(&state.engine, req.start_ts, muts)?;
            let commit_ts = state.clock.now();
            txn.commit(commit_ts).map(|()| commit_ts)
        })
        .await?;

        match res {
            Ok(commit_ts) => Ok(Response::new(kv::CommitResponse { commit_ts, error: None })),
            Err(e) => match classify(e) {
                Classified::Key(ke) => {
                    Ok(Response::new(kv::CommitResponse { commit_ts: 0, error: Some(ke) }))
                }
                Classified::Status(s) => Err(s),
            },
        }
    }

    async fn get(
        &self,
        request: Request<kv::GetRequest>,
    ) -> Result<Response<kv::GetResponse>, Status> {
        let req = request.into_inner();
        if let Some(ke) = self.state.check_context(&req.context, &req.key) {
            return Ok(Response::new(kv::GetResponse {
                found: false,
                value: vec![],
                error: Some(ke),
                read_ts: 0,
            }));
        }
        let state = self.state.clone();
        // Allocate read_ts and read in one blocking hop so the (possibly PD-backed)
        // clock is never touched from the reactor, and read_ts is known on every path.
        let (res, read_ts) = run_blocking(move || {
            let read_ts = if req.read_ts == 0 { state.clock.now() } else { req.read_ts };
            (state.engine.mvcc_get(&req.key, read_ts), read_ts)
        })
        .await?;

        match res {
            Ok(Some(value)) => {
                Ok(Response::new(kv::GetResponse { found: true, value, error: None, read_ts }))
            }
            Ok(None) => Ok(Response::new(kv::GetResponse {
                found: false,
                value: vec![],
                error: None,
                read_ts,
            })),
            Err(e) => match classify(e) {
                Classified::Key(ke) => Ok(Response::new(kv::GetResponse {
                    found: false,
                    value: vec![],
                    error: Some(ke),
                    read_ts,
                })),
                Classified::Status(s) => Err(s),
            },
        }
    }

    async fn put(
        &self,
        request: Request<kv::PutRequest>,
    ) -> Result<Response<kv::PutResponse>, Status> {
        let req = request.into_inner();
        if let Some(ke) = self.state.check_context(&req.context, &req.key) {
            return Ok(Response::new(kv::PutResponse { commit_ts: 0, error: Some(ke) }));
        }
        let res = self.autocommit(Mutation::put(req.key, req.value)).await?;
        match res {
            Ok(commit_ts) => Ok(Response::new(kv::PutResponse { commit_ts, error: None })),
            Err(e) => match classify(e) {
                Classified::Key(ke) => Ok(Response::new(kv::PutResponse { commit_ts: 0, error: Some(ke) })),
                Classified::Status(s) => Err(s),
            },
        }
    }

    async fn delete(
        &self,
        request: Request<kv::DeleteRequest>,
    ) -> Result<Response<kv::DeleteResponse>, Status> {
        let req = request.into_inner();
        if let Some(ke) = self.state.check_context(&req.context, &req.key) {
            return Ok(Response::new(kv::DeleteResponse { commit_ts: 0, error: Some(ke) }));
        }
        let res = self.autocommit(Mutation::delete(req.key)).await?;
        match res {
            Ok(commit_ts) => Ok(Response::new(kv::DeleteResponse { commit_ts, error: None })),
            Err(e) => match classify(e) {
                Classified::Key(ke) => {
                    Ok(Response::new(kv::DeleteResponse { commit_ts: 0, error: Some(ke) }))
                }
                Classified::Status(s) => Err(s),
            },
        }
    }

    async fn scan(
        &self,
        _request: Request<kv::ScanRequest>,
    ) -> Result<Response<kv::ScanResponse>, Status> {
        Err(Status::unimplemented(
            "scan lands with the Phase 1b merging iterator; the wire shape is frozen",
        ))
    }

    async fn split_region(
        &self,
        request: Request<kv::SplitRegionRequest>,
    ) -> Result<Response<kv::SplitRegionResponse>, Status> {
        let split_key = request.into_inner().split_key;
        let regions = self.state.regions.clone();
        // The split is authoritative here (the node owns its epochs); persisting it is
        // a small fsync, so do it off the reactor.
        let (left, right) = run_blocking(move || regions.split(&split_key))
            .await?
            .map_err(|e| Status::invalid_argument(format!("split: {e}")))?;
        // Tell PD about the new topology so clients re-routing after a RegionStale see it.
        if let Err(e) = self.state.heartbeat().await {
            return Err(Status::internal(format!("split applied but PD heartbeat failed: {e}")));
        }
        Ok(Response::new(kv::SplitRegionResponse {
            left: Some(region_info(&left)),
            right: Some(region_info(&right)),
        }))
    }

    async fn merge_region(
        &self,
        request: Request<kv::MergeRegionRequest>,
    ) -> Result<Response<kv::MergeRegionResponse>, Status> {
        let boundary = request.into_inner().boundary_key;
        let regions = self.state.regions.clone();
        // Authoritative here (the node owns both halves' epochs); persist off the reactor.
        let merged = run_blocking(move || regions.merge(&boundary))
            .await?
            .map_err(|e| Status::invalid_argument(format!("merge: {e}")))?;
        // Tell PD about the new topology so clients re-routing after a RegionStale see it.
        if let Err(e) = self.state.heartbeat().await {
            return Err(Status::internal(format!("merge applied but PD heartbeat failed: {e}")));
        }
        Ok(Response::new(kv::MergeRegionResponse { merged: Some(region_info(&merged)) }))
    }
}

impl KvApi {
    /// Run a single-key transaction (begin → prewrite → commit) inside one blocking
    /// task, returning the engine result so callers can map errors uniformly.
    async fn autocommit(&self, m: Mutation) -> Result<Result<u64, Error>, Status> {
        let state = self.state.clone();
        run_blocking(move || {
            let start_ts = state.clock.now();
            let txn = Transaction::new(&state.engine, start_ts, vec![m])?;
            txn.prewrite(start_ts.saturating_add(AUTOCOMMIT_LEASE))?;
            let commit_ts = state.clock.now();
            txn.commit(commit_ts).map(|()| commit_ts)
        })
        .await
    }
}

// ------------------------------------------------------------------------------------
// Raft service — all stubbed; shapes frozen for Phase 4.
// ------------------------------------------------------------------------------------

#[derive(Clone)]
pub struct RaftApi;

#[tonic::async_trait]
impl RaftService for RaftApi {
    type AppendEntriesStream = Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<
                    Item = Result<raft::AppendEntriesResponse, Status>,
                > + Send
                + 'static,
        >,
    >;

    async fn request_vote(
        &self,
        _request: Request<raft::RequestVoteRequest>,
    ) -> Result<Response<raft::RequestVoteResponse>, Status> {
        Err(Status::unimplemented("Raft arrives in Phase 4"))
    }

    async fn append_entries(
        &self,
        _request: Request<raft::AppendEntriesRequest>,
    ) -> Result<Response<Self::AppendEntriesStream>, Status> {
        Err(Status::unimplemented("Raft arrives in Phase 4"))
    }

    async fn install_snapshot(
        &self,
        _request: Request<raft::InstallSnapshotRequest>,
    ) -> Result<Response<raft::InstallSnapshotResponse>, Status> {
        Err(Status::unimplemented("Raft arrives in Phase 4"))
    }
}

// ------------------------------------------------------------------------------------
// Wiring
// ------------------------------------------------------------------------------------

/// Serve the KV (+ stubbed Raft) services on an already-bound listener until `shutdown`
/// resolves. Used by both [`serve`]/[`serve_with_pd`] and the integration tests (which
/// bind an ephemeral port). PD is a *separate* service (the `arcux-pd` binary); the node
/// is only a PD client.
pub async fn serve_on<F>(
    state: Arc<AppState>,
    listener: TcpListener,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    // When PD-connected, keep PD's view of this node fresh with periodic heartbeats (which
    // also re-adopt our placement). Splits/merges heartbeat inline; this is just liveness.
    // The task is tied to serve_on's lifetime — stopping the node stops its heartbeats, so
    // PD's failure detector can notice.
    let hb_handle = state.pd.as_ref().map(|_| {
        let hb = state.clone();
        tokio::spawn(async move {
            let ms = hb.hb_interval_ms.load(std::sync::atomic::Ordering::Relaxed).max(1);
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(e) = hb.heartbeat().await {
                    eprintln!("heartbeat to PD failed: {e}");
                }
            }
        })
    });

    let incoming = TcpListenerStream::new(listener);
    let result = Server::builder()
        .add_service(KvServiceServer::new(KvApi { state }))
        .add_service(RaftServiceServer::new(RaftApi))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await;

    if let Some(h) = hb_handle {
        h.abort();
    }
    result
}

/// Open the engine in direct mode, bind `addr`, and serve until Ctrl-C.
pub async fn serve(
    opts: Options,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = AppState::open(opts)?;
    let listener = TcpListener::bind(addr).await?;
    eprintln!("arcux-server listening on {} (direct mode, no PD)", listener.local_addr()?);
    serve_on(state, listener, shutdown_signal()).await?;
    Ok(())
}

/// Open the engine connected to PD at `pd_endpoint`, bind `addr`, and serve until
/// Ctrl-C, heartbeating the node's regions to PD periodically. `advertise` is the
/// endpoint clients should reach this node at (defaults to the bound address when empty).
pub async fn serve_with_pd(
    opts: Options,
    addr: SocketAddr,
    pd_endpoint: String,
    node_id: u64,
    advertise: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Bind first so the advertised address reflects the real (possibly ephemeral) port.
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let address = advertise.unwrap_or_else(|| format!("http://{bound}"));
    let state =
        AppState::open_with_pd(opts, pd_endpoint.clone(), node_id, address.clone()).await?;
    eprintln!("arcux-server listening on {bound} as {address} (node {node_id}, PD {pd_endpoint})");

    // The periodic liveness heartbeat is run by `serve_on` (tied to the serve lifetime).
    serve_on(state, listener, shutdown_signal()).await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("arcux-server shutting down");
}

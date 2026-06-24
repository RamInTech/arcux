//! arcux Phase 2 — the gRPC server: a thin async shim over the Phase-1 engine.
//!
//! Each handler validates its request, dispatches the (blocking) engine work onto a
//! `spawn_blocking` thread so the tokio reactor is never stalled by an fsync, and maps
//! the engine's `Result` back onto the wire types. No transactional logic lives here —
//! prewrite/commit reuse [`arcux_engine::Transaction`] and reads reuse
//! [`arcux_engine::Engine::mvcc_get`] verbatim.
//!
//! The node hosts the TSO stand-in ([`arcux_engine::Tso`]): it is the single timestamp
//! authority for begin (`start_ts`), commit (`commit_ts`), and `pd.GetTimestamp`.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arcux_engine::{Engine, Error, Mutation, Options, Transaction, Tso};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use arcux_rpc::kv::kv_service_server::{KvService, KvServiceServer};
use arcux_rpc::kv::{self, key_error::Kind, KeyError};
use arcux_rpc::pd::pd_service_server::{PdService, PdServiceServer};
use arcux_rpc::pd;
use arcux_rpc::raft::raft_service_server::{RaftService, RaftServiceServer};
use arcux_rpc::raft;

/// A logical lease (in TSO ticks) added to `start_ts` to form a lock's expiry. The TSO
/// is a monotonic counter (not wall-clock), so a generous lease keeps autocommit locks
/// from ever looking expired during their brief prewrite→commit window.
const AUTOCOMMIT_LEASE: u64 = 1 << 32;

/// Shared node state behind an `Arc`: the storage engine plus the timestamp oracle.
pub struct AppState {
    pub engine: Engine,
    pub tso: Tso,
}

impl AppState {
    /// Open the engine and create a fresh TSO.
    pub fn open(opts: Options) -> arcux_engine::Result<Arc<AppState>> {
        let engine = Engine::open(opts)?;
        Ok(Arc::new(AppState { engine, tso: Tso::new() }))
    }
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
        Ok(Response::new(kv::BeginResponse { start_ts: self.state.tso.now() }))
    }

    async fn prewrite(
        &self,
        request: Request<kv::PrewriteRequest>,
    ) -> Result<Response<kv::PrewriteResponse>, Status> {
        let req = request.into_inner();
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
            let commit_ts = state.tso.now();
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
        let read_ts = if req.read_ts == 0 { self.state.tso.now() } else { req.read_ts };
        let state = self.state.clone();
        let res = run_blocking(move || state.engine.mvcc_get(&req.key, read_ts)).await?;

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
}

impl KvApi {
    /// Run a single-key transaction (begin → prewrite → commit) inside one blocking
    /// task, returning the engine result so callers can map errors uniformly.
    async fn autocommit(&self, m: Mutation) -> Result<Result<u64, Error>, Status> {
        let state = self.state.clone();
        run_blocking(move || {
            let start_ts = state.tso.now();
            let txn = Transaction::new(&state.engine, start_ts, vec![m])?;
            txn.prewrite(start_ts.saturating_add(AUTOCOMMIT_LEASE))?;
            let commit_ts = state.tso.now();
            txn.commit(commit_ts).map(|()| commit_ts)
        })
        .await
    }
}

// ------------------------------------------------------------------------------------
// PD service — GetTimestamp served from the node TSO; the rest stubbed.
// ------------------------------------------------------------------------------------

#[derive(Clone)]
pub struct PdApi {
    state: Arc<AppState>,
}

#[tonic::async_trait]
impl PdService for PdApi {
    async fn get_timestamp(
        &self,
        request: Request<pd::GetTimestampRequest>,
    ) -> Result<Response<pd::GetTimestampResponse>, Status> {
        let count = request.into_inner().count.max(1);
        let first = self.state.tso.now();
        for _ in 1..count {
            self.state.tso.now(); // reserve the contiguous range
        }
        Ok(Response::new(pd::GetTimestampResponse { timestamp: first, count }))
    }

    async fn get_region(
        &self,
        _request: Request<pd::GetRegionRequest>,
    ) -> Result<Response<pd::GetRegionResponse>, Status> {
        Err(Status::unimplemented("regions/PD arrive in Phase 3+"))
    }

    async fn heartbeat(
        &self,
        _request: Request<pd::HeartbeatRequest>,
    ) -> Result<Response<pd::HeartbeatResponse>, Status> {
        Err(Status::unimplemented("PD heartbeat arrives in Phase 3+"))
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

/// Serve all three services on an already-bound listener until `shutdown` resolves.
/// Used by both [`serve`] and the integration tests (which bind an ephemeral port).
pub async fn serve_on<F>(
    state: Arc<AppState>,
    listener: TcpListener,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let incoming = TcpListenerStream::new(listener);
    Server::builder()
        .add_service(KvServiceServer::new(KvApi { state: state.clone() }))
        .add_service(PdServiceServer::new(PdApi { state: state.clone() }))
        .add_service(RaftServiceServer::new(RaftApi))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
}

/// Open the engine, bind `addr`, and serve until Ctrl-C.
pub async fn serve(
    opts: Options,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = AppState::open(opts)?;
    let listener = TcpListener::bind(addr).await?;
    eprintln!("arcux-server listening on {}", listener.local_addr()?);
    serve_on(state, listener, async {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("arcux-server shutting down");
    })
    .await?;
    Ok(())
}

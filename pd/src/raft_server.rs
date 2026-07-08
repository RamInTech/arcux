//! Serving a **replicated** PD node: the `PdService` (leader-only, with redirect) and the
//! `RaftService` (peer-to-peer replication) over one `tonic` server, both backed by a
//! [`PdGroup`].
//!
//! A follower answers no PD request itself — timestamps and placement must come from the one
//! leader, or snapshot isolation could see two timestamp streams. Instead it fails the call
//! with `UNAVAILABLE` and the **current leader's address**, so the caller retries there. Reads
//! (`GetRegion`/`ListRegions`) are served from the leader's up-to-date [`PdFsm`]; writes
//! (`GetTimestamp`/`Heartbeat`) go through Raft.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use arcux_rpc::pd::pd_service_server::{PdService, PdServiceServer};
use arcux_rpc::pd::{
    GetRegionRequest, GetRegionResponse, GetTimestampRequest, GetTimestampResponse,
    HeartbeatRequest, HeartbeatResponse, ListRegionsRequest, ListRegionsResponse,
};
use arcux_rpc::raft::raft_service_server::{RaftService, RaftServiceServer};
use arcux_rpc::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

use crate::cluster::now_ms;
use crate::convert::{from_proto, placed_to_proto, to_proto};
use crate::raft_group::{self, PdGroup, PdGroupOptions};

/// The `PdService` handler for a replicated node. Clones share the same [`PdGroup`].
#[derive(Clone)]
pub struct ReplicatedPdApi {
    group: PdGroup,
}

impl ReplicatedPdApi {
    pub fn new(group: PdGroup) -> ReplicatedPdApi {
        ReplicatedPdApi { group }
    }

    /// The error a follower returns so the caller retries against the leader.
    fn redirect(&self) -> Status {
        match self.group.leader_addr() {
            Some(addr) => Status::unavailable(format!("not pd leader; leader at {addr}")),
            None => Status::unavailable("not pd leader; no leader elected yet"),
        }
    }
}

#[tonic::async_trait]
impl PdService for ReplicatedPdApi {
    async fn get_timestamp(
        &self,
        request: Request<GetTimestampRequest>,
    ) -> Result<Response<GetTimestampResponse>, Status> {
        let count = request.into_inner().count.max(1);
        match self.group.alloc_ts(count as u64).await {
            Some((first, n)) => {
                Ok(Response::new(GetTimestampResponse { timestamp: first, count: n as u32 }))
            }
            None => Err(self.redirect()),
        }
    }

    async fn get_region(
        &self,
        request: Request<GetRegionRequest>,
    ) -> Result<Response<GetRegionResponse>, Status> {
        if !self.group.is_leader() {
            return Err(self.redirect());
        }
        let key = request.into_inner().key;
        match self.group.fsm().route(&key) {
            Some(p) => Ok(Response::new(GetRegionResponse {
                region_id: p.region.id,
                start_key: p.region.start,
                end_key: p.region.end,
                epoch: p.region.epoch,
                node_id: p.node_id,
                address: p.address,
            })),
            None => Err(Status::not_found("no live region covers the key")),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let reported = req.regions.iter().map(from_proto).collect();
        match self.group.heartbeat(req.node_id, req.address, reported, now_ms()).await {
            Some(assigned) => {
                Ok(Response::new(HeartbeatResponse { regions: assigned.iter().map(to_proto).collect() }))
            }
            None => Err(self.redirect()),
        }
    }

    async fn list_regions(
        &self,
        _request: Request<ListRegionsRequest>,
    ) -> Result<Response<ListRegionsResponse>, Status> {
        if !self.group.is_leader() {
            return Err(self.redirect());
        }
        let regions = self.group.fsm().list().iter().map(placed_to_proto).collect();
        Ok(Response::new(ListRegionsResponse { regions }))
    }
}

/// The `RaftService` handler — PD replicas replicating to one another. PD is a single group, so
/// the request's `group_id` is not dispatched on.
#[derive(Clone)]
pub struct PdRaftApi {
    group: PdGroup,
}

impl PdRaftApi {
    pub fn new(group: PdGroup) -> PdRaftApi {
        PdRaftApi { group }
    }
}

#[tonic::async_trait]
impl RaftService for PdRaftApi {
    type AppendEntriesStream = Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<Item = Result<AppendEntriesResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        Ok(Response::new(self.group.handle_request_vote(request.into_inner()).await))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<Self::AppendEntriesStream>, Status> {
        let resp = self.group.handle_append_entries(request.into_inner()).await;
        Ok(Response::new(Box::pin(tokio_stream::once(Ok(resp)))))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        Ok(Response::new(self.group.handle_install_snapshot(request.into_inner()).await))
    }
}

/// Serve a replicated PD node (both services) on an already-bound listener until `shutdown`
/// resolves. Runs the failure-detector sweep on whichever node is leader. Used by [`serve`] and
/// the in-process cluster test.
pub async fn serve_on<F>(
    group: PdGroup,
    listener: TcpListener,
    fd_timeout_ms: u64,
    fd_interval_ms: u64,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    // Failure detector: the leader marks silent nodes down in its (replicated) membership view.
    let sweeper = group.clone();
    let interval = fd_interval_ms.max(1);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(interval));
        loop {
            tick.tick().await;
            if sweeper.is_leader() {
                for id in sweeper.fsm().members().sweep(now_ms(), fd_timeout_ms) {
                    eprintln!("[pd raft] leader: node {id} marked down (no heartbeat in {fd_timeout_ms}ms)");
                }
            }
        }
    });

    Server::builder()
        .add_service(PdServiceServer::new(ReplicatedPdApi::new(group.clone())))
        .add_service(RaftServiceServer::new(PdRaftApi::new(group)))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

/// Build a PD Raft group for `node_id` in the `addrs` topology and start it. The returned handle
/// drives replication; pair it with [`serve_on`] to expose the services.
pub fn start_group(node_id: u64, addrs: std::collections::HashMap<u64, String>) -> PdGroup {
    let voters: Vec<u64> = {
        let mut v: Vec<u64> = addrs.keys().copied().collect();
        v.sort_unstable();
        v
    };
    raft_group::start(PdGroupOptions { id: node_id, voters, addrs })
}

/// Default failure-detector timing for a replicated PD (mirrors the single-process server).
pub const DEFAULT_FD_TIMEOUT_MS: u64 = 6_000;
pub const DEFAULT_FD_INTERVAL_MS: u64 = 1_000;

/// Open a replicated PD node and serve until Ctrl-C. `addrs` maps every voter id to its PD
/// serving address (including this node's own `listen`).
pub async fn serve(
    node_id: u64,
    addrs: std::collections::HashMap<u64, String>,
    listen: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let group = start_group(node_id, addrs);
    let listener = TcpListener::bind(listen).await?;
    eprintln!("arcux-pd (replicated) node {node_id} listening on {}", listener.local_addr()?);
    let shutdown = async move {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("arcux-pd node {node_id} shutting down");
    };
    serve_on(group, listener, DEFAULT_FD_TIMEOUT_MS, DEFAULT_FD_INTERVAL_MS, shutdown).await?;
    Ok(())
}

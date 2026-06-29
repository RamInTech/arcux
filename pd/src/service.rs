//! The `pd.PdService` gRPC implementation, backed by the [`Tso`](crate::Tso) and the
//! per-node [`Membership`](crate::Membership) registry.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use arcux_rpc::pd::pd_service_server::PdService;
use arcux_rpc::pd::{
    GetRegionRequest, GetRegionResponse, GetTimestampRequest, GetTimestampResponse,
    HeartbeatRequest, HeartbeatResponse, ListRegionsRequest, ListRegionsResponse,
};

use crate::cluster::now_ms;
use crate::convert::{from_proto, placed_to_proto, to_proto};
use crate::{Membership, Tso};

/// The PD service handler. Cheap to clone (shares the oracle + membership).
#[derive(Clone)]
pub struct PdApi {
    tso: Arc<Tso>,
    members: Arc<Membership>,
}

impl PdApi {
    pub fn new(tso: Arc<Tso>, members: Arc<Membership>) -> PdApi {
        PdApi { tso, members }
    }
}

#[tonic::async_trait]
impl PdService for PdApi {
    /// Allocate a contiguous block of `count` timestamps from the authoritative oracle.
    async fn get_timestamp(
        &self,
        request: Request<GetTimestampRequest>,
    ) -> Result<Response<GetTimestampResponse>, Status> {
        let count = request.into_inner().count.max(1);
        let first = self
            .tso
            .alloc(count as u64)
            .map_err(|e| Status::internal(format!("tso alloc failed: {e}")))?;
        Ok(Response::new(GetTimestampResponse { timestamp: first, count }))
    }

    /// Route a single key to its owning region **and node** (from PD's live view).
    async fn get_region(
        &self,
        request: Request<GetRegionRequest>,
    ) -> Result<Response<GetRegionResponse>, Status> {
        let key = request.into_inner().key;
        match self.members.route(&key) {
            Some(p) => Ok(Response::new(GetRegionResponse {
                region_id: p.region.id,
                start_key: p.region.start,
                end_key: p.region.end,
                epoch: p.region.epoch,
                node_id: p.node_id,
                address: p.address,
            })),
            // No live node owns a region covering this key (none reported, or it is down).
            None => Err(Status::not_found("no live region covers the key")),
        }
    }

    /// A node reports the regions it owns + its serving address; PD records its liveness
    /// and returns the regions it should authoritatively own (seeding a fresh node).
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let reported = req.regions.iter().map(from_proto).collect();
        let assigned =
            self.members.heartbeat(req.node_id, req.address, reported, now_ms());
        Ok(Response::new(HeartbeatResponse {
            regions: assigned.iter().map(to_proto).collect(),
        }))
    }

    /// The whole live region view, tagged with owners (for client routing caches/tooling).
    async fn list_regions(
        &self,
        _request: Request<ListRegionsRequest>,
    ) -> Result<Response<ListRegionsResponse>, Status> {
        let regions = self.members.list().iter().map(placed_to_proto).collect();
        Ok(Response::new(ListRegionsResponse { regions }))
    }
}

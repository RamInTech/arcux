//! The `pd.PdService` gRPC implementation, backed by the [`Tso`](crate::Tso) and the
//! aggregated [`RegionRegistry`](crate::RegionRegistry).

use std::sync::Arc;

use tonic::{Request, Response, Status};

use arcux_rpc::pd::pd_service_server::PdService;
use arcux_rpc::pd::{
    GetRegionRequest, GetRegionResponse, GetTimestampRequest, GetTimestampResponse,
    HeartbeatRequest, HeartbeatResponse, ListRegionsRequest, ListRegionsResponse,
};

use crate::convert::{from_proto, to_proto};
use crate::{RegionRegistry, Tso};

/// The PD service handler. Cheap to clone (shares the oracle + registry).
#[derive(Clone)]
pub struct PdApi {
    tso: Arc<Tso>,
    regions: Arc<RegionRegistry>,
}

impl PdApi {
    pub fn new(tso: Arc<Tso>, regions: Arc<RegionRegistry>) -> PdApi {
        PdApi { tso, regions }
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

    /// Route a single key to its owning region (from PD's aggregated view).
    async fn get_region(
        &self,
        request: Request<GetRegionRequest>,
    ) -> Result<Response<GetRegionResponse>, Status> {
        let key = request.into_inner().key;
        match self.regions.route(&key) {
            Some(r) => Ok(Response::new(GetRegionResponse {
                region_id: r.id,
                start_key: r.start,
                end_key: r.end,
                epoch: r.epoch,
            })),
            // No node has reported a region covering this key yet.
            None => Err(Status::not_found("no region covers the key (no node has reported it)")),
        }
    }

    /// A node reports the regions it currently owns; PD adopts them as its view.
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        if !req.regions.is_empty() {
            self.regions.replace(req.regions.iter().map(from_proto).collect());
        }
        Ok(Response::new(HeartbeatResponse {}))
    }

    /// The whole aggregated region view (for client routing caches and tooling).
    async fn list_regions(
        &self,
        _request: Request<ListRegionsRequest>,
    ) -> Result<Response<ListRegionsResponse>, Status> {
        let regions = self.regions.list().iter().map(to_proto).collect();
        Ok(Response::new(ListRegionsResponse { regions }))
    }
}
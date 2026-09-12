//! The `pd.PdService` gRPC implementation, backed by the [`Tso`](crate::Tso) and the
//! per-node [`Membership`](crate::Membership) registry.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use arcux_rpc::pd::pd_service_server::PdService;
use arcux_rpc::pd::{
    GetRegionRequest, GetRegionResponse, GetTimestampRequest, GetTimestampResponse,
    CreateTableRequest, CreateTableResponse, HeartbeatRequest, HeartbeatResponse,
    ListRegionsRequest, ListRegionsResponse, ListTablesRequest, ListTablesResponse,
};

use crate::cluster::now_ms;
use crate::convert::{
    list_tables_response, placed_to_proto, replica_set_from_proto, replica_set_to_proto,
    table_decl_from_proto,
};
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
        let reported = req.regions.iter().map(replica_set_from_proto).collect();
        let tables = req.tables.iter().map(table_decl_from_proto).collect();
        let assigned =
            self.members.heartbeat(req.node_id, req.address, reported, tables, now_ms());
        Ok(Response::new(HeartbeatResponse {
            regions: assigned.iter().map(replica_set_to_proto).collect(),
            // Single-process PD holds no authoritative region table, so it never carves and its
            // assignment is simply the node's own report echoed back. Version 0 means "nothing to
            // adopt", which is exactly what a node should conclude.
            catalog_version: 0,
            tables: Vec::new(),
        }))
    }

    /// Not served here. Carving a table cluster-wide needs the replicated region table, which
    /// only the PD-on-Raft path has — duplicating it into the single-process path would mean two
    /// authorities for the same state.
    async fn create_table(
        &self,
        _request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        Err(Status::unimplemented(
            "create_table needs a replicated PD (start it with `arcux-pd -n 1 -c 1` or a 3-node \
             group); the single-process PD keeps no region table to carve",
        ))
    }

    /// The cluster-wide catalog PD has heard, plus any table two nodes describe differently.
    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        Ok(Response::new(list_tables_response(
            self.members.tables(),
            self.members.table_conflicts(),
        )))
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

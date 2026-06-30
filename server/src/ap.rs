//! AP (always-available) replication — leaderless, HLC-stamped, Last-Writer-Wins.
//!
//! An AP region has **no leader and no Raft**. The coordinator (whichever node a client hits)
//! stamps the write with its [`Hlc`](crate::hlc::Hlc), applies it locally, then **fans it
//! out** to the other replicas via the internal `kv.ReplicateAp` RPC — best-effort, acking
//! the client after the *local* write (W=1). So the write succeeds even if peers are
//! unreachable: that's the always-available property (the CP path would refuse without a
//! majority). Re-delivery is idempotent — the HLC timestamp is the MVCC version, so applying
//! the same write twice rewrites identical bytes — and reads return the highest-HLC version
//! (Last-Writer-Wins).

use std::collections::HashMap;

use arcux_rpc::kv;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use tonic::transport::Channel;

/// One AP region this node hosts: lazy clients to its other replicas.
struct ApRegion {
    peers: Vec<KvServiceClient<Channel>>,
}

/// The node's AP regions, keyed by region id.
pub struct ApReplication {
    regions: HashMap<u64, ApRegion>,
}

impl ApReplication {
    pub fn new() -> ApReplication {
        ApReplication { regions: HashMap::new() }
    }

    /// Register an AP region with the addresses of its other replicas.
    pub fn insert(&mut self, region_id: u64, peer_addrs: &HashMap<u64, String>) {
        let peers = peer_addrs
            .values()
            .filter_map(|addr| {
                Channel::from_shared(addr.clone())
                    .ok()
                    .map(|e| KvServiceClient::new(e.connect_lazy()))
            })
            .collect();
        self.regions.insert(region_id, ApRegion { peers });
    }

    /// Whether this node hosts AP region `region_id`.
    pub fn hosts(&self, region_id: u64) -> bool {
        self.regions.contains_key(&region_id)
    }

    /// Fan a write out to the region's peer replicas — best-effort, each in its own task.
    /// Returns immediately; the coordinator has already applied locally and acked (W=1).
    pub fn fanout(&self, region_id: u64, key: Vec<u8>, value: Vec<u8>, is_delete: bool, hlc_ts: u64) {
        let Some(region) = self.regions.get(&region_id) else { return };
        for client in &region.peers {
            let mut client = client.clone();
            let req = kv::ReplicateApRequest {
                region_id,
                key: key.clone(),
                value: value.clone(),
                is_delete,
                hlc_ts,
            };
            tokio::spawn(async move {
                let _ = client.replicate_ap(req).await; // best-effort; lost peers are fine
            });
        }
    }
}

impl Default for ApReplication {
    fn default() -> Self {
        ApReplication::new()
    }
}

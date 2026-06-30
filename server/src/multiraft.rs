//! MultiRaft — a node hosting **many** region Raft groups at once.
//!
//! Phase 4b/4b+ ran one whole-keyspace group. Phase 4b++ makes each region its own group,
//! all multiplexed over one `RaftService` (an inbound RPC carries a `group_id`; the
//! [`MultiRaft`] map routes it). The groups share the node's single engine — regions own
//! disjoint key ranges, so their applies never collide — mirroring TiKV's raftstore, where
//! one store hosts thousands of region replicas over one RocksDB.
//!
//! The per-region groups are built in [`crate::AppState::open_multiraft`] (which owns the
//! engine + apply closure + `WalStorage`); this module is the registry around them.

use std::collections::HashMap;

use crate::raft_group::RaftGroup;

/// How a region is placed: its range/epoch and the replica set hosting its group. Passed to
/// [`crate::AppState::open_multiraft`] — one entry per region this node hosts.
pub struct RegionPlacement {
    pub region_id: u64,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub epoch: u64,
    /// All replica node ids (including this node).
    pub voters: Vec<u64>,
    /// The other replicas' serving addresses (excludes this node), for the Raft transport.
    pub peers: HashMap<u64, String>,
}

/// The node's live region groups, keyed by region id (= the wire `group_id`).
pub struct MultiRaft {
    groups: HashMap<u64, RaftGroup>,
}

impl MultiRaft {
    pub fn new(groups: HashMap<u64, RaftGroup>) -> MultiRaft {
        MultiRaft { groups }
    }

    /// The group for `region_id`, if this node hosts it.
    pub fn group(&self, region_id: u64) -> Option<&RaftGroup> {
        self.groups.get(&region_id)
    }

    /// Stop every group (cascades each actor/ticker/sender down). Called when the node stops.
    pub fn shutdown(&self) {
        for g in self.groups.values() {
            g.shutdown();
        }
    }
}

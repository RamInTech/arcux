//! arcux Phase 2 wire contract — generated gRPC types for the kv / raft / pd services.
//!
//! The schemas are **frozen and versioned** (see each `.proto`): field numbers are
//! append-only and removed fields are `reserved`. The `server` and `client` crates both
//! depend on this crate so they share identical generated message types.

/// Wire-contract version. Bump only for compatible (append-only) schema changes.
/// v2 (Phase 3): added `kv.Context` routing + `kv.SplitRegion`, and `pd.Region` /
/// `pd.ListRegions` / regions on `pd.Heartbeat`. v3 (Phase 3b): added node addressing
/// (`node_id`/`address` on `pd.Region` + `GetRegionResponse`, `address` on
/// `HeartbeatRequest`) and the assigned-regions list on `HeartbeatResponse`. v4 (Phase 4b++):
/// added `group_id` to the Raft RPC requests so one `RaftService` multiplexes many region
/// groups (MultiRaft). v5 (Phase 5): added the internal `kv.ReplicateAp` RPC for the
/// leaderless AP write path. v6 (Phase 4b++ rest): added `LogEntry.entry_type` (config-change
/// entries) and `InstallSnapshotRequest.conf_state` (membership carried by a snapshot) for
/// single-server membership changes. v7 (Phase 5, cross-region Percolator): added the
/// `kv.CheckTxnStatus` + `kv.ResolveLock` RPCs (consult the primary region for a txn's fate;
/// roll a region's keys forward/back through its own Raft log). v8 (Phase 4b++ rest, auto
/// re-replication): added `InstallSnapshotRequest.learners` (non-voting membership carried by
/// a snapshot) and the `raft.TimeoutNow` RPC (leadership transfer). All additions are
/// field-/RPC-additive, so older peers stay wire-compatible. v9 (Phase 5b, AP anti-entropy):
/// added the `kv.ApDigest` + `kv.ApFetch` RPCs (Merkle-digest reconciliation + read-repair for
/// the leaderless AP path). All additions are field-/RPC-additive.
pub const VERSION: u32 = 9;

/// KV API v1 — the transactional + autocommit surface (fully implemented in Phase 2).
pub mod kv {
    tonic::include_proto!("kv");
}

/// Raft RPCs — shapes frozen now, implemented in Phase 4 (handlers `Unimplemented`).
pub mod raft {
    tonic::include_proto!("raft");
}

/// Placement Driver RPCs — `GetTimestamp` served from the node TSO; the rest are
/// frozen and `Unimplemented` until Phase 3+.
pub mod pd {
    tonic::include_proto!("pd");
}

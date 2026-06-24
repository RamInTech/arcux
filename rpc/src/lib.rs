//! arcux Phase 2 wire contract — generated gRPC types for the kv / raft / pd services.
//!
//! The schemas are **frozen and versioned** (see each `.proto`): field numbers are
//! append-only and removed fields are `reserved`. The `server` and `client` crates both
//! depend on this crate so they share identical generated message types.

/// Wire-contract version. Bump only for compatible (append-only) schema changes.
pub const VERSION: u32 = 1;

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

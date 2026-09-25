//! arcux Phase 3 — the **Placement Driver** (PD).
//!
//! PD is the cluster's coordination authority. It owns two things every other component
//! needs but no single data node can own alone:
//!
//! * the **Timestamp Oracle** ([`Tso`]) — one monotonic, restart-safe source of the
//!   `start_ts`/`commit_ts` values transactions are ordered by; and
//! * the **region router** ([`RegionRegistry`]) — the map from a key to the region (and
//!   thus the node) that owns it, aggregated from the regions data nodes report.
//!
//! The [`service`] module exposes both over the frozen `pd.PdService` gRPC contract; the
//! `arcux-pd` binary ([`server`]) runs it. Data nodes are PD *clients* (pulling
//! timestamps, reporting regions); KV clients are PD *clients* too (resolving routes).
//!
//! ## PD-on-Raft (Phase 4b++)
//!
//! A single PD process is a single point of failure — lose it and the cluster loses its TSO
//! and router. The [`replicated`] module removes that: PD's two pieces of authoritative state
//! (the TSO high-water and the placement/liveness view) become a **replicated state machine**
//! ([`PdFsm`]) driven by the hand-rolled [`arcux_raft`] core ([`PdReplica`]), so a three-node
//! PD group survives a leader failure with no lost placement and — critically — no reissued
//! timestamp. Built core-first and proven by a deterministic failover test; the gRPC transport
//! that stands up a real multi-process PD cluster is the mechanical next step.

pub mod cluster;
pub mod convert;
pub mod format;
mod persist;
pub mod raft_group;
pub mod raft_server;
pub mod raft_wire;
pub mod region;
pub mod replicated;
pub mod server;
pub mod service;
pub mod tso;

pub use cluster::{Membership, PlacedRegion, Regime, ReplicaSet, TableConflict};
pub use region::{name_prefix, prefix_successor};
pub use region::{
    strip_table_prefix, table_id_of, table_key, table_prefix, table_range, TableId,
    DEFAULT_TABLE_ID, DEFAULT_TABLE_NAME, TABLE_PREFIX_LEN,
};
pub use raft_group::{PdGroup, PdGroupOptions};
pub use region::{region_id, Region, RegionRegistry};
pub use replicated::{PdCmd, PdFsm, PdReplica, Ready};
pub use server::Pd;
pub use service::PdApi;
pub use tso::Tso;
/// Bind a server's listening socket, turning the one failure an operator hits routinely — the
/// port is taken, usually by a copy of the same process left running — into a sentence instead
/// of `Os { code: 48, kind: AddrInUse, … }`. Shared by `arcux-pd` and `arcux-server`.
pub async fn bind(addr: std::net::SocketAddr, what: &str) -> std::io::Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::AddrInUse {
            std::io::Error::new(
                e.kind(),
                format!("{addr} is already in use — is another {what} running there?"),
            )
        } else {
            e
        }
    })
}

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
mod persist;
pub mod raft_group;
pub mod raft_server;
pub mod raft_wire;
pub mod region;
pub mod replicated;
pub mod server;
pub mod service;
pub mod tso;

pub use cluster::{Membership, PlacedRegion};
pub use raft_group::{PdGroup, PdGroupOptions};
pub use region::{region_id, Region, RegionRegistry};
pub use replicated::{PdCmd, PdFsm, PdReplica, Ready};
pub use server::Pd;
pub use service::PdApi;
pub use tso::Tso;
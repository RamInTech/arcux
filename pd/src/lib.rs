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
//! Phase 4 will move region ownership under per-region Raft, but the PD contract — one
//! TSO, epoch-versioned routing — is unchanged by that.

pub mod cluster;
pub mod convert;
mod persist;
pub mod region;
pub mod server;
pub mod service;
pub mod tso;

pub use cluster::{Membership, PlacedRegion};
pub use region::{region_id, Region, RegionRegistry};
pub use server::Pd;
pub use service::PdApi;
pub use tso::Tso;
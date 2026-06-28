//! arcux Phase 4 — a hand-rolled Raft consensus core.
//!
//! This crate is the **algorithm**, not its deployment: leader election, log
//! replication, the commit-safety rules (Log Matching + the Figure-8 current-term
//! restriction), and persistence — implemented against Figure 2 of the Raft paper
//! (Ongaro & Ousterhout) and nothing else. It is deliberately transport-free and
//! single-threaded so the whole protocol can be exercised by a deterministic,
//! in-process cluster (see `tests/cluster.rs`) under partitions, restarts and
//! message reordering, with the safety invariants asserted directly.
//!
//! # Two integration seams
//! The core touches the outside world only through:
//! - [`Storage`] — durable term/vote/log; an engine-backed (`WalStorage`) impl
//!   replaces [`MemStorage`] at integration time.
//! - [`Message`] / [`Entry`] — a model that maps 1:1 onto the frozen
//!   [`raft.proto`](../rpc/proto/raft.proto) RPCs; a `tonic` transport later
//!   converts between the two.
//!
//! Both are intentionally narrow so the Phase-4 integration step (binding a
//! region to a group, routing to the leader, applying committed entries into the
//! region's engine state) is wiring rather than a rewrite.
//!
//! # Driving a node
//! ```
//! use arcux_raft::{Config, MemStorage, RaftNode};
//! let mut n = RaftNode::new(Config::new(1, vec![1]), MemStorage::new());
//! while !n.is_leader() {
//!     n.tick(); // a single-node group elects itself
//! }
//! let idx = n.propose(b"hello".to_vec()).unwrap();
//! assert_eq!(idx, 1);
//! assert_eq!(n.take_committed().len(), 1);
//! ```
//!
//! Not yet in this milestone (the next Phase-4 step): `InstallSnapshot` / log
//! compaction and single-server membership changes. The wire contract already
//! reserves room for both.

pub mod message;
pub mod node;
pub mod storage;

pub use message::{Entry, HardState, Message, MessageBody};
pub use node::{Config, ProposeError, RaftNode, Role};
pub use storage::{MemStorage, Storage};

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
//! `InstallSnapshot` / log compaction land in Phase 4b++ (see [`Snapshot`] and the
//! [`Storage`] compaction methods): a node snapshots its committed state, discards the log
//! below it, and fast-forwards a far-behind replica with a snapshot instead of the whole
//! log. **Single-server membership changes** also land in 4b++ (see [`ConfChange`] and
//! [`RaftNode::propose_conf_change`]): one add/remove at a time, effective on append, with the
//! resulting voter set recorded in the log + snapshot so a newcomer adopts it directly.
//! PD-on-Raft and PD-driven automatic re-replication are the remaining Phase-4 work.

pub mod message;
pub mod node;
pub mod storage;

pub use message::{ConfChange, Entry, EntryType, HardState, Message, MessageBody};
pub use node::{Config, ProposeError, RaftNode, Role};
pub use storage::{MemStorage, Snapshot, Storage};

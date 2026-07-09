//! arcux storage engine — Phase 1 (single-node correctness slice).
//!
//! A durable, multi-version, transactional key-value engine:
//!
//! * an append-only **WAL** with group-commit fsync,
//! * a per-CF **skiplist memtable** that flushes to immutable **SSTables**,
//! * **MVCC** over Lock/Default/Write column families, and
//! * a single-node **Percolator** for snapshot-isolated transactions,
//!
//! all recoverable across `kill -9` with zero acknowledged-write loss.
//!
//! Deferred to Phase 1b: leveled compaction, bloom filters, block cache, and the
//! full version-edit manifest (see the project plan).

pub mod batch;
pub mod clock;
pub mod db;
pub mod encoding;
pub mod error;
pub mod keys;
pub mod manifest;
pub mod memtable;
pub mod mvcc;
pub mod options;
pub mod percolator;
pub mod scan;
pub mod sstable;
pub mod wal;

pub use batch::{WriteBatch, WriteOp};
pub use clock::Tso;
pub use db::Engine;
pub use error::{Error, Result};
pub use keys::{Cf, Lock, LockKind, Value};
pub use memtable::MemValue;
pub use mvcc::{Snapshot, TxnStatus};
pub use options::{FsyncMode, Options};
pub use percolator::{Mutation, Transaction};

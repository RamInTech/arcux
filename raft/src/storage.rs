//! Raft persistent state, behind a trait.
//!
//! The core writes its crash-critical state — `HardState` (term + vote) and the
//! log — through [`Storage`]. The safety proof requires this to be durable
//! *before* a node acts on it (vote before replying `RequestVote`, entry before
//! replying `AppendEntries`), so a real impl `fsync`s in these methods; the core
//! just calls them in the right order. [`MemStorage`] is the in-memory impl used
//! by tests and is also what a restart test clones to simulate a reboot.
//!
//! The eventual engine-backed impl (`WalStorage`, Phase-4 integration) persists
//! the log via the Phase-1 WAL subsystem; the trait shape is chosen so that swap
//! is mechanical.

use crate::message::{Entry, HardState};

/// Durable Raft state. The log is 1-indexed and contiguous: index `1` is the
/// first entry, and there are no gaps (snapshots, which would introduce a
/// non-zero starting index, are a later Phase-4 milestone).
pub trait Storage {
    /// The last persisted `HardState`. Returned `(0, None)` for a fresh node.
    fn hard_state(&self) -> HardState;
    /// Durably record term + vote. Must not return until it is crash-safe.
    fn save_hard_state(&mut self, hs: HardState);

    /// Index of the last log entry, or `0` if the log is empty.
    fn last_index(&self) -> u64;
    /// Term of the entry at `index`. `index == 0` is the sentinel `Some(0)`
    /// (the empty-log "before the first entry" point); past the end is `None`.
    fn term(&self, index: u64) -> Option<u64>;
    /// Entries in the inclusive range `[low, high]`, clamped to what exists.
    fn entries(&self, low: u64, high: u64) -> Vec<Entry>;

    /// Append contiguous entries (each `index` must equal `last_index() + 1` at
    /// the moment it is pushed). Must be durable before returning.
    fn append(&mut self, entries: &[Entry]);
    /// Delete every entry with `index >= from` (used to resolve a log conflict).
    fn truncate_suffix(&mut self, from: u64);
}

/// In-memory [`Storage`]. `Clone` so tests can snapshot a node's durable state
/// and rebuild a fresh node from it — i.e. simulate a process restart.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    hard: HardState,
    log: Vec<Entry>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Storage for MemStorage {
    fn hard_state(&self) -> HardState {
        self.hard
    }

    fn save_hard_state(&mut self, hs: HardState) {
        self.hard = hs;
    }

    fn last_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    fn term(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log.get((index - 1) as usize).map(|e| e.term)
    }

    fn entries(&self, low: u64, high: u64) -> Vec<Entry> {
        if low == 0 || low > high {
            return Vec::new();
        }
        let lo = (low - 1) as usize;
        let hi = (high as usize).min(self.log.len());
        if lo >= hi {
            return Vec::new();
        }
        self.log[lo..hi].to_vec()
    }

    fn append(&mut self, entries: &[Entry]) {
        for e in entries {
            debug_assert_eq!(
                e.index,
                self.last_index() + 1,
                "Storage::append requires contiguous indices"
            );
            self.log.push(e.clone());
        }
    }

    fn truncate_suffix(&mut self, from: u64) {
        if from == 0 {
            self.log.clear();
        } else {
            self.log.truncate((from - 1) as usize);
        }
    }
}

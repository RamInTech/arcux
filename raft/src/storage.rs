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

/// A point-in-time snapshot: the state machine's committed state through
/// `last_included_index` (opaque `data`, produced/applied by the integration layer), the
/// group membership as of that index (`conf_state`, Phase 4b++ rest), and the log position it
/// covers. Once taken, the log keeps only entries **above** the snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub conf_state: Vec<u64>,
    pub data: Vec<u8>,
}

/// Durable Raft state. The log is contiguous and starts at [`first_index`](Storage::first_index)
/// — `1` for an uncompacted log, or `snapshot index + 1` once the log has been **compacted**
/// (Phase 4b++). Indices are absolute; entries below `first_index` live inside the snapshot.
pub trait Storage {
    /// The last persisted `HardState`. Returned `(0, None)` for a fresh node.
    fn hard_state(&self) -> HardState;
    /// Durably record term + vote. Must not return until it is crash-safe.
    fn save_hard_state(&mut self, hs: HardState);

    /// Index of the last log entry, or the snapshot index (`0` if neither exists).
    fn last_index(&self) -> u64;
    /// Term of the entry at `index`. `index == 0` is the sentinel `Some(0)`; the snapshot
    /// boundary (`index == snapshot.last_included_index`) returns its term; an index compacted
    /// away (below `first_index`, not the boundary) or past the end returns `None`.
    fn term(&self, index: u64) -> Option<u64>;
    /// Entries in the inclusive range `[low, high]`, clamped to what the log still holds.
    fn entries(&self, low: u64, high: u64) -> Vec<Entry>;

    /// Append contiguous entries (each `index` must equal `last_index() + 1` at
    /// the moment it is pushed). Must be durable before returning.
    fn append(&mut self, entries: &[Entry]);
    /// Delete every entry with `index >= from` (used to resolve a log conflict).
    fn truncate_suffix(&mut self, from: u64);

    // --- snapshots / log compaction (Phase 4b++) ---

    /// The stored snapshot (the compaction point + its state), or `None` if uncompacted.
    fn snapshot(&self) -> Option<Snapshot>;
    /// The first index the log could still hold: `snapshot index + 1` (or `1`).
    fn first_index(&self) -> u64;
    /// Record a snapshot of committed state through `index` (already applied by the caller),
    /// with the membership `conf_state` as of that index, and discard log entries with index
    /// `<= index`. Durable before returning.
    fn compact(&mut self, index: u64, term: u64, conf_state: Vec<u64>, data: Vec<u8>);
    /// Install a snapshot received from the leader: adopt its meta + data and drop the log
    /// (its entries are superseded). Durable before returning.
    fn apply_snapshot(&mut self, snap: Snapshot);
}

/// In-memory [`Storage`]. `Clone` so tests can snapshot a node's durable state
/// and rebuild a fresh node from it — i.e. simulate a process restart.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    hard: HardState,
    /// Entries strictly above `base()`; `log[i]` has absolute index `base() + i + 1`.
    log: Vec<Entry>,
    snap: Option<Snapshot>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// The snapshot's `last_included_index` (0 when uncompacted) — the log's index offset.
    fn base(&self) -> u64 {
        self.snap.as_ref().map(|s| s.last_included_index).unwrap_or(0)
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
        self.log.last().map(|e| e.index).unwrap_or_else(|| self.base())
    }

    fn term(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        let base = self.base();
        if index == base {
            return self.snap.as_ref().map(|s| s.last_included_term);
        }
        if index < base {
            return None; // compacted away
        }
        self.log.get((index - base - 1) as usize).map(|e| e.term)
    }

    fn entries(&self, low: u64, high: u64) -> Vec<Entry> {
        let base = self.base();
        let low = low.max(base + 1);
        if low > high {
            return Vec::new();
        }
        let lo = (low - base - 1) as usize;
        let hi = ((high - base) as usize).min(self.log.len());
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
        let base = self.base();
        if from <= base + 1 {
            self.log.clear();
        } else {
            self.log.truncate((from - base - 1) as usize);
        }
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.snap.clone()
    }

    fn first_index(&self) -> u64 {
        self.base() + 1
    }

    fn compact(&mut self, index: u64, term: u64, conf_state: Vec<u64>, data: Vec<u8>) {
        let base = self.base();
        if index <= base {
            return; // already compacted at or past this point
        }
        let drop = ((index - base) as usize).min(self.log.len());
        self.log.drain(0..drop);
        self.snap = Some(Snapshot {
            last_included_index: index,
            last_included_term: term,
            conf_state,
            data,
        });
    }

    fn apply_snapshot(&mut self, snap: Snapshot) {
        self.log.clear();
        self.snap = Some(snap);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ent(term: u64, index: u64) -> Entry {
        Entry::normal(term, index, vec![index as u8])
    }

    fn seed(count: u64) -> MemStorage {
        let mut s = MemStorage::new();
        s.append(&(1..=count).map(|i| ent(1, i)).collect::<Vec<_>>());
        s
    }

    #[test]
    fn compact_drops_prefix_and_keeps_offset_math() {
        let mut s = seed(5);
        assert_eq!(s.first_index(), 1);
        assert_eq!(s.last_index(), 5);

        s.compact(3, 1, vec![1, 2, 3], b"snap@3".to_vec());

        // Boundary + offsets.
        assert_eq!(s.first_index(), 4);
        assert_eq!(s.last_index(), 5);
        assert_eq!(s.term(3), Some(1)); // snapshot boundary returns its term
        assert_eq!(s.term(2), None); // compacted away
        assert_eq!(s.term(4), Some(1)); // still in the log
        assert_eq!(s.term(0), Some(0)); // sentinel

        // Entries below first_index are gone; the tail survives with absolute indices.
        assert_eq!(s.entries(1, 5), vec![ent(1, 4), ent(1, 5)]);
        assert_eq!(s.entries(4, 5), vec![ent(1, 4), ent(1, 5)]);

        // Snapshot is retrievable.
        let snap = s.snapshot().unwrap();
        assert_eq!(snap.last_included_index, 3);
        assert_eq!(snap.last_included_term, 1);
        assert_eq!(snap.conf_state, vec![1, 2, 3]);
        assert_eq!(snap.data, b"snap@3");
    }

    #[test]
    fn append_after_compaction_continues_at_absolute_index() {
        let mut s = seed(5);
        s.compact(5, 1, vec![], b"snap@5".to_vec());
        assert_eq!(s.first_index(), 6);
        assert_eq!(s.last_index(), 5); // no entries above the snapshot yet

        s.append(&[ent(2, 6), ent(2, 7)]);
        assert_eq!(s.last_index(), 7);
        assert_eq!(s.term(6), Some(2));
        assert_eq!(s.term(5), Some(1)); // boundary term from the snapshot
        assert_eq!(s.entries(6, 7), vec![ent(2, 6), ent(2, 7)]);
    }

    #[test]
    fn compact_is_idempotent_and_ignores_stale_index() {
        let mut s = seed(5);
        s.compact(3, 1, vec![], b"a".to_vec());
        s.compact(2, 1, vec![], b"stale".to_vec()); // below current snapshot: ignored
        assert_eq!(s.first_index(), 4);
        assert_eq!(s.snapshot().unwrap().data, b"a");
    }

    #[test]
    fn truncate_suffix_after_compaction() {
        let mut s = seed(6);
        s.compact(3, 1, vec![], b"snap".to_vec());
        s.truncate_suffix(5); // drop indices >= 5
        assert_eq!(s.last_index(), 4);
        assert_eq!(s.entries(4, 6), vec![ent(1, 4)]);

        // Truncating into the snapshot clears the whole tail.
        s.truncate_suffix(4);
        assert_eq!(s.last_index(), 3); // back to the snapshot boundary
        assert!(s.entries(4, 6).is_empty());
    }

    #[test]
    fn apply_snapshot_supersedes_the_log() {
        let mut s = seed(5);
        s.apply_snapshot(Snapshot {
            last_included_index: 10,
            last_included_term: 4,
            conf_state: vec![1, 2, 3],
            data: b"installed".to_vec(),
        });
        assert_eq!(s.first_index(), 11);
        assert_eq!(s.last_index(), 10);
        assert_eq!(s.term(10), Some(4));
        assert_eq!(s.term(5), None); // old log entries are gone
        assert!(s.entries(1, 10).is_empty());

        // Replication resumes above the installed snapshot.
        s.append(&[ent(4, 11)]);
        assert_eq!(s.last_index(), 11);
        assert_eq!(s.entries(11, 11), vec![ent(4, 11)]);
    }
}

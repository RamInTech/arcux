//! In-flight single-key writes on a region's leader — the window a fast-path `put` opens, and
//! the reason a read must sometimes wait for one.
//!
//! A CP `put` takes `start_ts` and `commit_ts` from the oracle and proposes an **already
//! committed** write: one Raft round, no lock. That is what makes it fast, and it leaves a
//! window. Between taking `commit_ts = C` and the entry applying, a transaction can take
//! `start_ts = R > C`, read the key, and miss the write — it is not in the engine yet, and there
//! is no lock to announce it. The transaction then writes the key itself; Percolator's conflict
//! check asks whether any commit is `>= R`, and `C < R`, so it sees none. **The put is silently
//! overwritten.**
//!
//! So a leader records every fast-path write it has proposed and not yet resolved, and a read at
//! `read_ts >= C` waits for it. This is what TiKV's concurrency manager does for its one-phase
//! commits, for the same reason. The record is memory on one leader, which is enough: it only has
//! to cover the gap on the node serving both the write and the read, and a leader that dies or
//! steps down cannot serve the read either — the new leader applies every entry of the previous
//! term (including this one, if it committed) before it may serve a read at all.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// The keys a leader has proposed a fast-path write for and not yet resolved, each with the
/// commit timestamps in flight for it (a key can have more than one at a time).
#[derive(Default)]
pub struct InFlight {
    keys: Mutex<BTreeMap<Vec<u8>, Vec<u64>>>,
}

impl InFlight {
    pub fn new() -> Arc<InFlight> {
        Arc::new(InFlight::default())
    }

    /// Record that `key` has a write in flight committing at `commit_ts`. Dropping the returned
    /// guard clears it, whatever the outcome — committed, rejected, or the leader stepping down.
    pub fn register(self: &Arc<Self>, key: Vec<u8>, commit_ts: u64) -> InFlightGuard {
        self.keys.lock().expect("in-flight poisoned").entry(key.clone()).or_default().push(commit_ts);
        InFlightGuard { owner: self.clone(), key, commit_ts }
    }

    /// Is there a write in flight in `[start, end)` — `end` empty meaning "to the end of the
    /// keyspace" — committing at or below `read_ts`? Such a write belongs in that read's
    /// snapshot but is not in the engine yet, so the read must wait for it.
    pub fn blocks_read(&self, start: &[u8], end: &[u8], read_ts: u64) -> bool {
        let keys = self.keys.lock().expect("in-flight poisoned");
        keys.range(start.to_vec()..)
            .take_while(|(k, _)| end.is_empty() || k.as_slice() < end)
            .any(|(_, tss)| tss.iter().any(|ts| *ts <= read_ts))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.keys.lock().unwrap().len()
    }
}

/// Clears its key's in-flight record when dropped.
pub struct InFlightGuard {
    owner: Arc<InFlight>,
    key: Vec<u8>,
    commit_ts: u64,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut keys = self.owner.keys.lock().expect("in-flight poisoned");
        if let Some(tss) = keys.get_mut(&self.key) {
            if let Some(i) = tss.iter().position(|ts| *ts == self.commit_ts) {
                tss.remove(i);
            }
            if tss.is_empty() {
                keys.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_at_or_after_the_pending_commit_waits_for_it() {
        let f = InFlight::new();
        let _g = f.register(b"k".to_vec(), 100);
        assert!(f.blocks_read(b"k", b"l", 100), "a read exactly at the commit must see it");
        assert!(f.blocks_read(b"k", b"l", 101));
        assert!(!f.blocks_read(b"k", b"l", 99), "a read below it never included this write");
        assert!(!f.blocks_read(b"a", b"b", 200), "another key's write is irrelevant");
    }

    #[test]
    fn a_scan_sees_any_pending_write_inside_its_range() {
        let f = InFlight::new();
        let _g = f.register(b"m".to_vec(), 50);
        assert!(f.blocks_read(b"a", b"z", 60), "inside the range");
        assert!(f.blocks_read(b"a", b"", 60), "an empty end means the rest of the keyspace");
        assert!(!f.blocks_read(b"n", b"z", 60), "starts after it");
        assert!(!f.blocks_read(b"a", b"m", 60), "ends at it — the range is half-open");
    }

    #[test]
    fn the_record_clears_however_the_write_ends() {
        let f = InFlight::new();
        {
            let _g = f.register(b"k".to_vec(), 7);
            assert_eq!(f.len(), 1);
        }
        assert_eq!(f.len(), 0, "dropped on success, failure, or a step-down alike");
        assert!(!f.blocks_read(b"k", b"l", 9));
    }

    #[test]
    fn two_writes_on_one_key_clear_independently() {
        let f = InFlight::new();
        let g1 = f.register(b"k".to_vec(), 10);
        let _g2 = f.register(b"k".to_vec(), 20);
        drop(g1);
        assert!(!f.blocks_read(b"k", b"l", 15), "the first is gone");
        assert!(f.blocks_read(b"k", b"l", 25), "the second still stands");
    }
}

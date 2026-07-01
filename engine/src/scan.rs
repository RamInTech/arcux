//! Merging range iterator + MVCC range scan (Phase 1b).
//!
//! A [`MergeIter`] walks one column family in key order across **all** LSM levels — the active
//! memtable, the immutable memtables, and every SSTable — newest level winning on identical
//! keys. It's built directly on the engine's existing *merged seek*
//! ([`Engine::seek_cf_raw`](crate::db::Engine::seek_cf_raw)): each step seeks the smallest key
//! `≥ cursor`, then advances the cursor just past it. [`Engine::scan`] layers MVCC on top —
//! iterating the Write CF, it resolves each distinct user key's visible value (reusing
//! [`mvcc_get`](crate::db::Engine::mvcc_get)) to produce a snapshot range scan.

use std::collections::HashSet;

use crate::db::Engine;
use crate::error::{Error, Result};
use crate::keys::{decode_data_key, Cf};
use crate::memtable::MemValue;

/// A forward merging iterator over one CF, in key order across all levels (newest-wins).
pub struct MergeIter<'a> {
    engine: &'a Engine,
    cf: Cf,
    /// The next key to seek from; `None` once exhausted.
    cursor: Option<Vec<u8>>,
}

impl Iterator for MergeIter<'_> {
    type Item = Result<(Vec<u8>, MemValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        let from = self.cursor.take()?;
        match self.engine.seek_cf_raw(self.cf, &from) {
            Ok(Some((key, value))) => {
                // Advance past this key: the smallest key strictly greater is `key ‖ 0x00`.
                let mut next = key.clone();
                next.push(0);
                self.cursor = Some(next);
                Some(Ok((key, value)))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl Engine {
    /// A merging iterator over `cf`, yielding every entry `≥ from` in key order.
    pub fn iter_cf(&self, cf: Cf, from: &[u8]) -> MergeIter<'_> {
        MergeIter { engine: self, cf, cursor: Some(from.to_vec()) }
    }

    /// Snapshot range scan: the committed `(user_key, value)` for each user key in
    /// `[start, end)` visible at `read_ts`, in key order, up to `limit` (`0` = unlimited).
    ///
    /// Walks the Write CF with [`iter_cf`](Self::iter_cf), and for each **distinct** user key
    /// resolves its value with [`mvcc_get`](Self::mvcc_get) (or the non-mutating
    /// [`mvcc_get_unresolved`](Self::mvcc_get_unresolved) when `resolve` is false — replicated
    /// reads must not resolve locks). Deleted keys are skipped; a key still locked by an
    /// in-flight txn (`KeyIsLocked`) is skipped (not visible at this snapshot).
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        read_ts: u64,
        limit: usize,
        resolve: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        let mut seen: HashSet<Vec<u8>> = HashSet::new();

        for entry in self.iter_cf(Cf::Write, start) {
            let (wkey, _mv) = entry?;
            let Some((user_key, _commit_ts)) = decode_data_key(&wkey) else {
                continue; // not a data key (shouldn't happen in the Write CF)
            };
            // Half-open [start, end): stop once we've passed the range.
            if !end.is_empty() && user_key >= end {
                break;
            }
            if !seen.insert(user_key.to_vec()) {
                continue; // a version of this user key was already resolved
            }

            let resolved = if resolve {
                self.mvcc_get(user_key, read_ts)
            } else {
                self.mvcc_get_unresolved(user_key, read_ts)
            };
            match resolved {
                Ok(Some(value)) => {
                    out.push((user_key.to_vec(), value));
                    if limit != 0 && out.len() >= limit {
                        break;
                    }
                }
                Ok(None) => {} // no visible version (tombstone / all newer than read_ts)
                Err(Error::KeyIsLocked(_)) => {} // in-flight lock — invisible at this snapshot
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::db::Engine;
    use crate::options::Options;
    use crate::percolator::{Mutation, Transaction};

    fn open() -> (tempfile::TempDir, Engine) {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(Options::new(dir.path())).unwrap();
        (dir, e)
    }

    /// Commit `key=value` in its own transaction at `(start_ts, commit_ts)`.
    fn put(e: &Engine, key: &[u8], value: &[u8], start_ts: u64, commit_ts: u64) {
        let txn = Transaction::new(e, start_ts, vec![Mutation::put(key.to_vec(), value.to_vec())]).unwrap();
        txn.prewrite(commit_ts + 1_000_000).unwrap();
        txn.commit(commit_ts).unwrap();
    }

    fn del(e: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let txn = Transaction::new(e, start_ts, vec![Mutation::delete(key.to_vec())]).unwrap();
        txn.prewrite(commit_ts + 1_000_000).unwrap();
        txn.commit(commit_ts).unwrap();
    }

    fn keys(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
        pairs.iter().map(|(k, _)| k.clone()).collect()
    }

    #[test]
    fn scans_a_range_in_key_order() {
        let (_d, e) = open();
        for (i, k) in [b"a", b"b", b"c", b"d"].iter().enumerate() {
            put(&e, *k, b"v", 10 + i as u64, 20 + i as u64);
        }
        let all = e.scan(b"", b"", 1000, 0, true).unwrap();
        assert_eq!(keys(&all), vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
        // Half-open [b, d): b, c — d excluded.
        let mid = e.scan(b"b", b"d", 1000, 0, true).unwrap();
        assert_eq!(keys(&mid), vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn scan_sees_the_version_at_read_ts() {
        let (_d, e) = open();
        put(&e, b"k", b"v1", 10, 20);
        put(&e, b"k", b"v2", 30, 40);
        assert_eq!(e.scan(b"k", b"z", 25, 0, true).unwrap(), vec![(b"k".to_vec(), b"v1".to_vec())]);
        assert_eq!(e.scan(b"k", b"z", 45, 0, true).unwrap(), vec![(b"k".to_vec(), b"v2".to_vec())]);
        // Before any commit → empty.
        assert!(e.scan(b"k", b"z", 5, 0, true).unwrap().is_empty());
    }

    #[test]
    fn scan_skips_deleted_keys() {
        let (_d, e) = open();
        put(&e, b"a", b"1", 10, 20);
        put(&e, b"b", b"2", 10, 20);
        del(&e, b"a", 30, 40);
        let live = e.scan(b"", b"", 1000, 0, true).unwrap();
        assert_eq!(keys(&live), vec![b"b".to_vec()], "the deleted key drops out of the scan");
    }

    #[test]
    fn scan_respects_limit() {
        let (_d, e) = open();
        for k in [b"a", b"b", b"c", b"d"] {
            put(&e, k, b"v", 10, 20);
        }
        let two = e.scan(b"", b"", 1000, 2, true).unwrap();
        assert_eq!(keys(&two), vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn scan_merges_across_memtable_and_sstables() {
        // A tiny memtable threshold forces flushes, so the data spans multiple SSTables; the
        // merge must still return a single ordered stream (and the newest version per key).
        let dir = tempfile::tempdir().unwrap();
        let mut opts = Options::new(dir.path());
        opts.memtable_size_threshold = 1; // flush aggressively
        let e = Engine::open(opts).unwrap();

        put(&e, b"a", b"1", 10, 20);
        put(&e, b"c", b"3", 10, 20);
        put(&e, b"b", b"2", 10, 20);
        put(&e, b"b", b"2b", 30, 40); // a newer version of b, in a later level
        assert!(e.sstable_count() > 0, "the tiny threshold should have flushed SSTables");

        let all = e.scan(b"", b"", 1000, 0, true).unwrap();
        assert_eq!(
            all,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2b".to_vec()), // newest version wins across levels
                (b"c".to_vec(), b"3".to_vec()),
            ],
        );
    }
}

//! In-memory write buffer: per-CF concurrent skiplists.
//!
//! Per the project plan we use `crossbeam-skiplist` rather than hand-rolling the
//! lock-free skiplist (A1 targets the LSM/WAL machinery, not generic containers).
//! Each column family is its own ordered map; a `Memtable` owns all three and
//! shares a single WAL with its siblings via the engine's committer.

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

use crate::batch::{WriteBatch, WriteOp};
use crate::keys::Cf;

/// A stored memtable value. `Delete` is a tombstone that *shadows* older entries
/// in lower memtables / SSTables (it is not the same as simply being absent).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemValue {
    Put(Vec<u8>),
    Delete,
}

const MV_PUT: u8 = 0;
const MV_DELETE: u8 = 1;

impl MemValue {
    /// Collapse to an optional value, treating a tombstone as "absent".
    #[inline]
    pub fn into_present(self) -> Option<Vec<u8>> {
        match self {
            MemValue::Put(v) => Some(v),
            MemValue::Delete => None,
        }
    }

    /// Tagged encoding stored as the value bytes inside an SSTable entry.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            MemValue::Put(v) => {
                let mut out = Vec::with_capacity(1 + v.len());
                out.push(MV_PUT);
                out.extend_from_slice(v);
                out
            }
            MemValue::Delete => vec![MV_DELETE],
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<MemValue> {
        match bytes.split_first() {
            Some((&MV_PUT, rest)) => Some(MemValue::Put(rest.to_vec())),
            Some((&MV_DELETE, _)) => Some(MemValue::Delete),
            _ => None,
        }
    }
}

pub struct Memtable {
    cfs: [SkipMap<Vec<u8>, MemValue>; 3],
    size: AtomicUsize,
    max_seq: AtomicU64,
}

impl Memtable {
    pub fn new() -> Memtable {
        Memtable {
            cfs: std::array::from_fn(|_| SkipMap::new()),
            size: AtomicUsize::new(0),
            max_seq: AtomicU64::new(0),
        }
    }

    #[inline]
    fn cf(&self, cf: Cf) -> &SkipMap<Vec<u8>, MemValue> {
        &self.cfs[cf as usize]
    }

    /// Apply an atomic batch at log sequence `seq`. Safe to call concurrently
    /// (skiplist inserts take `&self`), though the engine drives it single-threaded.
    pub fn apply(&self, seq: u64, batch: &WriteBatch) {
        let mut added = 0usize;
        for op in &batch.ops {
            let map = self.cf(op.cf());
            match op {
                WriteOp::Put { key, value, .. } => {
                    added += key.len() + value.len() + 24;
                    map.insert(key.clone(), MemValue::Put(value.clone()));
                }
                WriteOp::Delete { key, .. } => {
                    added += key.len() + 24;
                    map.insert(key.clone(), MemValue::Delete);
                }
            }
        }
        self.size.fetch_add(added, Ordering::Relaxed);
        self.max_seq.fetch_max(seq, Ordering::Relaxed);
    }

    /// Exact point lookup. `Some(Delete)` = tombstone present (authoritative,
    /// shadows lower levels); `None` = not in this memtable at all.
    pub fn get_cf(&self, cf: Cf, key: &[u8]) -> Option<MemValue> {
        self.cf(cf).get(key).map(|e| e.value().clone())
    }

    /// Forward seek: the smallest key ≥ `from` in `cf`, returned by value. Because
    /// data keys embed a *descending* timestamp, seeking to `encode_data_key(user,
    /// read_ts)` lands on the newest version at-or-after `read_ts` — the MVCC
    /// "latest visible" primitive.
    pub fn seek_cf(&self, cf: Cf, from: &[u8]) -> Option<(Vec<u8>, MemValue)> {
        self.cf(cf)
            .lower_bound(Bound::Included(from))
            .map(|e| (e.key().clone(), e.value().clone()))
    }

    /// Iterate every entry of `cf` in key order (used when flushing to an SSTable).
    pub fn iter_cf(&self, cf: Cf) -> impl Iterator<Item = (Vec<u8>, MemValue)> + '_ {
        self.cf(cf).iter().map(|e| (e.key().clone(), e.value().clone()))
    }

    pub fn approx_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    pub fn max_seq(&self) -> u64 {
        self.max_seq.load(Ordering::Relaxed)
    }
    pub fn is_empty(&self) -> bool {
        self.size.load(Ordering::Relaxed) == 0
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_get_seek() {
        let m = Memtable::new();
        let mut b = WriteBatch::new();
        b.put(Cf::Default, b"a".to_vec(), b"1".to_vec());
        b.put(Cf::Default, b"c".to_vec(), b"3".to_vec());
        b.delete(Cf::Lock, b"x".to_vec());
        m.apply(5, &b);

        assert_eq!(m.get_cf(Cf::Default, b"a"), Some(MemValue::Put(b"1".to_vec())));
        assert_eq!(m.get_cf(Cf::Lock, b"x"), Some(MemValue::Delete));
        assert_eq!(m.get_cf(Cf::Default, b"missing"), None);
        assert_eq!(m.max_seq(), 5);

        // seek("b") -> first key >= "b" is "c"
        assert_eq!(m.seek_cf(Cf::Default, b"b"), Some((b"c".to_vec(), MemValue::Put(b"3".to_vec()))));
        // seek past the end -> None
        assert_eq!(m.seek_cf(Cf::Default, b"z"), None);
    }

    #[test]
    fn later_write_overwrites() {
        let m = Memtable::new();
        let mut b = WriteBatch::new();
        b.put(Cf::Default, b"k".to_vec(), b"old".to_vec());
        m.apply(1, &b);
        let mut b2 = WriteBatch::new();
        b2.put(Cf::Default, b"k".to_vec(), b"new".to_vec());
        m.apply(2, &b2);
        assert_eq!(m.get_cf(Cf::Default, b"k"), Some(MemValue::Put(b"new".to_vec())));
    }
}

//! The cluster's Timestamp Oracle.
//!
//! A single authoritative source of strictly-increasing `u64` timestamps. Every
//! `start_ts` and `commit_ts` in the system is drawn from here, so timestamps are
//! globally ordered and `commit_ts > start_ts` always holds — the property Percolator
//! snapshot isolation depends on. This replaces the Phase-1/2 per-node stand-in
//! ([`arcux_engine::Tso`]); in Phase 3 the data node pulls its timestamps from *this*
//! oracle over `pd.GetTimestamp`.
//!
//! ## Restart safety
//!
//! A TSO that handed out timestamp `T` and then crashed must never hand out `≤ T`
//! again, or two different transactions could share a timestamp. We get this without
//! fsyncing on every allocation by reserving timestamps in **windows**: before handing
//! out any timestamp `t`, the oracle has durably persisted an `upper` watermark `> t`.
//! On restart it reloads `upper` and resumes from there — discarding at most one
//! window of never-used timestamps, but never reusing one. (This is exactly how
//! TiDB's PD allocates time.)

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::persist::{atomic_write, read_optional};

const TSO_FILE: &str = "tso";
/// How many timestamps to reserve (and persist) at a time. Larger ⇒ fewer fsyncs on
/// the allocation path, at the cost of more timestamps potentially skipped on restart.
const WINDOW: u64 = 1 << 16;

struct Inner {
    /// Next timestamp to hand out.
    next: u64,
    /// Highest timestamp durably reserved on disk. Invariant: `next <= upper`, and a
    /// timestamp is only ever returned once `next < upper` is guaranteed.
    upper: u64,
}

/// The authoritative timestamp oracle. Cheap to share behind an `Arc`.
pub struct Tso {
    inner: Mutex<Inner>,
    /// Where the `upper` watermark is persisted; `None` ⇒ ephemeral (tests).
    path: Option<PathBuf>,
}

impl Tso {
    /// An in-memory oracle with no restart safety — for tests and the in-process
    /// single-node path where durability across a PD restart is not required.
    pub fn ephemeral() -> Tso {
        Tso { inner: Mutex::new(Inner { next: 1, upper: 0 }), path: None }
    }

    /// Open (creating if absent) a restart-safe oracle whose watermark lives under
    /// `dir`. On reopen it resumes strictly above every previously issued timestamp.
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Tso> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join(TSO_FILE);
        let (next, upper) = match read_optional(&path)? {
            // Reload: resume *at* the reserved watermark. The `[old_next, upper)` tail
            // may already have been handed out before the crash, so skip it entirely;
            // `next == upper` forces the first allocation to reserve a fresh window.
            Some(bytes) if bytes.len() >= 8 => {
                let upper = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                (upper, upper)
            }
            // Fresh cluster: start at 1 (0 is reserved as "before everything").
            _ => (1, 0),
        };
        Ok(Tso { inner: Mutex::new(Inner { next, upper }), path: Some(path) })
    }

    /// Allocate `count` (≥ 1) contiguous timestamps, returning the first. The range
    /// `[first, first + count)` is reserved for the caller.
    pub fn alloc(&self, count: u64) -> std::io::Result<u64> {
        let count = count.max(1);
        let mut g = self.inner.lock().expect("tso poisoned");
        if g.next + count > g.upper {
            // Reserve a fresh window covering at least this request, and persist it
            // *before* handing any of it out.
            let new_upper = g.next + count + WINDOW;
            if let Some(path) = &self.path {
                atomic_write(path, &new_upper.to_be_bytes())?;
            }
            g.upper = new_upper;
        }
        let first = g.next;
        g.next += count;
        Ok(first)
    }

    /// Allocate a single timestamp.
    pub fn now(&self) -> std::io::Result<u64> {
        self.alloc(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strictly_monotonic() {
        let tso = Tso::ephemeral();
        let a = tso.now().unwrap();
        let b = tso.now().unwrap();
        let c = tso.alloc(5).unwrap();
        assert!(a < b && b < c);
        // alloc(5) reserved a contiguous range above c.
        assert!(tso.now().unwrap() >= c + 5);
    }

    #[test]
    fn never_regresses_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let issued = {
            let tso = Tso::open(dir.path()).unwrap();
            let mut last = 0;
            for _ in 0..10 {
                last = tso.now().unwrap();
                assert!(last > 0);
            }
            last
        };
        // Reopen: every newly issued timestamp must exceed everything issued before
        // the "crash", even though we never cleanly flushed `next`.
        let tso = Tso::open(dir.path()).unwrap();
        let after = tso.now().unwrap();
        assert!(after > issued, "TSO regressed: {after} <= {issued}");
    }

    #[test]
    fn batch_window_is_contiguous() {
        let tso = Tso::ephemeral();
        let first = tso.alloc(100).unwrap();
        let next = tso.now().unwrap();
        assert_eq!(next, first + 100, "batch must reserve a contiguous range");
    }
}
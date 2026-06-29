//! The cluster's Timestamp Oracle — an HLC-style hybrid logical clock.
//!
//! A single authoritative source of strictly-increasing `u64` timestamps. Every
//! `start_ts` and `commit_ts` in the system is drawn from here, so timestamps are
//! globally ordered and `commit_ts > start_ts` always holds — the property Percolator
//! snapshot isolation depends on. This replaces the Phase-1/2 per-node stand-in
//! ([`arcux_engine::Tso`]); the data node pulls its timestamps from *this* oracle over
//! `pd.GetTimestamp`.
//!
//! ## Hybrid logical clock (Phase 3b)
//!
//! A timestamp packs a **physical** wall-clock component (milliseconds) in its high bits
//! and a **logical** counter in its low [`LOGICAL_BITS`] bits:
//!
//! ```text
//!   63                     LOGICAL_BITS                 0
//!   [ physical milliseconds ][      logical counter      ]
//! ```
//!
//! Before each allocation the oracle bumps `next` up to `now_ms() << LOGICAL_BITS`, so
//! timestamps **track wall-clock time** while the logical bits absorb many allocations
//! within the same millisecond. The `max(next, …)` guard means a wall clock that jumps
//! *backwards* never makes a timestamp regress — physical time can only pull `next`
//! forward, never back. (The pure logical counter of Phase 3 is the degenerate case
//! where the physical bits never advance.)
//!
//! ## Restart safety
//!
//! A TSO that handed out timestamp `T` and then crashed must never hand out `≤ T`
//! again, or two different transactions could share a timestamp. We get this without
//! fsyncing on every allocation by reserving timestamps in **windows**: before handing
//! out any timestamp `t`, the oracle has durably persisted an `upper` watermark `> t`.
//! On restart it reloads `upper` and resumes from there — discarding at most one
//! window of never-used timestamps, but never reusing one. (This is exactly how
//! TiDB's PD allocates time.) The physical bump composes with this: a restart after a
//! long downtime simply jumps `next` forward to the current wall clock.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::persist::{atomic_write, read_optional};

const TSO_FILE: &str = "tso";
/// How many timestamps to reserve (and persist) at a time. Larger ⇒ fewer fsyncs on
/// the allocation path, at the cost of more timestamps potentially skipped on restart.
const WINDOW: u64 = 1 << 16;

/// Width of the logical (low) component of a hybrid timestamp. The remaining high bits
/// hold physical milliseconds, leaving room for `2^18 ≈ 262k` allocations per ms and
/// physical timestamps out to the year ~10889 — comfortably more than enough.
pub const LOGICAL_BITS: u32 = 18;

/// Split a hybrid timestamp into its `(physical_ms, logical)` components.
#[inline]
pub fn split(ts: u64) -> (u64, u64) {
    (ts >> LOGICAL_BITS, ts & ((1 << LOGICAL_BITS) - 1))
}

/// Current wall-clock time in milliseconds since the Unix epoch (0 if the clock is
/// before the epoch, which only happens on a badly misconfigured host).
fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

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
    /// Wall-clock source, in ms. Injectable so tests can drive the physical component
    /// deterministically; production uses [`now_ms`].
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl Tso {
    /// An in-memory oracle with no restart safety — for tests and the in-process
    /// single-node path where durability across a PD restart is not required.
    pub fn ephemeral() -> Tso {
        Tso { inner: Mutex::new(Inner { next: 1, upper: 0 }), path: None, clock: Box::new(now_ms) }
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
        Ok(Tso {
            inner: Mutex::new(Inner { next, upper }),
            path: Some(path),
            clock: Box::new(now_ms),
        })
    }

    /// An ephemeral oracle driven by an explicit clock closure (tests). The closure
    /// returns the current physical time in milliseconds.
    pub fn with_clock(clock: impl Fn() -> u64 + Send + Sync + 'static) -> Tso {
        Tso { inner: Mutex::new(Inner { next: 1, upper: 0 }), path: None, clock: Box::new(clock) }
    }

    /// Allocate `count` (≥ 1) contiguous timestamps, returning the first. The range
    /// `[first, first + count)` is reserved for the caller.
    pub fn alloc(&self, count: u64) -> std::io::Result<u64> {
        let count = count.max(1);
        let mut g = self.inner.lock().expect("tso poisoned");

        // Hybrid step: pull `next` forward to wall-clock time if the physical clock has
        // advanced. A backwards clock can never regress `next` (the `max`), preserving
        // strict monotonicity.
        let physical_floor = (self.clock)() << LOGICAL_BITS;
        if physical_floor > g.next {
            g.next = physical_floor;
        }

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
    use std::sync::atomic::{AtomicU64, Ordering};

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
    fn batch_reserves_contiguous_block() {
        // With a frozen physical clock the logical bits alone advance, so a batch hands
        // back an exactly contiguous block (no millisecond tick to bump the floor).
        let tso = Tso::with_clock(|| 0);
        let first = tso.alloc(100).unwrap();
        let next = tso.now().unwrap();
        assert_eq!(next, first + 100, "a batch must reserve a contiguous range");
    }

    #[test]
    fn physical_component_tracks_wall_clock() {
        // Drive the clock forward by one ms between allocations: the physical high bits
        // must advance, and the timestamp jumps to the new millisecond's floor.
        let ms = AtomicU64::new(1000);
        let tso = Tso::with_clock(move || ms.fetch_add(1, Ordering::SeqCst));
        let t1 = tso.now().unwrap();
        let t2 = tso.now().unwrap();
        let (p1, _) = split(t1);
        let (p2, _) = split(t2);
        assert!(p2 > p1, "physical component must advance with wall-clock: {p1} -> {p2}");
        assert!(t2 > t1, "and the timestamp stays strictly monotonic");
    }

    #[test]
    fn backwards_clock_never_regresses() {
        // A wall clock that jumps backwards must not produce a smaller timestamp.
        let seq = AtomicU64::new(0);
        let tso = Tso::with_clock(move || {
            // returns 5000, then 10 (a backwards jump), then 5001…
            match seq.fetch_add(1, Ordering::SeqCst) {
                0 => 5000,
                1 => 10,
                n => 5000 + n,
            }
        });
        let a = tso.now().unwrap();
        let b = tso.now().unwrap(); // clock went backwards here
        let c = tso.now().unwrap();
        assert!(a < b && b < c, "monotonic despite a backwards clock: {a} {b} {c}");
    }
}

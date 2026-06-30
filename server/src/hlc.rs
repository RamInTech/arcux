//! A per-node **hybrid logical clock** (HLC) — the timestamp source for the AP write path.
//!
//! The CP path orders writes with the cluster TSO (one central oracle). The AP path is
//! **leaderless** — any replica accepts a write — so there is no central clock to ask. Each
//! node instead keeps its own HLC: a `u64` packing **physical milliseconds** in the high bits
//! and a **logical counter** in the low [`LOGICAL_BITS`] (the same layout as the Phase-3b
//! TSO). Two rules keep timestamps globally comparable across nodes without coordination:
//!
//! - [`now`](Hlc::now) returns a strictly-increasing stamp that tracks wall-clock time;
//! - [`observe`](Hlc::observe) pulls this clock **forward** to any timestamp seen on an
//!   incoming write, so a node that receives a remote write never then issues a smaller stamp.
//!
//! That `max`-on-receive is what makes Last-Writer-Wins well-defined cluster-wide: the write
//! with the highest HLC wins, and HLCs can't drift backwards relative to writes they've seen.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Width of the logical (low) component — matches the TSO so timestamps read the same way.
pub const LOGICAL_BITS: u32 = 18;

/// Split a hybrid timestamp into `(physical_ms, logical)`.
#[inline]
pub fn split(ts: u64) -> (u64, u64) {
    (ts >> LOGICAL_BITS, ts & ((1 << LOGICAL_BITS) - 1))
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// A node-local hybrid logical clock. Cheap to share behind an `Arc`.
pub struct Hlc {
    /// The last timestamp issued/observed; the next `now` is strictly greater.
    last: Mutex<u64>,
}

impl Hlc {
    pub fn new() -> Hlc {
        Hlc { last: Mutex::new(0) }
    }

    /// Issue the next timestamp: strictly greater than every prior `now`/`observe`, and at
    /// least the current wall-clock floor (so stamps track real time).
    pub fn now(&self) -> u64 {
        let mut last = self.last.lock().expect("hlc poisoned");
        let physical_floor = now_ms() << LOGICAL_BITS;
        let next = (*last + 1).max(physical_floor);
        *last = next;
        next
    }

    /// Pull the clock forward to a timestamp seen on an incoming write, so a later `now`
    /// exceeds it. Keeps Last-Writer-Wins consistent across nodes.
    pub fn observe(&self, remote: u64) {
        let mut last = self.last.lock().expect("hlc poisoned");
        *last = (*last).max(remote);
    }
}

impl Default for Hlc {
    fn default() -> Self {
        Hlc::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_strictly_monotonic() {
        let hlc = Hlc::new();
        let mut prev = 0;
        for _ in 0..1000 {
            let t = hlc.now();
            assert!(t > prev, "HLC must strictly increase: {t} <= {prev}");
            prev = t;
        }
    }

    #[test]
    fn observe_pulls_the_clock_forward() {
        let hlc = Hlc::new();
        let a = hlc.now();
        // A remote write far in the "future" (a node whose wall clock is ahead).
        let remote = a + 1_000_000;
        hlc.observe(remote);
        // Our next stamp must exceed what we observed — so LWW stays well-defined.
        assert!(hlc.now() > remote, "now() must exceed an observed remote timestamp");
    }

    #[test]
    fn physical_component_tracks_wall_clock() {
        let hlc = Hlc::new();
        let (phys, _) = split(hlc.now());
        // The physical part is a recent epoch-ms (sanity: well past year 2020).
        assert!(phys > 1_600_000_000_000, "physical component should be epoch-ms");
    }
}

//! A minimal monotonic timestamp source — a stand-in for the Placement Driver's
//! Timestamp Oracle (TSO) that arrives in Phase 3/4.
//!
//! Every call to [`Tso::now`] returns a strictly larger value, so timestamps drawn
//! for `start_ts` and (later) `commit_ts` are globally ordered and `commit_ts >
//! start_ts` always holds — exactly the property Percolator's snapshot isolation
//! depends on.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct Tso {
    next: AtomicU64,
}

impl Tso {
    /// Start issuing timestamps from 1 (0 is reserved as "before everything").
    pub fn new() -> Tso {
        Tso { next: AtomicU64::new(1) }
    }

    /// Allocate the next strictly-monotonic timestamp.
    pub fn now(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst)
    }
}

impl Default for Tso {
    fn default() -> Self {
        Tso::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strictly_monotonic() {
        let tso = Tso::new();
        let a = tso.now();
        let b = tso.now();
        let c = tso.now();
        assert!(a < b && b < c);
    }
}

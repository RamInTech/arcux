//! Up-replication — growing a region's Raft group toward the voter set PD wants.
//!
//! A region is founded by the nodes that were registered when it was created; on a fresh
//! cluster that is one node. As more nodes register, PD lists them in the region's `desired`
//! set, and the region's **leader** adds them one at a time by membership change:
//!
//! ```text
//!   joining node                 region leader
//!   ────────────                 ─────────────
//!   hosts a blank replica   →    AddLearner(n)   (no quorum weight: writes keep flowing)
//!   catches up (append /         wait until peer_caught_up(n)
//!     snapshot)             →    AddNode(n)      (promote: instant, n is already warm)
//! ```
//!
//! Learner first for the same reason `repair.rs` does it: adding a cold node straight as a voter
//! grows the quorum before the node can vote, and stalls every write until it catches up.
//!
//! [`next_step`] is pure and reads the group's **current** membership each time, so the driver
//! calling it needs no state of its own: an interrupted sequence — a lost leader, a change still
//! in flight — just resumes from wherever the membership says it got to.

use arcux_raft::ConfChange;

/// The next membership change that moves `voters` toward `desired`, or `None` if there is
/// nothing to do right now (already there, or a learner is still catching up).
///
/// One change at a time, lowest node id first — the Raft core refuses a second change while
/// one is uncommitted anyway, so planning more would only be discarded. Never removes a voter:
/// shrinking a group is not this path's job.
pub fn next_step(
    voters: &[u64],
    learners: &[u64],
    desired: &[u64],
    caught_up: impl Fn(u64) -> bool,
) -> Option<ConfChange> {
    let mut missing: Vec<u64> = desired.iter().copied().filter(|id| !voters.contains(id)).collect();
    missing.sort_unstable();
    let next = *missing.first()?;
    if !learners.contains(&next) {
        Some(ConfChange::AddLearner(next))
    } else if caught_up(next) {
        Some(ConfChange::AddNode(next))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_node_is_added_as_a_learner_first() {
        assert_eq!(next_step(&[1], &[], &[1, 2], |_| false), Some(ConfChange::AddLearner(2)));
    }

    #[test]
    fn a_learner_is_promoted_only_once_caught_up() {
        assert_eq!(next_step(&[1], &[2], &[1, 2], |_| false), None, "still catching up: wait");
        assert_eq!(next_step(&[1], &[2], &[1, 2], |id| id == 2), Some(ConfChange::AddNode(2)));
    }

    #[test]
    fn nothing_to_do_when_every_desired_node_is_a_voter() {
        assert_eq!(next_step(&[1, 2, 3], &[], &[1, 2, 3], |_| true), None);
        // A voter PD no longer lists is left alone: removal is not this path's job.
        assert_eq!(next_step(&[1, 2, 3], &[], &[1, 2], |_| true), None);
    }

    #[test]
    fn the_lowest_missing_node_goes_first() {
        assert_eq!(next_step(&[1], &[], &[1, 3, 2], |_| true), Some(ConfChange::AddLearner(2)));
    }
}

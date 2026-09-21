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

/// How a region reads at founding when PD wants more voters than it has — or `None` once it has
/// them all. The first node on a fresh cluster founds every region **alone** and elects itself,
/// which is intended (the group's configuration really is `{1}`, so a majority is 1 of 1), but a
/// single-voter group holds the only copy of every write until the others join. That is worth
/// saying out loud: read as `LEADER` with no other context, founding alone looks like a quorum
/// rule being skipped.
///
/// `target` is PD's replication target (`--replicas`), which is the number that matters on a
/// fresh cluster: `desired` there is just this node, so it cannot distinguish a complete one-node
/// cluster from the first node of three. A `target` of 0 means PD did not say (the single-process
/// PD, or a peer older than wire v18), and then `desired` is all there is to go on.
///
/// A target below the voters a region already has — `--replicas` lowered after the fact — is not
/// under-replication, so it is `None` rather than a negative count.
pub fn under_replicated_note(voters: &[u64], desired: &[u64], target: usize) -> Option<String> {
    let want = target.max(desired.len());
    if voters.len() >= want {
        return None;
    }
    let mut missing: Vec<u64> = desired.iter().copied().filter(|id| !voters.contains(id)).collect();
    missing.sort_unstable();
    let waiting = match missing.as_slice() {
        // Nobody else has registered yet, so PD cannot name who will join — only how many.
        [] => format!("waiting for {} more node(s) to register", want - voters.len()),
        ids => format!(
            "waiting for node{} {} to register",
            if ids.len() == 1 { "" } else { "s" },
            ids.iter().map(u64::to_string).collect::<Vec<_>>().join(", ")
        ),
    };
    Some(format!("{} of {want} voters — no redundancy yet; {waiting}", voters.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The case that prompted this: the very first node of a fresh 3-replica cluster. PD lists
    /// only this node as desired — nobody else has registered — so the target is the only thing
    /// that says the region is one of three, not one of one.
    #[test]
    fn founding_alone_on_a_fresh_cluster_counts_against_the_target() {
        let note = under_replicated_note(&[1], &[1], 3).expect("under-replicated");
        assert_eq!(note, "1 of 3 voters — no redundancy yet; waiting for 2 more node(s) to register");
    }

    #[test]
    fn known_nodes_are_named() {
        let note = under_replicated_note(&[1], &[1, 2, 3], 3).expect("under-replicated");
        assert_eq!(note, "1 of 3 voters — no redundancy yet; waiting for nodes 2, 3 to register");
    }

    #[test]
    fn a_partly_grown_region_names_only_what_is_still_missing() {
        let note = under_replicated_note(&[1, 2], &[1, 2, 3], 3).expect("under-replicated");
        assert_eq!(note, "2 of 3 voters — no redundancy yet; waiting for node 3 to register");
    }

    #[test]
    fn a_full_region_says_nothing() {
        assert_eq!(under_replicated_note(&[1, 2, 3], &[1, 2, 3], 3), None);
        // `--replicas 1`: a one-voter region is exactly what was asked for.
        assert_eq!(under_replicated_note(&[1], &[1], 1), None);
        // A target lowered below what a region already has is not under-replication.
        assert_eq!(under_replicated_note(&[1, 2, 3], &[1, 2, 3], 2), None);
    }

    /// An older PD, or the single-process one, sends no target. `desired` is then the only
    /// evidence — never a guess that every cluster wants three.
    #[test]
    fn no_target_falls_back_to_the_desired_set() {
        assert_eq!(under_replicated_note(&[1], &[1], 0), None, "no evidence of a bigger cluster");
        let note = under_replicated_note(&[1], &[1, 2], 0).expect("under-replicated");
        assert_eq!(note, "1 of 2 voters — no redundancy yet; waiting for node 2 to register");
    }

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

//! A deterministic, in-process Raft cluster — the Phase-4 test harness.
//!
//! There is no real clock, network or thread: a [`Cluster`] owns N [`RaftNode`]s
//! and a connectivity map. `tick()` advances every node one logical tick and then
//! `pump()`s the resulting messages to quiescence, dropping any that cross a
//! partition boundary. Because delivery is total and deterministic, the safety
//! invariants can be asserted directly after every step.
//!
//! The invariants checked here are the ones the Raft paper proves:
//! - **Election Safety** — at most one leader per term.
//! - **Log Matching** — logs never diverge below a committed index.
//! - **State Machine Safety** — no two nodes apply different entries at the same
//!   index (the [`Checker`] enforces this continuously).

use std::collections::BTreeMap;

use arcux_raft::{ConfChange, Config, Entry, MemStorage, RaftNode, Role};

/// Tracks every entry each node has *applied* (taken via `take_committed`) and
/// asserts they never disagree at any index — State Machine Safety.
#[derive(Default)]
struct Checker {
    applied: BTreeMap<u64, Vec<Entry>>,
    // index -> the (term, data) first observed as applied there
    committed: BTreeMap<u64, (u64, Vec<u8>)>,
    // highest index each node has caught up to, whether by applying an entry or
    // installing a snapshot — the anchor for the per-node contiguity check.
    applied_hi: BTreeMap<u64, u64>,
}

impl Checker {
    fn record(&mut self, id: u64, entries: Vec<Entry>) {
        let log = self.applied.entry(id).or_default();
        for e in entries {
            if let Some((term, data)) = self.committed.get(&e.index) {
                assert!(
                    *term == e.term && *data == e.data,
                    "STATE MACHINE SAFETY violated at index {}: node {id} applied \
                     (term {}, {:?}) but another node applied (term {}, {:?})",
                    e.index,
                    e.term,
                    e.data,
                    term,
                    data,
                );
            } else {
                self.committed.insert(e.index, (e.term, e.data.clone()));
            }
            // applied indices must be contiguous and monotonic per node. After a
            // snapshot the anchor is the snapshot index, not the last Vec entry.
            let hi = self.applied_hi.entry(id).or_default();
            assert_eq!(
                e.index,
                *hi + 1,
                "node {id} applied index {} out of order (expected {})",
                e.index,
                *hi + 1
            );
            *hi = e.index;
            log.push(e);
        }
    }

    /// A snapshot installed on `id` fast-forwards its state to `index` without
    /// replaying the individual entries below it. The snapshot came from a leader
    /// that had those entries committed, so per-index safety is already covered by
    /// `record`; here we only advance the contiguity anchor so the entries the node
    /// applies *after* the snapshot (starting at `index + 1`) line up.
    fn record_snapshot(&mut self, id: u64, index: u64) {
        let hi = self.applied_hi.entry(id).or_default();
        assert!(
            index >= *hi,
            "node {id} installed a snapshot at {index} below its applied point {}",
            *hi
        );
        *hi = index;
    }
}

struct Cluster {
    nodes: BTreeMap<u64, RaftNode<MemStorage>>,
    // partition id per node; two nodes can exchange messages iff equal.
    part: BTreeMap<u64, u32>,
    checker: Checker,
}

impl Cluster {
    fn new(ids: &[u64]) -> Self {
        let mut nodes = BTreeMap::new();
        let mut part = BTreeMap::new();
        for &id in ids {
            nodes.insert(id, RaftNode::new(Config::new(id, ids.to_vec()), MemStorage::new()));
            part.insert(id, 0);
        }
        Cluster {
            nodes,
            part,
            checker: Checker::default(),
        }
    }

    fn reachable(&self, a: u64, b: u64) -> bool {
        self.part[&a] == self.part[&b]
    }

    fn collect_applied(&mut self) {
        for (id, node) in self.nodes.iter_mut() {
            // Drain an installed snapshot before committed entries — it moves the
            // applied point forward and the entries that follow build on top of it.
            if let Some((idx, _data)) = node.take_snapshot() {
                self.checker.record_snapshot(*id, idx);
            }
            let committed = node.take_committed();
            if !committed.is_empty() {
                self.checker.record(*id, committed);
            }
        }
    }

    /// Drive a node's log compaction: snapshot its applied state through `index`
    /// and discard the log at or below it (the driver's job in production).
    fn compact(&mut self, id: u64, index: u64) {
        self.nodes
            .get_mut(&id)
            .unwrap()
            .compact(index, format!("snap@{index}").into_bytes());
    }

    /// Bring up a fresh node with an **empty** bootstrap config — a learner that never
    /// campaigns until a committed `AddNode` makes it a voter (mirrors starting a new replica
    /// that discovers its membership from the leader).
    fn add_learner(&mut self, id: u64) {
        self.nodes.insert(id, RaftNode::new(Config::new(id, vec![]), MemStorage::new()));
        self.part.insert(id, 0);
    }

    /// Propose a single-server membership change on `leader` and settle the messages.
    fn propose_conf_change(&mut self, leader: u64, cc: ConfChange) {
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose_conf_change(cc)
            .expect("argument must be the leader with no change in flight");
        self.pump();
    }

    /// Simulate decommissioning a removed node: drop it from the harness entirely (a removed
    /// server is shut down, so it can no longer send or receive).
    fn decommission(&mut self, id: u64) {
        self.nodes.remove(&id);
        self.part.remove(&id);
    }

    fn voters(&self, id: u64) -> Vec<u64> {
        self.nodes[&id].voters().to_vec()
    }

    /// Deliver all in-flight messages until the cluster goes quiet.
    fn pump(&mut self) {
        for _ in 0..200_000 {
            let mut msgs = Vec::new();
            for node in self.nodes.values_mut() {
                msgs.extend(node.take_messages());
            }
            if msgs.is_empty() {
                break;
            }
            for m in msgs {
                if self.reachable(m.from, m.to) {
                    if let Some(dst) = self.nodes.get_mut(&m.to) {
                        dst.step(m);
                    }
                }
                // else: dropped at the partition boundary
            }
            self.collect_applied();
        }
        self.collect_applied();
        self.assert_election_safety();
    }

    fn tick(&mut self) {
        for node in self.nodes.values_mut() {
            node.tick();
        }
        self.pump();
    }

    fn tick_n(&mut self, n: usize) {
        for _ in 0..n {
            self.tick();
        }
    }

    /// The highest-term leader, if any.
    fn leader(&self) -> Option<u64> {
        let mut best: Option<(u64, u64)> = None;
        for (id, n) in &self.nodes {
            if n.role() == Role::Leader {
                let t = n.current_term();
                if best.is_none_or(|(bt, _)| t > bt) {
                    best = Some((t, *id));
                }
            }
        }
        best.map(|(_, id)| id)
    }

    fn run_until_leader(&mut self, max_ticks: usize) -> u64 {
        for _ in 0..max_ticks {
            self.tick();
            if let Some(l) = self.leader() {
                return l;
            }
        }
        panic!("no leader elected within {max_ticks} ticks");
    }

    fn propose(&mut self, leader: u64, data: &[u8]) -> u64 {
        let idx = self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .propose(data.to_vec())
            .expect("argument must be the leader");
        self.pump();
        idx
    }

    /// Propose to whichever node currently believes itself leader (may be a
    /// minority leader whose entry never commits — that's fine, safety must hold
    /// regardless). No-op if there is no leader.
    fn try_propose(&mut self, data: &[u8]) {
        if let Some(l) = self.leader() {
            let _ = self.nodes.get_mut(&l).unwrap().propose(data.to_vec());
            self.pump();
        }
    }

    fn isolate(&mut self, id: u64) {
        *self.part.get_mut(&id).unwrap() = 10_000 + id as u32;
    }

    fn set_partition(&mut self, groups: &[&[u64]]) {
        for (g, members) in groups.iter().enumerate() {
            for &m in *members {
                *self.part.get_mut(&m).unwrap() = g as u32;
            }
        }
    }

    fn heal(&mut self) {
        for v in self.part.values_mut() {
            *v = 0;
        }
    }

    fn assert_election_safety(&self) {
        let mut leaders_by_term: BTreeMap<u64, u64> = BTreeMap::new();
        for (id, n) in &self.nodes {
            if n.role() == Role::Leader {
                if let Some(prev) = leaders_by_term.insert(n.current_term(), *id) {
                    panic!(
                        "ELECTION SAFETY violated: nodes {prev} and {id} both leader in term {}",
                        n.current_term()
                    );
                }
            }
        }
    }

    /// After healing + settling, every node's log must be byte-for-byte identical.
    fn assert_logs_converged(&self) {
        let mut iter = self.nodes.values();
        let first = iter.next().unwrap().log_entries();
        for n in self.nodes.values() {
            assert_eq!(
                n.log_entries(),
                first,
                "logs did not converge: node {} differs",
                n.id()
            );
        }
    }

    fn commit_index(&self, id: u64) -> u64 {
        self.nodes[&id].commit_index()
    }
    fn last_index(&self, id: u64) -> u64 {
        self.nodes[&id].last_log_index()
    }
}

// ---------------------------------------------------------------------------

#[test]
fn single_node_elects_and_commits() {
    let mut c = Cluster::new(&[1]);
    let leader = c.run_until_leader(50);
    assert_eq!(leader, 1);

    let i1 = c.propose(1, b"x");
    let i2 = c.propose(1, b"y");
    assert_eq!((i1, i2), (1, 2));
    assert_eq!(c.commit_index(1), 2);
    assert_eq!(
        c.checker.applied[&1],
        vec![
            Entry::normal(1, 1, b"x".to_vec()),
            Entry::normal(1, 2, b"y".to_vec()),
        ]
    );
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.run_until_leader(60);
    let leaders: Vec<u64> = c
        .nodes
        .values()
        .filter(|n| n.role() == Role::Leader)
        .map(|n| n.id())
        .collect();
    assert_eq!(leaders, vec![leader], "exactly one leader");
    // followers agree on who the leader is
    for n in c.nodes.values() {
        if !n.is_leader() {
            assert_eq!(n.leader_id(), Some(leader));
        }
    }
}

#[test]
fn leader_replicates_and_commits_on_all() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.run_until_leader(60);
    let term = c.nodes[&leader].current_term();

    c.propose(leader, b"a");
    c.propose(leader, b"b");
    c.propose(leader, b"c");
    // let the advanced commit index ride out on the next heartbeats
    c.tick_n(3);

    let expected = vec![
        Entry::normal(term, 1, b"a".to_vec()),
        Entry::normal(term, 2, b"b".to_vec()),
        Entry::normal(term, 3, b"c".to_vec()),
    ];
    for id in [1, 2, 3] {
        assert_eq!(c.commit_index(id), 3, "node {id} commit index");
        assert_eq!(c.checker.applied[&id], expected, "node {id} applied log");
    }
}

#[test]
fn isolated_follower_catches_up_after_heal() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.run_until_leader(60);
    let follower = [1, 2, 3].into_iter().find(|x| *x != leader).unwrap();

    // Cut the follower off; the remaining two still form a majority and commit.
    c.isolate(follower);
    c.propose(leader, b"a");
    c.propose(leader, b"b");
    c.propose(leader, b"c");
    c.tick_n(3);
    assert_eq!(c.last_index(follower), 0, "isolated follower saw nothing");
    assert_eq!(c.commit_index(leader), 3, "majority still commits");

    // Reconnect: the leader rewinds next_index and back-fills the follower.
    c.heal();
    c.tick_n(8);
    assert_eq!(c.last_index(follower), 3);
    assert_eq!(c.commit_index(follower), 3);
    c.assert_logs_converged();
}

#[test]
fn far_behind_follower_catches_up_via_snapshot() {
    // A follower kept offline long enough that the leader *compacts away* the
    // entries it still needs must be caught up with an `InstallSnapshot`, not a
    // log replay — the whole point of log compaction.
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.run_until_leader(60);
    let follower = [1, 2, 3].into_iter().find(|x| *x != leader).unwrap();
    let in_sync = [1, 2, 3].into_iter().find(|x| *x != leader && *x != follower).unwrap();

    // Cut the follower off; the leader + the other node still commit a burst.
    c.isolate(follower);
    for i in 0..6 {
        c.propose(leader, format!("v{i}").as_bytes());
    }
    c.tick_n(3);
    assert_eq!(c.commit_index(leader), 6, "majority commits the burst");
    assert_eq!(c.last_index(follower), 0, "isolated follower saw nothing");

    // Both in-sync nodes compact their whole log up to the commit point, dropping
    // exactly the entries the offline follower is missing.
    c.compact(leader, 6);
    c.compact(in_sync, 6);
    assert!(
        c.nodes[&leader].log_entries().is_empty(),
        "leader log compacted away"
    );

    // Reconnect. The leader can no longer build an AppendEntries for the follower
    // (next_index is below first_index), so it ships a snapshot instead.
    c.heal();
    c.tick_n(8);

    assert_eq!(c.commit_index(follower), 6, "follower caught up to the commit point");
    assert_eq!(c.last_index(follower), 6);
    assert!(
        c.nodes[&follower].log_entries().is_empty(),
        "follower installed a snapshot rather than replaying 6 entries"
    );

    // Replication resumes cleanly on top of the snapshot boundary.
    c.propose(leader, b"after-snap");
    c.tick_n(3);
    for id in [1, 2, 3] {
        assert_eq!(c.commit_index(id), 7, "node {id} commits the post-snapshot entry");
    }
    c.assert_logs_converged();
    assert_eq!(
        c.checker.committed[&7],
        (c.nodes[&leader].current_term(), b"after-snap".to_vec()),
        "the post-snapshot entry is agreed everywhere",
    );
}

#[test]
fn minority_leader_cannot_commit_majority_elects_and_proceeds() {
    let mut c = Cluster::new(&[1, 2, 3, 4, 5]);
    let old_leader = c.run_until_leader(80);
    c.propose(old_leader, b"committed-before");
    c.tick_n(3);
    for id in 1..=5 {
        assert_eq!(c.commit_index(id), 1, "node {id} sees the pre-partition commit");
    }

    // Partition: old leader + one follower (minority) | the other three (majority).
    let others: Vec<u64> = (1..=5).filter(|x| *x != old_leader).collect();
    let minority = [old_leader, others[0]];
    let majority = [others[1], others[2], others[3]];
    c.set_partition(&[&minority, &majority]);

    // Old leader proposes into the minority: it replicates to its one reachable
    // follower but can never reach a 3-of-5 majority, so commit stays put.
    let _ = c.nodes.get_mut(&old_leader).unwrap().propose(b"stuck".to_vec());
    c.pump();
    c.tick_n(5);
    assert_eq!(c.commit_index(old_leader), 1, "minority leader is stuck at the old commit");

    // The majority side elects a fresh leader (higher term) and makes progress.
    let mut new_leader = None;
    for _ in 0..120 {
        c.tick();
        for &m in &majority {
            if c.nodes[&m].is_leader() {
                new_leader = Some(m);
            }
        }
        if new_leader.is_some() {
            break;
        }
    }
    let new_leader = new_leader.expect("majority must elect a leader");
    assert!(c.nodes[&new_leader].current_term() > c.nodes[&old_leader].current_term());

    let committed = c.propose(new_leader, b"after-partition");
    c.tick_n(3);
    for &m in &majority {
        assert_eq!(c.commit_index(m), committed, "majority commits the new entry");
    }

    // Heal: the old leader hears the higher term, steps down, and its
    // uncommitted "stuck" entry is overwritten by the majority's log.
    c.heal();
    c.tick_n(12);
    assert_eq!(c.nodes[&old_leader].role(), Role::Follower);
    c.assert_logs_converged();
    // The conflicting uncommitted write never became committed anywhere.
    assert!(
        c.checker
            .committed
            .values()
            .all(|(_, d)| d.as_slice() != b"stuck"),
        "an uncommitted minority write must never commit"
    );
}

#[test]
fn restart_preserves_term_vote_and_log() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.run_until_leader(60);
    c.propose(leader, b"a");
    let term = c.nodes[&leader].current_term();

    // Snapshot the leader's durable state and rebuild a node from it — a reboot.
    let persisted = c.nodes[&leader].storage().clone();
    let restarted = RaftNode::new(Config::new(leader, vec![1, 2, 3]), persisted);

    assert_eq!(restarted.current_term(), term, "term survives restart");
    assert_eq!(restarted.last_log_index(), 1, "log survives restart");
    assert_eq!(restarted.voted_for(), Some(leader), "vote survives restart");
    assert_eq!(restarted.role(), Role::Follower, "role is volatile, comes back as follower");
    assert_eq!(restarted.commit_index(), 0, "commit index is volatile, rebuilt from leader");
}

#[test]
fn randomized_partitions_preserve_safety() {
    // Continuous fault injection: random partitions, heals and proposes. The
    // Checker asserts State Machine Safety after every single step; at the end we
    // heal and require full log convergence.
    for seed in 0..24u64 {
        let mut c = Cluster::new(&[1, 2, 3, 4, 5]);
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        c.run_until_leader(100);
        let mut counter = 0u64;
        for _ in 0..160 {
            match next() % 6 {
                0 => {
                    // random two-way partition
                    let mut g0 = Vec::new();
                    let mut g1 = Vec::new();
                    for id in 1..=5u64 {
                        if next() % 2 == 0 {
                            g0.push(id);
                        } else {
                            g1.push(id);
                        }
                    }
                    c.set_partition(&[&g0, &g1]);
                }
                1 => c.heal(),
                _ => {}
            }
            if next() % 2 == 0 {
                counter += 1;
                c.try_propose(format!("v{counter}").as_bytes());
            }
            c.tick();
            // (Checker ran inside every pump; safety already asserted.)
        }

        // Heal, then settle on a single leader. Convergence of *uncommitted*
        // tails requires the new leader to actually produce entries of its own
        // term (a stale uncommitted tail is only overwritten on a conflicting
        // append — never truncated speculatively), so drive a few proposals.
        c.heal();
        c.tick_n(40);
        let leader = c.leader().expect("seed: a leader should exist after healing");
        for _ in 0..6 {
            counter += 1;
            c.propose(leader, format!("v{counter}").as_bytes());
        }
        c.tick_n(20);
        c.assert_logs_converged();
    }
}

#[test]
fn membership_add_then_remove_a_voter() {
    // A live 3-node group grows to 4 and shrinks back to 3 via single-server changes, staying
    // safe and available across both — the Phase 4b++ (rest) membership mechanism.
    let mut c = Cluster::new(&[1, 2, 3]);
    let leader = c.run_until_leader(60);
    c.propose(leader, b"a");
    c.propose(leader, b"b");
    c.tick_n(3); // let the commit index ride out to the followers
    for id in [1, 2, 3] {
        assert_eq!(c.commit_index(id), 2);
    }

    // --- ADD a 4th voter -------------------------------------------------
    c.add_learner(4);
    assert!(c.voters(4).is_empty(), "a learner starts with no membership of its own");
    c.propose_conf_change(leader, ConfChange::AddNode(4));
    c.tick_n(10); // node 4 catches up; the AddNode entry commits and propagates

    for id in [1, 2, 3, 4] {
        assert_eq!(c.voters(id), vec![1, 2, 3, 4], "node {id} adopted the 4-member config");
    }
    assert_eq!(c.last_index(4), c.last_index(leader), "the new voter caught up to the log");
    let add_commit = c.commit_index(leader);
    for id in [1, 2, 3, 4] {
        assert_eq!(c.commit_index(id), add_commit, "node {id} committed the AddNode");
    }
    c.assert_logs_converged();

    // The group now runs at RF=4: kill one follower and the remaining three still commit
    // (majority is 3 of 4), proving the newcomer counts toward quorum.
    let downed = [1, 2, 3, 4].into_iter().find(|x| *x != leader).unwrap();
    c.isolate(downed);
    let c_idx = c.propose(leader, b"c");
    c.tick_n(4);
    for id in [1, 2, 3, 4].into_iter().filter(|x| *x != downed) {
        assert_eq!(c.commit_index(id), c_idx, "3 of 4 commit with one replica down");
    }
    c.heal();
    c.tick_n(6);

    // --- REMOVE a voter --------------------------------------------------
    let victim = [1, 2, 3, 4].into_iter().find(|x| *x != leader).unwrap();
    c.propose_conf_change(leader, ConfChange::RemoveNode(victim));
    c.decommission(victim); // a removed server is shut down — it can no longer disrupt
    c.tick_n(8);

    let survivors: Vec<u64> = [1, 2, 3, 4].into_iter().filter(|x| *x != victim).collect();
    for &id in &survivors {
        assert_eq!(c.voters(id), survivors, "node {id} dropped the removed member");
    }
    let d_idx = c.propose(leader, b"d");
    c.tick_n(4);
    for &id in &survivors {
        assert_eq!(c.commit_index(id), d_idx, "the shrunk 3-member group still commits");
    }
    c.assert_logs_converged();
}

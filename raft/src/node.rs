//! The Raft state machine — a faithful implementation of Figure 2 of the
//! Raft paper (Ongaro & Ousterhout), built as a pure, I/O-free core.
//!
//! Driving it is three calls:
//! - [`RaftNode::tick`] advances the logical clock (election + heartbeat timers);
//! - [`RaftNode::step`] feeds in one inbound [`Message`];
//! - [`RaftNode::propose`] (leader only) appends a client command.
//!
//! Each may produce outbound messages and newly-committed entries, drained with
//! [`RaftNode::take_messages`] and [`RaftNode::take_committed`]. There is no
//! clock, no socket and no thread inside — time is whatever the caller ticks and
//! delivery is whatever the caller routes, which is exactly what makes the
//! deterministic cluster test possible.
//!
//! Persistence ordering follows the safety proof: term/vote and log entries are
//! written through [`Storage`] *before* the corresponding reply is emitted.

use std::collections::{BTreeMap, BTreeSet};

use crate::message::{Entry, HardState, Message, MessageBody};
use crate::storage::Storage;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Static configuration for one Raft peer.
#[derive(Clone, Debug)]
pub struct Config {
    /// This node's id.
    pub id: u64,
    /// All voting members of the group, *including* `id`.
    pub voters: Vec<u64>,
    /// Base election timeout, in ticks. The effective timeout is randomized in
    /// `[election_timeout, 2 * election_timeout)` to break up split votes.
    pub election_timeout: u32,
    /// Heartbeat period, in ticks. Must be `< election_timeout` so a live leader
    /// keeps followers from timing out.
    pub heartbeat_timeout: u32,
    /// Seed for the per-node election-timeout randomization (kept explicit so
    /// tests are fully deterministic).
    pub seed: u64,
}

impl Config {
    /// A config with sensible defaults (election 10 ticks, heartbeat 1 tick) and
    /// a per-id seed so distinct nodes randomize to distinct timeouts.
    pub fn new(id: u64, voters: Vec<u64>) -> Self {
        Self {
            id,
            voters,
            election_timeout: 10,
            heartbeat_timeout: 1,
            seed: id.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProposeError {
    /// Only the leader may append commands; the caller should redirect to
    /// [`RaftNode::leader_id`].
    NotLeader,
}

pub struct RaftNode<S: Storage> {
    id: u64,
    voters: Vec<u64>,
    storage: S,
    role: Role,

    // Persistent state (mirrored from `storage`'s HardState; written through on
    // every change before the node acts on it).
    current_term: u64,
    voted_for: Option<u64>,

    // Volatile state, all nodes.
    commit_index: u64,
    last_applied: u64,
    leader_id: Option<u64>,

    // Volatile state, candidate.
    votes: BTreeSet<u64>,

    // Volatile state, leader.
    next_index: BTreeMap<u64, u64>,
    match_index: BTreeMap<u64, u64>,

    // Timers (in ticks).
    election_elapsed: u32,
    heartbeat_elapsed: u32,
    randomized_election_timeout: u32,
    election_timeout: u32,
    heartbeat_timeout: u32,
    rng: u64,

    // Outbox, drained by `take_messages`.
    messages: Vec<Message>,
}

impl<S: Storage> RaftNode<S> {
    /// Construct a node, recovering persistent state from `storage`. A restarted
    /// node always comes back as a `Follower` (role is volatile) but with its
    /// term, vote and log intact — so it never double-votes within a term.
    pub fn new(cfg: Config, storage: S) -> Self {
        let hs = storage.hard_state();
        let mut node = Self {
            id: cfg.id,
            voters: cfg.voters,
            storage,
            role: Role::Follower,
            current_term: hs.current_term,
            voted_for: hs.voted_for,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_election_timeout: cfg.election_timeout,
            election_timeout: cfg.election_timeout.max(1),
            heartbeat_timeout: cfg.heartbeat_timeout.max(1),
            rng: cfg.seed | 1,
            messages: Vec::new(),
        };
        node.reset_election_timer();
        node
    }

    // ---- inspection (read-only accessors; some exist for tests) ----------

    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn current_term(&self) -> u64 {
        self.current_term
    }
    pub fn voted_for(&self) -> Option<u64> {
        self.voted_for
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }
    pub fn last_log_index(&self) -> u64 {
        self.storage.last_index()
    }
    pub fn leader_id(&self) -> Option<u64> {
        self.leader_id
    }
    pub fn storage(&self) -> &S {
        &self.storage
    }
    /// The full log, for assertions in tests.
    pub fn log_entries(&self) -> Vec<Entry> {
        self.storage.entries(1, self.storage.last_index())
    }

    // ---- driver inputs ---------------------------------------------------

    /// Advance the logical clock by one tick.
    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.heartbeat_timeout {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append();
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.randomized_election_timeout {
                    self.start_election();
                }
            }
        }
    }

    /// Append a client command (leader only). Returns the assigned log index.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }
        let index = self.storage.last_index() + 1;
        let entry = Entry {
            term: self.current_term,
            index,
            data,
        };
        self.storage.append(&[entry]);
        self.match_index.insert(self.id, index);
        self.next_index.insert(self.id, index + 1);
        // A single-node group commits immediately; otherwise this is a no-op
        // until followers ack.
        self.maybe_advance_commit();
        self.broadcast_append();
        Ok(index)
    }

    /// Process one inbound message.
    pub fn step(&mut self, msg: Message) {
        // Uniform term rule (Figure 2 §5.1): a message from a newer term means
        // we are stale — adopt the term and revert to follower before handling.
        if msg.term > self.current_term {
            let leader = match msg.body {
                MessageBody::AppendEntries { .. } => Some(msg.from),
                _ => None,
            };
            self.become_follower(msg.term, leader);
        }

        match msg.body {
            MessageBody::RequestVote {
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(msg.from, msg.term, last_log_index, last_log_term),
            MessageBody::RequestVoteResp { granted } => {
                self.handle_request_vote_resp(msg.from, msg.term, granted)
            }
            MessageBody::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                msg.from,
                msg.term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            MessageBody::AppendEntriesResp {
                success,
                match_index,
            } => self.handle_append_resp(msg.from, msg.term, success, match_index),
        }
    }

    // ---- driver outputs --------------------------------------------------

    /// Take the queued outbound messages.
    pub fn take_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.messages)
    }

    /// Take entries that have become committed since the last call, advancing
    /// `last_applied`. The caller must apply them to the state machine in order.
    pub fn take_committed(&mut self) -> Vec<Entry> {
        if self.commit_index <= self.last_applied {
            return Vec::new();
        }
        let entries = self.storage.entries(self.last_applied + 1, self.commit_index);
        self.last_applied = self.commit_index;
        entries
    }

    /// Whether there is outbound work or newly-committed state to drain.
    pub fn has_ready(&self) -> bool {
        !self.messages.is_empty() || self.commit_index > self.last_applied
    }

    // ---- role transitions ------------------------------------------------

    fn start_election(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.persist_hard_state();
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer();

        if self.votes.len() >= self.majority() {
            // Single-node group: elected unopposed.
            self.become_leader();
            return;
        }

        let term = self.current_term;
        let last_log_index = self.storage.last_index();
        let last_log_term = self.last_log_term();
        for p in self.peers() {
            self.send(
                p,
                term,
                MessageBody::RequestVote {
                    last_log_index,
                    last_log_term,
                },
            );
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let last = self.storage.last_index();
        self.next_index.clear();
        self.match_index.clear();
        for v in self.voters.clone() {
            self.next_index.insert(v, last + 1);
            self.match_index.insert(v, 0);
        }
        self.match_index.insert(self.id, last);
        self.heartbeat_elapsed = 0;
        // Assert leadership immediately so followers reset their election timers.
        self.broadcast_append();
    }

    fn become_follower(&mut self, term: u64, leader: Option<u64>) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.persist_hard_state();
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.reset_election_timer();
    }

    // ---- RPC handlers ----------------------------------------------------

    fn handle_request_vote(
        &mut self,
        from: u64,
        term: u64,
        cand_last_index: u64,
        cand_last_term: u64,
    ) {
        let mut granted = false;
        // A lower term is stale; otherwise consider the request (a higher term
        // was already adopted in `step`, clearing `voted_for`).
        if term >= self.current_term {
            let log_ok = cand_last_term > self.last_log_term()
                || (cand_last_term == self.last_log_term()
                    && cand_last_index >= self.storage.last_index());
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(from);
            if can_vote && log_ok {
                granted = true;
                self.voted_for = Some(from);
                self.persist_hard_state();
                // Granting a vote counts as hearing from a viable leader.
                self.reset_election_timer();
            }
        }
        let term = self.current_term;
        self.send(from, term, MessageBody::RequestVoteResp { granted });
    }

    fn handle_request_vote_resp(&mut self, from: u64, term: u64, granted: bool) {
        if self.role != Role::Candidate || term != self.current_term {
            return; // stale or no longer campaigning
        }
        if granted {
            self.votes.insert(from);
            if self.votes.len() >= self.majority() {
                self.become_leader();
            }
        }
    }

    fn handle_append_entries(
        &mut self,
        from: u64,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    ) {
        // Reject a leader from an older term.
        if term < self.current_term {
            let t = self.current_term;
            self.send(
                from,
                t,
                MessageBody::AppendEntriesResp {
                    success: false,
                    match_index: 0,
                },
            );
            return;
        }

        // Valid leader for our term: (re)acknowledge it and refresh the timer.
        self.role = Role::Follower;
        self.leader_id = Some(from);
        self.reset_election_timer();

        // Log Matching: our entry at prev_log_index must have prev_log_term.
        if self.storage.term(prev_log_index) != Some(prev_log_term) {
            let t = self.current_term;
            self.send(
                from,
                t,
                MessageBody::AppendEntriesResp {
                    success: false,
                    match_index: 0,
                },
            );
            return;
        }

        // Splice in the new entries, truncating at the first conflict.
        let mut append_from = None;
        for (k, e) in entries.iter().enumerate() {
            let idx = prev_log_index + 1 + k as u64;
            if idx <= self.storage.last_index() {
                if self.storage.term(idx) != Some(e.term) {
                    self.storage.truncate_suffix(idx);
                    append_from = Some(k);
                    break;
                }
                // identical entry already present — skip
            } else {
                append_from = Some(k);
                break;
            }
        }
        if let Some(k) = append_from {
            self.storage.append(&entries[k..]);
        }

        let last_new_index = prev_log_index + entries.len() as u64;
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(last_new_index);
        }

        let t = self.current_term;
        self.send(
            from,
            t,
            MessageBody::AppendEntriesResp {
                success: true,
                match_index: last_new_index,
            },
        );
    }

    fn handle_append_resp(&mut self, from: u64, term: u64, success: bool, match_index: u64) {
        if self.role != Role::Leader || term != self.current_term {
            return; // stale response
        }
        if success {
            let prev = *self.match_index.get(&from).unwrap_or(&0);
            // Reordered responses must never regress a peer's match point.
            let m = match_index.max(prev);
            self.match_index.insert(from, m);
            self.next_index.insert(from, m + 1);
            self.maybe_advance_commit();
        } else {
            // Log mismatch: back off `next_index` and retry immediately so the
            // follower converges in O(divergence) round trips.
            let next = self.next_index.get(&from).copied().unwrap_or(1);
            self.next_index.insert(from, next.saturating_sub(1).max(1));
            self.send_append(from);
        }
    }

    // ---- commit / replication helpers ------------------------------------

    /// Advance `commit_index` to the highest N such that a majority has
    /// `match_index >= N` **and** `log[N].term == current_term` (the Figure-8
    /// rule: a leader only commits entries from earlier terms indirectly, by
    /// committing one of its own).
    fn maybe_advance_commit(&mut self) {
        let majority = self.majority();
        let mut n = self.storage.last_index();
        while n > self.commit_index {
            if self.storage.term(n) == Some(self.current_term) {
                let count = self
                    .voters
                    .iter()
                    .filter(|v| *self.match_index.get(v).unwrap_or(&0) >= n)
                    .count();
                if count >= majority {
                    self.commit_index = n;
                    return;
                }
            }
            n -= 1;
        }
    }

    fn broadcast_append(&mut self) {
        for p in self.peers() {
            self.send_append(p);
        }
    }

    fn send_append(&mut self, to: u64) {
        let last = self.storage.last_index();
        let next = self.next_index.get(&to).copied().unwrap_or(last + 1);
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.storage.term(prev_log_index).unwrap_or(0);
        let entries = self.storage.entries(next, last);
        let term = self.current_term;
        let leader_commit = self.commit_index;
        self.send(
            to,
            term,
            MessageBody::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            },
        );
    }

    // ---- small utilities -------------------------------------------------

    fn send(&mut self, to: u64, term: u64, body: MessageBody) {
        self.messages.push(Message {
            from: self.id,
            to,
            term,
            body,
        });
    }

    fn persist_hard_state(&mut self) {
        self.storage.save_hard_state(HardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
        });
    }

    fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// All voters except this node (materialized to release the `self` borrow
    /// before the caller sends).
    fn peers(&self) -> Vec<u64> {
        self.voters.iter().copied().filter(|p| *p != self.id).collect()
    }

    fn last_log_term(&self) -> u64 {
        self.storage.term(self.storage.last_index()).unwrap_or(0)
    }

    fn next_rand(&mut self) -> u64 {
        // xorshift64 — deterministic, dependency-free.
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        let span = self.election_timeout;
        self.randomized_election_timeout = span + (self.next_rand() % span as u64) as u32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;

    fn node(id: u64, voters: &[u64]) -> RaftNode<MemStorage> {
        RaftNode::new(Config::new(id, voters.to_vec()), MemStorage::new())
    }

    #[test]
    fn single_node_self_elects_on_timeout() {
        let mut n = node(1, &[1]);
        for _ in 0..40 {
            n.tick();
            if n.is_leader() {
                break;
            }
        }
        assert!(n.is_leader());
        assert_eq!(n.current_term(), 1);
    }

    #[test]
    fn candidate_votes_for_itself_and_persists_term() {
        let mut n = node(1, &[1, 2, 3]);
        // Force an election by ticking past the timeout.
        for _ in 0..40 {
            n.tick();
            if n.role() == Role::Candidate {
                break;
            }
        }
        assert_eq!(n.role(), Role::Candidate);
        assert_eq!(n.voted_for(), Some(1));
        assert_eq!(n.storage().hard_state().current_term, n.current_term());
        assert_eq!(n.storage().hard_state().voted_for, Some(1));
    }

    #[test]
    fn rejects_vote_for_shorter_log() {
        // A leader builds a 2-entry log, then a candidate with an empty log asks
        // for a vote in a higher term: the up-to-date check must reject it.
        let mut leader = node(1, &[1, 2, 3]);
        // Drive to candidate, then hand it a majority by injecting one granted
        // vote (self + node 2 = 2 of 3).
        for _ in 0..40 {
            leader.tick();
            if leader.role() == Role::Candidate {
                break;
            }
        }
        let term = leader.current_term();
        let _ = leader.take_messages();
        leader.step(Message {
            from: 2,
            to: 1,
            term,
            body: MessageBody::RequestVoteResp { granted: true },
        });
        assert!(leader.is_leader());
        // drain election traffic
        let _ = leader.take_messages();
        leader.propose(b"a".to_vec()).unwrap();
        leader.propose(b"b".to_vec()).unwrap();
        let _ = leader.take_messages();

        leader.step(Message {
            from: 2,
            to: 1,
            term: leader.current_term() + 5,
            body: MessageBody::RequestVote {
                last_log_index: 0,
                last_log_term: 0,
            },
        });
        let out = leader.take_messages();
        let granted = matches!(
            out.last().map(|m| &m.body),
            Some(MessageBody::RequestVoteResp { granted: true })
        );
        assert!(!granted, "must not vote for a candidate with a shorter log");
        // It did adopt the higher term, though.
        assert_eq!(leader.role(), Role::Follower);
    }
}

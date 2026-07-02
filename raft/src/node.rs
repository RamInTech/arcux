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

use crate::message::{ConfChange, Entry, EntryType, HardState, Message, MessageBody};
use crate::storage::{Snapshot, Storage};

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
    /// A membership change is already in flight (uncommitted). Raft allows only **one**
    /// config change at a time; retry after the pending one commits.
    ConfChangeInProgress,
    /// A leader may not remove itself (that needs a leadership transfer first, deferred).
    /// Transfer leadership, then remove the old leader from the new one.
    CannotRemoveLeader,
}

pub struct RaftNode<S: Storage> {
    id: u64,
    /// The active voter set, **derived** from `bootstrap_conf` (or the snapshot's `conf_state`)
    /// folded with every [`EntryType::ConfChange`] entry in the live log — so a membership
    /// change takes effect the instant its entry is in the log (Raft dissertation §4.1), and
    /// truncating that entry reverts it. Recomputed on any log mutation via `recompute_conf`.
    voters: Vec<u64>,
    /// The initial membership (from [`Config`]) — the base for `voters` when the log holds no
    /// snapshot. Never changes.
    bootstrap_conf: Vec<u64>,
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

    // A snapshot just installed from the leader, awaiting `take_snapshot` so the caller can
    // load it into the state machine (drained like `take_committed`).
    pending_snapshot: Option<(u64, Vec<u8>)>,
}

impl<S: Storage> RaftNode<S> {
    /// Construct a node, recovering persistent state from `storage`. A restarted
    /// node always comes back as a `Follower` (role is volatile) but with its
    /// term, vote and log intact — so it never double-votes within a term.
    pub fn new(cfg: Config, storage: S) -> Self {
        let hs = storage.hard_state();
        let bootstrap_conf = cfg.voters.clone();
        let mut node = Self {
            id: cfg.id,
            voters: cfg.voters,
            bootstrap_conf,
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
            pending_snapshot: None,
        };
        node.reset_election_timer();
        // Recover the membership from durable state (a restart may have config changes in its
        // log or a snapshot with a `conf_state` that supersede the bootstrap config).
        node.recompute_conf();
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
    /// The lowest index the log still holds — `snapshot index + 1` after compaction, else `1`.
    /// The driver uses `last_applied - first_index` as its log-length compaction trigger.
    pub fn first_index(&self) -> u64 {
        self.storage.first_index()
    }
    pub fn leader_id(&self) -> Option<u64> {
        self.leader_id
    }
    /// The current voting members, derived from the log (Phase 4b++ rest). Used by the driver
    /// to learn a group's replica set after a membership change, and by tests.
    pub fn voters(&self) -> &[u64] {
        &self.voters
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
                // A node not in the current voter set (a removed member, or a newly-added one
                // still catching up) must never campaign — it would only disrupt the cluster.
                if self.election_elapsed >= self.randomized_election_timeout && self.is_voter() {
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
        let entry = Entry::normal(self.current_term, index, data);
        self.storage.append(&[entry]);
        self.match_index.insert(self.id, index);
        self.next_index.insert(self.id, index + 1);
        // A single-node group commits immediately; otherwise this is a no-op
        // until followers ack.
        self.maybe_advance_commit();
        self.broadcast_append();
        Ok(index)
    }

    /// Append a single-server membership change (leader only). Adding a voter starts
    /// replicating to it immediately; removing one shrinks the quorum. The change takes
    /// effect on **append** (so `voters` updates now), and commits under the new majority —
    /// which for an added node means committing waits until that node catches up (a brief
    /// availability dip; a non-voting learner catch-up phase is deferred). Returns the entry's
    /// log index.
    pub fn propose_conf_change(&mut self, cc: ConfChange) -> Result<u64, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }
        // Raft permits only one in-flight change; the previous must commit first.
        if self.has_pending_conf_change() {
            return Err(ProposeError::ConfChangeInProgress);
        }
        if matches!(cc, ConfChange::RemoveNode(id) if id == self.id) {
            return Err(ProposeError::CannotRemoveLeader);
        }

        // Record the *resulting* membership in the entry (not just the delta) so any replica
        // can adopt it directly.
        let mut new_conf = self.voters.clone();
        apply_conf_change(&mut new_conf, cc);
        new_conf.sort_unstable();
        new_conf.dedup();

        let index = self.storage.last_index() + 1;
        let entry = Entry {
            term: self.current_term,
            index,
            entry_type: EntryType::ConfChange,
            data: cc.encode(&new_conf),
        };
        self.storage.append(&[entry]);
        self.match_index.insert(self.id, index);
        self.next_index.insert(self.id, index + 1);
        // Membership is effective on append — refresh the voter set before we count acks.
        self.recompute_conf();
        // A freshly-added voter starts with an empty log: replicate from the beginning
        // (`send_append` sends a snapshot instead if the leader has already compacted).
        if let ConfChange::AddNode(id) = cc {
            self.next_index.insert(id, 1);
            self.match_index.insert(id, 0);
        }
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
                MessageBody::AppendEntries { .. } | MessageBody::InstallSnapshot { .. } => {
                    Some(msg.from)
                }
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
            MessageBody::InstallSnapshot {
                last_included_index,
                last_included_term,
                conf_state,
                data,
            } => self.handle_install_snapshot(
                msg.from,
                msg.term,
                last_included_index,
                last_included_term,
                conf_state,
                data,
            ),
            MessageBody::InstallSnapshotResp { match_index } => {
                self.handle_install_snapshot_resp(msg.from, msg.term, match_index)
            }
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

    /// Take a just-installed snapshot `(last_included_index, data)`, if one arrived from the
    /// leader since the last call. The caller **loads it into the state machine** (replacing
    /// state through that index) — drain this *before* [`take_committed`](Self::take_committed).
    pub fn take_snapshot(&mut self) -> Option<(u64, Vec<u8>)> {
        self.pending_snapshot.take()
    }

    /// Compact the log: record a snapshot of state through `index` (which the caller has
    /// already captured + applied) and discard entries at or below it. `index` must be
    /// `<= last_applied`. A no-op if already compacted past `index`.
    pub fn compact(&mut self, index: u64, data: Vec<u8>) {
        if index <= self.storage.snapshot().map(|s| s.last_included_index).unwrap_or(0)
            || index > self.last_applied
        {
            return;
        }
        let term = self.storage.term(index).expect("compact index must be in the log");
        let conf_state = self.conf_at(index);
        self.storage.compact(index, term, conf_state, data);
    }

    /// Whether there is outbound work or newly-committed / snapshot state to drain.
    pub fn has_ready(&self) -> bool {
        !self.messages.is_empty()
            || self.commit_index > self.last_applied
            || self.pending_snapshot.is_some()
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

        // Our snapshot boundary (0 if uncompacted). Entries at or below it are already
        // committed and immutable; an append that reaches into that region is a stale or
        // reordered message from before we snapshotted (the async transport can deliver one
        // after an `InstallSnapshot`). It must never truncate or rewrite snapshotted state.
        let base = self.storage.first_index() - 1;
        let last_new_index = prev_log_index + entries.len() as u64;

        // Fully covered by our snapshot ⇒ trivially satisfied; report our true match (`base`).
        if last_new_index <= base {
            let t = self.current_term;
            self.send(
                from,
                t,
                MessageBody::AppendEntriesResp { success: true, match_index: base },
            );
            return;
        }

        // Log Matching: our entry at prev_log_index must have prev_log_term. Check only when
        // prev_log_index lies within our live log (≥ base); below `base` the matching entry
        // is inside the snapshot and is assumed to match (it is committed).
        if prev_log_index >= base && self.storage.term(prev_log_index) != Some(prev_log_term) {
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

        // Splice in the new entries, truncating at the first conflict — but never touch an
        // entry at or below the snapshot boundary.
        let mut append_from = None;
        for (k, e) in entries.iter().enumerate() {
            let idx = prev_log_index + 1 + k as u64;
            if idx <= base {
                continue; // covered by the snapshot — immutable
            }
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
            // Appended entries may carry membership changes (and a truncation above may have
            // dropped one) — re-derive the voter set from the log.
            self.recompute_conf();
        }

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

    /// Follower: install a snapshot the leader sent because it had compacted the entries we
    /// still needed. Adopt it as our state (through `last_included_index`) and surface the
    /// data for the caller to load into the engine via [`take_snapshot`](Self::take_snapshot).
    fn handle_install_snapshot(
        &mut self,
        from: u64,
        term: u64,
        last_included_index: u64,
        last_included_term: u64,
        conf_state: Vec<u64>,
        data: Vec<u8>,
    ) {
        // Reject a leader from an older term.
        if term < self.current_term {
            let t = self.current_term;
            self.send(from, t, MessageBody::InstallSnapshotResp { match_index: 0 });
            return;
        }
        // Valid leader for our term: (re)acknowledge it and refresh the timer.
        self.role = Role::Follower;
        self.leader_id = Some(from);
        self.reset_election_timer();

        // Already at or past this snapshot → nothing to install; just report our match point.
        if last_included_index <= self.commit_index {
            let t = self.current_term;
            let m = self.commit_index;
            self.send(from, t, MessageBody::InstallSnapshotResp { match_index: m });
            return;
        }

        // Install: the snapshot supersedes our log and state through its index.
        self.storage.apply_snapshot(Snapshot {
            last_included_index,
            last_included_term,
            conf_state,
            data: data.clone(),
        });
        self.commit_index = last_included_index;
        self.last_applied = last_included_index;
        self.pending_snapshot = Some((last_included_index, data));
        // Adopt the membership the snapshot carried (the log below it, incl. any config
        // changes, is gone — `conf_state` captured their effect).
        self.recompute_conf();

        let t = self.current_term;
        self.send(
            from,
            t,
            MessageBody::InstallSnapshotResp { match_index: last_included_index },
        );
    }

    fn handle_install_snapshot_resp(&mut self, from: u64, term: u64, match_index: u64) {
        if self.role != Role::Leader || term != self.current_term {
            return; // stale response
        }
        let prev = *self.match_index.get(&from).unwrap_or(&0);
        let m = match_index.max(prev);
        self.match_index.insert(from, m);
        self.next_index.insert(from, m + 1);
        self.maybe_advance_commit();
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
        // If the follower needs entries the leader has already compacted away, catch it up
        // with a snapshot instead of an `AppendEntries` it can't anchor.
        if next < self.storage.first_index() {
            self.send_snapshot(to);
            return;
        }
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

    fn send_snapshot(&mut self, to: u64) {
        let Some(snap) = self.storage.snapshot() else {
            return; // nothing compacted yet — nothing to send
        };
        let term = self.current_term;
        self.send(
            to,
            term,
            MessageBody::InstallSnapshot {
                last_included_index: snap.last_included_index,
                last_included_term: snap.last_included_term,
                conf_state: snap.conf_state,
                data: snap.data,
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

    /// Whether this node is a current voting member of the group.
    fn is_voter(&self) -> bool {
        self.voters.contains(&self.id)
    }

    // ---- membership (single-server config changes, Phase 4b++ rest) -------

    /// Whether an uncommitted [`EntryType::ConfChange`] sits in the live log — Raft allows
    /// only one membership change in flight at a time.
    fn has_pending_conf_change(&self) -> bool {
        let lo = (self.commit_index + 1).max(self.storage.first_index());
        self.storage
            .entries(lo, self.storage.last_index())
            .iter()
            .any(|e| e.entry_type == EntryType::ConfChange)
    }

    /// The voter set as of `index`: the base config (snapshot `conf_state`, else bootstrap)
    /// folded with every config change in the log up to and including `index`.
    fn conf_at(&self, index: u64) -> Vec<u64> {
        let mut conf = self
            .storage
            .snapshot()
            .map(|s| s.conf_state)
            .unwrap_or_else(|| self.bootstrap_conf.clone());
        // Each ConfChange entry records the absolute resulting membership, so the config as of
        // `index` is simply the one from the highest-indexed change at or below it.
        for e in self.storage.entries(self.storage.first_index(), index) {
            if e.entry_type == EntryType::ConfChange {
                if let Some((_, new_conf)) = ConfChange::decode(&e.data) {
                    conf = new_conf;
                }
            }
        }
        conf.sort_unstable();
        conf.dedup();
        conf
    }

    /// Re-derive [`voters`](Self::voters) from durable state after any log mutation. A leader
    /// that finds itself removed from the group steps down.
    fn recompute_conf(&mut self) {
        self.voters = self.conf_at(self.storage.last_index());
        if self.role == Role::Leader && !self.is_voter() {
            self.become_follower(self.current_term, None);
        }
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

/// Fold one membership change into a voter set (add if absent, remove if present).
fn apply_conf_change(conf: &mut Vec<u64>, cc: ConfChange) {
    match cc {
        ConfChange::AddNode(id) => {
            if !conf.contains(&id) {
                conf.push(id);
            }
        }
        ConfChange::RemoveNode(id) => conf.retain(|v| *v != id),
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

    fn ent(term: u64, index: u64) -> Entry {
        Entry::normal(term, index, vec![index as u8])
    }

    #[test]
    fn stale_append_below_snapshot_is_ignored() {
        // Reproduces the integration bug: with an async, reordering transport a follower can
        // receive an `AppendEntries` referencing entries it has already snapshotted away. It
        // must neither panic nor rewrite the immutable snapshot region.
        let mut n = node(2, &[1, 2, 3]);
        // Install a snapshot up to index 64 (as if the leader compacted past us).
        n.step(Message {
            from: 1,
            to: 2,
            term: 1,
            body: MessageBody::InstallSnapshot {
                last_included_index: 64,
                last_included_term: 1,
                conf_state: vec![1, 2, 3],
                data: b"snap".to_vec(),
            },
        });
        assert_eq!(n.first_index(), 65);
        assert_eq!(n.last_log_index(), 64);
        assert_eq!(n.commit_index(), 64);
        let _ = n.take_snapshot(); // drain the installed snapshot
        let _ = n.take_messages();

        // A stale, fully-covered append (entries 1..=5) arrives late — trivially satisfied,
        // must not touch the log.
        n.step(Message {
            from: 1,
            to: 2,
            term: 1,
            body: MessageBody::AppendEntries {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: (1..=5).map(|i| ent(1, i)).collect(),
                leader_commit: 64,
            },
        });
        assert_eq!(n.first_index(), 65, "snapshot boundary preserved");
        assert_eq!(n.last_log_index(), 64, "no entries below the snapshot were spliced in");
        // It acks its true match point (the snapshot index).
        let ack = n.take_messages();
        assert!(matches!(
            ack.last().map(|m| &m.body),
            Some(MessageBody::AppendEntriesResp { success: true, match_index: 64 })
        ));

        // A stale append that *starts* below the snapshot but extends above it must splice in
        // only the portion above the boundary (indices 65..=70).
        n.step(Message {
            from: 1,
            to: 2,
            term: 1,
            body: MessageBody::AppendEntries {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: (1..=70).map(|i| ent(1, i)).collect(),
                leader_commit: 70,
            },
        });
        assert_eq!(n.last_log_index(), 70, "the above-snapshot tail was appended");
        assert_eq!(n.first_index(), 65, "snapshot boundary still intact");
        assert_eq!(n.storage().term(65), Some(1));
    }

    /// Drive a fresh node to leadership in a group where it holds a majority alone-ish; here we
    /// use a single-node group so it self-elects, which is enough to exercise the leader-only
    /// membership-change guards.
    fn solo_leader(id: u64) -> RaftNode<MemStorage> {
        let mut n = node(id, &[id]);
        for _ in 0..40 {
            n.tick();
            if n.is_leader() {
                break;
            }
        }
        assert!(n.is_leader());
        n
    }

    #[test]
    fn conf_change_is_effective_on_append_and_gated_one_at_a_time() {
        let mut n = solo_leader(1);
        assert_eq!(n.voters(), &[1]);

        // Adding a voter takes effect immediately (on append), before commit.
        let idx = n.propose_conf_change(ConfChange::AddNode(2)).unwrap();
        assert_eq!(n.voters(), &[1, 2]);
        assert!(idx >= 1);

        // A second change while the first is uncommitted is rejected (node 2 never acks in this
        // solo harness, so the AddNode stays pending).
        assert_eq!(
            n.propose_conf_change(ConfChange::AddNode(3)),
            Err(ProposeError::ConfChangeInProgress)
        );
    }

    #[test]
    fn leader_refuses_to_remove_itself() {
        let mut n = solo_leader(1);
        // A leader can't remove *itself* (that needs a leadership transfer first, deferred).
        assert_eq!(
            n.propose_conf_change(ConfChange::RemoveNode(1)),
            Err(ProposeError::CannotRemoveLeader)
        );
    }

    #[test]
    fn non_leader_conf_change_is_rejected() {
        let mut n = node(2, &[1, 2, 3]); // a follower
        assert_eq!(
            n.propose_conf_change(ConfChange::AddNode(4)),
            Err(ProposeError::NotLeader)
        );
    }

    #[test]
    fn config_recovers_from_the_log_on_restart() {
        // A leader adds a voter, then we rebuild a node from the same durable storage: the
        // recovered config must include the added voter (derived from the log), not the stale
        // bootstrap config.
        let storage = {
            let mut n = solo_leader(1);
            n.propose_conf_change(ConfChange::AddNode(2)).unwrap();
            assert_eq!(n.voters(), &[1, 2]);
            n.storage().clone()
        };
        // Restart with the *bootstrap* config [1] — the log's ConfChange must override it.
        let restarted = RaftNode::new(Config::new(1, vec![1]), storage);
        assert_eq!(restarted.voters(), &[1, 2], "membership recovered from the log");
    }
}

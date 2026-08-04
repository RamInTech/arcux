//! Wire-agnostic Raft message types.
//!
//! These mirror the frozen [`raft.proto`](../../rpc/proto/raft.proto) shapes —
//! `RequestVote` / `AppendEntries` and their responses — but carry no transport
//! dependency, so the core is a pure state machine that can be driven by a
//! deterministic in-process harness. The Phase-4 integration step maps these
//! 1:1 onto the generated protobuf structs (`raft::RequestVoteRequest`, etc.);
//! the field names here are deliberately the same.

/// What a log entry carries: an opaque state-machine command the core forwards to the
/// application ([`EntryType::Normal`]), or a Raft **membership change** the core itself
/// interprets to adjust the voter set ([`EntryType::ConfChange`], Phase 4b++ rest).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EntryType {
    #[default]
    Normal,
    ConfChange,
}

/// A single Raft log entry. `index` is 1-based and contiguous; `term` is the
/// leader's term when the entry was created; `data` is the opaque command the
/// state machine will apply (the region's engine mutation, post-integration) — or, for a
/// [`EntryType::ConfChange`] entry, an encoded [`ConfChange`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub entry_type: EntryType,
    pub data: Vec<u8>,
}

impl Entry {
    /// A normal (state-machine command) entry.
    pub fn normal(term: u64, index: u64, data: Vec<u8>) -> Self {
        Self { term, index, entry_type: EntryType::Normal, data }
    }
}

/// A single-server membership change (Raft dissertation §4.1): change **exactly one**
/// member at a time, so the old and new majorities always overlap and no joint consensus is
/// needed. Encoded into the `data` of an [`EntryType::ConfChange`] entry.
///
/// `AddLearner` adds a **non-voting** replica (Phase 4b++ rest, auto re-replication): it
/// receives the log (append/snapshot) like any follower but never votes, never campaigns, and
/// never counts toward the commit majority — so adding a cold replica can't stall commits.
/// Once caught up, `AddNode` on the learner **promotes** it to a voter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfChange {
    /// Add `id` as a voter — or, if it is currently a learner, promote it.
    AddNode(u64),
    /// Remove `id` from the group entirely (voter or learner).
    RemoveNode(u64),
    /// Add `id` as a non-voting learner (a no-op if it is already a voter).
    AddLearner(u64),
}

impl ConfChange {
    /// The affected node id.
    pub fn node_id(&self) -> u64 {
        match self {
            ConfChange::AddNode(id) | ConfChange::RemoveNode(id) | ConfChange::AddLearner(id) => {
                *id
            }
        }
    }

    /// Encode into a [`EntryType::ConfChange`] entry's payload: the operation **and the
    /// resulting membership** (voters + learners). Recording the absolute membership (not just
    /// the delta) lets any replica — including a freshly-added one that never saw the group's
    /// initial config — adopt the new membership directly, instead of folding deltas onto a
    /// base it may not share. Layout: `[op:u8][node_id:u64 BE][nv:u32 BE][voter:u64 BE * nv]
    /// [nl:u32 BE][learner:u64 BE * nl]` (op 1 = add, 2 = remove, 3 = add-learner); the
    /// learner section is absent in pre-learner entries and decodes as empty.
    pub fn encode(&self, voters: &[u64], learners: &[u64]) -> Vec<u8> {
        let (op, id) = match self {
            ConfChange::AddNode(id) => (1u8, *id),
            ConfChange::RemoveNode(id) => (2u8, *id),
            ConfChange::AddLearner(id) => (3u8, *id),
        };
        let mut b = Vec::with_capacity(17 + (voters.len() + learners.len()) * 8);
        b.push(op);
        b.extend_from_slice(&id.to_be_bytes());
        b.extend_from_slice(&(voters.len() as u32).to_be_bytes());
        for v in voters {
            b.extend_from_slice(&v.to_be_bytes());
        }
        b.extend_from_slice(&(learners.len() as u32).to_be_bytes());
        for l in learners {
            b.extend_from_slice(&l.to_be_bytes());
        }
        b
    }

    /// Inverse of [`encode`](Self::encode) → `(change, resulting voters, resulting learners)`;
    /// `None` on a malformed payload. An entry written before learners existed carries no
    /// learner section — decoded as an empty learner set.
    pub fn decode(bytes: &[u8]) -> Option<(ConfChange, Vec<u64>, Vec<u64>)> {
        if bytes.len() < 13 {
            return None;
        }
        let id = u64::from_be_bytes(bytes[1..9].try_into().ok()?);
        let cc = match bytes[0] {
            1 => ConfChange::AddNode(id),
            2 => ConfChange::RemoveNode(id),
            3 => ConfChange::AddLearner(id),
            _ => return None,
        };
        let mut p = 13;
        let nv = u32::from_be_bytes(bytes[9..13].try_into().ok()?) as usize;
        let mut voters = Vec::with_capacity(nv);
        for _ in 0..nv {
            if p + 8 > bytes.len() {
                return None;
            }
            voters.push(u64::from_be_bytes(bytes[p..p + 8].try_into().ok()?));
            p += 8;
        }
        // Learner section (absent in pre-learner entries → empty).
        let mut learners = Vec::new();
        if p + 4 <= bytes.len() {
            let nl = u32::from_be_bytes(bytes[p..p + 4].try_into().ok()?) as usize;
            p += 4;
            for _ in 0..nl {
                if p + 8 > bytes.len() {
                    return None;
                }
                learners.push(u64::from_be_bytes(bytes[p..p + 8].try_into().ok()?));
                p += 8;
            }
        }
        Some((cc, voters, learners))
    }
}

/// The persistent, crash-critical scalar state from Figure 2: the node's current
/// term and the candidate it voted for in that term. The log (the third piece of
/// persistent state) lives in [`Storage`](crate::storage::Storage).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
}

/// An addressed Raft message. `term` is the sender's term — every Figure-2 RPC
/// and response carries one, and the uniform term rule ("revert to follower on a
/// higher term, reject a lower one") keys off it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub from: u64,
    pub to: u64,
    pub term: u64,
    pub body: MessageBody,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageBody {
    /// Candidate → peers: solicit a vote (`raft.RequestVoteRequest`).
    RequestVote {
        last_log_index: u64,
        last_log_term: u64,
    },
    /// Peer → candidate: the vote decision (`raft.RequestVoteResponse`).
    RequestVoteResp { granted: bool },
    /// Leader → followers: replicate entries / heartbeat (`raft.AppendEntriesRequest`).
    AppendEntries {
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    /// Follower → leader: append result + the follower's new match point
    /// (`raft.AppendEntriesResponse`).
    AppendEntriesResp { success: bool, match_index: u64 },
    /// Leader → follower: install a snapshot of committed state through
    /// `last_included_index`, sent when the leader has already **compacted** the log the
    /// follower still needs (`raft.InstallSnapshotRequest`). `conf_state` carries the group's
    /// voters and `learners` its non-voting replicas as of that index, so a replica catching
    /// up by snapshot also learns the current membership.
    InstallSnapshot {
        last_included_index: u64,
        last_included_term: u64,
        conf_state: Vec<u64>,
        learners: Vec<u64>,
        data: Vec<u8>,
    },
    /// Follower → leader: the snapshot was installed up to `match_index`
    /// (`raft.InstallSnapshotResponse`).
    InstallSnapshotResp { match_index: u64 },
    /// Leader → transfer target: campaign **now** (leadership transfer, Raft dissertation
    /// §3.10). Sent once the target's log is fully caught up; the target starts an election
    /// immediately without waiting out its randomized timeout, and wins because its log is
    /// as up-to-date as the (still live) leader's (`raft.TimeoutNow`).
    TimeoutNow,
}

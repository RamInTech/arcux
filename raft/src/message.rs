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

/// A single-server membership change (Raft dissertation §4.1): add or remove **exactly one**
/// voter at a time, so the old and new majorities always overlap and no joint consensus is
/// needed. Encoded into the `data` of an [`EntryType::ConfChange`] entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfChange {
    AddNode(u64),
    RemoveNode(u64),
}

impl ConfChange {
    /// The affected node id.
    pub fn node_id(&self) -> u64 {
        match self {
            ConfChange::AddNode(id) | ConfChange::RemoveNode(id) => *id,
        }
    }

    /// Encode into a [`EntryType::ConfChange`] entry's payload: the operation **and the
    /// resulting voter set**. Recording the absolute membership (not just the delta) lets any
    /// replica — including a freshly-added one that never saw the group's initial config —
    /// adopt the new membership directly, instead of folding deltas onto a base it may not
    /// share. Layout: `[op:u8][node_id:u64 BE][n:u32 BE][voter:u64 BE * n]` (op 1 = add, 2 =
    /// remove).
    pub fn encode(&self, new_conf: &[u64]) -> Vec<u8> {
        let (op, id) = match self {
            ConfChange::AddNode(id) => (1u8, *id),
            ConfChange::RemoveNode(id) => (2u8, *id),
        };
        let mut b = Vec::with_capacity(13 + new_conf.len() * 8);
        b.push(op);
        b.extend_from_slice(&id.to_be_bytes());
        b.extend_from_slice(&(new_conf.len() as u32).to_be_bytes());
        for v in new_conf {
            b.extend_from_slice(&v.to_be_bytes());
        }
        b
    }

    /// Inverse of [`encode`](Self::encode) → `(change, resulting voter set)`; `None` on a
    /// malformed payload.
    pub fn decode(bytes: &[u8]) -> Option<(ConfChange, Vec<u64>)> {
        if bytes.len() < 13 {
            return None;
        }
        let id = u64::from_be_bytes(bytes[1..9].try_into().ok()?);
        let cc = match bytes[0] {
            1 => ConfChange::AddNode(id),
            2 => ConfChange::RemoveNode(id),
            _ => return None,
        };
        let n = u32::from_be_bytes(bytes[9..13].try_into().ok()?) as usize;
        let mut conf = Vec::with_capacity(n);
        let mut p = 13;
        for _ in 0..n {
            if p + 8 > bytes.len() {
                return None;
            }
            conf.push(u64::from_be_bytes(bytes[p..p + 8].try_into().ok()?));
            p += 8;
        }
        Some((cc, conf))
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
    /// follower still needs (`raft.InstallSnapshotRequest`). `conf_state` carries the group
    /// membership as of that index, so a replica catching up by snapshot also learns the
    /// current voter set.
    InstallSnapshot {
        last_included_index: u64,
        last_included_term: u64,
        conf_state: Vec<u64>,
        data: Vec<u8>,
    },
    /// Follower → leader: the snapshot was installed up to `match_index`
    /// (`raft.InstallSnapshotResponse`).
    InstallSnapshotResp { match_index: u64 },
}

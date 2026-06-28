//! Wire-agnostic Raft message types.
//!
//! These mirror the frozen [`raft.proto`](../../rpc/proto/raft.proto) shapes —
//! `RequestVote` / `AppendEntries` and their responses — but carry no transport
//! dependency, so the core is a pure state machine that can be driven by a
//! deterministic in-process harness. The Phase-4 integration step maps these
//! 1:1 onto the generated protobuf structs (`raft::RequestVoteRequest`, etc.);
//! the field names here are deliberately the same.

/// A single Raft log entry. `index` is 1-based and contiguous; `term` is the
/// leader's term when the entry was created; `data` is the opaque command the
/// state machine will apply (the region's engine mutation, post-integration).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
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
}

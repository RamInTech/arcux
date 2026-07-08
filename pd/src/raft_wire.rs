//! Wire ⇄ core conversions for PD's Raft transport.
//!
//! PD replicates its state over the same frozen [`raft.proto`](../../rpc/proto/raft.proto)
//! `RaftService` the data-node groups speak, so this maps the transport-free
//! [`Message`](arcux_raft::Message) the core emits to/from the generated protobuf structs — the
//! same shape as `server/src/raft_transport.rs`, kept here so `pd` needn't depend on `server`.
//! PD is a **single** Raft group, so every request is tagged with the fixed [`PD_GROUP_ID`].

use arcux_raft::{Entry, EntryType, Message, MessageBody};
use arcux_rpc::raft;

/// PD's one Raft group id. A data-node region group is keyed by its region id; PD has exactly
/// one group, so it uses a fixed id (its RPCs never collide — PD nodes serve only this group).
pub const PD_GROUP_ID: u64 = 0;

const ENTRY_NORMAL: u32 = 0;
const ENTRY_CONF_CHANGE: u32 = 1;

fn entry_to_proto(e: &Entry) -> raft::LogEntry {
    let entry_type = match e.entry_type {
        EntryType::Normal => ENTRY_NORMAL,
        EntryType::ConfChange => ENTRY_CONF_CHANGE,
    };
    raft::LogEntry { term: e.term, index: e.index, data: e.data.clone(), entry_type }
}

fn entry_from_proto(p: &raft::LogEntry) -> Entry {
    let entry_type =
        if p.entry_type == ENTRY_CONF_CHANGE { EntryType::ConfChange } else { EntryType::Normal };
    Entry { term: p.term, index: p.index, entry_type, data: p.data.clone() }
}

// ---- outbound Message → request (sender side) ----------------------------------------

pub fn vote_request(m: &Message) -> raft::RequestVoteRequest {
    match &m.body {
        MessageBody::RequestVote { last_log_index, last_log_term } => raft::RequestVoteRequest {
            term: m.term,
            candidate_id: m.from,
            last_log_index: *last_log_index,
            last_log_term: *last_log_term,
            group_id: PD_GROUP_ID,
        },
        _ => unreachable!("vote_request on a non-RequestVote message"),
    }
}

pub fn append_request(m: &Message) -> raft::AppendEntriesRequest {
    match &m.body {
        MessageBody::AppendEntries { prev_log_index, prev_log_term, entries, leader_commit } => {
            raft::AppendEntriesRequest {
                term: m.term,
                leader_id: m.from,
                prev_log_index: *prev_log_index,
                prev_log_term: *prev_log_term,
                entries: entries.iter().map(entry_to_proto).collect(),
                leader_commit: *leader_commit,
                group_id: PD_GROUP_ID,
            }
        }
        _ => unreachable!("append_request on a non-AppendEntries message"),
    }
}

pub fn install_snapshot_request(m: &Message) -> raft::InstallSnapshotRequest {
    match &m.body {
        MessageBody::InstallSnapshot { last_included_index, last_included_term, conf_state, data } => {
            raft::InstallSnapshotRequest {
                term: m.term,
                leader_id: m.from,
                last_included_index: *last_included_index,
                last_included_term: *last_included_term,
                data: data.clone(),
                group_id: PD_GROUP_ID,
                conf_state: conf_state.clone(),
            }
        }
        _ => unreachable!("install_snapshot_request on a non-InstallSnapshot message"),
    }
}

// ---- inbound request → Message (server side) -----------------------------------------

pub fn vote_request_to_msg(req: &raft::RequestVoteRequest, self_id: u64) -> Message {
    Message {
        from: req.candidate_id,
        to: self_id,
        term: req.term,
        body: MessageBody::RequestVote {
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        },
    }
}

pub fn append_request_to_msg(req: &raft::AppendEntriesRequest, self_id: u64) -> Message {
    Message {
        from: req.leader_id,
        to: self_id,
        term: req.term,
        body: MessageBody::AppendEntries {
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: req.entries.iter().map(entry_from_proto).collect(),
            leader_commit: req.leader_commit,
        },
    }
}

pub fn install_snapshot_request_to_msg(req: &raft::InstallSnapshotRequest, self_id: u64) -> Message {
    Message {
        from: req.leader_id,
        to: self_id,
        term: req.term,
        body: MessageBody::InstallSnapshot {
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            conf_state: req.conf_state.clone(),
            data: req.data.clone(),
        },
    }
}

// ---- reply Message → response (server side) ------------------------------------------

pub fn vote_response(reply: Option<&Message>) -> raft::RequestVoteResponse {
    match reply.map(|m| (&m.body, m.term)) {
        Some((MessageBody::RequestVoteResp { granted }, term)) => {
            raft::RequestVoteResponse { term, vote_granted: *granted }
        }
        _ => raft::RequestVoteResponse { term: 0, vote_granted: false },
    }
}

pub fn append_response(reply: Option<&Message>) -> raft::AppendEntriesResponse {
    match reply.map(|m| (&m.body, m.term)) {
        Some((MessageBody::AppendEntriesResp { success, match_index }, term)) => {
            raft::AppendEntriesResponse { term, success: *success, match_index: *match_index }
        }
        _ => raft::AppendEntriesResponse { term: 0, success: false, match_index: 0 },
    }
}

pub fn install_snapshot_response(reply: Option<&Message>) -> raft::InstallSnapshotResponse {
    raft::InstallSnapshotResponse { term: reply.map(|m| m.term).unwrap_or(0) }
}

// ---- response → Message (sender side, fed back in) -----------------------------------

pub fn vote_response_to_msg(resp: &raft::RequestVoteResponse, peer: u64, self_id: u64) -> Message {
    Message {
        from: peer,
        to: self_id,
        term: resp.term,
        body: MessageBody::RequestVoteResp { granted: resp.vote_granted },
    }
}

pub fn append_response_to_msg(resp: &raft::AppendEntriesResponse, peer: u64, self_id: u64) -> Message {
    Message {
        from: peer,
        to: self_id,
        term: resp.term,
        body: MessageBody::AppendEntriesResp { success: resp.success, match_index: resp.match_index },
    }
}

pub fn install_snapshot_response_to_msg(
    resp: &raft::InstallSnapshotResponse,
    peer: u64,
    self_id: u64,
    sent_index: u64,
) -> Message {
    Message {
        from: peer,
        to: self_id,
        term: resp.term,
        body: MessageBody::InstallSnapshotResp { match_index: sent_index },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_round_trips() {
        let out = Message {
            from: 2,
            to: 1,
            term: 5,
            body: MessageBody::RequestVote { last_log_index: 9, last_log_term: 4 },
        };
        let req = vote_request(&out);
        assert_eq!((req.candidate_id, req.term, req.group_id), (2, 5, PD_GROUP_ID));
        assert_eq!(vote_request_to_msg(&req, 1), out);
    }

    #[test]
    fn append_round_trips() {
        let out = Message {
            from: 1,
            to: 3,
            term: 7,
            body: MessageBody::AppendEntries {
                prev_log_index: 2,
                prev_log_term: 6,
                entries: vec![Entry::normal(7, 3, b"x".to_vec())],
                leader_commit: 2,
            },
        };
        let req = append_request(&out);
        assert_eq!(append_request_to_msg(&req, 3), out);

        let reply = Message {
            from: 3,
            to: 1,
            term: 7,
            body: MessageBody::AppendEntriesResp { success: true, match_index: 3 },
        };
        let resp = append_response(Some(&reply));
        assert_eq!(append_response_to_msg(&resp, 3, 1), reply);
    }
}

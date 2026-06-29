//! Wire ⇄ core conversions for the Raft transport.
//!
//! The Raft core ([`arcux_raft`]) is transport-free: it speaks [`Message`] / [`Entry`],
//! whose fields line up 1:1 with the frozen [`raft.proto`](../../rpc/proto/raft.proto)
//! RPCs. This module is the only place that maps between the two, in both directions:
//!
//! - **server side** — an inbound `RequestVote`/`AppendEntries` request becomes a
//!   [`Message`] to `step` into the local node; the node's reply [`Message`] becomes the
//!   RPC response.
//! - **sender side** — an outbound [`Message`] becomes the request to ship to a peer; the
//!   peer's RPC response becomes a reply [`Message`] to `step` back in.
//!
//! `term` on every message is the sender's term; `from`/`to` are node ids. The proto
//! carries `candidate_id`/`leader_id` (the sender) but not the receiver, so the receiver
//! id (`self_id` / `peer`) is supplied by the caller from its own context.

use arcux_raft::{Entry, Message, MessageBody};
use arcux_rpc::raft;

pub fn entry_to_proto(e: &Entry) -> raft::LogEntry {
    raft::LogEntry { term: e.term, index: e.index, data: e.data.clone() }
}

pub fn entry_from_proto(p: &raft::LogEntry) -> Entry {
    Entry { term: p.term, index: p.index, data: p.data.clone() }
}

// ---- outbound Message → request (sender side) ----------------------------------------

/// Build a `RequestVote` request from an outbound vote Message, tagged with the sender's
/// `group_id` (the region group) so the receiver routes it. Panics if the body is not a
/// `RequestVote` (the sender only calls this for that variant).
pub fn vote_request(m: &Message, group_id: u64) -> raft::RequestVoteRequest {
    match &m.body {
        MessageBody::RequestVote { last_log_index, last_log_term } => raft::RequestVoteRequest {
            term: m.term,
            candidate_id: m.from,
            last_log_index: *last_log_index,
            last_log_term: *last_log_term,
            group_id,
        },
        _ => unreachable!("vote_request on a non-RequestVote message"),
    }
}

/// Build an `AppendEntries` request from an outbound append Message, tagged with `group_id`.
pub fn append_request(m: &Message, group_id: u64) -> raft::AppendEntriesRequest {
    match &m.body {
        MessageBody::AppendEntries { prev_log_index, prev_log_term, entries, leader_commit } => {
            raft::AppendEntriesRequest {
                term: m.term,
                leader_id: m.from,
                prev_log_index: *prev_log_index,
                prev_log_term: *prev_log_term,
                entries: entries.iter().map(entry_to_proto).collect(),
                leader_commit: *leader_commit,
                group_id,
            }
        }
        _ => unreachable!("append_request on a non-AppendEntries message"),
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

// ---- reply Message → response (server side) ------------------------------------------

/// Convert the node's reply Message into a `RequestVoteResponse`. Falls back to a
/// not-granted response if the reply is missing/unexpected (defensive — the core always
/// emits exactly one reply for a `RequestVote`).
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

// ---- response → Message (sender side, fed back in) -----------------------------------

pub fn vote_response_to_msg(resp: &raft::RequestVoteResponse, peer: u64, self_id: u64) -> Message {
    Message {
        from: peer,
        to: self_id,
        term: resp.term,
        body: MessageBody::RequestVoteResp { granted: resp.vote_granted },
    }
}

pub fn append_response_to_msg(
    resp: &raft::AppendEntriesResponse,
    peer: u64,
    self_id: u64,
) -> Message {
    Message {
        from: peer,
        to: self_id,
        term: resp.term,
        body: MessageBody::AppendEntriesResp { success: resp.success, match_index: resp.match_index },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_round_trips_through_the_wire() {
        let out = Message {
            from: 2,
            to: 1,
            term: 5,
            body: MessageBody::RequestVote { last_log_index: 9, last_log_term: 4 },
        };
        // sender builds the request (tagged with group 7); node 1 reconstructs the message.
        let req = vote_request(&out, 7);
        assert_eq!((req.candidate_id, req.term, req.last_log_index, req.group_id), (2, 5, 9, 7));
        let msg = vote_request_to_msg(&req, 1);
        assert_eq!(msg, out);
    }

    #[test]
    fn append_round_trips_with_entries() {
        let out = Message {
            from: 1,
            to: 3,
            term: 7,
            body: MessageBody::AppendEntries {
                prev_log_index: 2,
                prev_log_term: 6,
                entries: vec![Entry { term: 7, index: 3, data: b"x".to_vec() }],
                leader_commit: 2,
            },
        };
        let req = append_request(&out, 4);
        assert_eq!((req.leader_id, req.entries.len(), req.group_id), (1, 1, 4));
        assert_eq!(append_request_to_msg(&req, 3), out);
    }

    #[test]
    fn responses_convert_both_ways() {
        let reply = Message {
            from: 3,
            to: 1,
            term: 7,
            body: MessageBody::AppendEntriesResp { success: true, match_index: 3 },
        };
        let resp = append_response(Some(&reply));
        assert!(resp.success && resp.match_index == 3);
        let back = append_response_to_msg(&resp, 3, 1);
        assert_eq!(back, reply);
    }
}

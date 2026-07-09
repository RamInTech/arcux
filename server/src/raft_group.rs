//! `RaftGroup` — drives one [`RaftNode`] over the real `tonic` transport.
//!
//! The core is a synchronous, single-threaded state machine that `fsync`s inside its
//! [`Storage`](arcux_raft::Storage) calls, so it must not run on the tokio reactor. This
//! module wraps it as an **actor**: a dedicated OS thread owns the node and serializes
//! every interaction through one command channel —
//!
//! - a **ticker** task sends `Tick` at the heartbeat cadence;
//! - inbound `RequestVote`/`AppendEntries` RPCs arrive as `StepReply` (step + return the
//!   node's reply message as the RPC response);
//! - the **sender** task drains the node's outbound messages, ships each to its peer's
//!   `RaftService`, and feeds the response back in as a `Step`;
//! - committed entries are drained and **applied** to the engine (`WriteBatch::decode` →
//!   `Engine::write`), idempotent across restart because each batch writes MVCC-versioned
//!   keys (re-applying rewrites identical bytes);
//! - a leader `propose` parks a one-shot until that index commits (or the node loses
//!   leadership, which fails it with `NotLeader`).

use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc as smpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arcux_engine::Error;
use arcux_raft::{ConfChange, Config, EntryType, Message, MessageBody, RaftNode, Role};
use arcux_rpc::raft;
use arcux_rpc::raft::raft_service_client::RaftServiceClient;
use tonic::transport::Channel;

use crate::raft_transport as xport;
use crate::wal_storage::WalStorage;

/// Applies a committed entry's bytes to the state machine, returning the engine outcome.
/// The leader's proposer sees this result (a Percolator conflict surfaces here as `Err`);
/// followers apply and ignore it.
pub type ApplyFn = Arc<dyn Fn(&[u8]) -> Result<(), Error> + Send + Sync>;

/// Captures this region's committed state as opaque snapshot bytes (the driver's log
/// compaction handoff — serialized latest-value-per-key from `Engine::scan`).
pub type SnapshotFn = Arc<dyn Fn() -> Vec<u8> + Send + Sync>;

/// Loads snapshot bytes produced by a [`SnapshotFn`] back into the region's engine state
/// (used when this replica installs a leader's `InstallSnapshot`).
pub type RestoreFn = Arc<dyn Fn(&[u8]) + Send + Sync>;

/// How many applied entries may accumulate above the last snapshot before the driver
/// compacts. A blunt log-length trigger (size-/time-based policy is deferred).
const COMPACT_THRESHOLD: u64 = 64;

/// How many ticks a leader may sit with parked-but-uncommittable writes before we log that
/// it's stalled for lack of a follower majority. Comfortably longer than a heartbeat (so a
/// healthy commit never trips it) but short enough to explain a hang quickly (~0.5s at the
/// default 30ms tick).
const STALL_LOG_TICKS: u32 = 15;

/// Outcome of a leader `propose`.
#[derive(Debug)]
pub enum ProposeResult {
    /// The entry committed (a majority persisted it) and applied, with the apply result.
    Applied(Result<(), Error>),
    /// This node is not the leader; `leader_hint` is its best guess at who is.
    NotLeader { leader_hint: Option<u64> },
}

/// Commands serialized to the actor thread that owns the node.
enum Cmd {
    Tick,
    /// A response fed back by the sender (no reply expected).
    Step(Message),
    /// An inbound RPC: step it, then return the node's single reply message.
    StepReply(Message, tokio::sync::oneshot::Sender<Option<Message>>),
    /// Leader-only: append a command, parking `tx` until it commits or we step down.
    Propose(Vec<u8>, tokio::sync::oneshot::Sender<ProposeResult>),
    /// Leader-only: append a single-server membership change, parking `tx` until it commits.
    ProposeConf(ConfChange, tokio::sync::oneshot::Sender<ProposeResult>),
    /// Stop the actor (and, by cascade, the ticker + sender) — used to take a node down.
    Stop,
}

/// Observable, lock-protected snapshot of the node's role/leader (read without messaging
/// the actor — the hot path for routing decisions).
#[derive(Clone)]
struct Observable {
    role: Role,
    leader_id: Option<u64>,
    /// `true` once this leader has applied an entry of its **current** term (its election
    /// no-op). Until then it hasn't re-applied prior-term committed entries, so a read would
    /// not be linearizable — the read barrier (`readIndex`).
    read_ready: bool,
    /// The group's current voter set (derived from the log; changes on a committed membership
    /// change). Lets the routing/PD layer observe the replica set.
    voters: Vec<u64>,
}

/// A handle to a running Raft group. Cheap to clone (shares the command channel + the
/// observable state).
#[derive(Clone)]
pub struct RaftGroup {
    id: u64,
    cmd_tx: smpsc::Sender<Cmd>,
    obs: Arc<Mutex<Observable>>,
}

/// Configuration to [`start`] a group.
pub struct GroupOptions {
    /// The group's id — the region id this group replicates. Stamped on every outbound RPC
    /// so the receiving node routes it to the matching group (MultiRaft multiplexing).
    pub group_id: u64,
    /// This node's id (its identity within the group's `voters`; the same across all of the
    /// node's groups).
    pub id: u64,
    /// All voting members (including `id`).
    pub voters: Vec<u64>,
    /// Peer id → serving address (excludes `id`); where to ship outbound messages.
    pub peers: HashMap<u64, String>,
    /// Durable log/term/vote for this replica.
    pub storage: WalStorage,
    /// Applies each committed entry to the state machine (decode + execute), returning the
    /// engine outcome for the proposer.
    pub apply: ApplyFn,
    /// Captures the region's committed state for log compaction (drives `InstallSnapshot`).
    pub snapshot: SnapshotFn,
    /// Loads an installed snapshot's bytes back into the engine.
    pub restore: RestoreFn,
    /// Logical tick period (one heartbeat per tick; election timeout is ~10–20 ticks).
    pub tick: Duration,
}

impl RaftGroup {
    pub fn is_leader(&self) -> bool {
        self.obs.lock().unwrap().role == Role::Leader
    }

    /// Whether this node is a leader that has applied its current-term no-op, so reads it
    /// serves are linearizable (it has re-applied every prior committed entry).
    pub fn read_ready(&self) -> bool {
        self.obs.lock().unwrap().read_ready
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.obs.lock().unwrap().leader_id
    }

    /// The group's current voter set (its replica ids), as last observed by the actor.
    pub fn voters(&self) -> Vec<u64> {
        self.obs.lock().unwrap().voters.clone()
    }

    /// Propose a command (an encoded [`WriteBatch`]) and await its commit. Returns
    /// `NotLeader` immediately if this node isn't the leader.
    pub async fn propose(&self, data: Vec<u8>) -> ProposeResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.cmd_tx.send(Cmd::Propose(data, tx)).is_err() {
            return ProposeResult::NotLeader { leader_hint: None };
        }
        rx.await.unwrap_or(ProposeResult::NotLeader { leader_hint: None })
    }

    /// Propose a single-server membership change (leader only) and await its commit. Adds or
    /// removes one voter; the newly-added replica catches up via append/snapshot afterwards.
    pub async fn propose_conf_change(&self, cc: ConfChange) -> ProposeResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.cmd_tx.send(Cmd::ProposeConf(cc, tx)).is_err() {
            return ProposeResult::NotLeader { leader_hint: None };
        }
        rx.await.unwrap_or(ProposeResult::NotLeader { leader_hint: None })
    }

    /// Serve an inbound `RequestVote` RPC.
    pub async fn handle_request_vote(
        &self,
        req: raft::RequestVoteRequest,
    ) -> raft::RequestVoteResponse {
        let msg = xport::vote_request_to_msg(&req, self.id);
        xport::vote_response(self.step_reply(msg).await.as_ref())
    }

    /// Serve an inbound `AppendEntries` RPC.
    pub async fn handle_append_entries(
        &self,
        req: raft::AppendEntriesRequest,
    ) -> raft::AppendEntriesResponse {
        let msg = xport::append_request_to_msg(&req, self.id);
        xport::append_response(self.step_reply(msg).await.as_ref())
    }

    /// Serve an inbound `InstallSnapshot` RPC.
    pub async fn handle_install_snapshot(
        &self,
        req: raft::InstallSnapshotRequest,
    ) -> raft::InstallSnapshotResponse {
        let msg = xport::install_snapshot_request_to_msg(&req, self.id);
        xport::install_snapshot_response(self.step_reply(msg).await.as_ref())
    }

    async fn step_reply(&self, msg: Message) -> Option<Message> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.cmd_tx.send(Cmd::StepReply(msg, tx)).is_err() {
            return None;
        }
        rx.await.ok().flatten()
    }

    /// Stop the group: the actor breaks its loop, which drops the outbound channel (ending
    /// the sender) and orphans the command channel (ending the ticker). Used to take a node
    /// down — without it, a "killed" node keeps heartbeating peers and suppresses the
    /// re-election that should follow.
    pub fn shutdown(&self) {
        let _ = self.cmd_tx.send(Cmd::Stop);
    }
}

/// Start the actor thread, ticker, and sender, returning a handle. Must be called within a
/// tokio runtime (it spawns the ticker/sender tasks).
pub fn start(opts: GroupOptions) -> RaftGroup {
    let (cmd_tx, cmd_rx) = smpsc::channel::<Cmd>();
    let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let obs = Arc::new(Mutex::new(Observable {
        role: Role::Follower,
        leader_id: None,
        read_ready: false,
        voters: Vec::new(),
    }));

    // Pre-build lazy clients for each peer (connect_lazy does no I/O until first call).
    let mut clients: HashMap<u64, RaftServiceClient<Channel>> = HashMap::new();
    for (peer, addr) in &opts.peers {
        match Channel::from_shared(addr.clone()) {
            Ok(ep) => {
                clients.insert(*peer, RaftServiceClient::new(ep.connect_lazy()));
            }
            Err(e) => eprintln!("raft: bad peer address {addr}: {e}"),
        }
    }

    // The actor thread owns the node (blocking fsync lives here, off the reactor).
    let node = RaftNode::new(Config::new(opts.id, opts.voters), opts.storage);
    let apply = opts.apply;
    let snapshot = opts.snapshot;
    let restore = opts.restore;
    let obs_actor = obs.clone();
    let group_id = opts.group_id;
    let self_id = opts.id;
    std::thread::Builder::new()
        .name(format!("raft-{}", opts.id))
        .spawn(move || {
            run_actor(node, group_id, self_id, apply, snapshot, restore, cmd_rx, out_tx, obs_actor)
        })
        .expect("spawn raft actor thread");

    // Ticker: drive the logical clock. Stops when the actor is gone (send fails).
    let tick_tx = cmd_tx.clone();
    let tick = opts.tick;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if tick_tx.send(Cmd::Tick).is_err() {
                break;
            }
        }
    });

    // Sender: ship outbound messages to peers, feed responses back in.
    let feedback = cmd_tx.clone();
    tokio::spawn(run_sender(group_id, self_id, clients, out_rx, feedback));

    RaftGroup { id: opts.id, cmd_tx, obs }
}

/// The actor loop: own the node, process one command at a time, then drain its effects
/// (outbound messages, committed entries) and publish observable state.
/// Print a one-line Raft state transition for an operator watching the terminal: e.g. a node
/// starting an election (CANDIDATE), winning it (LEADER), or stepping down to follow another
/// node (FOLLOWER) — always with the election `term`, the unit of Raft's logical clock.
fn log_raft_transition(region: u64, node: u64, role: Role, term: u64, leader: Option<u64>) {
    let line = match role {
        Role::Candidate => format!("CANDIDATE — started an election for term {term}"),
        Role::Leader => format!("LEADER    — won term {term}, now serving writes"),
        Role::Follower => match leader {
            Some(l) => format!("FOLLOWER  — following node {l} in term {term}"),
            None => format!("FOLLOWER  — term {term}, no leader yet"),
        },
    };
    eprintln!("[raft region {region}] node {node}: {line}");
}

#[allow(clippy::too_many_arguments)]
fn run_actor(
    mut node: RaftNode<WalStorage>,
    group_id: u64,
    self_id: u64,
    apply: ApplyFn,
    snapshot: SnapshotFn,
    restore: RestoreFn,
    cmd_rx: smpsc::Receiver<Cmd>,
    out_tx: tokio::sync::mpsc::UnboundedSender<Message>,
    obs: Arc<Mutex<Observable>>,
) {
    // Pending leader proposals: log index → waiter.
    let mut pending: BTreeMap<u64, tokio::sync::oneshot::Sender<ProposeResult>> = BTreeMap::new();
    // Tracks leadership so we can commit a no-op on each election (see below).
    let mut was_leader = false;
    // Term of the last entry applied — drives the read barrier (`read_ready`).
    let mut applied_term = 0u64;
    // Last Raft state we logged, so we announce only genuine transitions (election activity).
    let (mut last_role, mut last_term, mut last_leader) =
        (node.role(), node.current_term(), node.leader_id());
    // The last (term, vote) we logged. Keyed on the *term* too, not just the target: a node can
    // grant its vote to the same candidate across consecutive terms (a re-campaign after a split
    // vote), and each of those is a distinct vote worth logging even though the target is unchanged.
    let mut last_vote = (node.current_term(), node.voted_for());
    // Votes this node has already tallied as a candidate, so we log each newly-arrived one.
    let mut last_votes: std::collections::BTreeSet<u64> = node.votes().into_iter().collect();
    // Leader-stall detection: consecutive ticks a leader has held uncommittable parked writes,
    // and whether we've already logged the current stall (so we announce it just once).
    let mut stall_ticks = 0u32;
    let mut stall_logged = false;

    while let Ok(cmd) = cmd_rx.recv() {
        // A StepReply expects exactly one reply message (addressed back to `from`).
        let mut reply: Option<(u64, tokio::sync::oneshot::Sender<Option<Message>>)> = None;
        // Only advance the stall clock on real time passing (a tick), not on every message.
        let ticked = matches!(cmd, Cmd::Tick);

        match cmd {
            Cmd::Tick => node.tick(),
            Cmd::Step(m) => node.step(m),
            Cmd::StepReply(m, tx) => {
                let from = m.from;
                node.step(m);
                reply = Some((from, tx));
            }
            Cmd::Propose(data, tx) => match node.propose(data) {
                Ok(index) => {
                    pending.insert(index, tx);
                }
                Err(_) => {
                    let _ = tx.send(ProposeResult::NotLeader { leader_hint: node.leader_id() });
                }
            },
            Cmd::ProposeConf(cc, tx) => match node.propose_conf_change(cc) {
                Ok(index) => {
                    pending.insert(index, tx);
                }
                Err(_) => {
                    let _ = tx.send(ProposeResult::NotLeader { leader_hint: node.leader_id() });
                }
            },
            Cmd::Stop => break,
        }

        // On winning an election, append a no-op in the new term. Raft only advances
        // `commit_index` to entries of the leader's *current* term (the Figure-8 rule), so
        // until the new leader commits something of its own, prior committed entries — though
        // safely in its log — aren't re-marked committed and thus aren't applied to its
        // engine. The no-op carries the commit point past them, so a read right after a
        // failover sees every acknowledged write. (Empty data ⇒ a no-op on apply.)
        if node.role() == Role::Leader && !was_leader {
            let _ = node.propose(Vec::new());
        }
        was_leader = node.role() == Role::Leader;

        // Log election activity *before* the role transition, so within the step that a
        // candidate receives its winning vote you read "received vote (2/2)" then "LEADER won".

        // This node granting its vote to *another* node (a follower backing a candidate); a
        // candidate voting for itself is already implied by the CANDIDATE transition below.
        let vote = (node.current_term(), node.voted_for());
        if vote != last_vote {
            if let (term, Some(cand)) = vote {
                if cand != self_id {
                    eprintln!(
                        "[raft region {group_id}] node {self_id}: voted for node {cand} in term {term}"
                    );
                }
            }
            last_vote = vote;
        }

        // Candidate side: each newly-received vote + the running tally toward a majority. No
        // role guard — the winning vote flips the node to Leader in the same step, and votes
        // only ever grow during an active campaign (a new election clears the set first).
        let votes: std::collections::BTreeSet<u64> = node.votes().into_iter().collect();
        let majority = node.voters().len() / 2 + 1;
        for granter in votes.difference(&last_votes).filter(|g| **g != self_id) {
            eprintln!(
                "[raft region {group_id}] node {self_id}: received vote from node {granter} ({}/{} for term {})",
                votes.len(),
                majority,
                node.current_term()
            );
        }
        last_votes = votes;

        // Announce the resulting Raft state transition: who's the candidate, who won, the term,
        // and who each node is following.
        let (role, term, leader) = (node.role(), node.current_term(), node.leader_id());
        if (role, term, leader) != (last_role, last_term, last_leader) {
            log_raft_transition(group_id, self_id, role, term, leader);
            last_role = role;
            last_term = term;
            last_leader = leader;
        }

        // Route outbound messages: the one reply (if any) back to the awaiting RPC, the
        // rest to the sender.
        for m in node.take_messages() {
            if let Some((from, _)) = &reply {
                if m.to == *from && is_response(&m.body) {
                    let (_, tx) = reply.take().unwrap();
                    let _ = tx.send(Some(m));
                    continue;
                }
            }
            let _ = out_tx.send(m);
        }
        if let Some((_, tx)) = reply.take() {
            let _ = tx.send(None); // no reply was produced (shouldn't happen)
        }

        // A snapshot the leader installed on us supersedes the log below its index — load its
        // state into the engine before applying anything above it. `take_snapshot` also
        // advances `applied_term` implicitly (the entries it covers are already applied).
        if let Some((_idx, data)) = node.take_snapshot() {
            restore(&data);
            applied_term = node.current_term();
        }

        // Apply newly-committed entries in order; answer each one's parked proposer (if we
        // are the leader that proposed it) with the apply outcome.
        for e in node.take_committed() {
            applied_term = e.term;
            // A config-change entry's membership effect was already applied by the core; the
            // state machine has nothing to run for it (its `data` is a ConfChange, not a
            // command), so skip the apply closure and report success to the proposer.
            let outcome = if e.entry_type == EntryType::ConfChange {
                Ok(())
            } else {
                apply(&e.data)
            };
            if let Some(tx) = pending.remove(&e.index) {
                let _ = tx.send(ProposeResult::Applied(outcome));
            }
        }

        // Bound the log: once enough entries have accumulated above the last snapshot,
        // capture the region's committed state and compact everything at/below the applied
        // point. Every replica does this independently to cap its own log; only a leader ever
        // *ships* the resulting snapshot (to a follower it can no longer back-fill by append).
        if node.last_applied() + 1 >= node.first_index() + COMPACT_THRESHOLD {
            let index = node.last_applied();
            let bytes = snapshot();
            node.compact(index, bytes);
        }

        // If we've lost leadership, fail any still-parked proposals — their entries can no
        // longer commit on this node.
        let role = node.role();
        if role != Role::Leader && !pending.is_empty() {
            let hint = node.leader_id();
            for (_, tx) in std::mem::take(&mut pending) {
                let _ = tx.send(ProposeResult::NotLeader { leader_hint: hint });
            }
        }

        // Leader-without-majority: a leader appends a write to its own log, but `commit_index`
        // can only advance once a *majority* of voters persist it. If the other replicas are
        // down, parked proposals sit uncommitted forever and the write hangs (correct CP
        // behaviour — no split-brain). Detect that state and log it once, so an operator
        // watching sees *why* writes stopped landing, and log again when a majority returns.
        if ticked {
            let uncommitted = node.last_log_index().saturating_sub(node.commit_index());
            let stalled = role == Role::Leader && !pending.is_empty() && uncommitted > 0;
            if stalled {
                stall_ticks += 1;
                if stall_ticks >= STALL_LOG_TICKS && !stall_logged {
                    let voters = node.voters().len();
                    eprintln!(
                        "[raft region {group_id}] node {self_id}: LEADER cannot commit — waiting for majority ({} of {voters} voters needed; {uncommitted} write(s) parked in term {})",
                        voters / 2 + 1,
                        node.current_term(),
                    );
                    stall_logged = true;
                }
            } else {
                if stall_logged {
                    eprintln!(
                        "[raft region {group_id}] node {self_id}: majority restored — committing parked writes"
                    );
                }
                stall_ticks = 0;
                stall_logged = false;
            }
        }

        // A leader is read-ready once it has applied an entry of its current term (its
        // no-op) — only then has it re-applied every prior committed entry.
        let read_ready = role == Role::Leader && applied_term == node.current_term();
        *obs.lock().unwrap() = Observable {
            role,
            leader_id: node.leader_id(),
            read_ready,
            voters: node.voters().to_vec(),
        };
    }
}

/// Ship each outbound message to its peer and feed the response back as a `Step`. Each RPC
/// runs in its own task so one slow/dead peer never blocks the others; the core tolerates
/// the resulting reordering/duplication.
async fn run_sender(
    group_id: u64,
    self_id: u64,
    clients: HashMap<u64, RaftServiceClient<Channel>>,
    mut out_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    feedback: smpsc::Sender<Cmd>,
) {
    while let Some(m) = out_rx.recv().await {
        let Some(client) = clients.get(&m.to).cloned() else { continue };
        let peer = m.to;
        let feedback = feedback.clone();
        tokio::spawn(async move {
            let mut client = client;
            match &m.body {
                MessageBody::RequestVote { .. } => {
                    if let Ok(resp) = client.request_vote(xport::vote_request(&m, group_id)).await {
                        let reply = xport::vote_response_to_msg(&resp.into_inner(), peer, self_id);
                        let _ = feedback.send(Cmd::Step(reply));
                    }
                }
                MessageBody::AppendEntries { .. } => {
                    if let Ok(resp) = client.append_entries(xport::append_request(&m, group_id)).await {
                        // Server-streaming: the first message is the follower's reply.
                        if let Ok(Some(r)) = resp.into_inner().message().await {
                            let reply = xport::append_response_to_msg(&r, peer, self_id);
                            let _ = feedback.send(Cmd::Step(reply));
                        }
                    }
                }
                MessageBody::InstallSnapshot { last_included_index, .. } => {
                    // The ack carries only `term`; the follower is caught up to exactly the
                    // index we shipped, so we re-attach it as its match_index.
                    let sent = *last_included_index;
                    if let Ok(resp) =
                        client.install_snapshot(xport::install_snapshot_request(&m, group_id)).await
                    {
                        let reply = xport::install_snapshot_response_to_msg(
                            &resp.into_inner(),
                            peer,
                            self_id,
                            sent,
                        );
                        let _ = feedback.send(Cmd::Step(reply));
                    }
                }
                // Responses are returned by the RPC above, never shipped outbound.
                MessageBody::RequestVoteResp { .. }
                | MessageBody::AppendEntriesResp { .. }
                | MessageBody::InstallSnapshotResp { .. } => {}
            }
        });
    }
}

fn is_response(b: &MessageBody) -> bool {
    matches!(
        b,
        MessageBody::RequestVoteResp { .. }
            | MessageBody::AppendEntriesResp { .. }
            | MessageBody::InstallSnapshotResp { .. }
    )
}

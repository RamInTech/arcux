//! `PdGroup` — drives PD's single [`PdReplica`] over the real `tonic` transport.
//!
//! The same actor shape as the data node's `server/src/raft_group.rs`: a dedicated thread owns
//! the (synchronous) replica and serializes every interaction through one command channel; a
//! **ticker** drives the logical clock, a **sender** ships outbound messages to peers and feeds
//! their replies back, and inbound `RequestVote`/`AppendEntries` RPCs arrive as `StepReply`.
//! The two PD-specific commands are [`Cmd::Heartbeat`] (record a data node's placement through
//! Raft, answering with its assignment once committed) and [`Cmd::AllocTs`] (hand out
//! timestamps, reserving a fresh window *through Raft* when the current one is exhausted).
//! Reads (`GetRegion`/`ListRegions`) don't go through the actor at all — they read the shared
//! [`PdFsm`] directly.

use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc as smpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arcux_raft::{Message, MessageBody, Role};
use arcux_rpc::raft;
use arcux_rpc::raft::raft_service_client::RaftServiceClient;
use tonic::transport::Channel;

use crate::raft_wire as xport;
use crate::{PdCmd, PdFsm, PdReplica, Region};

/// Logical tick period. Election timeout is ~10–20 ticks (see [`Config`]), so at 50 ms/tick a
/// dead leader is replaced in well under a second.
pub const TICK: Duration = Duration::from_millis(50);

/// Commands serialized to the actor thread that owns the replica.
enum Cmd {
    Tick,
    /// A peer's response, fed back by the sender (no reply expected).
    Step(Message),
    /// An inbound RPC: step it, then return the replica's single reply message.
    StepReply(Message, tokio::sync::oneshot::Sender<Option<Message>>),
    /// Record a data node's heartbeat through Raft; reply with its assigned regions once the
    /// entry commits (`None` if this node isn't the leader).
    Heartbeat {
        node_id: u64,
        address: String,
        regions: Vec<Region>,
        now: u64,
        reply: tokio::sync::oneshot::Sender<Option<Vec<Region>>>,
    },
    /// Allocate `count` timestamps (`None` if not the leader). Served from the reserved window,
    /// reserving a fresh one through Raft first if exhausted.
    AllocTs { count: u64, reply: tokio::sync::oneshot::Sender<Option<(u64, u64)>> },
    Stop,
}

/// A parked heartbeat proposal: the node whose assignment to return, and where to send it once
/// the entry commits.
type PendingHeartbeat = (u64, tokio::sync::oneshot::Sender<Option<Vec<Region>>>);
/// A parked timestamp allocation: the count requested, and where to send `(first, count)` once
/// the reservation commits.
type PendingAllocTs = (u64, tokio::sync::oneshot::Sender<Option<(u64, u64)>>);

/// Observable role/leader, read without messaging the actor (the hot path for redirect).
#[derive(Clone)]
struct Observable {
    role: Role,
    leader_id: Option<u64>,
}

/// A handle to the running PD Raft group. Cheap to clone.
#[derive(Clone)]
pub struct PdGroup {
    self_id: u64,
    cmd_tx: smpsc::Sender<Cmd>,
    fsm: Arc<PdFsm>,
    obs: Arc<Mutex<Observable>>,
    /// Node id → serving address for every voter (including self), so a follower can hand a
    /// client the current leader's PD address to redirect to.
    addrs: HashMap<u64, String>,
}

/// Configuration to [`start`] a PD group.
pub struct PdGroupOptions {
    /// This node's id (its identity within `voters`).
    pub id: u64,
    /// All voting members (including `id`).
    pub voters: Vec<u64>,
    /// Every voter's serving address (including this node's own), used for peer RPCs and the
    /// leader-redirect hint.
    pub addrs: HashMap<u64, String>,
}

impl PdGroup {
    pub fn is_leader(&self) -> bool {
        self.obs.lock().unwrap().role == Role::Leader
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.obs.lock().unwrap().leader_id
    }

    /// The current leader's PD serving address, if one is known — what a follower tells a client
    /// to redirect to.
    pub fn leader_addr(&self) -> Option<String> {
        self.leader_id().and_then(|id| self.addrs.get(&id).cloned())
    }

    /// The shared state machine — read the routing view directly (no actor round-trip).
    pub fn fsm(&self) -> &Arc<PdFsm> {
        &self.fsm
    }

    /// Record a data node's heartbeat through Raft, returning its assigned regions once the
    /// entry commits. `None` if this node isn't the leader (the caller should redirect).
    pub async fn heartbeat(
        &self,
        node_id: u64,
        address: String,
        regions: Vec<Region>,
        now: u64,
    ) -> Option<Vec<Region>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cmd = Cmd::Heartbeat { node_id, address, regions, now, reply: tx };
        if self.cmd_tx.send(cmd).is_err() {
            return None;
        }
        rx.await.ok().flatten()
    }

    /// Allocate `count` timestamps from the authoritative oracle, `[first, first+count)`. `None`
    /// if this node isn't the leader.
    pub async fn alloc_ts(&self, count: u64) -> Option<(u64, u64)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.cmd_tx.send(Cmd::AllocTs { count: count.max(1), reply: tx }).is_err() {
            return None;
        }
        rx.await.ok().flatten()
    }

    pub async fn handle_request_vote(
        &self,
        req: raft::RequestVoteRequest,
    ) -> raft::RequestVoteResponse {
        let msg = xport::vote_request_to_msg(&req, self.self_id);
        xport::vote_response(self.step_reply(msg).await.as_ref())
    }

    pub async fn handle_append_entries(
        &self,
        req: raft::AppendEntriesRequest,
    ) -> raft::AppendEntriesResponse {
        let msg = xport::append_request_to_msg(&req, self.self_id);
        xport::append_response(self.step_reply(msg).await.as_ref())
    }

    pub async fn handle_install_snapshot(
        &self,
        req: raft::InstallSnapshotRequest,
    ) -> raft::InstallSnapshotResponse {
        let msg = xport::install_snapshot_request_to_msg(&req, self.self_id);
        xport::install_snapshot_response(self.step_reply(msg).await.as_ref())
    }

    async fn step_reply(&self, msg: Message) -> Option<Message> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.cmd_tx.send(Cmd::StepReply(msg, tx)).is_err() {
            return None;
        }
        rx.await.ok().flatten()
    }

    /// Stop the group (ends the actor, which cascades to the ticker + sender).
    pub fn shutdown(&self) {
        let _ = self.cmd_tx.send(Cmd::Stop);
    }
}

/// Start the actor thread, ticker, and sender, returning a handle. Must be called within a
/// tokio runtime.
pub fn start(opts: PdGroupOptions) -> PdGroup {
    let (cmd_tx, cmd_rx) = smpsc::channel::<Cmd>();
    let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let obs = Arc::new(Mutex::new(Observable { role: Role::Follower, leader_id: None }));

    let replica = PdReplica::new(opts.id, opts.voters.clone());
    let fsm = replica.fsm().clone();

    // Lazy clients for each peer (connect_lazy does no I/O until first call).
    let mut clients: HashMap<u64, RaftServiceClient<Channel>> = HashMap::new();
    for (&peer, addr) in opts.addrs.iter() {
        if peer == opts.id {
            continue;
        }
        match Channel::from_shared(addr.clone()) {
            Ok(ep) => {
                clients.insert(peer, RaftServiceClient::new(ep.connect_lazy()));
            }
            Err(e) => eprintln!("pd raft: bad peer address {addr}: {e}"),
        }
    }

    let self_id = opts.id;
    let obs_actor = obs.clone();
    std::thread::Builder::new()
        .name(format!("pd-raft-{self_id}"))
        .spawn(move || run_actor(replica, self_id, cmd_rx, out_tx, obs_actor))
        .expect("spawn pd raft actor thread");

    // Ticker: drive the logical clock. Stops when the actor is gone.
    let tick_tx = cmd_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TICK);
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
    tokio::spawn(run_sender(self_id, clients, out_rx, feedback));

    PdGroup { self_id, cmd_tx, fsm, obs, addrs: opts.addrs }
}

/// The actor loop: own the replica, process one command at a time, drain its effects, resolve
/// any proposals that just committed, and publish observable state.
fn run_actor(
    mut replica: PdReplica,
    self_id: u64,
    cmd_rx: smpsc::Receiver<Cmd>,
    out_tx: tokio::sync::mpsc::UnboundedSender<Message>,
    obs: Arc<Mutex<Observable>>,
) {
    // Proposals parked on the log index that will resolve them.
    let mut pending_hb: BTreeMap<u64, PendingHeartbeat> = BTreeMap::new();
    let mut pending_ts: BTreeMap<u64, PendingAllocTs> = BTreeMap::new();
    let (mut last_leader, mut last_term) = (replica.leader_id(), replica.current_term());

    while let Ok(cmd) = cmd_rx.recv() {
        let mut reply: Option<(u64, tokio::sync::oneshot::Sender<Option<Message>>)> = None;

        match cmd {
            Cmd::Tick => replica.tick(),
            Cmd::Step(m) => replica.step(m),
            Cmd::StepReply(m, tx) => {
                let from = m.from;
                replica.step(m);
                reply = Some((from, tx));
            }
            Cmd::Heartbeat { node_id, address, regions, now, reply: tx } => {
                if !replica.is_leader() {
                    let _ = tx.send(None);
                } else {
                    let cmd = PdCmd::Heartbeat { node_id, address, regions, now };
                    match replica.propose(&cmd) {
                        Ok(index) => {
                            pending_hb.insert(index, (node_id, tx));
                        }
                        Err(_) => {
                            let _ = tx.send(None);
                        }
                    }
                }
            }
            Cmd::AllocTs { count, reply: tx } => {
                if !replica.is_leader() {
                    let _ = tx.send(None);
                } else if let Some(first) = replica.hand_out(count) {
                    let _ = tx.send(Some((first, count)));
                } else {
                    // Reserved window exhausted — raise the high-water through Raft, then serve
                    // once it commits.
                    let cmd = replica.reserve_ts_cmd(count);
                    match replica.propose(&cmd) {
                        Ok(index) => {
                            pending_ts.insert(index, (count, tx));
                        }
                        Err(_) => {
                            let _ = tx.send(None);
                        }
                    }
                }
            }
            Cmd::Stop => break,
        }

        // Drain effects: apply committed entries into the FSM and collect outbound messages.
        let ready = replica.ready();

        // Route outbound messages: the one reply (if any) back to the awaiting RPC, the rest to
        // the sender.
        for m in ready.messages {
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
            let _ = tx.send(None);
        }

        // Resolve proposals whose entries just committed.
        for index in ready.committed {
            if let Some((node_id, tx)) = pending_hb.remove(&index) {
                let _ = tx.send(Some(replica.fsm().regions_of(node_id)));
            }
            if let Some((count, tx)) = pending_ts.remove(&index) {
                // The reservation raised the high-water enough that this now succeeds.
                let _ = tx.send(replica.hand_out(count).map(|first| (first, count)));
            }
        }

        // If we've lost leadership, fail every still-parked proposal.
        if !replica.is_leader() && (!pending_hb.is_empty() || !pending_ts.is_empty()) {
            for (_, (_, tx)) in std::mem::take(&mut pending_hb) {
                let _ = tx.send(None);
            }
            for (_, (_, tx)) in std::mem::take(&mut pending_ts) {
                let _ = tx.send(None);
            }
        }

        // Announce a leadership/term change for an operator watching the terminal.
        let (leader, term) = (replica.leader_id(), replica.current_term());
        if (leader, term) != (last_leader, last_term) {
            if replica.is_leader() {
                eprintln!("[pd raft] node {self_id}: LEADER — won term {term}, now serving PD");
            } else if let Some(l) = leader {
                eprintln!("[pd raft] node {self_id}: FOLLOWER — following node {l} in term {term}");
            } else {
                eprintln!("[pd raft] node {self_id}: no leader yet in term {term}");
            }
            last_leader = leader;
            last_term = term;
        }

        *obs.lock().unwrap() = Observable { role: role_of(&replica), leader_id: replica.leader_id() };
    }
}

fn role_of(replica: &PdReplica) -> Role {
    if replica.is_leader() {
        Role::Leader
    } else {
        // A follower vs candidate distinction isn't observed by the service layer (both are
        // "not leader"); report Follower for anything non-leader.
        Role::Follower
    }
}

/// Ship each outbound message to its peer and feed the response back as a `Step`. Each RPC runs
/// in its own task so one slow/dead peer never blocks the others.
async fn run_sender(
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
                    if let Ok(resp) = client.request_vote(xport::vote_request(&m)).await {
                        let reply = xport::vote_response_to_msg(&resp.into_inner(), peer, self_id);
                        let _ = feedback.send(Cmd::Step(reply));
                    }
                }
                MessageBody::AppendEntries { .. } => {
                    if let Ok(resp) = client.append_entries(xport::append_request(&m)).await {
                        // Server-streaming: the first message is the follower's reply.
                        if let Ok(Some(r)) = resp.into_inner().message().await {
                            let reply = xport::append_response_to_msg(&r, peer, self_id);
                            let _ = feedback.send(Cmd::Step(reply));
                        }
                    }
                }
                MessageBody::InstallSnapshot { last_included_index, .. } => {
                    let sent = *last_included_index;
                    if let Ok(resp) = client.install_snapshot(xport::install_snapshot_request(&m)).await
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

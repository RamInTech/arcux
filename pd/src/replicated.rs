//! PD-on-Raft — PD's authoritative state as a **replicated state machine**.
//!
//! Phase 3 ran PD as a single process: if it died, the cluster lost its timestamp oracle and
//! its region router. This module removes that single point of failure by replicating the two
//! pieces of PD state that no failover may lose or corrupt across an ordered Raft log, so a
//! three-node PD group can lose its leader and a new one resumes from exactly the committed
//! state:
//!
//! * the **TSO high-water** ([`PdCmd::ReserveTs`]) — the leader raises it *through Raft*
//!   before handing out any timestamp below it, so a new leader that resumes from the
//!   committed watermark can never reissue a timestamp an old leader already gave out (the
//!   property Percolator snapshot isolation depends on, preserved across a PD failover); and
//! * the **placement / liveness view** ([`PdCmd::Heartbeat`]) — each data node's heartbeat is
//!   a committed command, so every PD replica applies the identical [`Membership`] mutation
//!   and shares one routing view.
//!
//! The design follows the rest of arcux: the [`RaftNode`](arcux_raft::RaftNode) *core* is pure
//! and transport-free, so this layer — the command codec ([`PdCmd`]), the state machine
//! ([`PdFsm`]), and the single-group driver ([`PdReplica`]) — is proven by a deterministic,
//! in-process cluster (`tests/raft_pd.rs`) under failover before any gRPC transport is wired.
//! The transport integration (a real 3-process PD cluster with follower→leader redirect) is
//! the mechanical next step; it reuses the same `raft.proto` the data-node groups already
//! speak.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arcux_raft::{Config, EntryType, MemStorage, Message, ProposeError, RaftNode};

use crate::persist::{get_bytes, get_u32, get_u64, put_bytes};
use crate::{Membership, PlacedRegion, Region};

/// How far ahead of the current need a `ReserveTs` raises the high-water. Larger ⇒ fewer Raft
/// round-trips on the timestamp path, at the cost of more timestamps skipped on a failover
/// (the new leader resumes at the committed `upper`, discarding the leader's unused tail).
/// Mirrors the single-process oracle's window ([`crate::Tso`]).
const RESERVE_WINDOW: u64 = 1 << 16;

/// A command in PD's replicated log. Encoded into a Raft entry's opaque `data`; every replica
/// decodes and applies the identical sequence, so their [`PdFsm`]s stay bit-for-bit in sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PdCmd {
    /// Raise the replicated TSO high-water to `upper`. The leader commits this **before**
    /// serving any timestamp `< upper`, so the committed watermark is always an upper bound on
    /// every timestamp handed out — and a new leader resuming from it never regresses.
    ReserveTs { upper: u64 },
    /// Record a data node's heartbeat: its serving `address`, the `regions` it owns, and the
    /// wall-clock `now` (ms, for liveness). Applied via [`Membership::heartbeat`] on every
    /// replica, giving one shared placement + liveness view.
    Heartbeat { node_id: u64, address: String, regions: Vec<Region>, now: u64 },
}

impl PdCmd {
    /// Serialize to a Raft entry payload. Layout: `[tag:u8]` then the variant's fields
    /// (`ReserveTs` = `upper:u64 BE`; `Heartbeat` = `node_id:u64, now:u64, address, n:u32,
    /// region*`), using the crate's length-prefixed byte codec.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            PdCmd::ReserveTs { upper } => {
                out.push(1);
                out.extend_from_slice(&upper.to_be_bytes());
            }
            PdCmd::Heartbeat { node_id, address, regions, now } => {
                out.push(2);
                out.extend_from_slice(&node_id.to_be_bytes());
                out.extend_from_slice(&now.to_be_bytes());
                put_bytes(&mut out, address.as_bytes());
                out.extend_from_slice(&(regions.len() as u32).to_be_bytes());
                for r in regions {
                    put_region(&mut out, r);
                }
            }
        }
        out
    }

    /// Inverse of [`encode`](Self::encode); `None` on a malformed payload.
    pub fn decode(bytes: &[u8]) -> Option<PdCmd> {
        let mut pos = 0;
        let tag = *bytes.get(pos)?;
        pos += 1;
        match tag {
            1 => Some(PdCmd::ReserveTs { upper: get_u64(bytes, &mut pos)? }),
            2 => {
                let node_id = get_u64(bytes, &mut pos)?;
                let now = get_u64(bytes, &mut pos)?;
                let address = String::from_utf8(get_bytes(bytes, &mut pos)?.to_vec()).ok()?;
                let n = get_u32(bytes, &mut pos)? as usize;
                let mut regions = Vec::with_capacity(n);
                for _ in 0..n {
                    regions.push(get_region(bytes, &mut pos)?);
                }
                Some(PdCmd::Heartbeat { node_id, address, regions, now })
            }
            _ => None,
        }
    }
}

/// PD's replicated state machine: the placement/liveness registry plus the TSO high-water.
/// Every method is `&self` (interior mutability), so the gRPC handlers can read the routing
/// view and the driver can apply committed entries through the same shared `Arc`.
pub struct PdFsm {
    members: Membership,
    /// The committed TSO high-water — the max timestamp any leader has reserved. Monotonic:
    /// applying a stale/duplicate `ReserveTs` can only ever be a no-op.
    tso_upper: AtomicU64,
}

impl PdFsm {
    pub fn new() -> PdFsm {
        PdFsm { members: Membership::new(), tso_upper: AtomicU64::new(0) }
    }

    /// Apply one committed command. Deterministic given the state + bytes, so replicas that
    /// apply the same log converge. A malformed or empty payload (e.g. an election no-op) is
    /// ignored.
    pub fn apply(&self, bytes: &[u8]) {
        let Some(cmd) = PdCmd::decode(bytes) else { return };
        match cmd {
            // `fetch_max` keeps the watermark monotonic even if entries are re-applied on a
            // restart or arrive out of order relative to a snapshot.
            PdCmd::ReserveTs { upper } => {
                self.tso_upper.fetch_max(upper, Ordering::SeqCst);
            }
            PdCmd::Heartbeat { node_id, address, regions, now } => {
                self.members.heartbeat(node_id, address, regions, now);
            }
        }
    }

    /// The committed TSO high-water — an upper bound on every timestamp handed out so far.
    pub fn tso_upper(&self) -> u64 {
        self.tso_upper.load(Ordering::SeqCst)
    }

    /// Route a key to its owning region + node (leader-served in a running cluster).
    pub fn route(&self, key: &[u8]) -> Option<PlacedRegion> {
        self.members.route(key)
    }

    /// The whole placed-region view.
    pub fn list(&self) -> Vec<PlacedRegion> {
        self.members.list()
    }

    /// The regions currently assigned to `node_id` — what the leader echoes back in a
    /// heartbeat response once the heartbeat has committed.
    pub fn regions_of(&self, node_id: u64) -> Vec<Region> {
        self.members.list().into_iter().filter(|p| p.node_id == node_id).map(|p| p.region).collect()
    }

    /// The underlying membership registry (for the failure-detector sweep + introspection).
    pub fn members(&self) -> &Membership {
        &self.members
    }
}

impl Default for PdFsm {
    fn default() -> Self {
        PdFsm::new()
    }
}

/// What a [`PdReplica::ready`] call surfaces: the node's outbound messages to route, and the
/// log indices that just committed (so a driver can wake proposals parked on those indices).
pub struct Ready {
    pub messages: Vec<Message>,
    pub committed: Vec<u64>,
}

/// One PD replica: a Raft [`RaftNode`] driving the shared [`PdFsm`]. Transport-free — the
/// caller ([`tick`](Self::tick)s, [`step`](Self::step)s, and routes [`ready`](Self::ready)'s
/// outbound messages) — so it runs identically under the deterministic test harness and, later,
/// over gRPC.
pub struct PdReplica {
    node: RaftNode<MemStorage>,
    fsm: Arc<PdFsm>,
    /// The next timestamp this node may hand out locally (leader only). Kept `<= fsm.tso_upper`;
    /// reset to the committed high-water whenever this node wins leadership, so it starts
    /// strictly above every timestamp any prior leader could have issued.
    served: u64,
    was_leader: bool,
}

impl PdReplica {
    /// A replica of the `voters` group with the given `id`, starting as a follower with empty
    /// state (rebuilt from the committed log).
    pub fn new(id: u64, voters: Vec<u64>) -> PdReplica {
        PdReplica {
            node: RaftNode::new(Config::new(id, voters), MemStorage::new()),
            fsm: Arc::new(PdFsm::new()),
            served: 0,
            was_leader: false,
        }
    }

    pub fn id(&self) -> u64 {
        self.node.id()
    }
    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }
    pub fn leader_id(&self) -> Option<u64> {
        self.node.leader_id()
    }
    /// The current Raft term (for the driver's role-transition logging).
    pub fn current_term(&self) -> u64 {
        self.node.current_term()
    }
    /// The shared state machine (clone the `Arc` to share it with gRPC read handlers).
    pub fn fsm(&self) -> &Arc<PdFsm> {
        &self.fsm
    }

    /// Advance the logical clock (drives elections + heartbeats). Drain effects with
    /// [`ready`](Self::ready).
    pub fn tick(&mut self) {
        self.node.tick();
    }

    /// Feed in one inbound Raft message. Drain effects with [`ready`](Self::ready).
    pub fn step(&mut self, m: Message) {
        self.node.step(m);
    }

    /// Propose a command (leader only); the returned index commits once a majority persists it.
    /// `Err(NotLeader)` if this node isn't the leader.
    pub fn propose(&mut self, cmd: &PdCmd) -> Result<u64, ProposeError> {
        self.node.propose(cmd.encode())
    }

    /// Apply anything newly committed into the [`PdFsm`] and return the node's outbound
    /// messages plus the indices that just committed (so a driver can resolve parked
    /// proposals). Call after every [`tick`](Self::tick) / [`step`](Self::step) /
    /// [`propose`](Self::propose).
    pub fn ready(&mut self) -> Ready {
        // On winning an election, append a no-op so prior-term committed entries advance to the
        // commit point (the Figure-8 current-term rule) and reset the volatile TSO cursor to the
        // committed high-water — from here this leader hands out only timestamps strictly above
        // everything any prior leader reserved.
        let leader_now = self.node.is_leader();
        if leader_now && !self.was_leader {
            let _ = self.node.propose(Vec::new());
            self.served = self.fsm.tso_upper();
        }
        self.was_leader = leader_now;

        // Compaction is never requested by this driver yet, so `take_snapshot` cannot fire here
        // (a snapshot only arrives after a peer compacts). Drained defensively to keep the
        // "snapshot supersedes the log" invariant explicit for the transport slice.
        debug_assert!(self.node.take_snapshot().is_none(), "no compaction in the core slice");

        let mut committed = Vec::new();
        for e in self.node.take_committed() {
            committed.push(e.index);
            // Skip the election no-op (empty) and any config-change entry (the core already
            // applied its membership effect); only real commands touch the FSM.
            if e.entry_type == EntryType::Normal && !e.data.is_empty() {
                self.fsm.apply(&e.data);
            }
        }
        Ready { messages: self.node.take_messages(), committed }
    }

    /// Hand out `count` contiguous timestamps locally (leader only), or `None` if the reserved
    /// window is exhausted — the caller must first commit [`reserve_ts_cmd`](Self::reserve_ts_cmd)
    /// to raise the high-water, then retry.
    pub fn hand_out(&mut self, count: u64) -> Option<u64> {
        let count = count.max(1);
        if self.node.is_leader() && self.served + count <= self.fsm.tso_upper() {
            let first = self.served;
            self.served += count;
            Some(first)
        } else {
            None
        }
    }

    /// The `ReserveTs` a leader should propose to guarantee it can then [`hand_out`](Self::hand_out)
    /// at least `count` timestamps: it raises the high-water a whole [`RESERVE_WINDOW`] past the
    /// immediate need, amortizing the Raft round-trip.
    pub fn reserve_ts_cmd(&self, count: u64) -> PdCmd {
        let need = self.served + count.max(1);
        PdCmd::ReserveTs { upper: need.max(self.fsm.tso_upper()) + RESERVE_WINDOW }
    }

    /// The next timestamp this leader would hand out (test/introspection helper).
    pub fn served(&self) -> u64 {
        self.served
    }
}

// --- Region wire codec (length-prefixed, matching `region.rs`'s on-disk shape) ---

fn put_region(out: &mut Vec<u8>, r: &Region) {
    out.extend_from_slice(&r.id.to_be_bytes());
    out.extend_from_slice(&r.epoch.to_be_bytes());
    put_bytes(out, &r.start);
    put_bytes(out, &r.end);
}

fn get_region(buf: &[u8], pos: &mut usize) -> Option<Region> {
    let id = get_u64(buf, pos)?;
    let epoch = get_u64(buf, pos)?;
    let start = get_bytes(buf, pos)?.to_vec();
    let end = get_bytes(buf, pos)?.to_vec();
    Some(Region { id, start, end, epoch })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region(id: u64, start: &[u8], end: &[u8], epoch: u64) -> Region {
        Region { id, start: start.to_vec(), end: end.to_vec(), epoch }
    }

    #[test]
    fn cmd_round_trips() {
        let cmds = vec![
            PdCmd::ReserveTs { upper: 1 << 40 },
            PdCmd::Heartbeat {
                node_id: 7,
                address: "http://127.0.0.1:50051".into(),
                regions: vec![region(1, b"", b"m", 2), region(9, b"m", b"", 2)],
                now: 123_456,
            },
            PdCmd::Heartbeat { node_id: 3, address: String::new(), regions: vec![], now: 0 },
        ];
        for c in cmds {
            assert_eq!(PdCmd::decode(&c.encode()), Some(c));
        }
        // Garbage decodes to None, not a panic.
        assert_eq!(PdCmd::decode(&[]), None);
        assert_eq!(PdCmd::decode(&[9, 9, 9]), None);
    }

    #[test]
    fn fsm_applies_reserve_and_heartbeat() {
        let fsm = PdFsm::new();
        fsm.apply(&PdCmd::ReserveTs { upper: 1000 }.encode());
        assert_eq!(fsm.tso_upper(), 1000);
        // A lower reservation never regresses the watermark.
        fsm.apply(&PdCmd::ReserveTs { upper: 500 }.encode());
        assert_eq!(fsm.tso_upper(), 1000);

        fsm.apply(
            &PdCmd::Heartbeat {
                node_id: 7,
                address: "http://a".into(),
                regions: vec![region(1, b"", b"", 1)],
                now: 100,
            }
            .encode(),
        );
        let p = fsm.route(b"anything").unwrap();
        assert_eq!((p.node_id, p.address.as_str()), (7, "http://a"));
    }

    #[test]
    fn single_node_commits_and_serves_timestamps() {
        // A one-node PD group self-elects, then reserves + hands out timestamps through Raft.
        let mut r = PdReplica::new(1, vec![1]);
        for _ in 0..40 {
            r.tick();
            let _ = r.ready();
            if r.is_leader() {
                break;
            }
        }
        assert!(r.is_leader());

        // Exhausted before any reservation.
        assert_eq!(r.hand_out(1), None);
        let cmd = r.reserve_ts_cmd(10);
        r.propose(&cmd).unwrap();
        let _ = r.ready(); // single-node group commits immediately
        assert!(r.fsm().tso_upper() >= 10);

        let first = r.hand_out(5).expect("window reserved");
        let next = r.hand_out(5).unwrap();
        assert_eq!(next, first + 5, "contiguous, strictly increasing");
        assert!(next + 5 <= r.fsm().tso_upper());
    }
}

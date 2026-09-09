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

use arcux_raft::{Config, EntryType, MemStorage, Message, ProposeError, RaftNode, Storage};

use crate::persist::{get_bytes, get_u32, get_u64, put_bytes};
use crate::cluster::{regime_of, regime_tag};
use crate::{Membership, PlacedRegion, Regime, Region, ReplicaSet, TableConflict};

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
    Heartbeat {
        node_id: u64,
        address: String,
        regions: Vec<ReplicaSet>,
        /// The tables this node has declared, so PD holds the cluster-wide catalog view.
        tables: Vec<(String, Regime)>,
        now: u64,
    },
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
            PdCmd::Heartbeat { node_id, address, regions, tables, now } => {
                out.push(2);
                out.extend_from_slice(&node_id.to_be_bytes());
                out.extend_from_slice(&now.to_be_bytes());
                put_bytes(&mut out, address.as_bytes());
                out.extend_from_slice(&(regions.len() as u32).to_be_bytes());
                for rs in regions {
                    put_replica_set(&mut out, rs);
                }
                out.extend_from_slice(&(tables.len() as u32).to_be_bytes());
                for (name, regime) in tables {
                    put_bytes(&mut out, name.as_bytes());
                    out.push(regime_tag(*regime));
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
                    regions.push(get_replica_set(bytes, &mut pos)?);
                }
                let t = get_u32(bytes, &mut pos)? as usize;
                let mut tables = Vec::with_capacity(t);
                for _ in 0..t {
                    let name = String::from_utf8(get_bytes(bytes, &mut pos)?.to_vec()).ok()?;
                    let regime = regime_of(*bytes.get(pos)?)?;
                    pos += 1;
                    tables.push((name, regime));
                }
                Some(PdCmd::Heartbeat { node_id, address, regions, tables, now })
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
            PdCmd::Heartbeat { node_id, address, regions, tables, now } => {
                self.members.heartbeat(node_id, address, regions, tables, now);
            }
        }
    }

    /// The committed TSO high-water — an upper bound on every timestamp handed out so far.
    pub fn tso_upper(&self) -> u64 {
        self.tso_upper.load(Ordering::SeqCst)
    }

    /// The cluster-wide catalog — every table any node has declared, name-sorted.
    pub fn tables(&self) -> Vec<(String, Regime)> {
        self.members.tables()
    }

    /// Tables two nodes describe with different regimes: a misconfigured cluster.
    pub fn table_conflicts(&self) -> Vec<TableConflict> {
        self.members.table_conflicts()
    }

    /// Serialize the whole applied state, for a Raft snapshot at the compaction point.
    ///
    /// `tso_upper` is the field that **must** be exact: it is what stops a new leader reissuing
    /// a timestamp a previous leader already served, so losing it is a Snapshot Isolation
    /// violation. Membership is included for completeness — it would self-heal from the next
    /// round of heartbeats, but a snapshot that quietly omits state is a trap for later.
    pub fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.tso_upper().to_be_bytes());
        self.members.encode_into(&mut out);
        out
    }

    /// Adopt a snapshot's state, replacing what this replica had — used when a follower that
    /// fell behind the leader's log installs one. `fetch_max` on the watermark keeps it
    /// monotonic even against an older snapshot arriving late.
    pub fn restore(&self, bytes: &[u8]) -> bool {
        if bytes.len() < 8 {
            return false;
        }
        let upper = u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"));
        let Some(()) = self.members.decode_from(&bytes[8..]) else { return false };
        self.tso_upper.fetch_max(upper, Ordering::SeqCst);
        true
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
    pub fn regions_of(&self, node_id: u64) -> Vec<ReplicaSet> {
        self.members
            .list()
            .into_iter()
            .filter(|p| p.node_id == node_id)
            .map(|p| ReplicaSet { region: p.region, regime: p.regime, voters: p.voters })
            .collect()
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
/// How far the log may run past the last snapshot before this replica compacts. Mirrors the
/// region groups' threshold (`server/src/raft_group.rs`); PD's state is small, so a snapshot is
/// cheap and there is no reason to let the log grow.
const COMPACT_THRESHOLD: u64 = 64;

/// Generic over its [`Storage`] so the deterministic harness keeps running in memory (real
/// fsyncs would make `pd/tests/raft_pd.rs` slow and less deterministic, which is the whole point
/// of that harness) while a real node runs on a durable log.
pub struct PdReplica<S: Storage = MemStorage> {
    node: RaftNode<S>,
    fsm: Arc<PdFsm>,
    /// The next timestamp this node may hand out locally (leader only). Kept `<= fsm.tso_upper`;
    /// reset to the committed high-water whenever this node wins leadership, so it starts
    /// strictly above every timestamp any prior leader could have issued.
    served: u64,
    was_leader: bool,
    /// Index of the no-op appended on winning leadership. While set, this replica has won but
    /// has not yet applied its own term's entries, so the TSO cursor reset is still pending.
    pending_reset: Option<u64>,
}

impl PdReplica<MemStorage> {
    /// A replica of the `voters` group with the given `id`, starting as a follower with empty
    /// state (rebuilt from the committed log). **In-memory** — for the deterministic tests; a
    /// real node wants [`with_storage`](PdReplica::with_storage) so its state survives a restart.
    pub fn new(id: u64, voters: Vec<u64>) -> PdReplica<MemStorage> {
        PdReplica::with_storage(id, voters, MemStorage::new())
    }
}

impl<S: Storage> PdReplica<S> {
    /// A replica backed by `storage`. With a durable one, the committed TSO high-water and
    /// placement survive a full-cluster restart — which the replicated **catalog** will depend
    /// on, since unlike placement it is not rebuilt by the next round of heartbeats.
    pub fn with_storage(id: u64, voters: Vec<u64>, storage: S) -> PdReplica<S> {
        let node = RaftNode::new(Config::new(id, voters), storage);
        let fsm = Arc::new(PdFsm::new());
        // Rebuild the applied state from whatever the log/snapshot already held.
        let mut replica = PdReplica { node, fsm, served: 0, was_leader: false, pending_reset: None };
        replica.recover();
        replica
    }

    /// Seed the FSM from a snapshot durable storage already holds. Only the snapshot — the log
    /// entries above it replay through the normal [`ready`](Self::ready) path once this node
    /// learns the commit index again (a restarted node's commit index starts at 0 and is
    /// re-established by an election or the leader). The snapshot has no such path: its entries
    /// were compacted away, so nothing will ever re-deliver them.
    fn recover(&mut self) {
        let Some(snap) = self.node.storage().snapshot() else { return };
        self.fsm.restore(&snap.data);
        self.served = self.fsm.tso_upper();
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
    /// The first index the log still holds — `snapshot index + 1`, so it advances past 1 once
    /// this replica has compacted.
    pub fn first_index(&self) -> u64 {
        self.node.first_index()
    }

    pub fn propose(&mut self, cmd: &PdCmd) -> Result<u64, ProposeError> {
        self.node.propose(cmd.encode())
    }

    /// Apply anything newly committed into the [`PdFsm`] and return the node's outbound
    /// messages plus the indices that just committed (so a driver can resolve parked
    /// proposals). Call after every [`tick`](Self::tick) / [`step`](Self::step) /
    /// [`propose`](Self::propose).
    pub fn ready(&mut self) -> Ready {
        // On winning an election, append a no-op so prior-term committed entries advance to the
        // commit point (the Figure-8 current-term rule). The TSO cursor is reset to the committed
        // high-water too, but *not here* — see `pending_reset` below.
        let leader_now = self.node.is_leader();
        if leader_now && !self.was_leader {
            self.pending_reset = self.node.propose(Vec::new()).ok();
        }
        if !leader_now {
            self.pending_reset = None; // lost the election race; a later win re-arms it
        }
        self.was_leader = leader_now;

        // A snapshot installed by the leader supersedes this replica's log, so adopt its state
        // before applying anything above it (this fires when a follower fell far enough behind
        // that the leader had already compacted the entries it needed).
        if let Some((_index, data)) = self.node.take_snapshot() {
            self.fsm.restore(&data);
            self.served = self.served.max(self.fsm.tso_upper());
        }

        let mut committed = Vec::new();
        for e in self.node.take_committed() {
            committed.push(e.index);
            // Skip the election no-op (empty) and any config-change entry (the core already
            // applied its membership effect); only real commands touch the FSM.
            if e.entry_type == EntryType::Normal && !e.data.is_empty() {
                self.fsm.apply(&e.data);
            }
        }

        // Now that this term's entries are applied, jump the TSO cursor to the committed
        // high-water, so this leader hands out only timestamps strictly above everything any
        // prior leader reserved (its unused tail is discarded).
        //
        // Deferred until the no-op commits rather than done on the win itself: with a durable
        // log, a restarted node's replayed entries have not been applied at the moment it wins,
        // so `tso_upper` still reads 0 there and the cursor would be reset *below* timestamps
        // this node already served before the restart — reissuing them. Invisible on
        // `MemStorage`, where a restart leaves nothing to replay.
        if let Some(idx) = self.pending_reset {
            if self.node.last_applied() >= idx {
                self.served = self.fsm.tso_upper();
                self.pending_reset = None;
            }
        }

        // Bound the log: without this a durable log grows forever and a restart replays all of
        // it. Same blunt length trigger the region groups use (`server/src/raft_group.rs`); a
        // size-/time-based policy is a tracked deferral there and here.
        if self.node.last_applied() + 1 >= self.node.first_index() + COMPACT_THRESHOLD {
            self.node.compact(self.node.last_applied(), self.fsm.snapshot());
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

fn put_replica_set(out: &mut Vec<u8>, rs: &ReplicaSet) {
    out.extend_from_slice(&rs.region.id.to_be_bytes());
    out.extend_from_slice(&rs.region.epoch.to_be_bytes());
    put_bytes(out, &rs.region.start);
    put_bytes(out, &rs.region.end);
    out.push(regime_tag(rs.regime));
    out.extend_from_slice(&(rs.voters.len() as u32).to_be_bytes());
    for v in &rs.voters {
        out.extend_from_slice(&v.to_be_bytes());
    }
}

fn get_replica_set(buf: &[u8], pos: &mut usize) -> Option<ReplicaSet> {
    let id = get_u64(buf, pos)?;
    let epoch = get_u64(buf, pos)?;
    let start = get_bytes(buf, pos)?.to_vec();
    let end = get_bytes(buf, pos)?.to_vec();
    let regime = regime_of(*buf.get(*pos)?)?;
    *pos += 1;
    let mut voters = Vec::new();
    for _ in 0..get_u32(buf, pos)? {
        voters.push(get_u64(buf, pos)?);
    }
    Some(ReplicaSet { region: Region { id, start, end, epoch }, regime, voters })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region(id: u64, start: &[u8], end: &[u8], epoch: u64) -> Region {
        Region { id, start: start.to_vec(), end: end.to_vec(), epoch }
    }

    /// Drive a single-node replica until it wins its own election.
    fn elect<S: Storage>(r: &mut PdReplica<S>) {
        for _ in 0..40 {
            r.tick();
            let _ = r.ready();
            if r.is_leader() {
                return;
            }
        }
        panic!("single-node replica never became leader");
    }

    #[test]
    fn fsm_snapshot_round_trips_watermark_and_membership() {
        let a = PdFsm::new();
        a.apply(&PdCmd::ReserveTs { upper: 4242 }.encode());
        a.apply(
            &PdCmd::Heartbeat {
                node_id: 7,
                address: "http://n7".into(),
                regions: vec![region(1, b"", b"m", 3), region(2, b"m", b"", 3)]
                    .into_iter()
                    .map(ReplicaSet::bare)
                    .collect(),
                tables: vec![],
                now: 1_000,
            }
            .encode(),
        );

        let b = PdFsm::new();
        assert!(b.restore(&a.snapshot()));
        assert_eq!(b.tso_upper(), 4242);
        assert_eq!(b.list().len(), 2, "placement survives the snapshot");
        assert_eq!(b.route(b"z").map(|p| p.node_id), Some(7));
    }

    #[test]
    fn restore_never_regresses_the_watermark() {
        // An older snapshot arriving late must not walk the high-water backwards — that is what
        // stops a leader reissuing a timestamp it already served.
        let fsm = PdFsm::new();
        fsm.apply(&PdCmd::ReserveTs { upper: 9_000 }.encode());

        let older = PdFsm::new();
        older.apply(&PdCmd::ReserveTs { upper: 100 }.encode());
        assert!(fsm.restore(&older.snapshot()));
        assert_eq!(fsm.tso_upper(), 9_000);
    }

    #[test]
    fn a_truncated_snapshot_is_rejected_rather_than_half_applied() {
        let fsm = PdFsm::new();
        fsm.apply(&PdCmd::ReserveTs { upper: 500 }.encode());
        assert!(!fsm.restore(&[]));
        assert!(!fsm.restore(&[0u8; 4]));
        assert_eq!(fsm.tso_upper(), 500, "a bad image leaves the state untouched");
    }

    #[test]
    fn the_committed_watermark_survives_a_restart() {
        // The reason PD needs a durable log: on MemStorage this replica came back at zero and a
        // new leader could reissue a timestamp a previous one had already handed out.
        let dir = tempfile::tempdir().unwrap();
        let issued;
        let reserved;
        {
            let storage = arcux_raft_wal::WalStorage::open(dir.path()).unwrap();
            let mut r = PdReplica::with_storage(1, vec![1], storage);
            elect(&mut r);
            let cmd = r.reserve_ts_cmd(10);
            r.propose(&cmd).unwrap();
            let _ = r.ready();
            reserved = r.fsm().tso_upper();
            issued = r.hand_out(5).expect("window reserved");
            assert!(reserved >= 10);
        }

        let storage = arcux_raft_wal::WalStorage::open(dir.path()).unwrap();
        let mut r = PdReplica::with_storage(1, vec![1], storage);
        elect(&mut r);
        assert_eq!(r.fsm().tso_upper(), reserved, "the reserved high-water survived");
        assert!(
            r.hand_out(1).map(|ts| ts >= issued + 5).unwrap_or(true),
            "a restarted leader never reissues a timestamp it already served"
        );
    }

    #[test]
    fn the_log_is_compacted_and_its_snapshot_carries_the_state() {
        let dir = tempfile::tempdir().unwrap();
        let reserved;
        {
            let storage = arcux_raft_wal::WalStorage::open(dir.path()).unwrap();
            let mut r = PdReplica::with_storage(1, vec![1], storage);
            elect(&mut r);

            let hb = PdCmd::Heartbeat {
                node_id: 7,
                address: "http://n7".into(),
                regions: vec![region(1, b"", b"", 1)]
                    .into_iter()
                    .map(ReplicaSet::bare)
                    .collect(),
                tables: vec![],
                now: 1_000,
            };
            r.propose(&hb).unwrap();
            let _ = r.ready();

            // Enough reservations to cross COMPACT_THRESHOLD.
            for _ in 0..(COMPACT_THRESHOLD + 8) {
                let cmd = r.reserve_ts_cmd(4);
                r.propose(&cmd).unwrap();
                let _ = r.ready();
            }
            reserved = r.fsm().tso_upper();
            assert!(r.first_index() > 1, "the log was compacted, not grown without bound");
        }

        // Reopened: the entries below the compaction point are gone from the log, so whatever
        // state is present before this replica applies anything came from the snapshot.
        let storage = arcux_raft_wal::WalStorage::open(dir.path()).unwrap();
        let mut r = PdReplica::with_storage(1, vec![1], storage);
        let from_snapshot = r.fsm().tso_upper();
        assert!(from_snapshot > 0, "the snapshot alone seeded the watermark");
        assert_eq!(r.fsm().route(b"k").map(|p| p.node_id), Some(7), "and the placement view");

        // The tail above the snapshot replays once the node re-learns its commit index.
        elect(&mut r);
        assert_eq!(r.fsm().tso_upper(), reserved, "snapshot plus tail restores the full state");
    }

    #[test]
    fn cmd_round_trips() {
        let cmds = vec![
            PdCmd::ReserveTs { upper: 1 << 40 },
            PdCmd::Heartbeat {
                node_id: 7,
                address: "http://127.0.0.1:50051".into(),
                regions: vec![region(1, b"", b"m", 2), region(9, b"m", b"", 2)]
                    .into_iter()
                    .map(ReplicaSet::bare)
                    .collect(),
                tables: vec![],
                now: 123_456,
            },
            PdCmd::Heartbeat {
                node_id: 3,
                address: String::new(),
                regions: vec![],
                tables: vec![],
                now: 0,
            },
            // The v14 shape: a regime, a replica set, and declared tables all on the wire.
            PdCmd::Heartbeat {
                node_id: 4,
                address: "http://n4".into(),
                regions: vec![ReplicaSet {
                    region: region(2, b"a", b"z", 7),
                    regime: Regime::Ap,
                    voters: vec![4, 5, 6],
                }],
                tables: vec![
                    ("events".to_string(), Regime::Ap),
                    ("ledger".to_string(), Regime::Cp),
                ],
                now: 99,
            },
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
                regions: vec![region(1, b"", b"", 1)]
                    .into_iter()
                    .map(ReplicaSet::bare)
                    .collect(),
                tables: vec![],
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

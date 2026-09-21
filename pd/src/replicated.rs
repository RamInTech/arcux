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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use arcux_raft::{Config, EntryType, MemStorage, Message, ProposeError, RaftNode, Storage};

use crate::persist::{get_bytes, get_u32, get_u64, put_bytes};
use std::sync::Mutex;

use crate::cluster::{regime_of, regime_tag, Table};
use crate::region::{
    table_id_of, table_prefix, RegionRegistry, TableId, DEFAULT_TABLE_ID, DEFAULT_TABLE_NAME,
};
use crate::{Membership, PlacedRegion, Regime, Region, ReplicaSet};

/// How far ahead of the current need a `ReserveTs` raises the high-water. Larger ⇒ fewer Raft
/// round-trips on the timestamp path, at the cost of more timestamps skipped on a failover
/// (the new leader resumes at the committed `upper`, discarding the leader's unused tail).
/// Mirrors the single-process oracle's window ([`crate::Tso`]).
const RESERVE_WINDOW: u64 = 1 << 16;

/// How many voters PD grows each region to, unless `--replicas` says otherwise. TiKV's default.
pub const DEFAULT_REPLICAS: usize = 3;

/// A command in PD's replicated log. Encoded into a Raft entry's opaque `data`; every replica
/// decodes and applies the identical sequence, so their [`PdFsm`]s stay bit-for-bit in sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PdCmd {
    /// Raise the replicated TSO high-water to `upper`. The leader commits this **before**
    /// serving any timestamp `< upper`, so the committed watermark is always an upper bound on
    /// every timestamp handed out — and a new leader resuming from it never regresses.
    ReserveTs { upper: u64 },
    /// Declare a table cluster-wide. Applying it allocates the table's id, carves its range out
    /// of PD's region table (one `RegionRegistry::split` of the tail, which is empty by
    /// construction), and bumps the catalog version. Idempotent: a name already declared is a
    /// no-op, which matters because an entry can be re-applied after a restart — and is why the
    /// name check comes before the allocation, so a replay cannot burn an id.
    DeclareTable { name: String, regime: Regime },
    /// Record a data node's heartbeat: its serving `address`, the `regions` it owns, and the
    /// wall-clock `now` (ms, for liveness). Applied via [`Membership::heartbeat`] on every
    /// replica, giving one shared placement + liveness view.
    Heartbeat {
        node_id: u64,
        address: String,
        regions: Vec<ReplicaSet>,
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
            PdCmd::DeclareTable { name, regime } => {
                out.push(3);
                put_bytes(&mut out, name.as_bytes());
                out.push(regime_tag(*regime));
            }
            PdCmd::Heartbeat { node_id, address, regions, now } => {
                out.push(2);
                out.extend_from_slice(&node_id.to_be_bytes());
                out.extend_from_slice(&now.to_be_bytes());
                put_bytes(&mut out, address.as_bytes());
                out.extend_from_slice(&(regions.len() as u32).to_be_bytes());
                for rs in regions {
                    put_replica_set(&mut out, rs);
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
            3 => {
                let name = String::from_utf8(get_bytes(bytes, &mut pos)?.to_vec()).ok()?;
                let regime = regime_of(*bytes.get(pos)?)?;
                Some(PdCmd::DeclareTable { name, regime })
            }
            2 => {
                let node_id = get_u64(bytes, &mut pos)?;
                let now = get_u64(bytes, &mut pos)?;
                let address = String::from_utf8(get_bytes(bytes, &mut pos)?.to_vec()).ok()?;
                let n = get_u32(bytes, &mut pos)? as usize;
                let mut regions = Vec::with_capacity(n);
                for _ in 0..n {
                    regions.push(get_replica_set(bytes, &mut pos)?);
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
    /// The **authoritative** region table for a catalog cluster. In-memory and path-free, so it
    /// is a pure deterministic state machine: every replica applying the same log carves
    /// identically, and `split`'s monotonic `next_id` gives ids that stay stable across a
    /// declaration instead of renumbering the way a positional tiling would.
    regions: RegionRegistry,
    /// Declared tables, in declaration order. A region's regime is *derived* from this by the
    /// table id its start key carries, rather than stored per region, so there is one source of
    /// truth.
    catalog: Mutex<Vec<Table>>,
    /// The next table id to hand out. Bumped inside [`PdFsm::apply`], never by the proposing
    /// leader: the log order is what makes every replica allocate identically, and it is what
    /// stops a re-proposal after a lost election from issuing one id twice. Same discipline as
    /// [`RegionRegistry`]'s `next_id`.
    next_table_id: AtomicU32,
    /// Bumped on every accepted declaration. Nodes ignore an assignment carrying a version they
    /// have already applied, so a reordered or replayed response cannot walk them back.
    version: AtomicU64,
    /// Each region's **actual** Raft voters, by region id.
    ///
    /// Per region, because a Raft group's membership is fixed when it is founded and changes only
    /// by membership change. One global list recomputed on every heartbeat — what this replaced —
    /// let the same region be founded with different voters depending on when each node happened
    /// to reconcile, and three nodes could each found region 1 alone.
    ///
    /// Set once, inside `apply`, when a region is first founded; after that it only **grows**, when
    /// the region's leader reports a larger voter set it has reached by membership change.
    region_members: Mutex<BTreeMap<u64, Vec<u64>>>,
    /// The voter count each region is grown toward. Configuration, not replicated state — every
    /// PD replica must be started with the same value, as with its topology.
    replicas: AtomicUsize,
}

impl PdFsm {
    pub fn new() -> PdFsm {
        PdFsm {
            members: Membership::new(),
            tso_upper: AtomicU64::new(0),
            // Seeded with one whole-keyspace region, which is exactly what a catalog node with no
            // tables tiles to — so PD and a fresh node agree before anything is declared, and
            // `split` has a region to carve from.
            regions: RegionRegistry::in_memory(),
            // `default` exists from the first instant: its id is fixed, so it needs no allocation,
            // and the seeded whole-keyspace region above *is* its region until a table is carved
            // off the tail. That makes an omitted table name resolve on a cluster that has never
            // had a declaration.
            catalog: Mutex::new(vec![Table {
                id: DEFAULT_TABLE_ID,
                name: DEFAULT_TABLE_NAME.to_string(),
                regime: Regime::Cp,
            }]),
            next_table_id: AtomicU32::new(DEFAULT_TABLE_ID + 1),
            // 1, not 0: a node treats version 0 as "PD holds no catalog, nothing to adopt", which
            // is true only of the single-process PD. This PD has `default` from the start.
            version: AtomicU64::new(1),
            region_members: Mutex::new(BTreeMap::new()),
            replicas: AtomicUsize::new(DEFAULT_REPLICAS),
        }
    }

    /// Set the replication target (`arcux-pd --replicas N`). At least 1.
    pub fn set_replicas(&self, n: usize) {
        self.replicas.store(n.max(1), Ordering::SeqCst);
    }

    /// The replication target every region is grown to. Public so the service can send it with
    /// an assignment: a node measuring a region it founded against `desired` alone cannot tell a
    /// complete one-node cluster from the first node of three.
    pub fn replicas(&self) -> usize {
        self.replicas.load(Ordering::SeqCst)
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
                let is_new = !self.members.node_addrs().iter().any(|(id, _)| *id == node_id);
                let grown = self.adopt_grown_voters(node_id, &regions);
                self.members.heartbeat(node_id, address, regions, now);
                let founded = self.found_unfounded_regions();
                // A new node changes every region's desired set; a grown or founded region changes
                // its members. Any of them is a new assignment, and bumping the version is what
                // makes each node's version-gated reconcile pick it up.
                if is_new || grown || founded {
                    self.version.fetch_add(1, Ordering::SeqCst);
                }
            }
            PdCmd::DeclareTable { name, regime } => {
                self.declare_table(name, regime);
                // The carved region has no members yet: found it with the live nodes, so a table
                // created on a formed cluster starts fully replicated instead of growing into it.
                self.found_unfounded_regions();
            }
        }
    }

    /// Give every region with no members its founding set: the first `replicas` live nodes, in
    /// node-id order. Run inside `apply`, so every PD replica picks the same set.
    ///
    /// On a fresh cluster the first node to register is the only live one, so it founds `default`
    /// **alone** — the bootstrap TiKV and CockroachDB use too — and the region grows as more
    /// nodes register. Returns whether anything was founded.
    fn found_unfounded_regions(&self) -> bool {
        let live = self.members.live_nodes();
        if live.is_empty() {
            return false;
        }
        let founding: Vec<u64> = live.into_iter().take(self.replicas()).collect();
        let mut members = self.region_members.lock().expect("members poisoned");
        let mut founded = false;
        for region in self.regions.list() {
            let entry = members.entry(region.id).or_default();
            if entry.is_empty() {
                *entry = founding.clone();
                founded = true;
            }
        }
        founded
    }

    /// Adopt a larger voter set a node reports for a region it leads (a node reports voters only
    /// for regions it leads, so this is the group's own committed view).
    ///
    /// Only **growth** is taken: the report must contain every current member plus at least one
    /// more. Removing a voter is not something this path does, so a report that drops one is a
    /// stale view and is ignored rather than trusted. Returns whether anything grew.
    fn adopt_grown_voters(&self, node_id: u64, reported: &[ReplicaSet]) -> bool {
        let known: Vec<u64> = self.regions.list().iter().map(|r| r.id).collect();
        let mut members = self.region_members.lock().expect("members poisoned");
        let mut grown = false;
        for rs in reported {
            if !rs.voters.contains(&node_id) || !known.contains(&rs.region.id) {
                continue;
            }
            let current = members.entry(rs.region.id).or_default();
            if rs.voters.len() > current.len() && current.iter().all(|v| rs.voters.contains(v)) {
                let mut voters = rs.voters.clone();
                voters.sort_unstable();
                *current = voters;
                grown = true;
            }
        }
        grown
    }

    /// The voter set PD wants for a region: its members, plus live nodes not yet among them, in
    /// node-id order, until there are `replicas` of them. Never smaller than the members —
    /// removing a voter is out of this path's scope.
    fn desired(&self, members: &[u64], live: &[u64]) -> Vec<u64> {
        let mut desired = members.to_vec();
        for id in live {
            if desired.len() >= self.replicas() {
                break;
            }
            if !desired.contains(id) {
                desired.push(*id);
            }
        }
        desired
    }

    /// Allocate the table an id and carve its range out of the region table. Deterministic and
    /// idempotent, as every `PdFsm::apply` must be.
    ///
    /// **One split, and the carved range is empty by construction.** Ids are handed out in
    /// order, so the new table's range sits above every id ever issued — inside the region that
    /// runs to `+inf`, which holds no key that could belong to it. Splitting the tail at
    /// `be32(id)` therefore needs no emptiness check, and leaves:
    ///
    /// ```text
    /// ["", be32(1))  [be32(1), be32(2))  …  [be32(n), be32(n+1))  [be32(n+1), +inf)
    ///  default        table 1                table n               the new table
    /// ```
    ///
    /// Every region is a table and every table is a region: no untabled gap can exist.
    fn declare_table(&self, name: String, regime: Regime) {
        if name.is_empty() || name == DEFAULT_TABLE_NAME {
            return; // both name the built-in default table, which is never re-declarable
        }
        let id = {
            let mut catalog = self.catalog.lock().expect("catalog poisoned");
            // Before the allocation, not after: a committed entry re-applied on restart must not
            // burn an id, or replicas that did not replay would disagree about every later table.
            if catalog.iter().any(|t| t.name == name) {
                return;
            }
            let id = self.next_table_id.load(Ordering::SeqCst);
            if id == TableId::MAX {
                return; // exhausted — refuse rather than wrap onto `default`
            }
            self.next_table_id.store(id + 1, Ordering::SeqCst);
            catalog.push(Table { id, name, regime });
            id
        };

        let _ = self.regions.split(&table_prefix(id));
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    /// PD's catalog version — monotonic, bumped per accepted declaration.
    pub fn catalog_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// The authoritative region table, each region tagged with its derived regime, its actual
    /// voters, and the voters PD wants it to grow to. This is what a node reconciles against.
    pub fn assignment(&self) -> Vec<ReplicaSet> {
        let live = self.members.live_nodes();
        let members = self.region_members.lock().expect("members poisoned").clone();
        self.regions
            .list()
            .into_iter()
            .map(|region| {
                let regime = self.regime_for(&region.start);
                let voters = members.get(&region.id).cloned().unwrap_or_default();
                let desired = self.desired(&voters, &live);
                ReplicaSet { region, regime, voters, desired }
            })
            .collect()
    }

    /// A key's regime, read off the table id its prefix carries. A key shorter than the prefix
    /// belongs to `default` (see [`table_id_of`]), and an id with no catalog entry is `Cp` —
    /// strong by default, the same answer an undeclared range has always given.
    pub fn regime_for(&self, key: &[u8]) -> Regime {
        let id = table_id_of(key);
        self.catalog
            .lock()
            .expect("catalog poisoned")
            .iter()
            .find(|t| t.id == id)
            .map(|t| t.regime)
            .unwrap_or(Regime::Cp)
    }

    /// The tables PD holds, name-sorted. Always includes `default`.
    pub fn catalog(&self) -> Vec<Table> {
        let mut out = self.catalog.lock().expect("catalog poisoned").clone();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// The id allocated to `name`, if it is declared. How a parked declaration finds the region
    /// it carved, once the entry that allocated the id has been applied.
    pub fn table_id(&self, name: &str) -> Option<TableId> {
        let name = if name.is_empty() { DEFAULT_TABLE_NAME } else { name };
        self.catalog.lock().expect("catalog poisoned").iter().find(|t| t.name == name).map(|t| t.id)
    }

    /// The next id this PD would hand out — the allocator's high-water, for tests and snapshots.
    pub fn next_table_id(&self) -> TableId {
        self.next_table_id.load(Ordering::SeqCst)
    }

    /// The committed TSO high-water — an upper bound on every timestamp handed out so far.
    pub fn tso_upper(&self) -> u64 {
        self.tso_upper.load(Ordering::SeqCst)
    }

    /// Every node PD knows about and where to reach it — the voter addresses it hands out with
    /// an assignment, so a node can found a region with peers it was never given at startup.
    pub fn node_addrs(&self) -> Vec<(u64, String)> {
        self.members.node_addrs()
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

        // The region table, catalog, version and voter set are *not* self-healing the way
        // placement is — no heartbeat rebuilds them — so a snapshot that omitted them would lose
        // the cluster's tables the moment the log was compacted.
        out.extend_from_slice(&self.catalog_version().to_be_bytes());
        // The allocator's high-water rides with the catalog it hands ids to. It goes *here*, not
        // at the end: `restore` finishes by handing the rest of the buffer to the region table,
        // so anything appended after that would be read as region bytes.
        out.extend_from_slice(&self.next_table_id().to_be_bytes());
        let catalog = self.catalog.lock().expect("catalog poisoned").clone();
        out.extend_from_slice(&(catalog.len() as u32).to_be_bytes());
        for t in &catalog {
            put_bytes(&mut out, t.name.as_bytes());
            out.push(regime_tag(t.regime));
            out.extend_from_slice(&t.id.to_be_bytes());
        }
        // Each region's members, in the slot the old single voter list used: `restore` hands the
        // rest of the buffer to the region table, so nothing can go after it.
        let members = self.region_members.lock().expect("members poisoned").clone();
        out.extend_from_slice(&(members.len() as u32).to_be_bytes());
        for (region, voters) in &members {
            out.extend_from_slice(&region.to_be_bytes());
            out.extend_from_slice(&(voters.len() as u32).to_be_bytes());
            for v in voters {
                out.extend_from_slice(&v.to_be_bytes());
            }
        }
        self.regions.encode_into(&mut out);
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
        let Some(mut pos) = self.members.decode_from(&bytes[8..]) else { return false };
        pos += 8; // members' slice started at 8

        let Some(version) = get_u64(bytes, &mut pos) else { return false };
        let Some(next_table_id) = get_u32(bytes, &mut pos) else { return false };
        let Some(n) = get_u32(bytes, &mut pos) else { return false };
        let mut catalog = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let Some(name) = get_bytes(bytes, &mut pos).and_then(|b| String::from_utf8(b.to_vec()).ok())
            else {
                return false;
            };
            let Some(regime) = bytes.get(pos).copied().and_then(regime_of) else { return false };
            pos += 1;
            let Some(id) = get_u32(bytes, &mut pos) else { return false };
            catalog.push(Table { id, name, regime });
        }
        let Some(n) = get_u32(bytes, &mut pos) else { return false };
        let mut members = BTreeMap::new();
        for _ in 0..n {
            let Some(region) = get_u64(bytes, &mut pos) else { return false };
            let Some(len) = get_u32(bytes, &mut pos) else { return false };
            let mut voters = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let Some(v) = get_u64(bytes, &mut pos) else { return false };
                voters.push(v);
            }
            members.insert(region, voters);
        }
        if !self.regions.decode_from(&bytes[pos..]) {
            return false;
        }

        *self.catalog.lock().expect("catalog poisoned") = catalog;
        *self.region_members.lock().expect("members poisoned") = members;
        self.version.fetch_max(version, Ordering::SeqCst);
        // `fetch_max`, like the watermark above: an older snapshot arriving late may leak ids,
        // but the allocator must never regress and reissue one already in use.
        self.next_table_id.fetch_max(next_table_id, Ordering::SeqCst);
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
    /// What `node_id` should be hosting. Once anything has been declared, this is PD's
    /// **authoritative** region table filtered to the regions this node votes for. Before that
    /// (version 0 — a plain PD cluster with no catalog) it falls back to echoing the node's own
    /// report, which is the Phase-3b behaviour and must not change.
    pub fn assignment_for(&self, node_id: u64) -> Vec<ReplicaSet> {
        if self.catalog_version() == 0 {
            return self.regions_of(node_id);
        }
        // A node is sent every region it is a voter of, or one PD wants it to join.
        self.assignment()
            .into_iter()
            .filter(|rs| rs.voters.contains(&node_id) || rs.desired.contains(&node_id))
            .collect()
    }

    pub fn regions_of(&self, node_id: u64) -> Vec<ReplicaSet> {
        self.members
            .list()
            .into_iter()
            .filter(|p| p.node_id == node_id)
            .map(|p| ReplicaSet {
                region: p.region,
                regime: p.regime,
                voters: p.voters,
                desired: Vec::new(),
            })
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
    Some(ReplicaSet { region: Region { id, start, end, epoch }, regime, voters, desired: Vec::new() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region::table_key;

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
                now: 123_456,
            },
            PdCmd::Heartbeat {
                node_id: 3,
                address: String::new(),
                regions: vec![],
                now: 0,
            },
            // A regime and a replica set on the wire — what a node reports about its regions.
            PdCmd::Heartbeat {
                node_id: 4,
                address: "http://n4".into(),
                regions: vec![ReplicaSet {
                    region: region(2, b"a", b"z", 7),
                    regime: Regime::Ap,
                    voters: vec![4, 5, 6],
                    desired: Vec::new(),
                }],
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
                now: 100,
            }
            .encode(),
        );
        let p = fsm.route(b"anything").unwrap();
        assert_eq!((p.node_id, p.address.as_str()), (7, "http://a"));
    }

    /// Declare `name`, as a committed entry would.
    fn declare(fsm: &PdFsm, name: &str, regime: Regime) {
        fsm.apply(&PdCmd::DeclareTable { name: name.to_string(), regime }.encode());
    }

    #[test]
    fn the_default_table_exists_before_anything_is_declared() {
        let fsm = PdFsm::new();
        assert_eq!(fsm.table_id(DEFAULT_TABLE_NAME), Some(DEFAULT_TABLE_ID));
        // An omitted table name resolves to it, which is what makes zero-config use work.
        assert_eq!(fsm.table_id(""), Some(DEFAULT_TABLE_ID));
        assert_eq!(fsm.catalog().len(), 1);
        // Version 1, not 0: a node reads 0 as "PD holds no catalog, nothing to adopt".
        assert_eq!(fsm.catalog_version(), 1);
    }

    #[test]
    fn ids_are_allocated_in_order_and_never_reuse_the_default() {
        let fsm = PdFsm::new();
        declare(&fsm, "orders", Regime::Cp);
        declare(&fsm, "clicks", Regime::Ap);
        assert_eq!(fsm.table_id("orders"), Some(1));
        assert_eq!(fsm.table_id("clicks"), Some(2));
        assert_eq!(fsm.next_table_id(), 3);
        // The regime travels with the id, and is read back off a key's prefix.
        assert_eq!(fsm.regime_for(&table_key(2, b"post7")), Regime::Ap);
        assert_eq!(fsm.regime_for(&table_key(1, b"o1")), Regime::Cp);
        // An undeclared id is strong by default, as an undeclared range always was.
        assert_eq!(fsm.regime_for(&table_key(9, b"k")), Regime::Cp);
    }

    #[test]
    fn declaring_a_table_splits_exactly_one_region_and_leaves_no_gap() {
        let fsm = PdFsm::new();
        declare(&fsm, "orders", Regime::Cp);
        declare(&fsm, "clicks", Regime::Ap);

        // The headline invariant: one region per table, nothing in between.
        let regions = fsm.regions.list();
        assert_eq!(regions.len(), 3, "default + 2 tables, and no untabled gap regions");
        assert!(regions[0].start.is_empty(), "the default region starts at the keyspace start");
        assert!(regions[2].end.is_empty(), "the newest table runs to +inf");
        for w in regions.windows(2) {
            assert_eq!(w[0].end, w[1].start, "contiguous: a gap could only appear here");
        }
        assert_eq!(regions[1].start, table_prefix(1).to_vec());
        assert_eq!(regions[2].start, table_prefix(2).to_vec());
    }

    #[test]
    fn re_applying_a_declaration_does_not_burn_an_id() {
        // A restart replays the log. If the allocation ran before the name check, every later
        // table would shift and replicas that did not replay would disagree about every id.
        let fsm = PdFsm::new();
        let entry = PdCmd::DeclareTable { name: "orders".into(), regime: Regime::Cp }.encode();
        fsm.apply(&entry);
        fsm.apply(&entry);
        fsm.apply(&entry);

        assert_eq!(fsm.table_id("orders"), Some(1));
        assert_eq!(fsm.next_table_id(), 2, "the replays allocated nothing");
        assert_eq!(fsm.regions.list().len(), 2, "and carved nothing");
    }

    #[test]
    fn the_default_table_is_never_re_declarable() {
        let fsm = PdFsm::new();
        declare(&fsm, DEFAULT_TABLE_NAME, Regime::Ap);
        declare(&fsm, "", Regime::Ap);
        assert_eq!(fsm.catalog().len(), 1);
        assert_eq!(fsm.regime_for(&table_key(DEFAULT_TABLE_ID, b"k")), Regime::Cp);
        assert_eq!(fsm.next_table_id(), 1, "neither burned an id");
    }

    #[test]
    fn restore_never_regresses_the_allocator() {
        let a = PdFsm::new();
        declare(&a, "orders", Regime::Cp);
        declare(&a, "clicks", Regime::Ap);
        let image = a.snapshot();

        // A follower that is ahead must not adopt an older allocator: reissuing an id would give
        // two tables the same key range.
        let b = PdFsm::new();
        for name in ["a", "b", "c", "d"] {
            declare(&b, name, Regime::Cp);
        }
        assert_eq!(b.next_table_id(), 5);
        assert!(b.restore(&image));
        assert_eq!(b.next_table_id(), 5, "fetch_max, like the TSO watermark");

        // A fresh replica adopts the whole catalog, ids included.
        let c = PdFsm::new();
        assert!(c.restore(&image));
        assert_eq!(c.table_id("orders"), Some(1));
        assert_eq!(c.table_id("clicks"), Some(2));
        assert_eq!(c.next_table_id(), 3);
        assert_eq!(c.regime_for(&table_key(2, b"k")), Regime::Ap, "regimes survive the snapshot");
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

    /// A node's heartbeat, as a committed entry. `led` are regions it reports leading, with the
    /// voters its group has; everything else a real node reports carries no voters.
    fn heartbeat(fsm: &PdFsm, node: u64, led: Vec<(u64, Vec<u64>)>) {
        let regions = led
            .into_iter()
            .map(|(id, voters)| {
                let region = fsm.regions.list().into_iter().find(|r| r.id == id).expect("region");
                ReplicaSet { region, regime: Regime::Cp, voters, desired: Vec::new() }
            })
            .collect();
        fsm.apply(
            &PdCmd::Heartbeat { node_id: node, address: format!("http://n{node}"), regions, now: 1 }
                .encode(),
        );
    }

    fn default_region(fsm: &PdFsm) -> ReplicaSet {
        fsm.assignment().into_iter().next().expect("the default region")
    }

    #[test]
    fn the_first_node_founds_a_region_alone() {
        let fsm = PdFsm::new();
        heartbeat(&fsm, 1, vec![]);
        let r = default_region(&fsm);
        assert_eq!(r.voters, vec![1], "the bootstrap: founded by the only live node");
        assert_eq!(r.desired, vec![1]);
    }

    #[test]
    fn later_nodes_are_desired_not_members() {
        // The split-brain fix: a node registering later must not be handed the region as a
        // founding voter — it has to be added to the running group by membership change.
        let fsm = PdFsm::new();
        heartbeat(&fsm, 1, vec![]);
        heartbeat(&fsm, 2, vec![]);
        heartbeat(&fsm, 3, vec![]);
        let r = default_region(&fsm);
        assert_eq!(r.voters, vec![1], "members are fixed at founding");
        assert_eq!(r.desired, vec![1, 2, 3], "and grown toward the live nodes");
        // Node 3 is sent the region (to join it), though it is not a member.
        assert_eq!(fsm.assignment_for(3).len(), 1);
    }

    #[test]
    fn desired_is_capped_at_the_replication_target() {
        let fsm = PdFsm::new();
        fsm.set_replicas(2);
        for node in 1..=4 {
            heartbeat(&fsm, node, vec![]);
        }
        assert_eq!(default_region(&fsm).desired, vec![1, 2]);
        assert!(fsm.assignment_for(4).is_empty(), "a node beyond the target is given nothing");
    }

    #[test]
    fn a_leader_report_that_grows_the_group_is_adopted_and_bumps_the_version() {
        let fsm = PdFsm::new();
        heartbeat(&fsm, 1, vec![]);
        heartbeat(&fsm, 2, vec![]);
        let before = fsm.catalog_version();
        let id = default_region(&fsm).region.id;

        heartbeat(&fsm, 1, vec![(id, vec![1, 2])]);
        assert_eq!(default_region(&fsm).voters, vec![1, 2]);
        assert!(fsm.catalog_version() > before, "nodes re-reconcile on a membership change");
    }

    #[test]
    fn a_report_that_drops_a_voter_or_comes_from_outside_is_ignored() {
        let fsm = PdFsm::new();
        heartbeat(&fsm, 1, vec![]);
        heartbeat(&fsm, 2, vec![]);
        let id = default_region(&fsm).region.id;
        heartbeat(&fsm, 1, vec![(id, vec![1, 2])]);

        // Shrinking is not this path's to do: a report without node 1 is a stale view.
        heartbeat(&fsm, 2, vec![(id, vec![2, 3])]);
        assert_eq!(default_region(&fsm).voters, vec![1, 2]);
        // A node reporting a group it is not in says nothing about it.
        heartbeat(&fsm, 3, vec![(id, vec![1, 2, 4])]);
        assert_eq!(default_region(&fsm).voters, vec![1, 2]);
    }

    #[test]
    fn a_table_created_on_a_formed_cluster_starts_fully_replicated() {
        let fsm = PdFsm::new();
        for node in 1..=3 {
            heartbeat(&fsm, node, vec![]);
        }
        declare(&fsm, "orders", Regime::Cp);
        let orders = fsm
            .assignment()
            .into_iter()
            .find(|rs| rs.region.start == table_prefix(1).to_vec())
            .expect("orders region");
        assert_eq!(orders.voters, vec![1, 2, 3], "founded by every live node, no growing needed");
    }

    #[test]
    fn a_table_declared_before_any_node_is_founded_when_one_registers() {
        let fsm = PdFsm::new();
        declare(&fsm, "orders", Regime::Cp);
        assert!(fsm.assignment().iter().all(|rs| rs.voters.is_empty()), "no node to found it yet");
        heartbeat(&fsm, 1, vec![]);
        assert!(fsm.assignment().iter().all(|rs| rs.voters == vec![1]));
    }

    #[test]
    fn region_members_survive_a_snapshot() {
        let a = PdFsm::new();
        heartbeat(&a, 1, vec![]);
        heartbeat(&a, 2, vec![]);
        let id = default_region(&a).region.id;
        heartbeat(&a, 1, vec![(id, vec![1, 2])]);

        let b = PdFsm::new();
        assert!(b.restore(&a.snapshot()));
        assert_eq!(default_region(&b).voters, vec![1, 2], "members are not rebuilt by heartbeats");
    }
}

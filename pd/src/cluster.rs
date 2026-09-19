//! Cluster membership + region placement — PD's authoritative view of which data nodes
//! are alive and which regions each one owns.
//!
//! Phase 3 aggregated regions with a single global `replace`, which assumed exactly one
//! data node: a second node's heartbeat would clobber the first's regions. Phase 3b
//! tracks state **per node** instead — `node_id → {address, last_seen, regions}` — so the
//! routing view is the union across all live nodes, and the client can be told *which*
//! node (and address) owns each key.
//!
//! ## Placement authority
//!
//! A data node stays authoritative for the **epochs and splits** of the regions it holds
//! (it owns the data), but PD is authoritative for **placement** — which node hosts which
//! region. A node learns its assignment through the heartbeat *response*: it reports the
//! regions it currently has (empty on a fresh start), and PD replies with the regions it
//! should own. Bootstrap falls out of this:
//!
//! - a node reporting a non-empty set is taken at its word (it owns that data);
//! - a node reporting nothing is *assigned* — its [`seed`](Membership::seeded) partition if
//!   one was configured, otherwise the whole keyspace if the cluster is still empty (so the
//!   first node to register in an unseeded cluster bootstraps it, exactly as Phase 3 did).
//!
//! ## Failure detection
//!
//! Each heartbeat stamps `last_seen` from a caller-supplied clock (ms). [`sweep`] marks any
//! node whose `last_seen` is older than a timeout as **down**; a down node's regions drop
//! out of the routing view until it heartbeats again. The clock is passed in (not read from
//! the wall) so the detector is deterministically testable.
//!
//! [`sweep`]: Membership::sweep

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::persist::{get_bytes, get_u32, get_u64, put_bytes};
use crate::region::TableId;
use crate::Region;

/// Wall-clock milliseconds since the Unix epoch — the time base for `last_seen` and the
/// failure-detector sweep in the running server. Tests drive these paths with explicit
/// values instead.
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// How a range is served: **CP** through a Raft group, **AP** as a leaderless replica set.
/// PD's own copy of the concept — the wire enum (`pd.Regime`) and the data node's
/// (`multiraft::Regime`) convert at their boundaries, keeping this module proto-free the same
/// way [`crate::convert`] keeps `region.rs` proto-free.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Regime {
    #[default]
    Cp,
    Ap,
}

/// One table in PD's catalog: the id PD allocated for it, its name, and how it is served.
///
/// The id is the table's identity everywhere below this line — it prefixes every key the table
/// holds, and it fixes the table's region as `[be32(id), be32(id + 1))`. The name is a label PD
/// resolves for clients; nothing on disk depends on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub regime: Regime,
}

/// A region as a node reports it: the range, how it is served, and the replica set holding it.
/// `voters` is why this type exists — [`PlacedRegion::node_id`] names a single holder, which
/// cannot describe the three-voter regions a real CP cluster is made of.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaSet {
    pub region: Region,
    pub regime: Regime,
    /// The region's actual Raft voters, fixed at founding and grown only by membership change.
    pub voters: Vec<u64>,
    /// What PD wants the voter set to be: `voters` plus the live nodes it should grow to, capped at
    /// the replication target. Set only on PD's assignment; empty on a node's own report.
    pub desired: Vec<u64>,
}

impl ReplicaSet {
    /// A region with no regime or replica-set information — what a pre-v14 peer effectively
    /// reports, and the shape the unreplicated single-node paths still use.
    pub fn bare(region: Region) -> ReplicaSet {
        ReplicaSet { region, regime: Regime::Cp, voters: Vec::new(), desired: Vec::new() }
    }
}

/// A region tagged with the node that owns it — what PD hands back to a routing client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacedRegion {
    pub region: Region,
    pub node_id: u64,
    pub address: String,
    pub regime: Regime,
    /// The region's replica set, empty when the reporting node didn't say (pre-v14, or an
    /// unreplicated node).
    pub voters: Vec<u64>,
}

/// A table two nodes describe differently — a misconfigured cluster, since a table's regime
/// decides how its keys are tiled and routed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableConflict {
    pub name: String,
    pub regime: Regime,
    pub node_id: u64,
    pub other_regime: Regime,
    pub other_node_id: u64,
}

/// What PD knows about one data node.
struct NodeState {
    address: String,
    last_seen: u64,
    down: bool,
    regions: Vec<ReplicaSet>,
}

struct State {
    nodes: BTreeMap<u64, NodeState>,
    /// Optional initial placement: `(region, owning node_id)`. Consumed lazily — a node
    /// reporting empty adopts the seed entries assigned to it.
    seed: Vec<(Region, u64)>,
}

/// PD's per-node membership + placement registry.
pub struct Membership {
    state: Mutex<State>,
}

impl Membership {
    /// Serialize the per-node view into `out`, for PD's Raft snapshot (see
    /// `PdFsm::snapshot`). The `seed` is deliberately **not** included: it is startup
    /// configuration, not replicated state, and a replica restoring a snapshot already has
    /// whatever seed it was configured with.
    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        let g = self.state.lock().expect("membership poisoned");
        out.extend_from_slice(&(g.nodes.len() as u32).to_be_bytes());
        for (id, n) in &g.nodes {
            out.extend_from_slice(&id.to_be_bytes());
            put_bytes(out, n.address.as_bytes());
            out.extend_from_slice(&n.last_seen.to_be_bytes());
            out.push(n.down as u8);
            out.extend_from_slice(&(n.regions.len() as u32).to_be_bytes());
            for rs in &n.regions {
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
        }
    }

    /// Replace the per-node view from `bytes` (the inverse of [`encode_into`](Self::encode_into)),
    /// returning how many bytes it consumed so a caller decoding a larger image can continue
    /// after it. `None` on a truncated or malformed image, leaving the current view untouched —
    /// a bad snapshot must not half-apply.
    pub(crate) fn decode_from(&self, bytes: &[u8]) -> Option<usize> {
        let mut pos = 0usize;
        let mut nodes = BTreeMap::new();
        for _ in 0..get_u32(bytes, &mut pos)? {
            let id = get_u64(bytes, &mut pos)?;
            let address = String::from_utf8(get_bytes(bytes, &mut pos)?.to_vec()).ok()?;
            let last_seen = get_u64(bytes, &mut pos)?;
            let down = *bytes.get(pos)? != 0;
            pos += 1;
            let mut regions = Vec::new();
            for _ in 0..get_u32(bytes, &mut pos)? {
                let id = get_u64(bytes, &mut pos)?;
                let epoch = get_u64(bytes, &mut pos)?;
                let start = get_bytes(bytes, &mut pos)?.to_vec();
                let end = get_bytes(bytes, &mut pos)?.to_vec();
                let regime = regime_of(*bytes.get(pos)?)?;
                pos += 1;
                let mut voters = Vec::new();
                for _ in 0..get_u32(bytes, &mut pos)? {
                    voters.push(get_u64(bytes, &mut pos)?);
                }
                regions.push(ReplicaSet {
                    region: Region { id, start, end, epoch },
                    regime,
                    voters,
                    desired: Vec::new(),
                });
            }
            nodes.insert(id, NodeState { address, last_seen, down, regions });
        }
        let mut g = self.state.lock().expect("membership poisoned");
        g.nodes = nodes;
        Some(pos)
    }

    /// An empty cluster (no nodes, no seed). The first node to heartbeat with no regions
    /// bootstraps the whole keyspace.
    pub fn new() -> Membership {
        Membership { state: Mutex::new(State { nodes: BTreeMap::new(), seed: Vec::new() }) }
    }

    /// A cluster seeded with an explicit initial placement: each `(region, node_id)` is
    /// handed to that node the first time it heartbeats with no regions of its own. Used
    /// to stand up a keyspace pre-partitioned across several nodes.
    pub fn seeded(seed: Vec<(Region, u64)>) -> Membership {
        Membership { state: Mutex::new(State { nodes: BTreeMap::new(), seed }) }
    }

    /// Record a heartbeat from `node_id` and return the regions PD assigns it.
    ///
    /// `reported` is the node's own current region set (authoritative when non-empty);
    /// `now` is the current time in ms (for liveness). A node reporting nothing is given
    /// its seed partition, or the whole keyspace if the cluster is still empty.
    pub fn heartbeat(
        &self,
        node_id: u64,
        address: String,
        reported: Vec<ReplicaSet>,
        now: u64,
    ) -> Vec<ReplicaSet> {
        let mut g = self.state.lock().expect("membership poisoned");

        let assigned = if !reported.is_empty() {
            reported
        } else {
            // Keep whatever the node already had on record; only assign if it has none.
            let existing = g.nodes.get(&node_id).map(|n| n.regions.clone()).unwrap_or_default();
            if !existing.is_empty() {
                existing
            } else {
                assign(&g, node_id).into_iter().map(ReplicaSet::bare).collect()
            }
        };

        let entry = g.nodes.entry(node_id).or_insert_with(|| NodeState {
            address: address.clone(),
            last_seen: now,
            down: false,
            regions: Vec::new(),
        });
        entry.address = address;
        entry.last_seen = now;
        entry.down = false;
        entry.regions = assigned.clone();
        assigned
    }

    /// Every node PD knows about and where to reach it, node-id order.
    ///
    /// This is what a data node used to get from `--peer` flags. PD already learns each address
    /// from the heartbeat that registers the node, so handing it back with the assignment is what
    /// lets a node found a region with voters it was never told about at startup.
    pub fn node_addrs(&self) -> Vec<(u64, String)> {
        let g = self.state.lock().expect("membership poisoned");
        g.nodes.iter().map(|(id, n)| (*id, n.address.clone())).collect()
    }

    /// The nodes eligible to vote on a region: every registered node that is not marked down,
    /// in id order.
    ///
    /// **Scoped deliberately.** Every region gets every live node, which is what a catalog
    /// cluster did when the voter set came from matching `--table`/`--voters` flags on each node.
    /// Choosing a subset per region is placement policy — it needs a replication factor and a
    /// balancing rule, which is Phase-6 rebalancing, not this.
    pub fn live_nodes(&self) -> Vec<u64> {
        let g = self.state.lock().expect("membership poisoned");
        g.nodes.iter().filter(|(_, n)| !n.down).map(|(id, _)| *id).collect()
    }

    /// Mark every node whose last heartbeat is older than `timeout_ms` (relative to `now`)
    /// as down, and return the ids newly marked. A down node's regions leave the routing
    /// view until it heartbeats again.
    pub fn sweep(&self, now: u64, timeout_ms: u64) -> Vec<u64> {
        let mut g = self.state.lock().expect("membership poisoned");
        let mut downed = Vec::new();
        for (id, n) in g.nodes.iter_mut() {
            if !n.down && now.saturating_sub(n.last_seen) > timeout_ms {
                n.down = true;
                downed.push(*id);
            }
        }
        downed
    }

    /// The region (and owning node) covering `key`, considering only **live** nodes.
    pub fn route(&self, key: &[u8]) -> Option<PlacedRegion> {
        let g = self.state.lock().expect("membership poisoned");
        for (id, n) in g.nodes.iter() {
            if n.down {
                continue;
            }
            if let Some(rs) = n.regions.iter().find(|rs| rs.region.contains(key)) {
                return Some(PlacedRegion {
                    region: rs.region.clone(),
                    node_id: *id,
                    address: n.address.clone(),
                    regime: rs.regime,
                    voters: rs.voters.clone(),
                });
            }
        }
        None
    }

    /// Every placed region across all live nodes, sorted by start key (for routing caches
    /// and tooling).
    pub fn list(&self) -> Vec<PlacedRegion> {
        let g = self.state.lock().expect("membership poisoned");
        let mut out: Vec<PlacedRegion> = Vec::new();
        for (id, n) in g.nodes.iter() {
            if n.down {
                continue;
            }
            for rs in &n.regions {
                out.push(PlacedRegion {
                    region: rs.region.clone(),
                    node_id: *id,
                    address: n.address.clone(),
                    regime: rs.regime,
                    voters: rs.voters.clone(),
                });
            }
        }
        out.sort_by(|a, b| a.region.start.cmp(&b.region.start));
        out
    }

    /// Whether PD currently considers `node_id` down (test/introspection helper).
    pub fn is_down(&self, node_id: u64) -> bool {
        let g = self.state.lock().expect("membership poisoned");
        g.nodes.get(&node_id).map(|n| n.down).unwrap_or(false)
    }
}

impl Default for Membership {
    fn default() -> Self {
        Membership::new()
    }
}

/// Decide the regions to hand a node that reported none: its seed partition if present,
/// otherwise the whole keyspace if no region exists anywhere yet.
/// Wire tag for a regime in the length-prefixed codec. Explicit rather than `as u8` so the
/// on-disk encoding doesn't silently change if the enum is ever reordered.
pub(crate) fn regime_tag(regime: Regime) -> u8 {
    match regime {
        Regime::Cp => 0,
        Regime::Ap => 1,
    }
}

pub(crate) fn regime_of(tag: u8) -> Option<Regime> {
    match tag {
        0 => Some(Regime::Cp),
        1 => Some(Regime::Ap),
        _ => None,
    }
}

fn assign(state: &State, node_id: u64) -> Vec<Region> {
    let seeded: Vec<Region> =
        state.seed.iter().filter(|(_, n)| *n == node_id).map(|(r, _)| r.clone()).collect();
    if !seeded.is_empty() {
        return seeded;
    }
    let any_region = state.nodes.values().any(|n| !n.regions.is_empty())
        || !state.seed.is_empty();
    if any_region {
        Vec::new()
    } else {
        vec![Region { id: 1, start: Vec::new(), end: Vec::new(), epoch: 1 }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region(id: u64, start: &[u8], end: &[u8], epoch: u64) -> Region {
        Region { id, start: start.to_vec(), end: end.to_vec(), epoch }
    }

    /// Report bare regions — no regime, no voters. The shape a pre-v14 node effectively sends,
    /// and what most of these placement tests care about.
    fn hb(m: &Membership, id: u64, addr: &str, regions: Vec<Region>, now: u64) -> Vec<Region> {
        m.heartbeat(id, addr.into(), regions.into_iter().map(ReplicaSet::bare).collect(), now)
            .into_iter()
            .map(|rs| rs.region)
            .collect()
    }

    #[test]
    fn a_reported_region_keeps_its_regime_and_replica_set() {
        // `node_id` names one holder; `voters` is what lets PD describe a three-voter region.
        let m = Membership::new();
        let rs = ReplicaSet {
            region: region(1, b"", b"", 1),
            regime: Regime::Ap,
            voters: vec![1, 2, 3],
            desired: Vec::new(),
        };
        m.heartbeat(1, "http://a".into(), vec![rs], 100);

        let placed = m.route(b"k").unwrap();
        assert_eq!(placed.regime, Regime::Ap);
        assert_eq!(placed.voters, vec![1, 2, 3]);
    }

    #[test]
    fn pd_hands_out_every_node_address() {
        // What replaced the `--peer` flags: a node founds a region with voters it was never told
        // about, so PD has to be able to say where each one is.
        let m = Membership::new();
        hb(&m, 2, "http://b", vec![], 100);
        hb(&m, 1, "http://a", vec![], 100);
        assert_eq!(
            m.node_addrs(),
            vec![(1, "http://a".to_string()), (2, "http://b".to_string())],
            "node-id order, so every replica computes the same assignment"
        );
    }

    #[test]
    fn only_live_nodes_are_voters() {
        let m = Membership::new();
        hb(&m, 1, "http://a", vec![], 1_000);
        hb(&m, 2, "http://b", vec![], 100);
        assert_eq!(m.live_nodes(), vec![1, 2]);

        // Node 2 stops heartbeating: it must not be handed new regions to vote on.
        assert_eq!(m.sweep(2_000, 1_000), vec![2]);
        assert_eq!(m.live_nodes(), vec![1]);
    }

    #[test]
    fn encode_decode_round_trips_every_node_field() {
        let a = Membership::new();
        // Node 1 carries the interesting shape: a regime and a replica set. If either were
        // dropped here, PD's log compaction would silently lose it.
        a.heartbeat(
            1,
            "http://n1".into(),
            vec![ReplicaSet {
                region: region(1, b"", b"m", 2),
                regime: Regime::Ap,
                voters: vec![1, 2, 3],
                desired: Vec::new(),
            }],
            8_900,
        );
        hb(&a, 2, "http://n2", vec![region(2, b"m", b"", 2)], 1_000);
        // Only node 2 is stale, so the down flag has something to carry and node 1 still routes.
        assert_eq!(a.sweep(9_000, 1_000), vec![2]);

        let mut bytes = Vec::new();
        a.encode_into(&mut bytes);
        let b = Membership::new();
        assert!(b.decode_from(&bytes).is_some());

        assert_eq!(b.list().len(), a.list().len());
        let placed = b.route(b"a").expect("address and regions survive");
        assert_eq!(placed.node_id, 1);
        assert_eq!(placed.regime, Regime::Ap, "the regime survives");
        assert_eq!(placed.voters, vec![1, 2, 3], "the replica set survives");
        assert!(!b.is_down(1));
        assert!(b.is_down(2), "the down flag survives");
    }

    #[test]
    fn a_truncated_image_is_rejected_and_leaves_state_alone() {
        let m = Membership::new();
        hb(&m, 1, "http://n1", vec![region(1, b"", b"", 1)], 1_000);
        let mut bytes = Vec::new();
        m.encode_into(&mut bytes);

        let victim = Membership::new();
        hb(&victim, 9, "http://n9", vec![region(1, b"", b"", 1)], 1_000);
        assert!(victim.decode_from(&bytes[..bytes.len() - 3]).is_none());
        assert_eq!(victim.route(b"k").map(|p| p.node_id), Some(9), "state untouched");
    }

    #[test]
    fn fresh_cluster_bootstraps_whole_keyspace_to_first_node() {
        let m = Membership::new();
        let assigned = hb(&m, 1, "http://a", vec![], 100);
        assert_eq!(assigned, vec![region(1, b"", b"", 1)]);
        // Everything routes to node 1.
        let p = m.route(b"anything").unwrap();
        assert_eq!((p.node_id, p.address.as_str()), (1, "http://a"));
    }

    #[test]
    fn second_node_does_not_clobber_the_first() {
        let m = Membership::new();
        // Node 1 owns [-inf, m); node 2 owns [m, +inf).
        hb(&m, 1, "http://a", vec![region(1, b"", b"m", 2)], 100);
        hb(&m, 2, "http://b", vec![region(2, b"m", b"", 2)], 100);

        // Both nodes' regions coexist — the Phase-3 `replace` would have lost node 1's.
        assert_eq!(m.route(b"a").unwrap().node_id, 1);
        assert_eq!(m.route(b"z").unwrap().node_id, 2);
        assert_eq!(m.list().len(), 2);
    }

    #[test]
    fn seed_places_regions_across_nodes_on_first_heartbeat() {
        let m = Membership::seeded(vec![
            (region(1, b"", b"m", 1), 1),
            (region(2, b"m", b"", 1), 2),
        ]);
        // Each node reports empty and receives its seed partition.
        assert_eq!(hb(&m, 1, "http://a", vec![], 100), vec![region(1, b"", b"m", 1)]);
        assert_eq!(hb(&m, 2, "http://b", vec![], 100), vec![region(2, b"m", b"", 1)]);
        assert_eq!(m.route(b"a").unwrap().address, "http://a");
        assert_eq!(m.route(b"z").unwrap().address, "http://b");
    }

    #[test]
    fn failure_detector_marks_a_silent_node_down() {
        let m = Membership::new();
        hb(&m, 1, "http://a", vec![region(1, b"", b"", 1)], 1000);
        // Not yet past the timeout.
        assert!(m.sweep(1500, 1000).is_empty());
        assert!(!m.is_down(1));
        // Past the timeout → marked down, and its regions leave the routing view.
        assert_eq!(m.sweep(2500, 1000), vec![1]);
        assert!(m.is_down(1));
        assert!(m.route(b"x").is_none());
        // A fresh heartbeat revives it.
        hb(&m, 1, "http://a", vec![region(1, b"", b"", 1)], 3000);
        assert!(!m.is_down(1));
        assert!(m.route(b"x").is_some());
    }

    #[test]
    fn restarted_node_reporting_its_regions_is_taken_at_its_word() {
        let m = Membership::seeded(vec![(region(1, b"", b"", 1), 1)]);
        // After a split the node reports two regions; PD echoes them, not the seed.
        let reported = vec![region(1, b"", b"m", 2), region(7, b"m", b"", 2)];
        let assigned = hb(&m, 1, "http://a", reported.clone(), 100);
        assert_eq!(assigned, reported);
        assert_eq!(m.route(b"z").unwrap().region.id, 7);
    }
}

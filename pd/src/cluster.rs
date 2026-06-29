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

use crate::Region;

/// Wall-clock milliseconds since the Unix epoch — the time base for `last_seen` and the
/// failure-detector sweep in the running server. Tests drive these paths with explicit
/// values instead.
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// A region tagged with the node that owns it — what PD hands back to a routing client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacedRegion {
    pub region: Region,
    pub node_id: u64,
    pub address: String,
}

/// What PD knows about one data node.
struct NodeState {
    address: String,
    last_seen: u64,
    down: bool,
    regions: Vec<Region>,
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
        reported: Vec<Region>,
        now: u64,
    ) -> Vec<Region> {
        let mut g = self.state.lock().expect("membership poisoned");

        let assigned = if !reported.is_empty() {
            reported
        } else {
            // Keep whatever the node already had on record; only assign if it has none.
            let existing = g.nodes.get(&node_id).map(|n| n.regions.clone()).unwrap_or_default();
            if !existing.is_empty() {
                existing
            } else {
                assign(&g, node_id)
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
            if let Some(r) = n.regions.iter().find(|r| r.contains(key)) {
                return Some(PlacedRegion { region: r.clone(), node_id: *id, address: n.address.clone() });
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
            for r in &n.regions {
                out.push(PlacedRegion { region: r.clone(), node_id: *id, address: n.address.clone() });
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

    #[test]
    fn fresh_cluster_bootstraps_whole_keyspace_to_first_node() {
        let m = Membership::new();
        let assigned = m.heartbeat(1, "http://a".into(), vec![], 100);
        assert_eq!(assigned, vec![region(1, b"", b"", 1)]);
        // Everything routes to node 1.
        let p = m.route(b"anything").unwrap();
        assert_eq!((p.node_id, p.address.as_str()), (1, "http://a"));
    }

    #[test]
    fn second_node_does_not_clobber_the_first() {
        let m = Membership::new();
        // Node 1 owns [-inf, m); node 2 owns [m, +inf).
        m.heartbeat(1, "http://a".into(), vec![region(1, b"", b"m", 2)], 100);
        m.heartbeat(2, "http://b".into(), vec![region(2, b"m", b"", 2)], 100);

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
        assert_eq!(m.heartbeat(1, "http://a".into(), vec![], 100), vec![region(1, b"", b"m", 1)]);
        assert_eq!(m.heartbeat(2, "http://b".into(), vec![], 100), vec![region(2, b"m", b"", 1)]);
        assert_eq!(m.route(b"a").unwrap().address, "http://a");
        assert_eq!(m.route(b"z").unwrap().address, "http://b");
    }

    #[test]
    fn failure_detector_marks_a_silent_node_down() {
        let m = Membership::new();
        m.heartbeat(1, "http://a".into(), vec![region(1, b"", b"", 1)], 1000);
        // Not yet past the timeout.
        assert!(m.sweep(1500, 1000).is_empty());
        assert!(!m.is_down(1));
        // Past the timeout → marked down, and its regions leave the routing view.
        assert_eq!(m.sweep(2500, 1000), vec![1]);
        assert!(m.is_down(1));
        assert!(m.route(b"x").is_none());
        // A fresh heartbeat revives it.
        m.heartbeat(1, "http://a".into(), vec![region(1, b"", b"", 1)], 3000);
        assert!(!m.is_down(1));
        assert!(m.route(b"x").is_some());
    }

    #[test]
    fn restarted_node_reporting_its_regions_is_taken_at_its_word() {
        let m = Membership::seeded(vec![(region(1, b"", b"", 1), 1)]);
        // After a split the node reports two regions; PD echoes them, not the seed.
        let reported = vec![region(1, b"", b"m", 2), region(7, b"m", b"", 2)];
        let assigned = m.heartbeat(1, "http://a".into(), reported.clone(), 100);
        assert_eq!(assigned, reported);
        assert_eq!(m.route(b"z").unwrap().region.id, 7);
    }
}

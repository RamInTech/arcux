//! Conversions between the in-memory [`Region`] / [`PlacedRegion`] and their `pd.Region`
//! wire form. Kept out of `region.rs` so the core region logic stays free of any proto
//! dependency.
//!
//! Ownership (`node_id`/`address`) is asymmetric on the wire: a data node reporting its
//! own regions leaves them empty (PD tags each region with the heartbeat's `node_id`), and
//! PD fills them in when answering routing queries — see [`placed_to_proto`].

use arcux_rpc::pd;

use crate::{PlacedRegion, Region};

/// In-memory region → wire region, ownership left unset (the node→PD direction; PD
/// attributes ownership from the heartbeat's `node_id`).
pub fn to_proto(r: &Region) -> pd::Region {
    pd::Region {
        id: r.id,
        start_key: r.start.clone(),
        end_key: r.end.clone(),
        epoch: r.epoch,
        node_id: 0,
        address: String::new(),
    }
}

/// Placed region → wire region, carrying its owning node id + address (the PD→client
/// direction, so a routing client can dispatch to the owner).
pub fn placed_to_proto(p: &PlacedRegion) -> pd::Region {
    pd::Region {
        id: p.region.id,
        start_key: p.region.start.clone(),
        end_key: p.region.end.clone(),
        epoch: p.region.epoch,
        node_id: p.node_id,
        address: p.address.clone(),
    }
}

/// Wire region → in-memory region (drops ownership; the reporting node is authoritative
/// for its own region set and PD tags it from the heartbeat envelope).
pub fn from_proto(r: &pd::Region) -> Region {
    Region { id: r.id, start: r.start_key.clone(), end: r.end_key.clone(), epoch: r.epoch }
}

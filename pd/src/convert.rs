//! Conversions between the in-memory [`Region`] and its `pd.Region` wire form. Kept out
//! of `region.rs` so the core region logic stays free of any proto dependency.

use arcux_rpc::pd;

use crate::Region;

/// In-memory region → wire region.
pub fn to_proto(r: &Region) -> pd::Region {
    pd::Region { id: r.id, start_key: r.start.clone(), end_key: r.end.clone(), epoch: r.epoch }
}

/// Wire region → in-memory region.
pub fn from_proto(r: &pd::Region) -> Region {
    Region { id: r.id, start: r.start_key.clone(), end: r.end_key.clone(), epoch: r.epoch }
}
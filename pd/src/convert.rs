//! Conversions between the in-memory [`Region`] / [`PlacedRegion`] and their `pd.Region`
//! wire form. Kept out of `region.rs` so the core region logic stays free of any proto
//! dependency.
//!
//! Ownership (`node_id`/`address`) is asymmetric on the wire: a data node reporting its
//! own regions leaves them empty (PD tags each region with the heartbeat's `node_id`), and
//! PD fills them in when answering routing queries — see [`placed_to_proto`].

use arcux_rpc::pd;

use crate::{PlacedRegion, Regime, Region, ReplicaSet};

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
        regime: pd::Regime::Cp as i32,
        voters: Vec::new(),
    }
}

/// Replica set → wire region, carrying the regime and voters a node reports for it. Ownership
/// stays unset, as in [`to_proto`]: PD attributes it from the heartbeat's `node_id`.
pub fn replica_set_to_proto(rs: &ReplicaSet) -> pd::Region {
    pd::Region {
        id: rs.region.id,
        start_key: rs.region.start.clone(),
        end_key: rs.region.end.clone(),
        epoch: rs.region.epoch,
        node_id: 0,
        address: String::new(),
        regime: regime_to_proto(rs.regime) as i32,
        voters: rs.voters.clone(),
    }
}

/// Wire region → replica set. An older peer sends neither field, which decodes to the
/// strong-by-default `Cp` and an empty voter set — the same shape as an unreplicated node.
pub fn replica_set_from_proto(r: &pd::Region) -> ReplicaSet {
    ReplicaSet { region: from_proto(r), regime: regime_from_proto(r.regime), voters: r.voters.clone() }
}

pub fn regime_to_proto(regime: Regime) -> pd::Regime {
    match regime {
        Regime::Cp => pd::Regime::Cp,
        Regime::Ap => pd::Regime::Ap,
    }
}

/// Wire regime → in-memory. An unrecognised value decodes to `Cp`, matching the catalog's
/// strong-by-default rule rather than silently weakening a range's guarantee.
pub fn regime_from_proto(regime: i32) -> Regime {
    match pd::Regime::try_from(regime) {
        Ok(pd::Regime::Ap) => Regime::Ap,
        _ => Regime::Cp,
    }
}

/// One table's declaration, both directions.
pub fn table_decl_to_proto(name: &str, regime: Regime) -> pd::TableDecl {
    pd::TableDecl { name: name.to_string(), regime: regime_to_proto(regime) as i32 }
}

pub fn table_decl_from_proto(t: &pd::TableDecl) -> (String, Regime) {
    (t.name.clone(), regime_from_proto(t.regime))
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
        regime: regime_to_proto(p.regime) as i32,
        voters: p.voters.clone(),
    }
}

/// Wire region → in-memory region (drops ownership; the reporting node is authoritative
/// for its own region set and PD tags it from the heartbeat envelope).
pub fn from_proto(r: &pd::Region) -> Region {
    Region { id: r.id, start: r.start_key.clone(), end: r.end_key.clone(), epoch: r.epoch }
}

/// Build a `ListTables` reply from PD's in-memory view — shared by the single-process and
/// replicated services so both answer identically.
pub fn list_tables_response(
    tables: Vec<(String, Regime)>,
    conflicts: Vec<crate::TableConflict>,
) -> pd::ListTablesResponse {
    pd::ListTablesResponse {
        tables: tables.iter().map(|(n, r)| table_decl_to_proto(n, *r)).collect(),
        conflicts: conflicts
            .iter()
            .map(|c| pd::TableConflict {
                name: c.name.clone(),
                regime: regime_to_proto(c.regime) as i32,
                node_id: c.node_id,
                other_regime: regime_to_proto(c.other_regime) as i32,
                other_node_id: c.other_node_id,
            })
            .collect(),
    }
}

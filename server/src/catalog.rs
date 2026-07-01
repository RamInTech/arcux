//! The consistency **catalog** — a table's declared regime (Phase 5b).
//!
//! The headline is *"a table is declared CP or AP, and that selects the write path."* A
//! "table" here is simply a **key-prefix**: table `t` owns every key under `t/`. The catalog
//! is the map `prefix → `[`Regime`], populated by [`create_table`](Catalog::create_table); a
//! key's regime is the **longest** declared prefix it falls under (default `Cp` —
//! strong-by-default). The regime it yields becomes a region's regime at placement, which the
//! server then dispatches on (CP → Percolator+Raft, AP → leaderless HLC/LWW).
//!
//! This is the *declaration mechanism*; a PD-served, runtime `create_table` RPC (declare a
//! table on a live cluster and spin up its region) is a later step — it needs dynamic
//! region/group creation and the PD↔MultiRaft placement integration.

use std::collections::HashMap;

use crate::multiraft::{Regime, RegionPlacement};

/// A table → regime map. Tables own key-prefixes (`name/`); lookups are longest-prefix.
pub struct Catalog {
    tables: Vec<(Vec<u8>, Regime)>,
}

impl Catalog {
    pub fn new() -> Catalog {
        Catalog { tables: Vec::new() }
    }

    /// Declare `name` a CP or AP table — it owns the key-prefix `name/`. Re-declaring the same
    /// name overwrites its regime.
    pub fn create_table(&mut self, name: &str, regime: Regime) {
        let prefix = table_prefix(name);
        match self.tables.iter_mut().find(|(p, _)| *p == prefix) {
            Some(entry) => entry.1 = regime,
            None => self.tables.push((prefix, regime)),
        }
    }

    /// The regime for `key`: the regime of the **longest** declared prefix `key` starts with,
    /// or `Cp` if none (strong-by-default).
    pub fn regime_for(&self, key: &[u8]) -> Regime {
        self.tables
            .iter()
            .filter(|(prefix, _)| key.starts_with(prefix))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, regime)| *regime)
            .unwrap_or(Regime::Cp)
    }

    /// Build a [`RegionPlacement`] whose regime is **derived from the catalog** (by the region's
    /// start key) rather than hand-set — so `create_table` declarations drive placement.
    pub fn place(
        &self,
        region_id: u64,
        start: Vec<u8>,
        end: Vec<u8>,
        voters: Vec<u64>,
        peers: HashMap<u64, String>,
    ) -> RegionPlacement {
        RegionPlacement {
            region_id,
            regime: self.regime_for(&start),
            start,
            end,
            epoch: 1,
            voters,
            peers,
        }
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Catalog::new()
    }
}

/// A table `t` owns keys under `t/`.
fn table_prefix(name: &str) -> Vec<u8> {
    let mut p = name.as_bytes().to_vec();
    p.push(b'/');
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_tables_select_their_regime() {
        let mut cat = Catalog::new();
        cat.create_table("ledger", Regime::Cp);
        cat.create_table("likes", Regime::Ap);

        assert_eq!(cat.regime_for(b"ledger/acct42"), Regime::Cp);
        assert_eq!(cat.regime_for(b"likes/post7"), Regime::Ap);
    }

    #[test]
    fn undeclared_keys_default_to_cp() {
        let cat = Catalog::new();
        assert_eq!(cat.regime_for(b"anything"), Regime::Cp, "strong by default");
        // A key that doesn't fall under a declared table's prefix is also CP.
        let mut cat = Catalog::new();
        cat.create_table("likes", Regime::Ap);
        assert_eq!(cat.regime_for(b"other/x"), Regime::Cp);
        assert_eq!(cat.regime_for(b"likes_but_not_slashed"), Regime::Cp, "must be under `likes/`");
    }

    #[test]
    fn longest_prefix_wins() {
        let mut cat = Catalog::new();
        cat.create_table("a", Regime::Ap); // "a/"
        cat.create_table("a/b", Regime::Cp); // "a/b/" — more specific
        assert_eq!(cat.regime_for(b"a/x"), Regime::Ap);
        assert_eq!(cat.regime_for(b"a/b/y"), Regime::Cp, "the longer prefix takes precedence");
    }

    #[test]
    fn redeclaring_overwrites() {
        let mut cat = Catalog::new();
        cat.create_table("t", Regime::Cp);
        cat.create_table("t", Regime::Ap);
        assert_eq!(cat.regime_for(b"t/k"), Regime::Ap);
    }

    #[test]
    fn place_derives_regime_from_the_catalog() {
        let mut cat = Catalog::new();
        cat.create_table("feed", Regime::Ap);
        let p = cat.place(2, b"feed/".to_vec(), vec![], vec![1, 2, 3], HashMap::new());
        assert_eq!(p.regime, Regime::Ap);
        let p = cat.place(1, b"acct/".to_vec(), b"feed/".to_vec(), vec![1, 2, 3], HashMap::new());
        assert_eq!(p.regime, Regime::Cp, "undeclared range is CP");
    }
}

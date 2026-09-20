//! The node's **catalog** — the tables it knows about, by id and by name.
//!
//! A table is a numeric id, allocated by PD. Every key it holds is stored under that id's
//! 4-byte big-endian prefix, so table `id` owns exactly `[be32(id), be32(id + 1))` and one
//! region serves exactly one table. There is no space between two tables for anything to fall
//! into, which is the whole point: the untabled "gap" regions a name-prefix scheme left between
//! and around tables — each one a CP region with its own Raft group, elections and heartbeats —
//! cannot exist.
//!
//! Table `0` is `default`: the table an omitted name resolves to, so a client that never
//! declares anything still has somewhere to write. Its id is fixed, so unlike every other table
//! it needs no allocator and exists on a node that has never spoken to PD.
//!
//! **PD is the authority.** A node does not declare tables and does not persist them; it learns
//! the catalog from PD's assignment (see `AppState::reconcile`) and holds it here to answer two
//! questions on the request path: which id does this table name have, and how is that id served.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

pub(crate) use arcux_pd::{
    strip_table_prefix, table_id_of, table_key, table_range, TableId,
    DEFAULT_TABLE_ID, DEFAULT_TABLE_NAME,
};

use crate::multiraft::Regime;

/// One table: the id its keys are stored under, its name, and how it is served.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub id: TableId,
    pub name: String,
    pub regime: Regime,
}

impl Table {
    /// The built-in table an omitted name resolves to. Always CP, never declarable.
    pub fn default_table() -> Table {
        Table { id: DEFAULT_TABLE_ID, name: DEFAULT_TABLE_NAME.to_string(), regime: Regime::Cp }
    }
}

/// An immutable snapshot of the catalog: two indexes over one set of tables.
///
/// Immutable so the request path can hold an `Arc` to it without a lock — see [`Catalog`].
pub struct Tables {
    by_name: HashMap<String, TableId>,
    by_id: BTreeMap<TableId, Table>,
}

impl Tables {
    /// Just `default` — what a node knows before PD tells it anything.
    pub fn bootstrap() -> Tables {
        Tables::from(vec![Table::default_table()])
    }

    /// Build from a table set, adding `default` if it is absent. PD always sends it; adding it
    /// here means a node can never end up unable to resolve an omitted table name.
    pub fn from(tables: Vec<Table>) -> Tables {
        let mut by_id = BTreeMap::new();
        for t in tables {
            by_id.insert(t.id, t);
        }
        by_id.entry(DEFAULT_TABLE_ID).or_insert_with(Table::default_table);
        let by_name = by_id.values().map(|t| (t.name.clone(), t.id)).collect();
        Tables { by_name, by_id }
    }

    /// The id a request's table name resolves to. An empty name means `default`, so a client
    /// that names no table writes to a real table rather than a special case.
    pub fn id_of(&self, name: &str) -> Option<TableId> {
        if name.is_empty() {
            return Some(DEFAULT_TABLE_ID);
        }
        self.by_name.get(name).copied()
    }

    pub fn get(&self, id: TableId) -> Option<&Table> {
        self.by_id.get(&id)
    }

    /// Every table, id-ordered — which is also the order their regions tile the keyspace.
    pub fn list(&self) -> Vec<Table> {
        self.by_id.values().cloned().collect()
    }

    /// How the table owning `stored_key` is served. An id with no entry is `Cp`: strong by
    /// default, the same answer an undeclared range has always given.
    pub fn regime_of_key(&self, stored_key: &[u8]) -> Regime {
        self.get(table_id_of(stored_key)).map(|t| t.regime).unwrap_or(Regime::Cp)
    }

    /// `name/key`, for a log line or an error — a stored key is otherwise four bytes of binary
    /// followed by the user's key. An id the catalog doesn't know prints as `#id`.
    pub fn render_key(&self, stored_key: &[u8]) -> String {
        let id = table_id_of(stored_key);
        let name = match self.get(id) {
            Some(t) => t.name.clone(),
            None => format!("#{id}"),
        };
        format!("{name}/{}", String::from_utf8_lossy(strip_table_prefix(stored_key)))
    }
}

/// The node's live catalog: an [`Arc<Tables>`] swapped wholesale on each update.
///
/// Copy-on-write rather than a lock around a map. A read holds the lock only long enough to
/// clone one `Arc`, so the request path never contends with a `create table` and never holds a
/// lock across an `.await`; the writer builds the new set off to the side and swaps a pointer.
pub struct Catalog {
    current: RwLock<Arc<Tables>>,
}

impl Catalog {
    /// A catalog holding only `default`.
    pub fn bootstrap() -> Arc<Catalog> {
        Arc::new(Catalog { current: RwLock::new(Arc::new(Tables::bootstrap())) })
    }

    /// The current set. Cheap: one `Arc` clone under a read lock.
    pub fn snapshot(&self) -> Arc<Tables> {
        self.current.read().expect("catalog poisoned").clone()
    }

    /// Replace the catalog wholesale — what a node does with the table list PD sends alongside
    /// an assignment. PD is the authority, so this is a replace rather than a merge.
    pub fn install(&self, tables: Vec<Table>) {
        *self.current.write().expect("catalog poisoned") = Arc::new(Tables::from(tables));
    }

    /// Add one table, keeping the rest — the local echo of a `create table` this node just made,
    /// so it can serve the new table without waiting for the next assignment.
    pub fn insert(&self, table: Table) {
        let mut tables = self.snapshot().list();
        tables.retain(|t| t.id != table.id && t.name != table.name);
        tables.push(table);
        self.install(tables);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_catalog_resolves_only_the_default_table() {
        let tables = Tables::bootstrap();
        assert_eq!(tables.id_of(""), Some(DEFAULT_TABLE_ID), "an omitted name is `default`");
        assert_eq!(tables.id_of(DEFAULT_TABLE_NAME), Some(DEFAULT_TABLE_ID));
        assert_eq!(tables.id_of("orders"), None, "an undeclared table does not resolve");
        assert_eq!(tables.list().len(), 1);
    }

    #[test]
    fn installing_from_pd_always_keeps_a_default_table() {
        // Even if PD somehow omitted it, a node must be able to resolve an omitted table name.
        let tables = Tables::from(vec![Table {
            id: 1,
            name: "orders".into(),
            regime: Regime::Ap,
        }]);
        assert_eq!(tables.id_of(""), Some(DEFAULT_TABLE_ID));
        assert_eq!(tables.id_of("orders"), Some(1));
        assert_eq!(tables.list().len(), 2, "default is added back");
    }

    #[test]
    fn a_keys_regime_comes_from_the_id_it_carries() {
        let tables =
            Tables::from(vec![Table { id: 1, name: "clicks".into(), regime: Regime::Ap }]);
        assert_eq!(tables.regime_of_key(&table_key(1, b"post7")), Regime::Ap);
        assert_eq!(tables.regime_of_key(&table_key(DEFAULT_TABLE_ID, b"k")), Regime::Cp);
        assert_eq!(tables.regime_of_key(&table_key(9, b"k")), Regime::Cp, "strong by default");
    }

    #[test]
    fn a_stored_key_renders_as_table_slash_key() {
        let tables =
            Tables::from(vec![Table { id: 1, name: "orders".into(), regime: Regime::Cp }]);
        assert_eq!(tables.render_key(&table_key(1, b"o1")), "orders/o1");
        assert_eq!(tables.render_key(&table_key(0, b"k")), "default/k");
        // An id this node has not learned yet must print, not panic.
        assert_eq!(tables.render_key(&table_key(42, b"k")), "#42/k");
    }

    #[test]
    fn install_replaces_and_insert_adds() {
        let catalog = Catalog::bootstrap();
        catalog.install(vec![Table { id: 1, name: "a".into(), regime: Regime::Cp }]);
        assert_eq!(catalog.snapshot().id_of("a"), Some(1));

        catalog.insert(Table { id: 2, name: "b".into(), regime: Regime::Ap });
        let tables = catalog.snapshot();
        assert_eq!(tables.id_of("a"), Some(1), "insert keeps what was there");
        assert_eq!(tables.id_of("b"), Some(2));

        catalog.install(vec![Table { id: 3, name: "c".into(), regime: Regime::Cp }]);
        let tables = catalog.snapshot();
        assert_eq!(tables.id_of("a"), None, "install replaces wholesale — PD is authoritative");
        assert_eq!(tables.id_of("c"), Some(3));
    }
}

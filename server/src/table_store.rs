//! Durable storage for the node's table declarations — the `name → regime` map that
//! [`crate::catalog::Catalog`] is built from at startup.
//!
//! Without this, a table declared live via `kv.CreateTable` existed only in
//! [`crate::AppState`]'s memory: a restart re-tiled the keyspace from the `--table` flags
//! alone, so the table vanished, an AP table silently came back CP (undeclared keys default
//! to CP), and it could not even be re-declared because its range now held data. The
//! declarations are the only piece of a live-created table that wasn't already durable — the
//! engine holds the data and [`arcux_pd::RegionRegistry`] persists the carved ranges.
//!
//! The format is plain text, one `name=regime` per line, mirroring `--table name=cp|ap` so
//! the file reads like the flags that would produce it and can be inspected or hand-edited:
//!
//! ```text
//! # arcux table catalog — restored at startup; edit with care.
//! clicks=ap
//! orders=cp
//! ```
//!
//! Writes go through a `.tmp` + fsync + `rename`, the same crash-atomic discipline the engine
//! manifest and the region table use: a reader after a crash sees the old image or the new
//! one, never a torn write.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::multiraft::Regime;

const CATALOG_FILE: &str = "catalog";

const HEADER: &str = "# arcux table catalog — restored at startup; edit with care.\n";

/// The catalog file inside a node's data directory.
pub fn path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(CATALOG_FILE)
}

/// Read the declared tables, or an empty list if the node has never written one.
///
/// A malformed line is a hard error rather than a skipped line: silently dropping an entry
/// would downgrade that table to the default CP regime, which is exactly the failure this
/// module exists to prevent.
pub fn load(data_dir: impl AsRef<Path>) -> io::Result<Vec<(String, Regime)>> {
    let path = path(data_dir);
    let mut text = String::new();
    match File::open(&path) {
        Ok(mut f) => f.read_to_string(&mut text)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut tables = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, regime) = line
            .split_once('=')
            .ok_or_else(|| corrupt(&path, i, line, "expected <name>=cp|ap"))?;
        let regime = match regime.trim().to_ascii_lowercase().as_str() {
            "cp" => Regime::Cp,
            "ap" => Regime::Ap,
            _ => return Err(corrupt(&path, i, line, "regime must be cp or ap")),
        };
        let name = name.trim();
        if name.is_empty() {
            return Err(corrupt(&path, i, line, "table name must not be empty"));
        }
        tables.push((name.to_string(), regime));
    }
    Ok(tables)
}

/// Replace the catalog with `tables`, atomically. Entries are written name-sorted so the file
/// is stable across rewrites regardless of declaration order.
pub fn save(data_dir: impl AsRef<Path>, tables: &[(String, Regime)]) -> io::Result<()> {
    let mut sorted: Vec<&(String, Regime)> = tables.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = String::from(HEADER);
    for (name, regime) in sorted {
        out.push_str(name);
        out.push('=');
        out.push_str(if *regime == Regime::Ap { "ap" } else { "cp" });
        out.push('\n');
    }
    atomic_write(&path(data_dir), out.as_bytes())
}

/// Atomically replace `path`'s contents: write a `.tmp`, fsync it, then `rename` over the
/// target. Mirrors `pd`'s region-table persistence, which is private to that crate.
fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    // Best-effort directory fsync so the rename itself is durable.
    if let Some(dir) = path.parent() {
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

fn corrupt(path: &Path, line_no: usize, line: &str, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}:{}: {why} (got {line:?})", path.display(), line_no + 1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_both_regimes() {
        let dir = tempfile::tempdir().unwrap();
        let tables =
            vec![("orders".to_string(), Regime::Cp), ("clicks".to_string(), Regime::Ap)];
        save(dir.path(), &tables).unwrap();

        // Sorted on write, so the file is stable regardless of declaration order.
        assert_eq!(
            load(dir.path()).unwrap(),
            vec![("clicks".to_string(), Regime::Ap), ("orders".to_string(), Regime::Cp)]
        );
    }

    #[test]
    fn a_node_that_never_wrote_one_loads_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(path(dir.path()), "# a comment\n\n  orders=cp  \n").unwrap();
        assert_eq!(load(dir.path()).unwrap(), vec![("orders".to_string(), Regime::Cp)]);
    }

    #[test]
    fn a_malformed_line_is_an_error_not_a_silent_skip() {
        let dir = tempfile::tempdir().unwrap();

        // Dropping this line would silently downgrade `orders` to the default CP regime.
        std::fs::write(path(dir.path()), "orders=quorum\n").unwrap();
        assert!(load(dir.path()).is_err());

        std::fs::write(path(dir.path()), "orders\n").unwrap();
        assert!(load(dir.path()).is_err());

        std::fs::write(path(dir.path()), "=cp\n").unwrap();
        assert!(load(dir.path()).is_err());
    }

    #[test]
    fn save_replaces_rather_than_appends() {
        let dir = tempfile::tempdir().unwrap();
        save(dir.path(), &[("orders".to_string(), Regime::Cp)]).unwrap();
        save(dir.path(), &[("clicks".to_string(), Regime::Ap)]).unwrap();
        assert_eq!(load(dir.path()).unwrap(), vec![("clicks".to_string(), Regime::Ap)]);
    }
}

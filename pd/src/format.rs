//! The on-disk format marker — what stops a directory written before numeric table ids from
//! being served under them.
//!
//! Table keys used to be name-prefixed (`orders/o1`); they are now `be32(id) ++ o1`, and PD's
//! region table and catalog changed shape with them. Nothing detects that by itself: an old
//! directory opens cleanly and every key in it simply belongs to a table nobody can name. So a
//! directory states its format, and a node refuses one it does not recognize.
//!
//! There is no migration. This is a pre-production system, and rewriting every key of an old
//! directory is a great deal of machinery for data nobody has.
//!
//! Lives in `pd` because both a data node and PD need it, and `pd` is the lowest crate they
//! share (`server` depends on `pd`; `pd` does not depend on `engine`) — the same reasoning that
//! put the region-boundary helpers in [`crate::region`].

use std::io;
use std::path::Path;

use crate::persist::atomic_write;

/// The current on-disk format. Bump when a change makes an existing directory unreadable.
///
/// 1. name-prefixed table keys (`orders/o1`), node-declared catalog.
/// 2. numeric table ids: `be32(id) ++ key`, PD-allocated ids, PD-owned catalog.
pub const FORMAT: u32 = 2;

const FORMAT_FILE: &str = "format";
const MARKER_PREFIX: &str = "arcux-format";

/// Check `dir`'s format, stamping it if the directory is new or empty.
///
/// A format-1 directory has no marker at all, so absence alone cannot mean "fresh" — an
/// unmarked directory with anything in it is an old one, and is refused. `what` names the
/// directory in that error ("data", "PD data").
pub fn check_or_init(dir: impl AsRef<Path>, what: &str) -> io::Result<()> {
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir)?;
    let path = dir.join(FORMAT_FILE);

    if let Some(bytes) = crate::persist::read_optional(&path)? {
        let text = String::from_utf8_lossy(&bytes);
        let found: Option<u32> =
            text.trim().strip_prefix(MARKER_PREFIX).and_then(|v| v.trim().parse().ok());
        return match found {
            Some(FORMAT) => Ok(()),
            Some(other) => Err(mismatch(format!(
                "{what} directory '{}' is arcux format {other}, but this build uses format \
                 {FORMAT}. There is no migration — point it at a fresh directory",
                dir.display()
            ))),
            None => Err(mismatch(format!(
                "{what} directory '{}' has an unreadable format marker. Point it at a fresh \
                 directory",
                dir.display()
            ))),
        };
    }

    // No marker: fresh if the directory holds nothing, an old (format 1) one otherwise.
    if dir.read_dir()?.next().is_some() {
        return Err(mismatch(format!(
            "{what} directory '{}' was written by an older arcux: table keys were name-prefixed \
             ('orders/o1') and are now a 4-byte numeric table id. There is no migration — point \
             --data at a fresh directory, or delete this one to start over",
            dir.display()
        )));
    }
    atomic_write(&path, format!("{MARKER_PREFIX} {FORMAT}\n").as_bytes())
}

fn mismatch(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_directory_is_stamped_and_then_accepted() {
        let dir = tempfile::tempdir().expect("tempdir");
        check_or_init(dir.path(), "data").expect("fresh directory");
        assert!(dir.path().join(FORMAT_FILE).exists(), "the marker is written");
        // Idempotent: opening the same directory again is fine.
        check_or_init(dir.path(), "data").expect("already stamped");
    }

    #[test]
    fn a_directory_with_data_and_no_marker_is_refused() {
        // Exactly what a format-1 data directory looks like: files, no marker.
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("00000000000000000002.wal"), b"old").expect("write");

        let err = check_or_init(dir.path(), "data").expect_err("an old directory must be refused");
        assert!(err.to_string().contains("no migration"), "the error says what to do: {err}");
    }

    #[test]
    fn a_marker_from_another_format_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join(FORMAT_FILE), b"arcux-format 99\n").expect("write");
        let err = check_or_init(dir.path(), "PD data").expect_err("a future format is refused");
        assert!(err.to_string().contains("format 99"), "the error names what it found: {err}");
    }

    #[test]
    fn a_crashed_atomic_write_leaves_a_directory_that_is_not_fresh() {
        // A stray `.tmp` means this directory was in use, so it is not a fresh one to stamp.
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("regions.tmp"), b"partial").expect("write");
        assert!(check_or_init(dir.path(), "data").is_err());
    }
}

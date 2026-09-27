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
/// 3. CP timestamps from PD's oracle. Until now each node stamped from its own clock, whose
///    values track wall-clock milliseconds; PD's replicated oracle is a counter, and its
///    timestamps are far *below* them. Opening a format-2 directory under it would write new
///    versions beneath the old ones — MVCC orders versions by `commit_ts`, so reads would keep
///    returning the superseded value. Refused instead.
pub const FORMAT: u32 = 3;

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
            Some(2) => Err(mismatch(format!(
                "{what} directory '{}' was written when each node stamped CP writes from its own \
                 clock. Timestamps now come from PD's oracle and are far lower, so new writes \
                 would land beneath the versions already there and reads would return the old \
                 ones. There is no migration — point --data at a fresh directory, or delete this \
                 one to start over",
                dir.display()
            ))),
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

const IDENTITY_FILE: &str = "identity";

/// Bind `dir` to the process that owns it — `role` (`"node"` or `"pd"`) and its `id` — and
/// return the directory's **store id**, a random token minted the first time it is claimed.
///
/// A Raft replica is its node id *and* its disk: the log and the votes it holds were cast under
/// that id. Started under another id, the same disk would vote twice in one election; two
/// processes under one id would let a leader count one acknowledgement from the wrong disk.
/// Either breaks the majority-overlap argument Raft's safety rests on, and before this nothing
/// stopped either — a directory reused as `-n 5`, or a second `-n 2`, just started.
///
/// So the directory remembers its owner and refuses anyone else, and the store id lets PD tell
/// two processes claiming one node id apart. A directory from before this file existed is
/// adopted by whichever id opens it first, which is the most that can be known about it.
pub fn claim_identity(dir: impl AsRef<Path>, role: &str, id: u64) -> io::Result<String> {
    let dir = dir.as_ref();
    let path = dir.join(IDENTITY_FILE);
    if let Some(bytes) = crate::persist::read_optional(&path)? {
        let text = String::from_utf8_lossy(&bytes);
        let (owner, store) = parse_identity(&text).ok_or_else(|| {
            mismatch(format!(
                "'{}' is unreadable. Point this {role} at a fresh directory",
                path.display()
            ))
        })?;
        if owner != (role.to_string(), id) {
            return Err(mismatch(format!(
                "data directory '{}' belongs to {} {}; start it with -n {}, or give {role} {id} a \
                 fresh directory",
                dir.display(),
                owner.0,
                owner.1,
                owner.1
            )));
        }
        return Ok(store);
    }
    let store = new_store_id();
    atomic_write(&path, format!("{role} {id}\nstore {store}\n").as_bytes())?;
    Ok(store)
}

fn parse_identity(text: &str) -> Option<((String, u64), String)> {
    let mut lines = text.lines();
    let (role, id) = lines.next()?.trim().split_once(' ')?;
    let store = lines.next()?.trim().strip_prefix("store ")?.to_string();
    Some(((role.to_string(), id.trim().parse().ok()?), store))
}

/// 128 random bits, hex. `RandomState` seeds each instance from the OS's randomness, so two
/// hashers built here share no key — no crate needed for a token that only has to be unique.
fn new_store_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut out = String::with_capacity(32);
    for _ in 0..2 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        h.write_u32(std::process::id());
        out.push_str(&format!("{:016x}", h.finish()));
    }
    out
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
    fn a_directory_is_claimed_once_and_then_keeps_its_owner_and_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = claim_identity(dir.path(), "node", 1).expect("first claim");
        assert_eq!(store.len(), 32, "128 bits, hex");
        assert_eq!(claim_identity(dir.path(), "node", 1).expect("same owner"), store, "stable");
    }

    #[test]
    fn a_directory_started_under_another_id_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        claim_identity(dir.path(), "node", 1).expect("first claim");
        let err = claim_identity(dir.path(), "node", 5).expect_err("node 5 must not open node 1's disk");
        let msg = err.to_string();
        assert!(msg.contains("belongs to node 1") && msg.contains("-n 1"), "says whose it is: {msg}");
        // A PD directory is not a node directory either.
        assert!(claim_identity(dir.path(), "pd", 1).is_err());
    }

    #[test]
    fn two_directories_never_share_a_store_id() {
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        assert_ne!(claim_identity(a.path(), "node", 1).unwrap(), claim_identity(b.path(), "node", 1).unwrap());
    }

    #[test]
    fn a_crashed_atomic_write_leaves_a_directory_that_is_not_fresh() {
        // A stray `.tmp` means this directory was in use, so it is not a fresh one to stamp.
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("regions.tmp"), b"partial").expect("write");
        assert!(check_or_init(dir.path(), "data").is_err());
    }
}

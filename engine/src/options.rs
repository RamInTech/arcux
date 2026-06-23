//! Engine configuration.

use std::path::PathBuf;

/// How aggressively a group commit is pushed to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// `File::sync_all` after each group commit. Survives `kill -9` and, on most
    /// platforms, power loss. NOTE: on macOS this does not flush the drive cache
    /// (see [`FsyncMode::FullFsync`]).
    Fsync,
    /// macOS `fcntl(F_FULLFSYNC)` — flushes the drive cache for true power-loss
    /// durability at a latency cost. Falls back to `sync_all` on other platforms.
    FullFsync,
    /// No fsync (tests/benchmarks only). Survives `kill -9` via the OS page cache,
    /// but not power loss / OS crash.
    None,
}

#[derive(Debug, Clone)]
pub struct Options {
    /// Directory holding WAL segments, SSTables, and the manifest.
    pub data_dir: PathBuf,
    /// Freeze the active memtable once its estimated byte size exceeds this.
    pub memtable_size_threshold: usize,
    /// Rotate to a new WAL segment once the current one exceeds this many bytes.
    pub wal_segment_size: usize,
    pub fsync_mode: FsyncMode,
}

impl Options {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Options {
            data_dir: data_dir.into(),
            memtable_size_threshold: 4 * 1024 * 1024, // 4 MiB
            wal_segment_size: 64 * 1024 * 1024,       // 64 MiB
            fsync_mode: FsyncMode::Fsync,
        }
    }

    /// Builder-style override of the memtable freeze threshold (handy in tests
    /// that want to force many flushes from small inputs).
    pub fn with_memtable_threshold(mut self, bytes: usize) -> Self {
        self.memtable_size_threshold = bytes;
        self
    }

    pub fn with_fsync_mode(mut self, mode: FsyncMode) -> Self {
        self.fsync_mode = mode;
        self
    }
}

//! Engine error type. Library-style: every fallible path returns [`Result`].

use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// On-disk data failed an integrity check (CRC mismatch, bad magic, truncated
    /// record that is *not* a recoverable torn tail, etc.).
    #[error("corruption: {0}")]
    Corruption(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Percolator prewrite conflict: another txn holds a lock on the key, or a
    /// commit newer than our `start_ts` exists (write-after-snapshot).
    #[error("transaction conflict: {0}")]
    Conflict(String),

    /// A leftover lock was encountered whose primary is still pending with a
    /// valid TTL; the caller should back off and retry.
    #[error("key is locked (txn still in flight): {0}")]
    KeyIsLocked(String),
}

impl Error {
    pub fn corruption(msg: impl Into<String>) -> Error {
        Error::Corruption(msg.into())
    }
    pub fn invalid(msg: impl Into<String>) -> Error {
        Error::InvalidArgument(msg.into())
    }
    pub fn conflict(msg: impl Into<String>) -> Error {
        Error::Conflict(msg.into())
    }
}

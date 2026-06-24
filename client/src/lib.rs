//! arcux Phase 2 — async gRPC client SDK.
//!
//! A thin async wrapper over the generated `KvServiceClient` holding one shared HTTP/2
//! [`Channel`] (multiplexed; the connection-pool-per-peer generalization arrives in
//! Phase 3+). It mirrors the KV RPCs and adds a [`Client::transact`] convenience that
//! runs `begin → prewrite → commit`. Application-level conflicts/locks surface as a
//! typed [`ClientError::Key`]; the blocking client is deferred to Phase 2b.

use std::fmt;

use arcux_rpc::kv::key_error::Kind;
use arcux_rpc::kv::kv_service_client::KvServiceClient;
use arcux_rpc::kv::{self};
use tonic::transport::Channel;

pub use arcux_rpc::kv::Mutation;

/// A generous logical lease (TSO ticks) added to `start_ts` to form a lock's expiry in
/// the [`Client::transact`] convenience. The server TSO is a monotonic counter, so this
/// keeps a transaction's locks from ever looking expired mid-flight.
const DEFAULT_LEASE: u64 = 1 << 32;

pub type Result<T> = std::result::Result<T, ClientError>;

/// Errors a client call can surface.
#[derive(Debug)]
pub enum ClientError {
    /// Transport/connection failure (could not reach the server, bad URI, …).
    Transport(tonic::transport::Error),
    /// The RPC itself failed with a gRPC status (e.g. `Unimplemented`, `Internal`).
    Rpc(tonic::Status),
    /// A normal protocol outcome the server reported in-band (conflict, live lock, …).
    Key(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Transport(e) => write!(f, "transport error: {e}"),
            ClientError::Rpc(s) => write!(f, "rpc error: {} ({})", s.message(), s.code()),
            ClientError::Key(m) => write!(f, "key error: {m}"),
        }
    }
}

impl std::error::Error for ClientError {}

fn key_error(ke: kv::KeyError) -> ClientError {
    let msg = match ke.kind {
        Some(Kind::Conflict(c)) => format!("conflict: {}", c.detail),
        Some(Kind::Locked(l)) => format!("locked by primary {:?} (ttl {})", l.primary, l.ttl),
        Some(Kind::Invalid(s)) => format!("invalid: {s}"),
        Some(Kind::Retryable(s)) => format!("retryable: {s}"),
        Some(Kind::NotLeader(_)) => "not leader".to_string(),
        Some(Kind::RegionStale(_)) => "region stale".to_string(),
        None => "unspecified key error".to_string(),
    };
    ClientError::Key(msg)
}

/// Build a PUT mutation for use with [`Client::transact`].
pub fn put_mutation(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Mutation {
    Mutation { op: kv::Op::Put as i32, key: key.into(), value: value.into() }
}

/// Build a DELETE mutation for use with [`Client::transact`].
pub fn delete_mutation(key: impl Into<Vec<u8>>) -> Mutation {
    Mutation { op: kv::Op::Delete as i32, key: key.into(), value: vec![] }
}

/// An async client over one multiplexed HTTP/2 channel. Cheap to `clone` (the channel
/// is shared), so concurrent callers each take their own handle.
#[derive(Clone)]
pub struct Client {
    kv: KvServiceClient<Channel>,
}

impl Client {
    /// Connect lazily to `uri` (e.g. `"http://127.0.0.1:50051"`). The TCP/HTTP-2
    /// connection is established on the first request, so there is no startup race with
    /// a server that is still binding.
    pub fn connect(uri: impl Into<String>) -> Result<Client> {
        let channel = Channel::from_shared(uri.into())
            .map_err(|e| ClientError::Key(format!("invalid endpoint: {e}")))?
            .connect_lazy();
        Ok(Client { kv: KvServiceClient::new(channel) })
    }

    /// Allocate a transaction `start_ts`.
    pub async fn begin(&mut self) -> Result<u64> {
        let resp = self
            .kv
            .begin(kv::BeginRequest {})
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        Ok(resp.start_ts)
    }

    /// Prewrite all mutations (primary first). Returns the first per-key error if any.
    pub async fn prewrite(
        &mut self,
        start_ts: u64,
        primary: Vec<u8>,
        mutations: Vec<Mutation>,
        ttl: u64,
    ) -> Result<()> {
        let resp = self
            .kv
            .prewrite(kv::PrewriteRequest { start_ts, primary, mutations, ttl })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        match resp.errors.into_iter().next() {
            Some(ke) => Err(key_error(ke)),
            None => Ok(()),
        }
    }

    /// Commit a prewritten transaction; returns the server-assigned `commit_ts`.
    pub async fn commit(
        &mut self,
        start_ts: u64,
        primary: Vec<u8>,
        keys: Vec<Vec<u8>>,
    ) -> Result<u64> {
        let resp = self
            .kv
            .commit(kv::CommitRequest { start_ts, primary, keys })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        match resp.error {
            Some(ke) => Err(key_error(ke)),
            None => Ok(resp.commit_ts),
        }
    }

    /// Snapshot read at "now" (the server picks a fresh `read_ts`).
    pub async fn get(&mut self, key: impl Into<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        self.get_at(key, 0).await
    }

    /// Snapshot read at an explicit `read_ts` (0 ⇒ server picks "now").
    pub async fn get_at(&mut self, key: impl Into<Vec<u8>>, read_ts: u64) -> Result<Option<Vec<u8>>> {
        let resp = self
            .kv
            .get(kv::GetRequest { key: key.into(), read_ts })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        if let Some(ke) = resp.error {
            return Err(key_error(ke));
        }
        Ok(if resp.found { Some(resp.value) } else { None })
    }

    /// Autocommit single-key put; returns the `commit_ts`.
    pub async fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<u64> {
        let resp = self
            .kv
            .put(kv::PutRequest { key: key.into(), value: value.into() })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        match resp.error {
            Some(ke) => Err(key_error(ke)),
            None => Ok(resp.commit_ts),
        }
    }

    /// Autocommit single-key delete; returns the `commit_ts`.
    pub async fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<u64> {
        let resp = self
            .kv
            .delete(kv::DeleteRequest { key: key.into() })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        match resp.error {
            Some(ke) => Err(key_error(ke)),
            None => Ok(resp.commit_ts),
        }
    }

    /// Range scan — frozen in the wire contract, but the server returns `Unimplemented`
    /// until the Phase 1b iterator lands.
    pub async fn scan(
        &mut self,
        start_key: impl Into<Vec<u8>>,
        end_key: impl Into<Vec<u8>>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let resp = self
            .kv
            .scan(kv::ScanRequest {
                start_key: start_key.into(),
                end_key: end_key.into(),
                limit,
                read_ts: 0,
            })
            .await
            .map_err(ClientError::Rpc)?
            .into_inner();
        Ok(resp.pairs.into_iter().map(|p| (p.key, p.value)).collect())
    }

    /// Convenience: run a full transaction (`begin → prewrite → commit`) over `mutations`
    /// (the first is the primary). Returns the `commit_ts`.
    pub async fn transact(&mut self, mutations: Vec<Mutation>) -> Result<u64> {
        let primary = match mutations.first() {
            Some(m) => m.key.clone(),
            None => return Err(ClientError::Key("empty transaction".to_string())),
        };
        let keys: Vec<Vec<u8>> = mutations.iter().map(|m| m.key.clone()).collect();
        let start_ts = self.begin().await?;
        let ttl = start_ts.saturating_add(DEFAULT_LEASE);
        self.prewrite(start_ts, primary.clone(), mutations, ttl).await?;
        self.commit(start_ts, primary, keys).await
    }
}

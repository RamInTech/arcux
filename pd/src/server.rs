//! Running the PD as a gRPC server.

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use arcux_rpc::pd::pd_service_server::PdServiceServer;

use crate::cluster::now_ms;
use crate::{Membership, PdApi, Region, Tso};

/// Default failure-detector timeout: a node silent for longer than this is marked down.
pub const DEFAULT_FD_TIMEOUT_MS: u64 = 6_000;
/// Default sweep period for the failure detector.
pub const DEFAULT_FD_INTERVAL_MS: u64 = 1_000;

/// A PD instance's shared state: the authoritative TSO plus the per-node membership +
/// region placement that data nodes populate via heartbeats.
pub struct Pd {
    pub tso: Arc<Tso>,
    pub members: Arc<Membership>,
    /// A node silent for longer than this (ms) is marked down by the failure detector.
    pub fd_timeout_ms: u64,
    /// How often (ms) the failure-detector sweep runs.
    pub fd_interval_ms: u64,
}

impl Pd {
    /// Open a restart-safe PD rooted at `dir` (the TSO watermark persists there). The
    /// membership view starts empty and is rebuilt from node heartbeats.
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Pd> {
        Ok(Pd::from_parts(Arc::new(Tso::open(dir)?), Arc::new(Membership::new())))
    }

    /// An in-memory PD with no restart safety (tests / in-process harnesses).
    pub fn ephemeral() -> Pd {
        Pd::from_parts(Arc::new(Tso::ephemeral()), Arc::new(Membership::new()))
    }

    /// An in-memory PD seeded with an initial region→node placement, for standing up a
    /// keyspace pre-partitioned across several nodes (tests / bootstrap).
    pub fn seeded(seed: Vec<(Region, u64)>) -> Pd {
        Pd::from_parts(Arc::new(Tso::ephemeral()), Arc::new(Membership::seeded(seed)))
    }

    fn from_parts(tso: Arc<Tso>, members: Arc<Membership>) -> Pd {
        Pd {
            tso,
            members,
            fd_timeout_ms: DEFAULT_FD_TIMEOUT_MS,
            fd_interval_ms: DEFAULT_FD_INTERVAL_MS,
        }
    }

    /// Override the failure-detector timing (tests use a short timeout so a stopped node
    /// is detected quickly).
    pub fn with_failure_detector(mut self, timeout_ms: u64, interval_ms: u64) -> Pd {
        self.fd_timeout_ms = timeout_ms;
        self.fd_interval_ms = interval_ms.max(1);
        self
    }
}

/// Serve `PdService` on an already-bound listener until `shutdown` resolves, running the
/// failure-detector sweep in the background. Used by both [`serve`] and in-process tests.
pub async fn serve_on<F>(
    pd: Arc<Pd>,
    listener: TcpListener,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    // Failure detector: periodically mark silent nodes down. Detached; it stops when the
    // runtime is dropped (process exit / test teardown).
    let members = pd.members.clone();
    let timeout = pd.fd_timeout_ms;
    let interval = pd.fd_interval_ms;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(interval));
        loop {
            tick.tick().await;
            for id in members.sweep(now_ms(), timeout) {
                eprintln!("pd: node {id} marked down (no heartbeat within {timeout}ms)");
            }
        }
    });

    let api = PdApi::new(pd.tso.clone(), pd.members.clone());
    Server::builder()
        .add_service(PdServiceServer::new(api))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

/// Open a PD under `dir`, bind `addr`, and serve until Ctrl-C.
pub async fn serve(
    dir: impl AsRef<Path>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pd = Arc::new(Pd::open(dir)?);
    let listener = TcpListener::bind(addr).await?;
    eprintln!("arcux-pd listening on {}", listener.local_addr()?);
    serve_on(pd, listener, async {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("arcux-pd shutting down");
    })
    .await?;
    Ok(())
}

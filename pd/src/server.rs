//! Running the PD as a gRPC server.

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use arcux_rpc::pd::pd_service_server::PdServiceServer;

use crate::{PdApi, RegionRegistry, Tso};

/// A PD instance's shared state: the authoritative TSO plus the aggregated region view
/// that data nodes populate via heartbeats.
pub struct Pd {
    pub tso: Arc<Tso>,
    pub regions: Arc<RegionRegistry>,
}

impl Pd {
    /// Open a restart-safe PD rooted at `dir` (the TSO watermark persists there). The
    /// region view starts empty and is rebuilt from node heartbeats.
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Pd> {
        Ok(Pd { tso: Arc::new(Tso::open(dir)?), regions: Arc::new(RegionRegistry::empty()) })
    }

    /// An in-memory PD with no restart safety (tests / in-process harnesses).
    pub fn ephemeral() -> Pd {
        Pd { tso: Arc::new(Tso::ephemeral()), regions: Arc::new(RegionRegistry::empty()) }
    }
}

/// Serve `PdService` on an already-bound listener until `shutdown` resolves. Used by
/// both [`serve`] and in-process tests (which bind an ephemeral port).
pub async fn serve_on<F>(
    pd: Arc<Pd>,
    listener: TcpListener,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    let api = PdApi::new(pd.tso.clone(), pd.regions.clone());
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
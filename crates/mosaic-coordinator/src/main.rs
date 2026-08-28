use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use mosaic_coordinator::Coordinator;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Parser)]
#[command(name = "mosaic-coordinator", about = "Mosaic Fabric coordinator")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    #[arg(long, default_value_t = 15_000)]
    membership_ttl_ms: u64,
    #[arg(long, default_value_t = 60_000)]
    execution_ttl_ms: u64,
    #[arg(long, default_value_t = 250)]
    maintenance_interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mosaic_coordinator=info".into()),
        )
        .init();

    let args = Args::parse();
    let coordinator = Coordinator::new(args.membership_ttl_ms, args.execution_ttl_ms);
    let maintenance = coordinator.clone();
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_millis(args.maintenance_interval_ms.max(10)));
        loop {
            interval.tick().await;
            maintenance.maintenance_tick().await;
        }
    });

    let listener = TcpListener::bind(args.bind).await?;
    info!(address = %listener.local_addr()?, "coordinator listening");
    axum::serve(listener, coordinator.router()).await?;
    Ok(())
}

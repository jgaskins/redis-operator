use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Args, Parser, Subcommand};
use kube::Client;
use tracing_subscriber::EnvFilter;

mod controller;
mod crd;
mod error;
mod metrics;

use crate::controller::Context;
use crate::metrics::Metrics;
use crate::metrics::collector::PollConfig;

#[derive(Parser, Debug)]
#[command(name = "redis-operator", about = "Kubernetes operator for Redis")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    // Flattened onto the top level, not onto `Run`, so that a bare invocation
    // with no subcommand still accepts them — `Run` is the default command.
    #[command(flatten)]
    metrics: MetricsArgs,
}

#[derive(Args, Debug)]
struct MetricsArgs {
    /// Address for the metrics and health HTTP server. A bind failure is fatal:
    /// an operator that silently runs without its metrics endpoint is worse
    /// than one that refuses to start.
    #[arg(long, env = "METRICS_ADDR", default_value = "0.0.0.0:8080")]
    metrics_addr: SocketAddr,

    /// Seconds between INFO polls of the managed Redis pods. Metrics are at
    /// most this stale; keep it at or below the Prometheus scrape interval.
    #[arg(long, env = "METRICS_POLL_SECONDS", default_value_t = 15)]
    metrics_poll_seconds: u64,

    /// Per-pod INFO timeout, in seconds. Must stay well inside the poll
    /// interval, since the worst case is one timeout per concurrency slot.
    #[arg(long, env = "METRICS_SCRAPE_TIMEOUT_SECONDS", default_value_t = 2)]
    metrics_scrape_timeout_seconds: u64,

    /// Maximum pods scraped concurrently.
    #[arg(long, env = "METRICS_POLL_CONCURRENCY", default_value_t = 16)]
    metrics_poll_concurrency: usize,
}

impl MetricsArgs {
    fn poll_config(&self) -> PollConfig {
        PollConfig {
            interval: Duration::from_secs(self.metrics_poll_seconds),
            scrape_timeout: Duration::from_secs(self.metrics_scrape_timeout_seconds),
            concurrency: self.metrics_poll_concurrency.max(1),
        }
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the operator (default).
    Run,
    /// Print the CRD YAML to stdout.
    Crds,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Run) {
        Command::Run => run(&cli.metrics).await,
        Command::Crds => print_crds(),
    }
}

async fn run(args: &MetricsArgs) -> anyhow::Result<()> {
    let client = Client::try_default()
        .await
        .context("failed to create kube client")?;

    let metrics = Arc::new(Metrics::new());
    let ctx = Arc::new(Context::new(client, metrics.clone()));

    // `try_join!` still fits: it waits for every branch, so a server returning
    // Ok at shutdown is harmless, and it aborts everything on the first Err,
    // which is what a bind failure should do. The two new branches must return
    // on SIGTERM, or the process would sit here after the controllers stopped
    // until the kubelet lost patience — both honour `shutdown_signal()`.
    tokio::try_join!(
        controller::redis::run(ctx.clone()),
        controller::redis_cluster::run(ctx.clone()),
        metrics::collector::run(ctx.clone(), args.poll_config()),
        metrics::server::run(metrics, args.metrics_addr),
    )?;

    Ok(())
}

fn print_crds() -> anyhow::Result<()> {
    print!("{}", crd::render()?);
    Ok(())
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,kube=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

use std::sync::Arc;

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use kube::Client;
use tracing_subscriber::EnvFilter;

mod controller;
mod crd;
mod error;

use crate::controller::Context;

#[derive(Parser, Debug)]
#[command(name = "redis-operator", about = "Kubernetes operator for Redis")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
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
        Command::Run => run().await,
        Command::Crds => print_crds(),
    }
}

async fn run() -> anyhow::Result<()> {
    let client = Client::try_default()
        .await
        .context("failed to create kube client")?;

    let ctx = Arc::new(Context::new(client));

    tokio::try_join!(
        controller::redis::run(ctx.clone()),
        controller::redis_cluster::run(ctx.clone()),
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

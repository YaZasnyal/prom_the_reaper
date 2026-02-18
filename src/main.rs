mod config;
mod hasher;
mod parser;
mod scraper;
mod server;
mod state;
#[cfg(test)]
mod tests;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::AppConfig;
use crate::state::empty_state;

#[derive(Parser)]
#[command(name = "prom_the_reaper", about = "Prometheus metrics sharding proxy")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to config file
    #[arg(default_value = "config.toml")]
    config: PathBuf,
}

#[derive(Subcommand)]
enum Command {
    /// Print a sample configuration file and exit
    GenerateConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Return freed memory to the OS promptly instead of the default 1 s delay.
    // Can be overridden at runtime via MIMALLOC_PURGE_DELAY env var.
    if std::env::var_os("MIMALLOC_PURGE_DELAY").is_none() {
        // SAFETY: called before any threads are spawned (tokio runtime starts after this).
        unsafe { std::env::set_var("MIMALLOC_PURGE_DELAY", "0") };
    }

    let cli = Cli::parse();

    if let Some(Command::GenerateConfig) = cli.command {
        print!("{}", SAMPLE_CONFIG);
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = AppConfig::load(&cli.config)?;

    info!(
        listen = %config.listen,
        num_shards = config.num_shards,
        sources = config.sources.len(),
        "starting prom_the_reaper"
    );

    let num_shards = config.num_shards;
    let listen_addr = config.listen.clone();
    let config = Arc::new(config);
    let shared_state = Arc::new(ArcSwap::new(empty_state()));

    tokio::spawn(scraper::run_scrape_loop(
        config.clone(),
        shared_state.clone(),
    ));

    let app = server::router(shared_state, num_shards);
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    info!(addr = %listen_addr, "listening");
    axum::serve(listener, app).await?;

    Ok(())
}

const SAMPLE_CONFIG: &str = r#"# prom_the_reaper configuration

# Address to listen on
listen = "0.0.0.0:9090"

# Number of shards to split metrics into.
# Uses consistent hashing (xxh3 + jump hash), so changing this
# moves only ~1/N of metrics to different shards.
num_shards = 4

# How often to scrape upstream sources (seconds)
scrape_interval_secs = 30

# Upstream Prometheus-compatible metric sources.
# All sources are scraped in parallel.

[[sources]]
url = "http://ceph-exporter:9283/metrics"
timeout_secs = 25

# Scrape own operational metrics (shard sizes, scrape durations, etc.)
# exposed at /metrics. Adjust the address to match "listen" above.
[[sources]]
url = "http://127.0.0.1:9090/metrics"
timeout_secs = 5

# [[sources]]
# url = "http://node-exporter:9100/metrics"
# timeout_secs = 10
# headers = { "Authorization" = "Bearer token123" }
"#;

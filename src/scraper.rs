use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use tokio::task::JoinSet;
use tokio::time;
use tracing::{error, info, warn};

use crate::config::{AppConfig, SourceConfig};
use crate::parser::parse_families;
use crate::state::{ShardedState, SharedState, SourceStatus, build_shards};

pub async fn run_scrape_loop(config: Arc<AppConfig>, state: SharedState) {
    let client = Client::builder()
        .build()
        .expect("failed to build HTTP client");

    let mut interval = time::interval(Duration::from_secs(config.scrape_interval_secs));

    loop {
        interval.tick().await;
        info!("starting scrape cycle");
        let scrape_start = Instant::now();

        let results = scrape_all(&client, &config.sources).await;

        let mut all_families = Vec::new();
        let mut source_statuses = Vec::new();
        let mut any_success = false;

        for (url, result) in results {
            match result {
                Ok((families, duration)) => {
                    info!(
                        url = %url,
                        families = families.len(),
                        duration_ms = duration.as_millis() as u64,
                        "scraped source"
                    );
                    source_statuses.push(SourceStatus {
                        url: url.clone(),
                        success: true,
                        duration,
                        metric_families: families.len(),
                    });
                    all_families.extend(families);
                    any_success = true;
                }
                Err((e, duration)) => {
                    warn!(url = %url, error = %e, "failed to scrape source");
                    source_statuses.push(SourceStatus {
                        url,
                        success: false,
                        duration,
                        metric_families: 0,
                    });
                }
            }
        }

        if any_success {
            let shards = build_shards(all_families, config.num_shards);
            let new_state = Arc::new(ShardedState {
                shards,
                last_scrape: Instant::now(),
                source_status: source_statuses,
            });
            state.store(new_state);
            info!(
                duration_ms = scrape_start.elapsed().as_millis() as u64,
                "scrape cycle complete"
            );
        } else {
            error!("all sources failed, keeping stale data");
        }
    }
}

type ScrapeResult = (
    String,
    Result<(Vec<crate::parser::MetricFamily>, Duration), (String, Duration)>,
);

async fn scrape_all(client: &Client, sources: &[SourceConfig]) -> Vec<ScrapeResult> {
    let mut join_set: JoinSet<ScrapeResult> = JoinSet::new();

    for source in sources {
        let client = client.clone();
        let url = source.url.clone();
        let timeout = Duration::from_secs(source.timeout_secs);
        let headers = source.headers.clone();

        join_set.spawn(async move {
            let start = Instant::now();
            let mut req = client.get(&url).timeout(timeout);
            for (k, v) in &headers {
                req = req.header(k.as_str(), v.as_str());
            }

            let result = async {
                let body = req.send().await?.text().await?;
                Ok::<_, reqwest::Error>(body)
            }
            .await;

            match result {
                Ok(body) => {
                    let families = parse_families(&body);
                    let duration = start.elapsed();
                    (url, Ok((families, duration)))
                }
                Err(e) => {
                    let duration = start.elapsed();
                    (url, Err((e.to_string(), duration)))
                }
            }
        });
    }

    let mut results = Vec::with_capacity(sources.len());
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(item) => results.push(item),
            Err(e) => error!("scrape task panicked: {}", e),
        }
    }
    results
}

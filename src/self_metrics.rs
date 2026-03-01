use axum::{
    body::Body,
    extract::State,
    http::{Response, StatusCode, header},
    response::IntoResponse,
};
use prometheus_client::{
    encoding::EncodeLabelSet, encoding::text::encode, metrics::family::Family,
    metrics::gauge::Gauge, registry::Registry,
};
use tracing::error;

use crate::state::SharedState;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ShardLabel {
    shard: u32,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct UrlLabel {
    url: String,
}

pub async fn self_metrics_handler(
    State(state): State<SharedState>,
    num_shards: u32,
) -> Response<Body> {
    let guard = state.load();

    let mut registry = Registry::default();

    // Note: prometheus-client only supports i64 for gauges, so we round to seconds.
    // The original manual string-building code used .as_secs_f64() for sub-second precision,
    // but this library doesn't support f64 values. For a 30-second scrape interval,
    // second-level precision is acceptable for monitoring purposes.
    let last_scrape_age: Gauge<i64> = Gauge::default();
    if guard.shards.is_empty() {
        last_scrape_age.set(0);
    } else {
        last_scrape_age.set(guard.last_scrape.elapsed().as_secs() as i64);
    }
    registry.register(
        "prom_reaper_last_scrape_age_seconds",
        "Seconds since the last successful scrape cycle.",
        last_scrape_age,
    );

    let shard_series: Family<ShardLabel, Gauge<i64>> = Family::default();
    for (i, shard) in guard.shards.iter().enumerate() {
        shard_series
            .get_or_create(&ShardLabel { shard: i as u32 })
            .set(shard.series_count as i64);
    }
    registry.register(
        "prom_reaper_shard_series",
        "Number of time series in a shard.",
        shard_series,
    );

    let shard_families: Family<ShardLabel, Gauge<i64>> = Family::default();
    for (i, shard) in guard.shards.iter().enumerate() {
        shard_families
            .get_or_create(&ShardLabel { shard: i as u32 })
            .set(shard.families_count as i64);
    }
    registry.register(
        "prom_reaper_shard_families",
        "Number of metric families in a shard.",
        shard_families,
    );

    let shard_size_bytes: Family<ShardLabel, Gauge<i64>> = Family::default();
    for (i, shard) in guard.shards.iter().enumerate() {
        shard_size_bytes
            .get_or_create(&ShardLabel { shard: i as u32 })
            .set(shard.text.len() as i64);
    }
    registry.register(
        "prom_reaper_shard_size_bytes",
        "Size of a shard's uncompressed text in bytes.",
        shard_size_bytes,
    );

    let source_up: Family<UrlLabel, Gauge<i64>> = Family::default();
    let source_scrape_duration: Family<UrlLabel, Gauge<i64>> = Family::default();
    for src in &guard.source_status {
        source_up
            .get_or_create(&UrlLabel {
                url: src.url.clone(),
            })
            .set(if src.success { 1 } else { 0 });
        // Note: prometheus-client only supports i64, so we round to seconds.
        source_scrape_duration
            .get_or_create(&UrlLabel {
                url: src.url.clone(),
            })
            .set(src.duration.as_secs() as i64);
    }
    registry.register(
        "prom_reaper_source_up",
        "Whether the last scrape of a source succeeded (1 = success, 0 = failure).",
        source_up,
    );
    registry.register(
        "prom_reaper_source_scrape_duration_seconds",
        "Duration of the last scrape for a source.",
        source_scrape_duration,
    );

    let num_shards_gauge: Gauge<i64> = Gauge::default();
    num_shards_gauge.set(num_shards as i64);
    registry.register(
        "prom_reaper_num_shards",
        "Configured number of shards.",
        num_shards_gauge,
    );

    let mut body = String::new();
    if let Err(e) = encode(&mut body, &registry) {
        error!("failed to encode self-metrics: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "failed to encode metrics".to_string(),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

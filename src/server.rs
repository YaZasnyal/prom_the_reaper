use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::json;

use crate::state::SharedState;

pub fn router(state: SharedState, num_shards: u32) -> Router {
    Router::new()
        .route(
            "/metrics/shard/{id}",
            get(move |state, path, headers| shard_handler(state, path, headers, num_shards)),
        )
        .route("/health", get(health_handler))
        .route(
            "/status",
            get(move |state| status_handler(state, num_shards)),
        )
        .route(
            "/metrics",
            get(move |state| self_metrics_handler(state, num_shards)),
        )
        .with_state(state)
}

async fn shard_handler(
    State(state): State<SharedState>,
    Path(id): Path<u32>,
    headers: HeaderMap,
    num_shards: u32,
) -> Response {
    if id >= num_shards {
        return (
            StatusCode::NOT_FOUND,
            format!("shard {} not found, valid range is 0..{}", id, num_shards),
        )
            .into_response();
    }

    let guard = state.load();
    if guard.shards.is_empty() {
        return (StatusCode::SERVICE_UNAVAILABLE, "metrics not yet available").into_response();
    }

    let shard = &guard.shards[id as usize];

    let accepts_gzip = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("gzip"));

    if accepts_gzip {
        (
            StatusCode::OK,
            [
                (
                    header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                ),
                (header::CONTENT_ENCODING, "gzip"),
            ],
            shard.gzip.clone(),
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            shard.text.clone(),
        )
            .into_response()
    }
}

async fn health_handler(State(state): State<SharedState>) -> Response {
    let guard = state.load();
    if guard.shards.is_empty() {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    } else {
        (StatusCode::OK, "ok").into_response()
    }
}

async fn status_handler(State(state): State<SharedState>, num_shards: u32) -> Response {
    let guard = state.load();
    if guard.shards.is_empty() {
        return (StatusCode::SERVICE_UNAVAILABLE, "no data yet").into_response();
    }

    let shards: Vec<_> = guard
        .shards
        .iter()
        .enumerate()
        .map(|(i, s)| {
            json!({
                "id": i,
                "size_bytes": s.text.len(),
                "families": s.families_count,
                "series": s.series_count,
            })
        })
        .collect();

    let sources: Vec<_> = guard
        .source_status
        .iter()
        .map(|s| {
            json!({
                "url": s.url,
                "success": s.success,
                "duration_ms": s.duration.as_millis() as u64,
                "metric_families": s.metric_families,
            })
        })
        .collect();

    let body = json!({
        "num_shards": num_shards,
        "last_scrape_ago_secs": guard.last_scrape.elapsed().as_secs_f64(),
        "sources": sources,
        "shards": shards,
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

async fn self_metrics_handler(State(state): State<SharedState>, num_shards: u32) -> Response {
    let guard = state.load();
    let mut out = String::new();

    // last scrape age
    out.push_str("# HELP prom_reaper_last_scrape_age_seconds Seconds since the last successful scrape cycle.\n");
    out.push_str("# TYPE prom_reaper_last_scrape_age_seconds gauge\n");
    if guard.shards.is_empty() {
        out.push_str("prom_reaper_last_scrape_age_seconds NaN\n");
    } else {
        out.push_str(&format!(
            "prom_reaper_last_scrape_age_seconds {:.3}\n",
            guard.last_scrape.elapsed().as_secs_f64()
        ));
    }

    // per-shard series and families
    out.push_str("# HELP prom_reaper_shard_series Number of time series in a shard.\n");
    out.push_str("# TYPE prom_reaper_shard_series gauge\n");
    for (i, shard) in guard.shards.iter().enumerate() {
        out.push_str(&format!(
            "prom_reaper_shard_series{{shard=\"{}\"}} {}\n",
            i, shard.series_count
        ));
    }

    out.push_str("# HELP prom_reaper_shard_families Number of metric families in a shard.\n");
    out.push_str("# TYPE prom_reaper_shard_families gauge\n");
    for (i, shard) in guard.shards.iter().enumerate() {
        out.push_str(&format!(
            "prom_reaper_shard_families{{shard=\"{}\"}} {}\n",
            i, shard.families_count
        ));
    }

    out.push_str(
        "# HELP prom_reaper_shard_size_bytes Size of a shard's uncompressed text in bytes.\n",
    );
    out.push_str("# TYPE prom_reaper_shard_size_bytes gauge\n");
    for (i, shard) in guard.shards.iter().enumerate() {
        out.push_str(&format!(
            "prom_reaper_shard_size_bytes{{shard=\"{}\"}} {}\n",
            i,
            shard.text.len()
        ));
    }

    // per-source scrape status
    out.push_str("# HELP prom_reaper_source_up Whether the last scrape of a source succeeded (1 = success, 0 = failure).\n");
    out.push_str("# TYPE prom_reaper_source_up gauge\n");
    for src in &guard.source_status {
        out.push_str(&format!(
            "prom_reaper_source_up{{url=\"{}\"}} {}\n",
            src.url,
            if src.success { 1 } else { 0 }
        ));
    }

    out.push_str("# HELP prom_reaper_source_scrape_duration_seconds Duration of the last scrape for a source.\n");
    out.push_str("# TYPE prom_reaper_source_scrape_duration_seconds gauge\n");
    for src in &guard.source_status {
        out.push_str(&format!(
            "prom_reaper_source_scrape_duration_seconds{{url=\"{}\"}} {:.3}\n",
            src.url,
            src.duration.as_secs_f64()
        ));
    }

    out.push_str("# HELP prom_reaper_num_shards Configured number of shards.\n");
    out.push_str("# TYPE prom_reaper_num_shards gauge\n");
    out.push_str(&format!("prom_reaper_num_shards {num_shards}\n"));

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
        .into_response()
}

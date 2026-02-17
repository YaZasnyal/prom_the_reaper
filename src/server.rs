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

    let shard_sizes: Vec<usize> = guard.shards.iter().map(|s| s.text.len()).collect();
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
        "shard_sizes_bytes": shard_sizes,
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

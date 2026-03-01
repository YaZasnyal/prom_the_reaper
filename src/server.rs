use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::json;
use tower_http::compression::CompressionLayer;

use crate::self_metrics;
use crate::state::SharedState;

pub fn router(state: SharedState, num_shards: u32) -> Router {
    Router::new()
        .route(
            "/metrics/shard/{id}",
            get(move |state, path| shard_handler(state, path, num_shards)),
        )
        .route("/health", get(health_handler))
        .route(
            "/status",
            get(move |state| status_handler(state, num_shards)),
        )
        .route(
            "/metrics",
            get(move |state| self_metrics::self_metrics_handler(state, num_shards)),
        )
        .layer(CompressionLayer::new())
        .with_state(state)
}

async fn shard_handler(
    State(state): State<SharedState>,
    Path(id): Path<u32>,
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

    let text = guard.shards[id as usize].text.clone(); // O(1) ref-count bump
    match axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Body::from(text))
    {
        Ok(response) => response,
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to build response",
        )
            .into_response(),
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

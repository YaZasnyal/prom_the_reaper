use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use axum::Router;
use axum::http::{StatusCode, header};
use axum::routing::get;
use axum_test::TestServer;
use flate2::read::GzDecoder;

use crate::hasher::assign_shard;
use crate::parser::parse_families;
use crate::server::router;
use crate::state::{ShardedState, SharedState, SourceStatus, build_shards, empty_state};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const NUM_SHARDS: u32 = 4;

const SAMPLE_METRICS: &str = r#"# HELP go_goroutines Number of goroutines.
# TYPE go_goroutines gauge
go_goroutines 42
# HELP http_requests_total Total HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="GET",code="200"} 1000
http_requests_total{method="POST",code="200"} 500
# HELP request_duration_seconds Request duration histogram.
# TYPE request_duration_seconds histogram
request_duration_seconds_bucket{le="0.1"} 800
request_duration_seconds_bucket{le="0.5"} 950
request_duration_seconds_bucket{le="+Inf"} 1000
request_duration_seconds_sum 123.4
request_duration_seconds_count 1000
# HELP memory_bytes Current memory usage.
# TYPE memory_bytes gauge
memory_bytes 1048576
# HELP cpu_seconds_total Total CPU seconds.
# TYPE cpu_seconds_total counter
cpu_seconds_total{cpu="0"} 100.5
cpu_seconds_total{cpu="1"} 98.3
"#;

/// Builds a SharedState pre-populated with parsed metrics.
fn populated_state(metrics: &str, num_shards: u32) -> SharedState {
    let families = parse_families(metrics);
    let shards = build_shards(families, num_shards);
    let state = Arc::new(ShardedState {
        shards,
        last_scrape: Instant::now(),
        source_status: vec![SourceStatus {
            url: "http://mock-upstream/metrics".to_string(),
            success: true,
            duration: Duration::from_millis(42),
            metric_families: 5,
        }],
    });
    Arc::new(ArcSwap::new(state))
}

fn empty_shared_state() -> SharedState {
    Arc::new(ArcSwap::new(empty_state()))
}

fn test_server(state: SharedState, num_shards: u32) -> TestServer {
    let app = router(state, num_shards);
    TestServer::new(app).expect("failed to create test server")
}

// ---------------------------------------------------------------------------
// /health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_returns_503_before_first_scrape() {
    let server = test_server(empty_shared_state(), NUM_SHARDS);
    let resp = server.get("/health").await;
    resp.assert_status(StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn health_returns_200_after_scrape() {
    let server = test_server(populated_state(SAMPLE_METRICS, NUM_SHARDS), NUM_SHARDS);
    let resp = server.get("/health").await;
    resp.assert_status_ok();
}

// ---------------------------------------------------------------------------
// /metrics/shard/{id}
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shard_returns_503_before_first_scrape() {
    let server = test_server(empty_shared_state(), NUM_SHARDS);
    let resp = server.get("/metrics/shard/0").await;
    resp.assert_status(StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn shard_out_of_range_returns_404() {
    let server = test_server(populated_state(SAMPLE_METRICS, NUM_SHARDS), NUM_SHARDS);
    let resp = server.get("/metrics/shard/99").await;
    resp.assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn shard_returns_plain_text_by_default() {
    let server = test_server(populated_state(SAMPLE_METRICS, NUM_SHARDS), NUM_SHARDS);
    let resp = server.get("/metrics/shard/0").await;
    resp.assert_status_ok();
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("text/plain"),
        "expected text/plain content-type, got: {ct}"
    );
    // No content-encoding header for plain response
    assert!(
        resp.headers().get(header::CONTENT_ENCODING).is_none(),
        "expected no content-encoding for plain response"
    );
}

#[tokio::test]
async fn shard_returns_gzip_when_requested() {
    let server = test_server(populated_state(SAMPLE_METRICS, NUM_SHARDS), NUM_SHARDS);
    let resp = server
        .get("/metrics/shard/0")
        .add_header(header::ACCEPT_ENCODING, "gzip")
        .await;
    resp.assert_status_ok();

    let ce = resp
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(ce, "gzip", "expected gzip content-encoding");

    // Decompress and verify it's valid prometheus text
    let compressed = resp.as_bytes().to_vec();
    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut decompressed = String::new();
    decoder
        .read_to_string(&mut decompressed)
        .expect("failed to decompress gzip response");
    assert!(
        decompressed.contains("# TYPE") || decompressed.is_empty(),
        "decompressed content should be valid prometheus text"
    );
}

#[tokio::test]
async fn gzip_and_plain_shard_content_match() {
    let state = populated_state(SAMPLE_METRICS, NUM_SHARDS);
    let server = test_server(state, NUM_SHARDS);

    for shard_id in 0..NUM_SHARDS {
        let path = format!("/metrics/shard/{shard_id}");

        let plain_resp = server.get(&path).await;
        plain_resp.assert_status_ok();
        let plain_text = plain_resp.text();

        let gzip_resp = server
            .get(&path)
            .add_header(header::ACCEPT_ENCODING, "gzip")
            .await;
        gzip_resp.assert_status_ok();

        let compressed = gzip_resp.as_bytes().to_vec();
        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        decoder
            .read_to_string(&mut decompressed)
            .expect("failed to decompress");

        assert_eq!(
            plain_text, decompressed,
            "shard {shard_id}: plain and gzip-decompressed content differ"
        );
    }
}

#[tokio::test]
async fn all_metrics_present_across_shards() {
    let state = populated_state(SAMPLE_METRICS, NUM_SHARDS);
    let server = test_server(state, NUM_SHARDS);

    let mut combined = String::new();
    for shard_id in 0..NUM_SHARDS {
        let resp = server.get(&format!("/metrics/shard/{shard_id}")).await;
        resp.assert_status_ok();
        combined.push_str(&resp.text());
    }

    // Every metric family from the source should appear somewhere across all shards
    assert!(combined.contains("go_goroutines"), "missing go_goroutines");
    assert!(
        combined.contains("http_requests_total"),
        "missing http_requests_total"
    );
    assert!(
        combined.contains("request_duration_seconds"),
        "missing request_duration_seconds"
    );
    assert!(combined.contains("memory_bytes"), "missing memory_bytes");
    assert!(
        combined.contains("cpu_seconds_total"),
        "missing cpu_seconds_total"
    );
}

#[tokio::test]
async fn each_metric_family_in_exactly_one_shard() {
    let state = populated_state(SAMPLE_METRICS, NUM_SHARDS);
    let server = test_server(state, NUM_SHARDS);

    let metric_names = [
        "go_goroutines",
        "http_requests_total",
        "request_duration_seconds",
        "memory_bytes",
        "cpu_seconds_total",
    ];

    // Collect shard texts
    let mut shard_texts: Vec<String> = Vec::new();
    for shard_id in 0..NUM_SHARDS {
        let resp = server.get(&format!("/metrics/shard/{shard_id}")).await;
        shard_texts.push(resp.text());
    }

    for name in metric_names {
        let count = shard_texts
            .iter()
            .filter(|text| text.contains(name))
            .count();
        assert_eq!(
            count, 1,
            "metric family '{name}' should appear in exactly 1 shard, found in {count}"
        );
    }
}

#[tokio::test]
async fn shard_assignment_is_deterministic() {
    // Build state twice from the same input and verify identical shard assignment
    let s1 = populated_state(SAMPLE_METRICS, NUM_SHARDS);
    let s2 = populated_state(SAMPLE_METRICS, NUM_SHARDS);
    let server1 = test_server(s1, NUM_SHARDS);
    let server2 = test_server(s2, NUM_SHARDS);

    for shard_id in 0..NUM_SHARDS {
        let path = format!("/metrics/shard/{shard_id}");
        let text1 = server1.get(&path).await.text();
        let text2 = server2.get(&path).await.text();
        assert_eq!(
            text1, text2,
            "shard {shard_id} content differs between two identical builds"
        );
    }
}

// ---------------------------------------------------------------------------
// /status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_returns_503_before_first_scrape() {
    let server = test_server(empty_shared_state(), NUM_SHARDS);
    let resp = server.get("/status").await;
    resp.assert_status(StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn status_returns_valid_json() {
    let server = test_server(populated_state(SAMPLE_METRICS, NUM_SHARDS), NUM_SHARDS);
    let resp = server.get("/status").await;
    resp.assert_status_ok();

    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("application/json"),
        "expected JSON content-type"
    );

    let body: serde_json::Value = serde_json::from_str(&resp.text()).expect("invalid JSON");
    assert_eq!(body["num_shards"], NUM_SHARDS);
    assert!(
        body["last_scrape_ago_secs"].is_number(),
        "last_scrape_ago_secs should be a number"
    );
    assert!(
        body["shard_sizes_bytes"].is_array(),
        "shard_sizes_bytes should be an array"
    );
    assert_eq!(
        body["shard_sizes_bytes"].as_array().unwrap().len(),
        NUM_SHARDS as usize
    );
    assert!(body["sources"].is_array(), "sources should be an array");
    assert!(body["sources"][0]["success"].as_bool().unwrap_or(false));
}

// ---------------------------------------------------------------------------
// Mock upstream + full scrape integration
// ---------------------------------------------------------------------------

/// Spins up a real HTTP server serving static Prometheus metrics,
/// runs one scrape cycle against it, and verifies the resulting shards.
#[tokio::test]
async fn full_scrape_cycle_with_mock_upstream() {
    use crate::config::{AppConfig, SourceConfig};
    use crate::scraper::run_scrape_loop;
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    // Bind to a random port for the mock upstream
    let upstream_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind upstream listener");
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let upstream_url = format!("http://{}/metrics", upstream_addr);

    // Start mock upstream server
    let mock_app = Router::new().route("/metrics", get(|| async { SAMPLE_METRICS }));
    tokio::spawn(async move {
        axum::serve(upstream_listener, mock_app)
            .await
            .expect("mock upstream failed");
    });

    // Give the mock server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = Arc::new(AppConfig {
        listen: "127.0.0.1:0".to_string(),
        num_shards: NUM_SHARDS,
        scrape_interval_secs: 1,
        sources: vec![SourceConfig {
            url: upstream_url,
            timeout_secs: 5,
            headers: HashMap::new(),
        }],
    });

    let shared_state = empty_shared_state();

    // Spawn the scraper and wait for it to complete one cycle
    tokio::spawn(run_scrape_loop(config, shared_state.clone()));

    // Wait for the first scrape to complete (up to 3 seconds)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for first scrape");
        }
        let guard = shared_state.load();
        if !guard.shards.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Verify shards via HTTP
    let server = test_server(shared_state, NUM_SHARDS);

    // Health should now be OK
    server.get("/health").await.assert_status_ok();

    // All metrics should be present across shards
    let mut combined = String::new();
    for shard_id in 0..NUM_SHARDS {
        let resp = server.get(&format!("/metrics/shard/{shard_id}")).await;
        resp.assert_status_ok();
        combined.push_str(&resp.text());
    }
    assert!(combined.contains("go_goroutines"));
    assert!(combined.contains("http_requests_total"));
    assert!(combined.contains("request_duration_seconds_bucket"));
    assert!(combined.contains("cpu_seconds_total"));

    // Status should report success
    let status: serde_json::Value =
        serde_json::from_str(&server.get("/status").await.text()).unwrap();
    assert!(status["sources"][0]["success"].as_bool().unwrap_or(false));
}

// ---------------------------------------------------------------------------
// Consistent hashing — minimal movement on shard count change
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consistent_hashing_minimal_movement() {
    let families = parse_families(SAMPLE_METRICS);
    let names: Vec<String> = families.iter().map(|f| f.name.clone()).collect();

    let old_shards = NUM_SHARDS;
    let new_shards = NUM_SHARDS + 1;

    let moved = names
        .iter()
        .filter(|name| assign_shard(name, old_shards) != assign_shard(name, new_shards))
        .count();

    let ratio = moved as f64 / names.len() as f64;
    // Jump consistent hash guarantees ~1/new_shards movement (here ~20%)
    // We allow up to 35% as tolerance
    assert!(
        ratio < 0.35,
        "too many metrics moved between shards: {moved}/{} ({:.0}%)",
        names.len(),
        ratio * 100.0
    );
}

// ---------------------------------------------------------------------------
// Shard count edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_shard_contains_all_metrics() {
    let state = populated_state(SAMPLE_METRICS, 1);
    let server = test_server(state, 1);

    let resp = server.get("/metrics/shard/0").await;
    resp.assert_status_ok();
    let text = resp.text();

    assert!(text.contains("go_goroutines"));
    assert!(text.contains("http_requests_total"));
    assert!(text.contains("memory_bytes"));
}

#[tokio::test]
async fn single_shard_shard1_returns_404() {
    let state = populated_state(SAMPLE_METRICS, 1);
    let server = test_server(state, 1);
    server
        .get("/metrics/shard/1")
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

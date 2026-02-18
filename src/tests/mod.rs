use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use axum::Router;
use axum::http::{StatusCode, header};
use axum::routing::get;
use axum_test::TestServer;
use flate2::read::GzDecoder;

use crate::parser::{extract_sorted_label_key, parse_families};
use crate::server::router;
use crate::state::{ShardedState, SharedState, SourceStatus, build_shards, empty_state};

use crate::hasher::assign_shard;

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
    assert_eq!(ce, "gzip", "expected Content-Encoding: gzip");

    let mut decompressed = String::new();
    GzDecoder::new(resp.as_bytes().as_ref())
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

        let is_gzip = gzip_resp
            .headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("gzip"));

        let gzip_text = if is_gzip {
            let mut decompressed = String::new();
            GzDecoder::new(gzip_resp.as_bytes().as_ref())
                .read_to_string(&mut decompressed)
                .expect("failed to decompress");
            decompressed
        } else {
            gzip_resp.text()
        };

        assert_eq!(
            plain_text, gzip_text,
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
async fn shard_assignment_is_deterministic() {
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
// Per-series sharding with high-cardinality families
// ---------------------------------------------------------------------------

/// Verifies that series from the same metric family can be distributed
/// across multiple shards (high-cardinality case), and that each shard
/// that receives any series also gets the HELP/TYPE header.
#[tokio::test]
async fn high_cardinality_series_spread_across_shards() {
    // Build a metric with enough series that they spread across 4 shards.
    let mut input = String::from(
        "# HELP rpc_duration_seconds RPC call duration.\n\
         # TYPE rpc_duration_seconds histogram\n",
    );
    for i in 0..40 {
        let labels = format!(r#"rpc_duration_seconds_bucket{{shard="{i}",le="0.1"}} {i}"#);
        input.push_str(&labels);
        input.push('\n');
        let labels = format!(r#"rpc_duration_seconds_sum{{shard="{i}"}} {i}.5"#);
        input.push_str(&labels);
        input.push('\n');
        let labels = format!(r#"rpc_duration_seconds_count{{shard="{i}"}} {i}"#);
        input.push_str(&labels);
        input.push('\n');
    }

    let state = populated_state(&input, NUM_SHARDS);
    let server = test_server(state, NUM_SHARDS);

    let mut shards_with_family = 0u32;
    let mut total_series = 0usize;

    for shard_id in 0..NUM_SHARDS {
        let text = server
            .get(&format!("/metrics/shard/{shard_id}"))
            .await
            .text();
        if text.contains("rpc_duration_seconds") {
            shards_with_family += 1;
            // Every shard that has series must also have the HELP and TYPE header.
            assert!(
                text.contains("# HELP rpc_duration_seconds"),
                "shard {shard_id} has series but missing HELP header"
            );
            assert!(
                text.contains("# TYPE rpc_duration_seconds histogram"),
                "shard {shard_id} has series but missing TYPE header"
            );
            // Count _count lines as a proxy for number of series in this shard.
            total_series += text.lines().filter(|l| l.contains("_count{")).count();
        }
    }

    // With 40 series and 4 shards we expect distribution across multiple shards.
    assert!(
        shards_with_family > 1,
        "expected series spread across multiple shards, all ended up in {shards_with_family}"
    );
    // All 40 _count series must be accounted for across all shards.
    assert_eq!(
        total_series, 40,
        "expected 40 _count series total across all shards"
    );
}

/// Verifies that no series are lost or duplicated when sharding high-cardinality data.
#[tokio::test]
async fn no_series_lost_or_duplicated_across_shards() {
    let mut input = String::new();
    let n = 100u32;
    for i in 0..n {
        input.push_str(&format!("some_counter{{id=\"{i}\"}} {i}\n"));
    }

    let state = populated_state(&input, NUM_SHARDS);
    let server = test_server(state, NUM_SHARDS);

    let mut seen_ids = std::collections::HashSet::new();
    for shard_id in 0..NUM_SHARDS {
        let text = server
            .get(&format!("/metrics/shard/{shard_id}"))
            .await
            .text();
        for line in text.lines() {
            if line.starts_with("some_counter{") {
                // Extract id value
                if let Some(start) = line.find("id=\"") {
                    let rest = &line[start + 4..];
                    if let Some(end) = rest.find('"') {
                        let id: u32 = rest[..end].parse().expect("id should be a number");
                        assert!(
                            seen_ids.insert(id),
                            "series id={id} appeared in more than one shard"
                        );
                    }
                }
            }
        }
    }
    assert_eq!(
        seen_ids.len(),
        n as usize,
        "expected {n} unique series, got {}",
        seen_ids.len()
    );
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
    assert!(body["last_scrape_ago_secs"].is_number());
    let shards = body["shards"].as_array().unwrap();
    assert_eq!(shards.len(), NUM_SHARDS as usize);
    // Each shard entry must have size_bytes, families and series fields.
    for shard in shards {
        assert!(shard["size_bytes"].is_number(), "missing size_bytes");
        assert!(shard["families"].is_number(), "missing families");
        assert!(shard["series"].is_number(), "missing series");
    }
    assert!(body["sources"].is_array());
    assert!(body["sources"][0]["success"].as_bool().unwrap_or(false));
}

// ---------------------------------------------------------------------------
// Mock upstream + full scrape integration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_scrape_cycle_with_mock_upstream() {
    use crate::config::{AppConfig, SourceConfig};
    use crate::scraper::run_scrape_loop;
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    let upstream_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind upstream listener");
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let upstream_url = format!("http://{}/metrics", upstream_addr);

    let mock_app = Router::new().route("/metrics", get(|| async { SAMPLE_METRICS }));
    tokio::spawn(async move {
        axum::serve(upstream_listener, mock_app)
            .await
            .expect("mock upstream failed");
    });

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
    tokio::spawn(run_scrape_loop(config, shared_state.clone()));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for first scrape");
        }
        if !shared_state.load().shards.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let server = test_server(shared_state, NUM_SHARDS);
    server.get("/health").await.assert_status_ok();

    let mut combined = String::new();
    for shard_id in 0..NUM_SHARDS {
        combined.push_str(
            &server
                .get(&format!("/metrics/shard/{shard_id}"))
                .await
                .text(),
        );
    }
    assert!(combined.contains("go_goroutines"));
    assert!(combined.contains("http_requests_total"));
    assert!(combined.contains("request_duration_seconds_bucket"));
    assert!(combined.contains("cpu_seconds_total"));

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

    // Collect all (name, label_key) hash keys — same as what build_shards uses.
    let keys: Vec<String> = families
        .iter()
        .flat_map(|f| {
            f.samples
                .iter()
                .map(|s| format!("{}\x00{}", f.name, extract_sorted_label_key(&s.raw_line)))
        })
        .collect();

    let old_shards = NUM_SHARDS;
    let new_shards = NUM_SHARDS + 1;

    let moved = keys
        .iter()
        .filter(|k| assign_shard(k, old_shards) != assign_shard(k, new_shards))
        .count();

    let ratio = moved as f64 / keys.len() as f64;
    assert!(
        ratio < 0.35,
        "too many series moved between shards: {moved}/{} ({:.0}%)",
        keys.len(),
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
    let text = server.get("/metrics/shard/0").await.text();
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

# prom_the_reaper

A Prometheus metrics sharding proxy. Scrapes high-cardinality metric sources and
distributes individual time series across N shards, so each Prometheus instance
only scrapes a fraction of the total.

**Problem:** Ceph (and similar exporters) can emit 600k+ metrics from a single endpoint.
A single Prometheus can't handle this load efficiently.

**Solution:** Run prom_the_reaper in front of the exporter. It fetches the metrics,
splits them via consistent hashing, and exposes smaller shards at `/metrics/shard/0..N`.
Point a separate Prometheus at each shard.

## Features

- **Per-series sharding** — each time series is hashed independently by
  `metric_name + sorted labels`, so high-cardinality families spread evenly across shards
- **Consistent hashing** (xxh3 + jump hash) — when you change the shard count, only
  ~1/N of series move; the rest stay on the same shard
- **Multiple sources** — scrape several upstream exporters in parallel; all metrics are
  merged and sharded together
- **Zero-allocation serving** — shard responses are pre-built in the background and
  served via atomic pointer swap (ArcSwap); no locks on the hot path
- **Gzip** — all endpoints support `Accept-Encoding: gzip` via middleware
- **Self-monitoring** — `GET /metrics` exposes proxy health in Prometheus format
- **Stale data on failure** — if all upstreams are unavailable, the last successful
  scrape is served rather than an empty response

## Quick start

### Build

```bash
cargo build --release
```

### Configure

Generate a sample config and edit it:

```bash
./target/release/prom_the_reaper generate-config > config.toml
```

```toml
listen = "0.0.0.0:9090"
num_shards = 4
scrape_interval_secs = 30

[[sources]]
url = "http://ceph-exporter:9283/metrics"
timeout_secs = 25

# Add more sources as needed:
# [[sources]]
# url = "http://node-exporter:9100/metrics"
# timeout_secs = 10
# headers = { "Authorization" = "Bearer token" }
```

### Run

```bash
./target/release/prom_the_reaper config.toml
```

```
RUST_LOG=debug ./target/release/prom_the_reaper config.toml   # verbose
```

## HTTP API

| Endpoint | Description |
|----------|-------------|
| `GET /metrics/shard/{id}` | Prometheus exposition text for shard `id` (0-indexed). |
| `GET /metrics` | Proxy's own health metrics in Prometheus exposition format. |
| `GET /health` | `200 OK` once the first scrape completes, `503` before that. |
| `GET /status` | JSON diagnostics: last scrape time, per-source status, per-shard stats. |

All endpoints support `Accept-Encoding: gzip`. Returns `503` before the first successful
scrape cycle completes.

### /status response

```json
{
  "num_shards": 4,
  "last_scrape_ago_secs": 8.1,
  "sources": [
    {"url": "http://...", "success": true, "duration_ms": 342, "metric_families": 1500}
  ],
  "shards": [
    {"id": 0, "size_bytes": 145000, "families": 380, "series": 12400},
    {"id": 1, "size_bytes": 148000, "families": 375, "series": 12600},
    ...
  ]
}
```

### /metrics (self-monitoring)

```
prom_reaper_last_scrape_age_seconds 8.1
prom_reaper_shard_series{shard="0"} 12400
prom_reaper_shard_families{shard="0"} 380
prom_reaper_shard_size_bytes{shard="0"} 145000
prom_reaper_source_up{url="http://..."} 1
prom_reaper_source_scrape_duration_seconds{url="http://..."} 0.342
prom_reaper_num_shards 4
```

Add it as a regular scrape target to alert on scrape failures or shard imbalance.

## Prometheus configuration

Create one scrape job per shard, ideally sending each to a separate Prometheus instance:

```yaml
scrape_configs:
  - job_name: ceph_shard_0
    static_configs:
      - targets: ['prom-reaper:9090']
    metrics_path: /metrics/shard/0

  - job_name: ceph_shard_1
    static_configs:
      - targets: ['prom-reaper:9090']
    metrics_path: /metrics/shard/1

  # ... repeat for each shard

  - job_name: prom_reaper
    static_configs:
      - targets: ['prom-reaper:9090']
    metrics_path: /metrics
```

## Changing the shard count

prom_the_reaper uses jump consistent hash, so increasing `num_shards` from N to N+1
moves only ~1/(N+1) of series to a different shard. All other series stay put,
preserving metric continuity in Prometheus.

After changing `num_shards`, update your Prometheus scrape configs accordingly.

## Local testing

A mock exporter is included for local development:

```bash
# Terminal 1 — start mock upstream (10 metric families × 10 series each)
python3 contrib/mock_exporter.py

# Terminal 2 — run the proxy
./target/release/prom_the_reaper generate-config \
  | sed 's|http://ceph-exporter:9283/metrics|http://127.0.0.1:9100/metrics|' \
  | sed 's/num_shards = 4/num_shards = 2/' \
  > /tmp/test.toml
./target/release/prom_the_reaper /tmp/test.toml

# Explore
curl http://127.0.0.1:9090/metrics/shard/0
curl http://127.0.0.1:9090/metrics/shard/1
curl http://127.0.0.1:9090/metrics
curl http://127.0.0.1:9090/status | python3 -m json.tool
curl -H 'Accept-Encoding: gzip' http://127.0.0.1:9090/metrics/shard/0 | gunzip | head
```

## Running tests

```bash
cargo test
```

29 tests: unit tests for parser and hasher, integration tests covering HTTP endpoints,
gzip, high-cardinality sharding, no lost/duplicated series, and a full end-to-end scrape
cycle against a real mock upstream.

# prom_the_reaper — developer notes

## What this is

Prometheus metrics sharding proxy. Scrapes large metric sources (e.g. Ceph with 600k metrics),
splits individual time series via consistent hashing, and serves each shard on
`/metrics/shard/{id}`. Each Prometheus instance scrapes only its shard.

## Quick start

```bash
cargo build --release
cargo run -- generate-config > config.toml
# Edit config.toml: set URL, num_shards, scrape_interval_secs
./target/release/prom_the_reaper config.toml
```

`RUST_LOG=debug` for verbose output. Default log level is `info`.

## Git workflow

Always create a new branch for every change. Changes are merged via MR (merge request).

```bash
git checkout -b <type>/<short-description>
# make changes
git commit
# push and open MR
```

Never commit directly to `main`.

## Tests

```bash
cargo test                  # all unit + integration tests (29 total)
cargo test parser           # only parser unit tests
cargo test integration      # only integration tests (no such tag yet, use module path)
```

## Module map

| File | Responsibility |
|------|---------------|
| `config.rs` | TOML deserialization, startup validation |
| `hasher.rs` | xxh3_64 + jump consistent hash → shard id |
| `parser.rs` | Prometheus text exposition → `Vec<ParsedFamily>` |
| `state.rs` | `build_shards()`, `ShardedState`, `SharedState` type alias |
| `scraper.rs` | Background tokio interval: fetch all sources in parallel via `JoinSet`, parse, build shards, ArcSwap |
| `server.rs` | Axum router: `/metrics/shard/{id}`, `/health`, `/status` |
| `main.rs` | Entry point + clap CLI (`run` / `generate-config`) |
| `src/tests/mod.rs` | Integration tests (axum-test + mock upstream) |

## Data flow

```
tokio::interval tick
  └─ JoinSet: reqwest GET each source (parallel)
       └─ parse_families(body) → Vec<ParsedFamily>
            └─ build_shards(families, num_shards)
                 for each sample:
                   key = "metric_name\x00sorted_label_pairs"
                   shard_id = jump_hash(xxh3(key), num_shards)
                   if first time family in this shard: write HELP + TYPE
                   write sample line
                 gzip compress each shard text
            └─ ArcSwap::store(Arc::new(new_state))

GET /metrics/shard/{id}
  └─ ArcSwap::load()        ← atomic, near-zero cost
       └─ check Accept-Encoding: gzip
            └─ return pre-rendered ShardData.text or ShardData.gzip
```

## Sharding key

Hash key is `"{metric_name}\x00{sorted_label_pairs}"`.

- Labels are sorted lexicographically before hashing so `{b="2",a="1"}` and `{a="1",b="2"}` map to the same shard.
- The `\x00` separator prevents collisions between the name and the labels.
- Each series is independently distributed — high-cardinality families spread across shards.
- HELP/TYPE headers are written into a shard **once**, on the first series of that family.

## Consistent hashing

`assign_shard(key, n) = jump_consistent_hash(xxh3_64(key.as_bytes()), n)`

Jump consistent hash (Lamping & Veach, 2014): O(ln n) time, O(1) space, minimal key movement
when n changes. When going from N to N+1 shards, ~1/(N+1) of series move — everything else stays.

## Error handling policy

| Situation | Behaviour |
|-----------|-----------|
| One source fails | Log `warn`, mark as failed in `/status`, continue with other sources |
| All sources fail | Log `error`, keep serving **stale data** — never swap to empty state |
| No scrape yet | `/metrics/shard/{id}` and `/status` return 503 |
| Shard id out of range | 404 |
| Malformed metric line | Skip with `warn`, continue parsing |

## Pre-rendering rationale

Scrapes are infrequent (every 30 s). Prometheus scrapes each shard every 15 s × N instances.
Building `ShardData { text: String, gzip: Bytes }` once per scrape amortises parse + compression
cost. HTTP handlers do: one ArcSwap load + one `Bytes::clone` (reference count bump).

## Adding a new source type

Only `SourceConfig` in `config.rs` needs changing — add fields, update the reqwest call in
`scraper.rs::scrape_all`. Everything downstream (parser, hasher, state) is source-agnostic.

## Prometheus scrape_config example

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
```

One job per shard, each pointing to a different Prometheus instance or remote_write target.

## Local dev with mock exporter

```bash
python3 contrib/mock_exporter.py          # serves 10 families × 10 series on :9100
# in another terminal:
cargo run -- generate-config > /tmp/test.toml
# edit /tmp/test.toml: url = "http://127.0.0.1:9100/metrics", num_shards = 2
cargo run -- /tmp/test.toml
curl http://127.0.0.1:9090/metrics/shard/0
curl http://127.0.0.1:9090/status | python3 -m json.tool
```

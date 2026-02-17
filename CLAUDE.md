# prom_the_reaper

Prometheus metrics sharding proxy. Scrapes large metric sources, splits them into N shards via consistent hashing, and serves each shard on `/metrics/shard/{id}`.

## Build & Run

```bash
cargo build --release
./target/release/prom_the_reaper config.toml
```

Set `RUST_LOG=debug` for verbose logging.

## Test

```bash
cargo test
```

## Architecture

- `config.rs` — TOML config loading and validation
- `hasher.rs` — xxh3 + jump consistent hash for shard assignment
- `parser.rs` — Prometheus exposition format parser, groups lines into metric families
- `state.rs` — Shared state types, shard building with pre-rendered text + gzip
- `scraper.rs` — Background tokio interval task: scrape → parse → shard → ArcSwap
- `server.rs` — Axum HTTP handlers with gzip content negotiation
- `main.rs` — Entry point, wires everything together

## Key design decisions

- Hash by metric **name** (not labels) so all time series of the same metric stay in one shard
- Pre-render and pre-compress shard responses; HTTP handlers do zero allocation
- ArcSwap for lock-free atomic state replacement
- Stale data served if all sources fail (never empty response)

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;

use crate::hasher::assign_shard;
use crate::parser::MetricFamily;

pub type SharedState = Arc<ArcSwap<ShardedState>>;

pub struct ShardedState {
    pub shards: Vec<ShardData>,
    pub last_scrape: Instant,
    pub source_status: Vec<SourceStatus>,
}

pub struct ShardData {
    pub text: String,
    pub gzip: Bytes,
}

pub struct SourceStatus {
    pub url: String,
    pub success: bool,
    pub duration: Duration,
    pub metric_families: usize,
}

/// Builds pre-rendered shards from parsed metric families.
pub fn build_shards(families: Vec<MetricFamily>, num_shards: u32) -> Vec<ShardData> {
    let mut shard_texts: Vec<String> = (0..num_shards).map(|_| String::new()).collect();

    for family in &families {
        let shard_id = assign_shard(&family.name, num_shards) as usize;
        shard_texts[shard_id].push_str(&family.raw_text);
    }

    shard_texts
        .into_iter()
        .map(|text| {
            let gzip = gzip_compress(text.as_bytes());
            ShardData {
                text,
                gzip: Bytes::from(gzip),
            }
        })
        .collect()
}

fn gzip_compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(data).expect("gzip write failed");
    encoder.finish().expect("gzip finish failed")
}

pub fn empty_state() -> Arc<ShardedState> {
    Arc::new(ShardedState {
        shards: Vec::new(),
        last_scrape: Instant::now(),
        source_status: Vec::new(),
    })
}

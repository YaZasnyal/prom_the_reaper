use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use crate::hasher::assign_shard;
use crate::parser::ParsedFamily;

pub type SharedState = Arc<ArcSwap<ShardedState>>;

pub struct ShardedState {
    pub shards: Vec<ShardData>,
    pub last_scrape: Instant,
    pub source_status: Vec<SourceStatus>,
}

pub struct ShardData {
    pub text: String,
    /// Number of unique metric families in this shard.
    pub families_count: usize,
    /// Number of individual time series (samples) in this shard.
    pub series_count: usize,
}

pub struct SourceStatus {
    pub url: String,
    pub success: bool,
    pub duration: Duration,
    pub metric_families: usize,
}

/// Builds pre-rendered shards from parsed metric families.
///
/// Each sample is hashed by `metric_name + sorted_labels` for consistent
/// per-series distribution. HELP and TYPE headers are emitted into a shard
/// the first time any series of that family appears there.
pub fn build_shards(families: Vec<ParsedFamily>, num_shards: u32) -> Vec<ShardData> {
    let mut shard_texts: Vec<String> = (0..num_shards).map(|_| String::new()).collect();
    let mut shard_series: Vec<usize> = vec![0; num_shards as usize];
    // Tracks which (shard_idx, family_name) pairs have had their header written.
    let mut headers_written: HashSet<(usize, String)> = HashSet::new();

    for family in &families {
        for sample in &family.samples {
            let hash_key = format!("{}\x00{}", family.name, sample.label_key);
            let shard_id = assign_shard(&hash_key, num_shards) as usize;

            // Emit HELP/TYPE the first time this family appears in this shard.
            let header_key = (shard_id, family.name.clone());
            if !headers_written.contains(&header_key) {
                if let Some(help) = &family.help_line {
                    shard_texts[shard_id].push_str(help);
                }
                if let Some(type_line) = &family.type_line {
                    shard_texts[shard_id].push_str(type_line);
                }
                headers_written.insert(header_key);
            }

            shard_texts[shard_id].push_str(&sample.raw_line);
            shard_series[shard_id] += 1;
        }
    }

    shard_texts
        .into_iter()
        .enumerate()
        .map(|(i, text)| {
            let families_count = headers_written
                .iter()
                .filter(|(shard_id, _)| *shard_id == i)
                .count();
            ShardData {
                text,
                families_count,
                series_count: shard_series[i],
            }
        })
        .collect()
}

pub fn empty_state() -> Arc<ShardedState> {
    Arc::new(ShardedState {
        shards: Vec::new(),
        last_scrape: Instant::now(),
        source_status: Vec::new(),
    })
}

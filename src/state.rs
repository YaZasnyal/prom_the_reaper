use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;

use crate::hasher::assign_shard_from_parts;
use crate::parser::{ParsedFamily, extract_metric_name, extract_sorted_label_key};

pub type SharedState = Arc<ArcSwap<ShardedState>>;

pub struct ShardedState {
    pub shards: Vec<ShardData>,
    pub last_scrape: Instant,
    pub source_status: Vec<SourceStatus>,
}

pub struct ShardData {
    pub text: Bytes,
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
    // Uses &str borrowing from `families` to avoid cloning family names.
    let mut headers_written: HashSet<(usize, &str)> = HashSet::new();

    for family in &families {
        for sample in &family.samples {
            // Compute hash key inline from raw_line to avoid storing label_key in Sample.
            let sample_name = extract_metric_name(&sample.raw_line);
            let label_key = extract_sorted_label_key(&sample.raw_line);
            // Build hash key without a heap allocation: hash name + NUL + labels directly.
            let shard_id = assign_shard_from_parts(sample_name, &label_key, num_shards) as usize;

            // Emit HELP/TYPE the first time this family appears in this shard.
            if !headers_written.contains(&(shard_id, family.name.as_str())) {
                if let Some(help) = &family.help_line {
                    shard_texts[shard_id].push_str(help);
                }
                if let Some(type_line) = &family.type_line {
                    shard_texts[shard_id].push_str(type_line);
                }
                headers_written.insert((shard_id, family.name.as_str()));
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
                text: Bytes::from(text),
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

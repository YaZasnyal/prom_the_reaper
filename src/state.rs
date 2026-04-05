use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;

use crate::hasher::assign_shard_from_parts;
use crate::parser::{ParsedFamily, extract_metric_name};

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
    // Estimate total text size for pre-allocation.
    let total_size: usize = families
        .iter()
        .flat_map(|f| {
            let header_size = f.help_line.as_ref().map_or(0, |h| h.len())
                + f.type_line.as_ref().map_or(0, |t| t.len());
            // Worst case: every sample triggers a header copy in its shard.
            f.samples
                .iter()
                .map(move |s| s.raw_line.len() + header_size)
        })
        .sum();
    let per_shard = total_size / num_shards as usize + 1;

    let mut shard_texts: Vec<String> = (0..num_shards)
        .map(|_| String::with_capacity(per_shard))
        .collect();
    let mut shard_families: Vec<usize> = vec![0; num_shards as usize];
    let mut shard_series: Vec<usize> = vec![0; num_shards as usize];
    // Tracks which (shard_idx, family_name) pairs have had their header written.
    // Uses &str borrowing from `families` to avoid cloning family names.
    let mut headers_written: HashSet<(usize, &str)> = HashSet::new();

    for family in &families {
        for sample in &family.samples {
            let sample_name = extract_metric_name(&sample.raw_line);
            // Use pre-computed label_key from Sample instead of re-parsing.
            let shard_id =
                assign_shard_from_parts(sample_name, &sample.label_key, num_shards) as usize;

            // Emit HELP/TYPE the first time this family appears in this shard.
            // insert() returns true only when the key is newly added, so this
            // naturally guards against writing duplicate headers.
            if headers_written.insert((shard_id, family.name.as_str())) {
                if let Some(help) = &family.help_line {
                    shard_texts[shard_id].push_str(help);
                }
                if let Some(type_line) = &family.type_line {
                    shard_texts[shard_id].push_str(type_line);
                }
                shard_families[shard_id] += 1;
            }

            shard_texts[shard_id].push_str(&sample.raw_line);
            shard_series[shard_id] += 1;
        }
    }

    shard_texts
        .into_iter()
        .enumerate()
        .map(|(i, text)| ShardData {
            text: Bytes::from(text),
            families_count: shard_families[i],
            series_count: shard_series[i],
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

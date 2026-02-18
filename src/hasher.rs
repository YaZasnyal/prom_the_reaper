use xxhash_rust::xxh3::Xxh3;

/// Assigns a metric series to a shard by hashing `name\x00label_key` without
/// allocating an intermediate String.
pub fn assign_shard_from_parts(name: &str, label_key: &str, num_shards: u32) -> u32 {
    let mut h = Xxh3::new();
    h.update(name.as_bytes());
    h.update(b"\x00");
    h.update(label_key.as_bytes());
    jump_consistent_hash(h.digest(), num_shards)
}

/// Jump consistent hash algorithm (Lamping & Veach, 2014).
/// O(ln(n)) time, O(1) space, near-perfect balance and minimal movement.
fn jump_consistent_hash(mut key: u64, num_buckets: u32) -> u32 {
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < num_buckets as i64 {
        b = j;
        key = key.wrapping_mul(2862933555777941757).wrapping_add(1);
        j = ((b.wrapping_add(1)) as f64 * ((1i64 << 31) as f64) / ((key >> 33) as f64 + 1.0))
            as i64;
    }
    b as u32
}

/// Only compiled in test builds; used by unit tests in this module and integration tests.
#[cfg(test)]
pub(crate) fn assign_shard(metric_name: &str, num_shards: u32) -> u32 {
    use xxhash_rust::xxh3::xxh3_64;
    let hash = xxh3_64(metric_name.as_bytes());
    jump_consistent_hash(hash, num_shards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let shard = assign_shard("ceph_osd_op_latency", 4);
        for _ in 0..100 {
            assert_eq!(assign_shard("ceph_osd_op_latency", 4), shard);
        }
    }

    #[test]
    fn in_range() {
        for shards in 1..=16 {
            for i in 0..1000 {
                let name = format!("metric_{}", i);
                let shard = assign_shard(&name, shards);
                assert!(
                    shard < shards,
                    "shard {} out of range for {}",
                    shard,
                    shards
                );
            }
        }
    }

    #[test]
    fn minimal_movement_on_shard_change() {
        let num_metrics = 10000;
        let old_shards = 4;
        let new_shards = 5;
        let mut moved = 0;
        for i in 0..num_metrics {
            let name = format!("metric_{}", i);
            if assign_shard(&name, old_shards) != assign_shard(&name, new_shards) {
                moved += 1;
            }
        }
        // With jump consistent hash, ~1/new_shards of keys should move (20% for 4->5).
        // Allow some tolerance.
        let move_ratio = moved as f64 / num_metrics as f64;
        assert!(
            move_ratio < 0.30,
            "too many keys moved: {:.1}%",
            move_ratio * 100.0
        );
    }

    #[test]
    fn reasonable_balance() {
        let num_shards = 4;
        let num_metrics = 10000;
        let mut counts = vec![0u32; num_shards as usize];
        for i in 0..num_metrics {
            let name = format!("metric_{}", i);
            counts[assign_shard(&name, num_shards) as usize] += 1;
        }
        let expected = num_metrics as f64 / num_shards as f64;
        for (i, &count) in counts.iter().enumerate() {
            let ratio = count as f64 / expected;
            assert!(
                (0.7..1.3).contains(&ratio),
                "shard {} has {} metrics, expected ~{:.0} (ratio {:.2})",
                i,
                count,
                expected,
                ratio
            );
        }
    }
}

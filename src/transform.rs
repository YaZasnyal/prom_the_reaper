use std::collections::{BTreeMap, HashMap, HashSet};

use crate::parser::{ParsedFamily, extract_sorted_label_key};

/// Injects extra labels into every sample of the given families.
///
/// Labels are appended to any existing label set on each sample line (or
/// inserted as the only labels when the sample has none). Keys are sorted
/// alphabetically for deterministic output. Label values are escaped per the
/// Prometheus text format (`\` → `\\`, `"` → `\"`).
///
/// Because the labels are written into each `Sample::raw_line`, the existing
/// hashing pipeline (`extract_sorted_label_key` → `assign_shard_from_parts`)
/// automatically includes them in the consistent-hash key.
pub fn inject_labels(families: &mut [ParsedFamily], extra: &HashMap<String, String>) {
    if extra.is_empty() {
        return;
    }

    // BTreeMap gives us sorted-by-key iteration for free.
    let sorted: BTreeMap<&str, &str> = extra
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    // Pre-render once: `k1="v1",k2="v2"` (keys already in alphabetical order).
    let extra_str: String = sorted
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
        .collect::<Vec<_>>()
        .join(",");

    for family in families.iter_mut() {
        for sample in family.samples.iter_mut() {
            sample.raw_line = inject_into_line(&sample.raw_line, &extra_str);
            // Invalidate cached label_key since raw_line changed.
            sample.label_key = extract_sorted_label_key(&sample.raw_line);
        }
    }
}

/// Escapes a Prometheus label value: `\` → `\\`, `"` → `\"`.
fn escape_label_value(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Injects a pre-rendered `k="v",...` fragment into a single sample line.
///
/// Handles three cases:
/// - `metric{existing} value` → `metric{existing,extra} value`
/// - `metric{} value`         → `metric{extra} value`
/// - `metric value`           → `metric{extra} value`
///
/// The trailing `\n` is preserved.
fn inject_into_line(line: &str, extra_str: &str) -> String {
    let content = line.strip_suffix('\n').unwrap_or(line);

    if let Some(open) = content.find('{') {
        let close = content.rfind('}').unwrap_or(content.len());
        let existing = &content[open + 1..close];
        let after = &content[close + 1..];

        let labels = if existing.is_empty() {
            extra_str.to_owned()
        } else {
            format!("{},{}", existing, extra_str)
        };
        format!("{}{{{}}}{}\n", &content[..open], labels, after)
    } else {
        // No braces: `metric_name value [timestamp]`
        let space = content.find(' ').unwrap_or(content.len());
        format!(
            "{}{{{}}}{}\n",
            &content[..space],
            extra_str,
            &content[space..]
        )
    }
}

/// Statistics returned by [`merge_families`].
pub struct MergeStats {
    /// Total number of sample lines dropped because their `(family, label_key)` was already seen.
    pub duplicate_count: usize,
    /// Up to three human-readable examples of dropped series (for warn logging).
    pub examples: Vec<String>,
}

/// Merges `Vec<ParsedFamily>` collected from multiple sources into a deduplicated list.
///
/// When the same `(family_name, label_key)` appears more than once the **first** occurrence
/// is kept and all subsequent ones are silently dropped (first-wins).  Families with the
/// same name but distinct label sets are merged into one `ParsedFamily` entry, preserving
/// their HELP/TYPE from the first source that declared them.
pub fn merge_families(families: Vec<ParsedFamily>) -> (Vec<ParsedFamily>, MergeStats) {
    let mut merged: Vec<ParsedFamily> = Vec::new();
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut duplicate_count = 0usize;
    let mut examples: Vec<String> = Vec::new();

    for family in families {
        if let Some(&idx) = name_to_idx.get(&family.name) {
            // Family already present — merge samples, first-wins on label_key collisions.
            // Collect existing keys into an owned set, then partition incoming samples.
            let existing_keys: HashSet<&str> = merged[idx]
                .samples
                .iter()
                .map(|s| s.label_key.as_str())
                .collect();

            // Partition: separate new samples from duplicates without borrowing `merged`.
            let (new_samples, dups): (Vec<_>, Vec<_>) = family
                .samples
                .into_iter()
                .partition(|s| !existing_keys.contains(s.label_key.as_str()));

            for dup in &dups {
                duplicate_count += 1;
                if examples.len() < 3 {
                    let example = if dup.label_key.is_empty() {
                        family.name.clone()
                    } else {
                        format!("{}{{{}}}", family.name, dup.label_key)
                    };
                    examples.push(example);
                }
            }

            merged[idx].samples.extend(new_samples);
        } else {
            let idx = merged.len();
            name_to_idx.insert(family.name.clone(), idx);
            merged.push(family);
        }
    }

    (
        merged,
        MergeStats {
            duplicate_count,
            examples,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_families;

    // ------------------------------------------------------------------
    // merge_families tests
    // ------------------------------------------------------------------

    #[test]
    fn merge_families_no_overlap_is_passthrough() {
        let input = "# TYPE aaa gauge\naaa 1\n# TYPE bbb gauge\nbbb 2\n";
        let families = parse_families(input);
        let (merged, stats) = merge_families(families);
        assert_eq!(merged.len(), 2);
        assert_eq!(stats.duplicate_count, 0);
        assert!(stats.examples.is_empty());
    }

    #[test]
    fn merge_families_identical_label_key_first_wins() {
        // Two sources expose the same label-less metric.
        let mut families = parse_families("# TYPE up gauge\nup 1\n");
        families.extend(parse_families("# TYPE up gauge\nup 0\n"));
        let (merged, stats) = merge_families(families);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].samples.len(), 1, "duplicate must be dropped");
        // First value (1) must be kept.
        assert!(merged[0].samples[0].raw_line.contains("up 1"));
        assert_eq!(stats.duplicate_count, 1);
        assert_eq!(stats.examples, vec!["up"]);
    }

    #[test]
    fn merge_families_distinct_label_sets_both_kept() {
        // Same family name, different labels — no collision.
        let mut families = parse_families("cpu{cpu=\"0\"} 100\n");
        families.extend(parse_families("cpu{cpu=\"1\"} 200\n"));
        let (merged, stats) = merge_families(families);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].samples.len(), 2);
        assert_eq!(stats.duplicate_count, 0);
    }

    #[test]
    fn merge_families_partial_overlap() {
        // Source 1: cpu{cpu="0"} and cpu{cpu="1"}
        // Source 2: cpu{cpu="1"} (duplicate) and cpu{cpu="2"} (new)
        let mut families = parse_families("cpu{cpu=\"0\"} 10\ncpu{cpu=\"1\"} 20\n");
        families.extend(parse_families("cpu{cpu=\"1\"} 99\ncpu{cpu=\"2\"} 30\n"));
        let (merged, stats) = merge_families(families);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].samples.len(), 3, "0, 1 and 2 should be present");
        assert_eq!(stats.duplicate_count, 1);
        // The kept value for cpu="1" must be 20 (first-wins), not 99.
        let kept = merged[0]
            .samples
            .iter()
            .find(|s| s.label_key == r#"cpu="1""#)
            .expect("cpu=1 sample must exist");
        assert!(
            kept.raw_line.contains("20"),
            "first-seen value must be kept"
        );
    }

    #[test]
    fn merge_families_examples_capped_at_three() {
        // Four duplicate series — examples list must not exceed 3.
        let mut f1_input = String::new();
        let mut f2_input = String::new();
        for i in 0..4 {
            f1_input.push_str(&format!("m{{id=\"{i}\"}} 1\n"));
            f2_input.push_str(&format!("m{{id=\"{i}\"}} 2\n"));
        }
        let mut families = parse_families(&f1_input);
        families.extend(parse_families(&f2_input));
        let (_, stats) = merge_families(families);
        assert_eq!(stats.duplicate_count, 4);
        assert_eq!(stats.examples.len(), 3, "examples must be capped at 3");
    }

    // ------------------------------------------------------------------
    // inject_labels tests
    // ------------------------------------------------------------------

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn inject_labels_into_metric_without_labels() {
        let mut families = parse_families("up 1\n");
        inject_labels(&mut families, &labels(&[("cluster", "prod")]));
        assert_eq!(families[0].samples[0].raw_line, "up{cluster=\"prod\"} 1\n");
    }

    #[test]
    fn inject_labels_into_metric_with_existing_labels() {
        let mut families = parse_families("req{method=\"GET\"} 42\n");
        inject_labels(&mut families, &labels(&[("cluster", "prod")]));
        assert_eq!(
            families[0].samples[0].raw_line,
            "req{method=\"GET\",cluster=\"prod\"} 42\n"
        );
    }

    #[test]
    fn inject_labels_preserves_timestamp() {
        let mut families = parse_families("up 1 1700000000\n");
        inject_labels(&mut families, &labels(&[("dc", "eu")]));
        assert_eq!(
            families[0].samples[0].raw_line,
            "up{dc=\"eu\"} 1 1700000000\n"
        );
    }

    #[test]
    fn inject_labels_multiple_sorted_alphabetically() {
        let mut families = parse_families("up 1\n");
        inject_labels(
            &mut families,
            &labels(&[("zone", "a"), ("cluster", "prod")]),
        );
        // BTreeMap sorts keys: cluster < zone
        assert_eq!(
            families[0].samples[0].raw_line,
            "up{cluster=\"prod\",zone=\"a\"} 1\n"
        );
    }

    #[test]
    fn inject_labels_escapes_special_chars_in_value() {
        let mut families = parse_families("up 1\n");
        inject_labels(&mut families, &labels(&[("label", "val\\with\"quotes")]));
        assert_eq!(
            families[0].samples[0].raw_line,
            "up{label=\"val\\\\with\\\"quotes\"} 1\n"
        );
    }

    #[test]
    fn inject_labels_empty_extra_is_noop() {
        let input = "up 1\n";
        let mut families = parse_families(input);
        inject_labels(&mut families, &HashMap::new());
        assert_eq!(families[0].samples[0].raw_line, "up 1\n");
    }

    #[test]
    fn inject_labels_affects_shard_key() {
        // With extra labels, the label_key must be updated.
        let mut families = parse_families("up 1\n");
        inject_labels(&mut families, &labels(&[("cluster", "prod")]));
        assert_eq!(families[0].samples[0].label_key, r#"cluster="prod""#);
    }
}

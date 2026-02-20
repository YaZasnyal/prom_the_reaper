use std::collections::{BTreeMap, HashMap, HashSet};

/// A single parsed sample line, preserving the original text.
pub struct Sample {
    /// The original verbatim line (including trailing newline).
    pub raw_line: String,
}

/// A parsed metric family.
pub struct ParsedFamily {
    /// Base metric name (e.g. `http_requests`).
    pub name: String,
    /// Verbatim `# HELP ...` line with trailing newline, if present.
    pub help_line: Option<String>,
    /// Verbatim `# TYPE ...` line with trailing newline, if present.
    pub type_line: Option<String>,
    /// Individual sample lines.
    pub samples: Vec<Sample>,
}

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

/// Parses Prometheus exposition format text into metric families.
///
/// Groups HELP, TYPE, and sample lines by metric base name.
/// Histogram/summary suffixes (_bucket, _count, _sum, _total, _created, _info)
/// are grouped with their base metric via the TYPE declaration.
pub fn parse_families(input: &str) -> Vec<ParsedFamily> {
    let mut families: Vec<ParsedFamily> = Vec::new();
    // Index into `families` for the current family being built.
    let mut current_idx: Option<usize> = None;
    // The TYPE-declared base name (may differ from the sample name due to suffixes).
    let mut current_base: Option<String> = None;

    for line in input.lines() {
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix("# HELP ") {
            let name = first_token(rest).to_owned();
            let idx = get_or_insert(&mut families, &name);
            families[idx].help_line = Some(format!("{line}\n"));
            current_base = Some(name.clone());
            current_idx = Some(idx);
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let name = first_token(rest).to_owned();
            let idx = get_or_insert(&mut families, &name);
            families[idx].type_line = Some(format!("{line}\n"));
            current_base = Some(name.clone());
            current_idx = Some(idx);
        } else if line.starts_with('#') {
            // Non-HELP/TYPE comment — skip
        } else {
            // Sample line
            let sample_name = extract_metric_name(line);

            // Determine which family this sample belongs to.
            let idx = if current_base
                .as_deref()
                .is_some_and(|base| sample_belongs_to(sample_name, base))
            {
                // Belongs to the current TYPE-declared family.
                current_idx.unwrap_or_else(|| {
                    get_or_insert(&mut families, current_base.as_deref().unwrap())
                })
            } else {
                // New family encountered without a TYPE declaration.
                let base = base_name(sample_name);
                let idx = get_or_insert(&mut families, base);
                current_base = Some(base.to_owned());
                current_idx = Some(idx);
                idx
            };

            families[idx].samples.push(Sample {
                raw_line: format!("{line}\n"),
            });
        }
    }

    // Drop families with no samples (e.g. orphaned HELP/TYPE lines).
    families.retain(|f| !f.samples.is_empty());
    families
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
            let existing_keys: HashSet<String> = merged[idx]
                .samples
                .iter()
                .map(|s| extract_sorted_label_key(&s.raw_line))
                .collect();

            for sample in family.samples {
                let label_key = extract_sorted_label_key(&sample.raw_line);
                if existing_keys.contains(&label_key) {
                    duplicate_count += 1;
                    if examples.len() < 3 {
                        let example = if label_key.is_empty() {
                            family.name.clone()
                        } else {
                            format!("{}{{{}}}", family.name, label_key)
                        };
                        examples.push(example);
                    }
                } else {
                    merged[idx].samples.push(sample);
                }
            }
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

/// Returns the index of the family with the given name, inserting a new one if needed.
fn get_or_insert(families: &mut Vec<ParsedFamily>, name: &str) -> usize {
    if let Some(pos) = families.iter().position(|f| f.name == name) {
        return pos;
    }
    families.push(ParsedFamily {
        name: name.to_owned(),
        help_line: None,
        type_line: None,
        samples: Vec::new(),
    });
    families.len() - 1
}

/// Extracts the first whitespace-delimited token from a string.
fn first_token(s: &str) -> &str {
    s.split_whitespace().next().unwrap_or("")
}

/// Extracts the metric name from a sample line (everything before `{` or first space).
pub(crate) fn extract_metric_name(line: &str) -> &str {
    let end = line.find(['{', ' ']).unwrap_or(line.len());
    &line[..end]
}

/// For metrics without a TYPE declaration, strips known suffixes to find the base name.
fn base_name(sample_name: &str) -> &str {
    for suffix in &["_bucket", "_count", "_sum", "_total", "_created", "_info"] {
        if let Some(base) = sample_name.strip_suffix(suffix) {
            return base;
        }
    }
    sample_name
}

/// Checks if a sample metric name belongs to a base metric family.
/// Handles Prometheus suffixes: _bucket, _count, _sum, _total, _created, _info.
fn sample_belongs_to(sample_name: &str, base_name: &str) -> bool {
    if sample_name == base_name {
        return true;
    }
    if let Some(suffix) = sample_name.strip_prefix(base_name) {
        matches!(
            suffix,
            "_bucket" | "_count" | "_sum" | "_total" | "_created" | "_info"
        )
    } else {
        false
    }
}

/// Extracts label pairs from a sample line, sorts them, and returns a canonical key.
///
/// For `http_requests_total{method="GET",code="200"} 1` returns `code="200",method="GET"`.
/// For `up 1` (no labels) returns `""`.
pub(crate) fn extract_sorted_label_key(line: &str) -> String {
    let open = match line.find('{') {
        Some(i) => i,
        None => return String::new(),
    };
    let close = match line.rfind('}') {
        Some(i) => i,
        None => return String::new(),
    };
    if close <= open {
        return String::new();
    }
    let labels_str = &line[open + 1..close];
    if labels_str.is_empty() {
        return String::new();
    }

    // Split on commas that are not inside quotes.
    let mut pairs: Vec<&str> = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, ch) in labels_str.char_indices() {
        match ch {
            '"' => depth ^= 1,
            ',' if depth == 0 => {
                pairs.push(labels_str[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    pairs.push(labels_str[start..].trim());
    pairs.sort_unstable();
    pairs.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_gauge() {
        let input = "# HELP up Whether the target is up.\n# TYPE up gauge\nup 1\n";
        let families = parse_families(input);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name, "up");
        assert!(families[0].help_line.is_some());
        assert!(families[0].type_line.is_some());
        assert_eq!(families[0].samples.len(), 1);
        assert_eq!(
            extract_sorted_label_key(&families[0].samples[0].raw_line),
            ""
        );
    }

    #[test]
    fn label_key_is_sorted() {
        let input = "# TYPE req counter\nreq{z=\"1\",a=\"2\",m=\"3\"} 1\n";
        let families = parse_families(input);
        assert_eq!(
            extract_sorted_label_key(&families[0].samples[0].raw_line),
            r#"a="2",m="3",z="1""#
        );
    }

    #[test]
    fn histogram_grouped_together() {
        let input = r#"# HELP http_req_duration_seconds A histogram.
# TYPE http_req_duration_seconds histogram
http_req_duration_seconds_bucket{le="0.1"} 100
http_req_duration_seconds_bucket{le="+Inf"} 200
http_req_duration_seconds_sum 12.3
http_req_duration_seconds_count 200
"#;
        let families = parse_families(input);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name, "http_req_duration_seconds");
        assert_eq!(families[0].samples.len(), 4);
    }

    #[test]
    fn multiple_families() {
        let input = "# TYPE cpu counter\ncpu_total{cpu=\"0\"} 100\ncpu_total{cpu=\"1\"} 200\n# TYPE mem gauge\nmem 1024\n";
        let families = parse_families(input);
        assert_eq!(families.len(), 2);
        assert_eq!(families[0].samples.len(), 2);
        assert_eq!(families[1].samples.len(), 1);
    }

    #[test]
    fn no_help_or_type() {
        let input = "my_metric{label=\"a\"} 42\nmy_metric{label=\"b\"} 99\n";
        let families = parse_families(input);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name, "my_metric");
        assert_eq!(families[0].samples.len(), 2);
        assert!(families[0].help_line.is_none());
        assert!(families[0].type_line.is_none());
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        let input = "# Some random comment\n\n# HELP foo A foo.\n# TYPE foo gauge\nfoo 1\n\n";
        let families = parse_families(input);
        assert_eq!(families.len(), 1);
    }

    #[test]
    fn untyped_metrics_different_names_split() {
        let input = "aaa 1\nbbb 2\nccc 3\n";
        let families = parse_families(input);
        assert_eq!(families.len(), 3);
    }

    #[test]
    fn label_key_comma_in_value() {
        // A label value containing a comma should not be split.
        let input = "req{path=\"/a,b\",method=\"GET\"} 1\n";
        let families = parse_families(input);
        assert_eq!(
            extract_sorted_label_key(&families[0].samples[0].raw_line),
            r#"method="GET",path="/a,b""#
        );
    }

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
            .find(|s| extract_sorted_label_key(&s.raw_line) == r#"cpu="1""#)
            .expect("cpu=1 sample must exist");
        assert!(
            kept.raw_line.contains("20"),
            "first-seen value must be kept"
        );
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
        // With extra labels, extract_sorted_label_key must return a non-empty key.
        let mut families = parse_families("up 1\n");
        inject_labels(&mut families, &labels(&[("cluster", "prod")]));
        let key = extract_sorted_label_key(&families[0].samples[0].raw_line);
        assert_eq!(key, r#"cluster="prod""#);
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
}

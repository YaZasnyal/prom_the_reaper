/// A parsed metric family — the unit of sharding.
pub struct MetricFamily {
    /// The base metric name (used as the hash key for shard assignment).
    pub name: String,
    /// The verbatim text block: HELP + TYPE + all sample lines, including trailing newlines.
    pub raw_text: String,
}

/// Parses Prometheus exposition format text into metric families.
///
/// Groups HELP, TYPE, and sample lines by metric base name.
/// Histogram/summary suffixes (_bucket, _count, _sum, _total, _created, _info)
/// are grouped with their base metric via the TYPE declaration.
pub fn parse_families(input: &str) -> Vec<MetricFamily> {
    let mut families = Vec::new();
    let mut current_name: Option<String> = None;
    let mut current_text = String::new();

    for line in input.lines() {
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix("# HELP ") {
            let name = first_token(rest);
            if current_name.as_deref() != Some(name) {
                flush(&mut families, &mut current_name, &mut current_text);
                current_name = Some(name.to_owned());
            }
            current_text.push_str(line);
            current_text.push('\n');
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let name = first_token(rest);
            if current_name.as_deref() != Some(name) {
                flush(&mut families, &mut current_name, &mut current_text);
                current_name = Some(name.to_owned());
            }
            current_text.push_str(line);
            current_text.push('\n');
        } else if line.starts_with('#') {
            // Non-HELP/TYPE comment — skip
            continue;
        } else {
            // Sample line
            let sample_name = extract_metric_name(line);
            let belongs_to_current = current_name
                .as_deref()
                .is_some_and(|base| sample_belongs_to(sample_name, base));

            if belongs_to_current {
                current_text.push_str(line);
                current_text.push('\n');
            } else {
                flush(&mut families, &mut current_name, &mut current_text);
                current_name = Some(sample_name.to_owned());
                current_text.push_str(line);
                current_text.push('\n');
            }
        }
    }
    flush(&mut families, &mut current_name, &mut current_text);
    families
}

fn flush(families: &mut Vec<MetricFamily>, name: &mut Option<String>, text: &mut String) {
    if let Some(n) = name.take() {
        if !text.is_empty() {
            families.push(MetricFamily {
                name: n,
                raw_text: std::mem::take(text),
            });
        }
    }
    text.clear();
}

/// Extracts the first whitespace-delimited token from a string.
fn first_token(s: &str) -> &str {
    s.split_whitespace().next().unwrap_or("")
}

/// Extracts the metric name from a sample line (everything before `{` or first space).
fn extract_metric_name(line: &str) -> &str {
    let end = line
        .find(|c: char| c == '{' || c == ' ')
        .unwrap_or(line.len());
    &line[..end]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_gauge() {
        let input = r#"# HELP up Whether the target is up.
# TYPE up gauge
up 1
"#;
        let families = parse_families(input);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name, "up");
        assert!(families[0].raw_text.contains("# HELP up"));
        assert!(families[0].raw_text.contains("# TYPE up gauge"));
        assert!(families[0].raw_text.contains("up 1"));
    }

    #[test]
    fn histogram() {
        let input = r#"# HELP http_request_duration_seconds A histogram of request durations.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.1"} 24054
http_request_duration_seconds_bucket{le="0.5"} 33444
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320
"#;
        let families = parse_families(input);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name, "http_request_duration_seconds");
        // All 5 sample lines + 2 header lines
        let line_count = families[0].raw_text.lines().count();
        assert_eq!(line_count, 7);
    }

    #[test]
    fn multiple_families() {
        let input = r#"# TYPE cpu_seconds counter
cpu_seconds_total{cpu="0"} 100
cpu_seconds_total{cpu="1"} 200
# TYPE memory_bytes gauge
memory_bytes 1024000
"#;
        let families = parse_families(input);
        assert_eq!(families.len(), 2);
        assert_eq!(families[0].name, "cpu_seconds");
        assert_eq!(families[1].name, "memory_bytes");
    }

    #[test]
    fn no_help_or_type() {
        let input = "my_metric{label=\"value\"} 42\nmy_metric{label=\"other\"} 99\n";
        let families = parse_families(input);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name, "my_metric");
        assert_eq!(families[0].raw_text.lines().count(), 2);
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        let input =
            "# Some random comment\n\n# HELP foo A foo metric.\n# TYPE foo gauge\nfoo 1\n\n";
        let families = parse_families(input);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].name, "foo");
    }

    #[test]
    fn untyped_metrics_different_names_split() {
        let input = "aaa 1\nbbb 2\nccc 3\n";
        let families = parse_families(input);
        assert_eq!(families.len(), 3);
        assert_eq!(families[0].name, "aaa");
        assert_eq!(families[1].name, "bbb");
        assert_eq!(families[2].name, "ccc");
    }
}

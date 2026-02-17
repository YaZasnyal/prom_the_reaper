/// A single parsed sample line, preserving the original text.
pub struct Sample {
    /// Sorted, canonical label string used as the hash key (e.g. `cpu="0",mode="idle"`).
    /// Empty string for metrics without labels.
    pub label_key: String,
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

            let label_key = extract_sorted_label_key(line);
            families[idx].samples.push(Sample {
                label_key,
                raw_line: format!("{line}\n"),
            });
        }
    }

    // Drop families with no samples (e.g. orphaned HELP/TYPE lines).
    families.retain(|f| !f.samples.is_empty());
    families
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
fn extract_metric_name(line: &str) -> &str {
    let end = line
        .find(|c: char| c == '{' || c == ' ')
        .unwrap_or(line.len());
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
fn extract_sorted_label_key(line: &str) -> String {
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
        assert_eq!(families[0].samples[0].label_key, "");
    }

    #[test]
    fn label_key_is_sorted() {
        let input = "# TYPE req counter\nreq{z=\"1\",a=\"2\",m=\"3\"} 1\n";
        let families = parse_families(input);
        assert_eq!(families[0].samples[0].label_key, r#"a="2",m="3",z="1""#);
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
            families[0].samples[0].label_key,
            r#"method="GET",path="/a,b""#
        );
    }
}

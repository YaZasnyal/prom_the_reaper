use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, ensure};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub listen: String,
    pub num_shards: u32,
    pub scrape_interval_secs: u64,
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    pub url: String,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Extra labels to attach to every time series scraped from this source.
    /// Included in the consistent-hashing key, so they affect shard assignment.
    #[serde(default)]
    pub extra_labels: HashMap<String, String>,
}

fn default_timeout() -> u64 {
    30
}

impl AppConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        let config: AppConfig =
            toml::from_str(&content).with_context(|| "failed to parse config file")?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.num_shards > 0, "num_shards must be greater than 0");
        ensure!(!self.sources.is_empty(), "at least one source is required");
        ensure!(
            self.scrape_interval_secs > 0,
            "scrape_interval_secs must be greater than 0"
        );
        for (i, source) in self.sources.iter().enumerate() {
            ensure!(
                !source.url.is_empty(),
                "source[{}] url must not be empty",
                i
            );
            ensure!(
                source.timeout_secs > 0,
                "source[{}] timeout_secs must be greater than 0",
                i
            );
            for name in source.extra_labels.keys() {
                ensure!(
                    is_valid_label_name(name),
                    "source[{}] extra_labels: {:?} is not a valid Prometheus label name \
                     (must match [a-zA-Z_][a-zA-Z0-9_]*)",
                    i,
                    name
                );
            }
        }
        Ok(())
    }
}

/// Validates that a string is a legal Prometheus label name: `[a-zA-Z_][a-zA-Z0-9_]*`.
fn is_valid_label_name(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        None => false,
        Some(c) => {
            (c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
    }
}

//! Runtime configuration — settings for graph execution and node behavior

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for the Switchboard runtime
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Maximum time to wait for graceful shutdown before force-killing nodes
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,

    /// Enable tracing/logging for all node events
    #[serde(default = "default_tracing_enabled")]
    pub tracing_enabled: bool,

    /// Path to watch for YAML graph changes (hot-reload)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watch_path: Option<String>,

    /// Maximum buffer size per input queue (lagged subscriber threshold)
    #[serde(default = "default_max_buffer_size")]
    pub max_buffer_size: usize,

    /// Enable metrics collection
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
}

fn default_shutdown_timeout_ms() -> u64 {
    5000 // 5 seconds
}

fn default_tracing_enabled() -> bool {
    true
}

fn default_max_buffer_size() -> usize {
    1024
}

fn default_metrics_enabled() -> bool {
    false
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            shutdown_timeout_ms: default_shutdown_timeout_ms(),
            tracing_enabled: default_tracing_enabled(),
            watch_path: None,
            max_buffer_size: default_max_buffer_size(),
            metrics_enabled: default_metrics_enabled(),
        }
    }
}

impl RuntimeConfig {
    /// Get shutdown timeout as a Duration
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout_ms)
    }

    /// Load configuration from a YAML file
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config = serde_yaml::from_str(&contents)?;
        Ok(config)
    }

    /// Load configuration from a YAML string
    pub fn from_string(yaml_content: &str) -> anyhow::Result<Self> {
        let config = serde_yaml::from_str(yaml_content)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RuntimeConfig::default();
        assert_eq!(config.shutdown_timeout_ms, 5000);
        assert!(config.tracing_enabled);
        assert_eq!(config.max_buffer_size, 1024);
    }

    #[test]
    fn test_config_from_yaml() {
        let yaml = r#"
shutdown_timeout_ms: 10000
tracing_enabled: false
max_buffer_size: 512
"#;

        let config = RuntimeConfig::from_string(yaml).expect("should parse");
        assert_eq!(config.shutdown_timeout_ms, 10000);
        assert!(!config.tracing_enabled);
        assert_eq!(config.max_buffer_size, 512);
    }
}

//! Switchboard Runtime — Node lifecycle and configuration management
//!
//! Provides services for:
//! - Loading YAML graph definitions
//! - Managing node start/stop/restart lifecycle
//! - Configuration file watching for hot-reload
//! - Graceful shutdown coordination
//!
//! # Example
//!
//! ```ignore
//! use switchboard_runtime::Runtime;
//! let runtime = Runtime::from_yaml("graph.yaml")?;
//! runtime.start().await?;
//! // ... nodes running ...
//! runtime.shutdown().await?;
//! ```

mod config;
mod lifecycle;

pub use config::RuntimeConfig;
pub use lifecycle::Runtime;

use anyhow::Result;
use switchboard::router::Router;
use switchboard_flow::{GraphExecutor, YamlGraph};
use tracing::info;

/// Create and start a runtime from a YAML graph definition file
pub async fn from_yaml_file(path: &str) -> Result<Runtime> {
    let yaml = YamlGraph::from_file(path)?;
    let runtime = Runtime::new(yaml, Router::new());
    Ok(runtime)
}

/// Create and start a runtime from a YAML string
pub async fn from_yaml_string(yaml_content: &str) -> Result<Runtime> {
    let yaml = YamlGraph::from_string(yaml_content)?;
    let runtime = Runtime::new(yaml, Router::new());
    Ok(runtime)
}

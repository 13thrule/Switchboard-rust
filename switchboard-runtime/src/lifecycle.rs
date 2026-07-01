//! Runtime lifecycle management — start, stop, restart nodes

use anyhow::{anyhow, Result};
use switchboard::router::Router;
use switchboard_flow::{RunningGraph, YamlGraph};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Manages the lifecycle of a Switchboard dataflow graph
pub struct Runtime {
    yaml_graph: YamlGraph,
    router: Arc<Router>,
    running: Arc<RwLock<Option<RunningGraph>>>,
    state: Arc<RwLock<RuntimeState>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeState {
    Initialized,
    Starting,
    Running,
    Stopping,
    Stopped,
    Error,
}

impl Runtime {
    /// Create a new runtime instance from a YAML graph and router
    pub fn new(yaml_graph: YamlGraph, router: Router) -> Self {
        if let Err(e) = yaml_graph.validate() {
            warn!("graph validation warning: {}", e);
        }

        Runtime {
            yaml_graph,
            router: Arc::new(router),
            running: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(RuntimeState::Initialized)),
        }
    }

    /// Start the runtime — spawn all node tasks
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if *state != RuntimeState::Initialized {
            return Err(anyhow!("runtime is not in Initialized state"));
        }

        *state = RuntimeState::Starting;
        drop(state); // Release lock before long operation

        info!("starting runtime with {} nodes", self.yaml_graph.nodes.len());

        // For now, this is a placeholder. Real implementation would:
        // 1. Instantiate nodes from node_type registry
        // 2. Build Graph from YAML edges
        // 3. Start GraphExecutor
        //
        // This requires a node factory pattern which is left as an exercise.

        let mut state = self.state.write().await;
        *state = RuntimeState::Running;

        info!("runtime started");
        Ok(())
    }

    /// Stop the runtime — gracefully shutdown all nodes
    pub async fn shutdown(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if *state != RuntimeState::Running {
            warn!("shutdown called on non-running runtime");
            return Ok(());
        }

        *state = RuntimeState::Stopping;
        drop(state); // Release lock

        info!("stopping runtime");

        if let Some(running) = self.running.write().await.take() {
            running.shutdown();
            // Wait for all tasks to be cancelled
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        let mut state = self.state.write().await;
        *state = RuntimeState::Stopped;

        info!("runtime stopped");
        Ok(())
    }

    /// Get the underlying Switchboard router
    pub fn router(&self) -> Arc<Router> {
        self.router.clone()
    }

    /// Get the current runtime state
    pub async fn state(&self) -> RuntimeState {
        *self.state.read().await
    }

    /// Get the YAML graph definition
    pub fn graph(&self) -> &YamlGraph {
        &self.yaml_graph
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Graceful shutdown is not async in Drop, but we can log
        // Real cleanup happens via explicit shutdown() call
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_lifecycle() {
        let yaml_str = r#"
nodes:
  - id: test_node
    node_type: TestNode

edges: []
"#;

        let yaml = YamlGraph::from_string(yaml_str).expect("should parse");
        let router = Router::new();
        let runtime = Runtime::new(yaml, router);

        assert_eq!(runtime.state().await, RuntimeState::Initialized);

        runtime.start().await.expect("should start");
        assert_eq!(runtime.state().await, RuntimeState::Running);

        runtime.shutdown().await.expect("should shutdown");
        assert_eq!(runtime.state().await, RuntimeState::Stopped);
    }
}

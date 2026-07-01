//! YAML graph loading — describe dataflow pipelines in declarative configuration.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::{
    graph::{Edge, Graph, GraphBuilder},
    ids::{NodeId, PortId},
};

/// A YAML-serializable graph definition.
/// Allows describing nodes and edges declaratively without writing Rust code.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YamlGraph {
    /// List of nodes to instantiate
    pub nodes: Vec<YamlNode>,
    /// List of connections between nodes
    pub edges: Vec<YamlEdge>,
}

/// A node definition in YAML format
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YamlNode {
    /// Unique identifier for this node
    pub id: String,
    /// Node type/class (for factory instantiation)
    pub node_type: String,
    /// Optional configuration parameters
    #[serde(default)]
    pub config: serde_yaml::Value,
}

/// An edge definition connecting two node ports
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YamlEdge {
    /// Source node ID
    pub from_node: String,
    /// Source output port name
    pub from_port: String,
    /// Destination node ID
    pub to_node: String,
    /// Destination input port name
    pub to_port: String,
    /// Switchboard topic name for this edge
    pub topic: String,
}

impl YamlGraph {
    /// Load a YAML graph definition from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let graph = serde_yaml::from_str(&contents)?;
        Ok(graph)
    }

    /// Load a YAML graph definition from a string
    pub fn from_string(yaml_content: &str) -> Result<Self> {
        let graph = serde_yaml::from_str(yaml_content)?;
        Ok(graph)
    }

    /// Validate that all edges reference valid node IDs and ports
    pub fn validate(&self) -> Result<()> {
        let node_ids: std::collections::HashSet<_> = 
            self.nodes.iter().map(|n| n.id.as_str()).collect();

        for edge in &self.edges {
            if !node_ids.contains(edge.from_node.as_str()) {
                return Err(anyhow!(
                    "edge references unknown from_node: {}",
                    edge.from_node
                ));
            }
            if !node_ids.contains(edge.to_node.as_str()) {
                return Err(anyhow!(
                    "edge references unknown to_node: {}",
                    edge.to_node
                ));
            }
        }
        Ok(())
    }

    /// Get the YAML definition as a pretty-printed string
    pub fn to_yaml_string(&self) -> Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_yaml_graph_loading() {
        let yaml = r#"
nodes:
  - id: uppercase
    node_type: TextTransform
    config:
      operation: uppercase
  - id: exclaim
    node_type: TextTransform
    config:
      operation: suffix
      suffix: "!"

edges:
  - from_node: uppercase
    from_port: output
    to_node: exclaim
    to_port: input
    topic: transformed
"#;

        let graph = YamlGraph::from_string(yaml).expect("should parse");
        graph.validate().expect("should validate");
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
    }

    #[test]
    fn test_yaml_graph_validation() {
        let yaml = r#"
nodes:
  - id: node1
    node_type: TestNode

edges:
  - from_node: node1
    from_port: out
    to_node: nonexistent
    to_port: in
    topic: test
"#;

        let graph = YamlGraph::from_string(yaml).expect("should parse");
        let result = graph.validate();
        assert!(result.is_err(), "should fail validation for unknown node");
    }
}

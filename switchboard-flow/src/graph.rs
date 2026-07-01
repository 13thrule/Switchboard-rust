//! Graph definition: nodes and the topic-backed edges that connect them.

use std::collections::HashMap;

use bytes::Bytes;

use crate::{
    ids::{NodeId, PortId},
    node::Node,
};

/// One input or output connection for a node, bound to a Switchboard topic.
#[derive(Debug, Clone)]
pub struct Edge {
    pub node: NodeId,
    pub port: PortId,
    pub topic: Bytes,
}

/// Defines how a node's ports map onto Switchboard topics.
///
/// A `Graph` is a pure data structure — it doesn't run anything itself.
/// Pass it to a [`GraphExecutor`](crate::executor::GraphExecutor) to
/// actually wire it up to a live `Router` and start processing.
pub struct Graph {
    pub(crate) nodes: HashMap<NodeId, Box<dyn Node + Send>>,
    pub(crate) input_edges: Vec<Edge>,
    pub(crate) output_edges: Vec<Edge>,
}

impl Graph {
    pub fn builder() -> GraphBuilder {
        GraphBuilder::new()
    }

    pub fn node_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.keys()
    }
}

/// Builder for [`Graph`]. Add nodes, then wire their ports to topics.
#[derive(Default)]
pub struct GraphBuilder {
    nodes: HashMap<NodeId, Box<dyn Node + Send>>,
    input_edges: Vec<Edge>,
    output_edges: Vec<Edge>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        GraphBuilder {
            nodes: HashMap::new(),
            input_edges: Vec::new(),
            output_edges: Vec::new(),
        }
    }

    /// Register a node under the given id. The node must be wired to at
    /// least one input or output edge via [`input`](Self::input) /
    /// [`output`](Self::output) for it to actually do anything.
    pub fn node(mut self, id: impl Into<NodeId>, node: impl Node + 'static) -> Self {
        self.nodes.insert(id.into(), Box::new(node));
        self
    }

    /// Subscribe `node`'s `port` to messages published on `topic`.
    pub fn input(
        mut self,
        node: impl Into<NodeId>,
        port: impl Into<PortId>,
        topic: impl Into<Bytes>,
    ) -> Self {
        self.input_edges.push(Edge {
            node: node.into(),
            port: port.into(),
            topic: topic.into(),
        });
        self
    }

    /// Route `node`'s `port` output to be published on `topic`.
    pub fn output(
        mut self,
        node: impl Into<NodeId>,
        port: impl Into<PortId>,
        topic: impl Into<Bytes>,
    ) -> Self {
        self.output_edges.push(Edge {
            node: node.into(),
            port: port.into(),
            topic: topic.into(),
        });
        self
    }

    /// Finalize the graph.
    ///
    /// Returns an error if any edge references a node id that wasn't
    /// registered via [`node`](Self::node) — this is checked at build
    /// time rather than at run time so misconfigured graphs fail fast.
    pub fn build(self) -> anyhow::Result<Graph> {
        for edge in self.input_edges.iter().chain(self.output_edges.iter()) {
            if !self.nodes.contains_key(&edge.node) {
                anyhow::bail!(
                    "graph edge references unknown node id: {}",
                    edge.node
                );
            }
        }

        Ok(Graph {
            nodes: self.nodes,
            input_edges: self.input_edges,
            output_edges: self.output_edges,
        })
    }
}

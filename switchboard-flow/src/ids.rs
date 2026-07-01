//! Identifiers used throughout the graph engine.

use std::fmt;

/// Unique identifier for a node within a [`Graph`](crate::graph::Graph).
///
/// Cloning a `NodeId` is cheap (it wraps a `String`); graphs are expected
/// to be small enough (tens to low thousands of nodes) that this is not
/// a bottleneck. If that changes, this can become an interned `u32` later
/// without breaking the public API of `Node`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        NodeId(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        NodeId::new(s)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        NodeId::new(s)
    }
}

/// Identifier for a single input or output port on a node.
///
/// A node with one input and one output can just use `PortId::new("in")` /
/// `PortId::new("out")`, or even both `"default"` — the value is only
/// used to distinguish multiple ports on the *same* node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PortId(String);

impl PortId {
    pub fn new(id: impl Into<String>) -> Self {
        PortId(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convenience constructor for the common single-port case.
    pub fn default_port() -> Self {
        PortId::new("default")
    }
}

impl fmt::Display for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for PortId {
    fn from(s: &str) -> Self {
        PortId::new(s)
    }
}

impl From<String> for PortId {
    fn from(s: String) -> Self {
        PortId::new(s)
    }
}

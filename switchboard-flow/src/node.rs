//! The [`Node`] trait — the unit of computation in a Switchboard dataflow graph.

use bytes::Bytes;

use crate::ids::PortId;

/// A single processing unit in a [`Graph`](crate::graph::Graph).
///
/// A node receives messages on one or more input ports (each backed by a
/// Switchboard topic), and may produce zero or more output messages, each
/// tagged with the output port it should be published on.
///
/// # Fan-in
///
/// If a node has multiple input ports, [`GraphExecutor`](crate::executor::GraphExecutor)
/// runs them in **event-driven** mode by default: whichever input topic
/// receives a message first triggers `process`, with `input_port` telling
/// the node which one fired. This matches Switchboard's own waker-driven,
/// zero-polling model — there's no central loop deciding whose turn it is.
///
/// # Fan-out
///
/// A node can publish to multiple output topics by returning multiple
/// `(PortId, Bytes)` pairs from a single `process` call.
#[async_trait::async_trait]
pub trait Node: Send {
    /// Process a single input message.
    ///
    /// `input_port` identifies which input port the message arrived on,
    /// which matters for nodes with more than one input. Single-input
    /// nodes can ignore it.
    ///
    /// Returns zero or more `(output_port, payload)` pairs to publish.
    /// Returning an empty `Vec` is valid (e.g. for sink nodes, or nodes
    /// that buffer internally and only emit periodically).
    async fn process(
        &mut self,
        input_port: &PortId,
        input: Bytes,
    ) -> anyhow::Result<Vec<(PortId, Bytes)>>;

    /// Human-readable name for logs/metrics. Defaults to the type name.
    fn name(&self) -> &str {
        "node"
    }
}

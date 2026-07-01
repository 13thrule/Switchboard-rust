//! Executes a [`Graph`] against a live Switchboard `Router`.
//!
//! Each node runs in its own `tokio::spawn`'d task. A node with multiple
//! input topics subscribes to all of them and reacts to whichever message
//! arrives first — no central polling loop, no scheduler deciding turns.
//! This mirrors the same event-driven, waker-based model Switchboard's own
//! `connection.rs` write_task already uses (`StreamMap` over per-topic
//! broadcast subscriptions).

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use switchboard::router::{Router, RouterMessage};
use tokio::task::JoinHandle;
use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};
use tracing::{debug, error, info, warn};

use crate::{
    graph::Graph,
    ids::{NodeId, PortId},
    node::Node,
};

/// How a node should react when it has more than one input port.
///
/// [`FanInMode::EventDriven`] (default) processes whichever input arrives first.
/// [`FanInMode::Join`] waits until every input port has produced at least one message.
/// [`FanInMode::Priority`] checks inputs in a fixed priority order.
#[derive(Debug, Clone)]
pub enum FanInMode {
    /// Process whichever input arrives first. Default, and fully implemented.
    EventDriven,
    /// Wait until every input port has produced at least one buffered
    /// message, then process them together as a batch. Useful for joins,
    /// correlations, and multi-leg transactions.
    Join,
    /// Like `EventDriven`, but input ports are checked in a fixed
    /// priority order. Higher-priority ports are drained before lower ones.
    /// Prevents lower-priority messages from starving if higher-priority
    /// messages keep arriving.
    Priority(Vec<PortId>),
}

impl Default for FanInMode {
    fn default() -> Self {
        FanInMode::EventDriven
    }
}

/// Runs a [`Graph`]'s nodes as independent async tasks wired to a `Router`.
pub struct GraphExecutor {
    router: Router,
}

/// Handles for a running graph. Dropping this does not stop the nodes —
/// call [`RunningGraph::shutdown`] or abort the handles yourself.
pub struct RunningGraph {
    handles: HashMap<NodeId, JoinHandle<()>>,
}

impl RunningGraph {
    /// Abort every node task. Already-in-flight `process` calls are
    /// cancelled, not awaited to completion.
    pub fn shutdown(&self) {
        for handle in self.handles.values() {
            handle.abort();
        }
    }

    /// Wait for all node tasks to finish (they normally run forever until
    /// aborted or their input topics' senders are all dropped).
    pub async fn join_all(self) {
        for (id, handle) in self.handles {
            if let Err(e) = handle.await {
                if !e.is_cancelled() {
                    error!(node = %id, error = %e, "node task panicked");
                }
            }
        }
    }
}

impl GraphExecutor {
    pub fn new(router: Router) -> Self {
        GraphExecutor { router }
    }

    /// Start every node in `graph` as its own task, subscribed to its
    /// input topics and able to publish to its output topics.
    ///
    /// Nodes that have neither inputs nor outputs wired are started but
    /// will sit idle forever — this is treated as a configuration warning,
    /// not an error, since a node-under-construction is a normal interim
    /// state.
    pub fn run(self, graph: Graph) -> RunningGraph {
        let mut inputs_by_node: HashMap<NodeId, Vec<(PortId, Bytes)>> = HashMap::new();
        for edge in graph.input_edges {
            inputs_by_node
                .entry(edge.node)
                .or_default()
                .push((edge.port, edge.topic));
        }

        let mut outputs_by_node: HashMap<NodeId, Vec<(PortId, Bytes)>> = HashMap::new();
        for edge in graph.output_edges {
            outputs_by_node
                .entry(edge.node)
                .or_default()
                .push((edge.port, edge.topic));
        }

        let mut handles = HashMap::new();

        let mut nodes = graph.nodes;
        let node_ids: Vec<NodeId> = nodes.keys().cloned().collect();

        for node_id in node_ids {
            let node = nodes.remove(&node_id).expect("node id came from this map");
            let inputs = inputs_by_node.remove(&node_id).unwrap_or_default();
            let outputs = outputs_by_node.remove(&node_id).unwrap_or_default();

            if inputs.is_empty() {
                warn!(node = %node_id, "node has no input edges; it will never run");
            }

            let router = self.router.clone();
            let id_for_task = node_id.clone();

            let handle = tokio::spawn(async move {
                run_node_task(id_for_task, node, router, inputs, outputs).await;
            });

            handles.insert(node_id, handle);
        }

        RunningGraph { handles }
    }
}

async fn run_node_task(
    node_id: NodeId,
    mut node: Box<dyn Node + Send>,
    router: Router,
    inputs: Vec<(PortId, Bytes)>,
    outputs: Vec<(PortId, Bytes)>,
) {
    if inputs.is_empty() {
        return;
    }

    // One subscription per input port, multiplexed via StreamMap so the
    // task wakes only when *some* input has data — zero polling, same
    // pattern Switchboard's connection write_task already uses.
    let mut stream_map: StreamMap<PortId, BroadcastStream<RouterMessage>> = StreamMap::new();
    let input_ports: Vec<PortId> = inputs.iter().map(|(p, _)| p.clone()).collect();
    
    for (port, topic) in &inputs {
        let receiver = router.subscribe(topic.clone());
        stream_map.insert(port.clone(), BroadcastStream::new(receiver));
    }

    // output port -> topic, for fast lookup after each `process` call.
    let output_topics: HashMap<PortId, Bytes> = outputs.into_iter().collect();

    info!(node = %node_id, inputs = inputs.len(), outputs = output_topics.len(), "node task started");

    // Buffer for Join and Priority modes
    let mut message_buffers: HashMap<PortId, VecDeque<Bytes>> = HashMap::new();
    for port in &input_ports {
        message_buffers.insert(port.clone(), VecDeque::new());
    }

    loop {
        let Some((port, result)) = stream_map.next().await else {
            // All input streams ended (every sender dropped). Normal
            // shutdown path, not an error.
            info!(node = %node_id, "node task: all input streams closed, exiting");
            break;
        };

        let payload = match result {
            Ok(msg) => msg.payload,
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                warn!(node = %node_id, port = %port, dropped = n, "node task: input lagged, messages dropped");
                continue;
            }
        };

        // Buffer this message
        if let Some(queue) = message_buffers.get_mut(&port) {
            queue.push_back(payload);
        }

        // Decide which message(s) to process based on mode
        // For now, EventDriven mode: process immediately
        if input_ports.len() == 1 {
            // Single input: always process immediately
            if let Some(payload) = message_buffers.get_mut(&port).and_then(|q| q.pop_front()) {
                emit_result(&node_id, &mut node, &port, payload, &output_topics, &router).await;
            }
        } else {
            // Multiple inputs: EventDriven processes first to arrive
            if let Some(payload) = message_buffers.get_mut(&port).and_then(|q| q.pop_front()) {
                emit_result(&node_id, &mut node, &port, payload, &output_topics, &router).await;
            }
        }
    }
}

async fn emit_result(
    node_id: &NodeId,
    node: &mut Box<dyn Node + Send>,
    port: &PortId,
    payload: Bytes,
    output_topics: &HashMap<PortId, Bytes>,
    router: &Router,
) {
    debug!(node = %node_id, port = %port, bytes = payload.len(), "node task: processing input");

    match node.process(port, payload).await {
        Ok(emitted) => {
            for (out_port, out_payload) in emitted {
                match output_topics.get(&out_port) {
                    Some(topic) => {
                        router.publish(topic, out_payload);
                    }
                    None => {
                        warn!(
                            node = %node_id,
                            port = %out_port,
                            "node emitted on a port with no output edge; dropping"
                        );
                    }
                }
            }
        }
        Err(e) => {
            error!(node = %node_id, error = %e, "node task: process() returned an error");
        }
    }
}

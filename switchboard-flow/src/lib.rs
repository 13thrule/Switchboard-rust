//! `switchboard-flow` — a dataflow graph engine built on top of
//! Switchboard's zero-copy pub/sub `Router`.
//!
//! Nodes are units of async computation; edges are Switchboard topics.
//! Each node runs in its own task and reacts to whichever of its input
//! topics produces a message first (event-driven fan-in), matching
//! Switchboard's own waker-driven, zero-polling model.
//!
//! ```no_run
//! use switchboard::router::Router;
//! use switchboard_flow::{Graph, GraphExecutor, Node, PortId};
//! use bytes::Bytes;
//! use async_trait::async_trait;
//!
//! struct Uppercase;
//!
//! #[async_trait]
//! impl Node for Uppercase {
//!     async fn process(&mut self, _port: &PortId, input: Bytes) -> anyhow::Result<Vec<(PortId, Bytes)>> {
//!         let upper = String::from_utf8_lossy(&input).to_uppercase();
//!         Ok(vec![(PortId::default_port(), Bytes::from(upper))])
//!     }
//! }
//!
//! # async fn run() -> anyhow::Result<()> {
//! let router = Router::new();
//!
//! let graph = Graph::builder()
//!     .node("upper", Uppercase)
//!     .input("upper", PortId::default_port(), Bytes::from_static(b"in"))
//!     .output("upper", PortId::default_port(), Bytes::from_static(b"out"))
//!     .build()?;
//!
//! let running = GraphExecutor::new(router).run(graph);
//! // ... publish to "in", subscribe to "out" via the same router ...
//! running.shutdown();
//! # Ok(())
//! # }
//! ```

pub mod executor;
pub mod graph;
pub mod ids;
pub mod node;

pub use executor::{FanInMode, GraphExecutor, RunningGraph};
pub use graph::{Edge, Graph, GraphBuilder};
pub use ids::{NodeId, PortId};
pub use node::Node;

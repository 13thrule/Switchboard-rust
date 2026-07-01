//! Two-node pipeline running over a real, in-process Switchboard `Router`:
//!
//!   topic "raw"  -> [Uppercase] -> topic "shouted" -> [Exclaim] -> topic "final"
//!
//! Run with:
//!   cargo run --example uppercase_pipeline

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use switchboard::router::Router;
use switchboard_flow::{Graph, GraphExecutor, Node, PortId};

struct Uppercase;

#[async_trait]
impl Node for Uppercase {
    async fn process(
        &mut self,
        _port: &PortId,
        input: Bytes,
    ) -> anyhow::Result<Vec<(PortId, Bytes)>> {
        let upper = String::from_utf8_lossy(&input).to_uppercase();
        Ok(vec![(PortId::default_port(), Bytes::from(upper))])
    }

    fn name(&self) -> &str {
        "uppercase"
    }
}

struct Exclaim;

#[async_trait]
impl Node for Exclaim {
    async fn process(
        &mut self,
        _port: &PortId,
        input: Bytes,
    ) -> anyhow::Result<Vec<(PortId, Bytes)>> {
        let mut out = String::from_utf8_lossy(&input).into_owned();
        out.push('!');
        Ok(vec![(PortId::default_port(), Bytes::from(out))])
    }

    fn name(&self) -> &str {
        "exclaim"
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let router = Router::new();

    let graph = Graph::builder()
        .node("upper", Uppercase)
        .input("upper", PortId::default_port(), Bytes::from_static(b"raw"))
        .output("upper", PortId::default_port(), Bytes::from_static(b"shouted"))
        .node("exclaim", Exclaim)
        .input("exclaim", PortId::default_port(), Bytes::from_static(b"shouted"))
        .output("exclaim", PortId::default_port(), Bytes::from_static(b"final"))
        .build()?;

    // Subscribe to the final output *before* starting the graph and
    // publishing, so we don't race the broadcast channel's creation.
    let mut final_rx = router.subscribe(Bytes::from_static(b"final"));

    let running = GraphExecutor::new(router.clone()).run(graph);

    // Give the node tasks a moment to subscribe to their input topics.
    tokio::time::sleep(Duration::from_millis(50)).await;

    router.publish(&Bytes::from_static(b"raw"), Bytes::from_static(b"hello switchboard"));

    let msg = tokio::time::timeout(Duration::from_secs(2), final_rx.recv())
        .await
        .expect("timed out waiting for pipeline output")
        .expect("router channel closed");

    println!("pipeline output: {}", String::from_utf8_lossy(&msg.payload));

    running.shutdown();
    Ok(())
}

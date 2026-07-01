use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use switchboard::router::Router;
use switchboard_flow::{Graph, GraphExecutor, Node, PortId};

struct Echo;

#[async_trait]
impl Node for Echo {
    async fn process(
        &mut self,
        _port: &PortId,
        input: Bytes,
    ) -> anyhow::Result<Vec<(PortId, Bytes)>> {
        Ok(vec![(PortId::default_port(), input)])
    }
}

/// Tags every input payload with which port it arrived on, e.g.
/// input `b"x"` on port "a" becomes `b"a:x"`. Used to verify fan-in.
struct TagPort;

#[async_trait]
impl Node for TagPort {
    async fn process(
        &mut self,
        port: &PortId,
        input: Bytes,
    ) -> anyhow::Result<Vec<(PortId, Bytes)>> {
        let mut out = Vec::with_capacity(port.as_str().len() + 1 + input.len());
        out.extend_from_slice(port.as_str().as_bytes());
        out.push(b':');
        out.extend_from_slice(&input);
        Ok(vec![(PortId::default_port(), Bytes::from(out))])
    }
}

/// Emits two outputs (on different ports) per input, to verify fan-out.
struct Duplicate;

#[async_trait]
impl Node for Duplicate {
    async fn process(
        &mut self,
        _port: &PortId,
        input: Bytes,
    ) -> anyhow::Result<Vec<(PortId, Bytes)>> {
        Ok(vec![
            (PortId::new("a"), input.clone()),
            (PortId::new("b"), input),
        ])
    }
}

#[tokio::test]
async fn single_node_single_edge_roundtrip() {
    let router = Router::new();

    let graph = Graph::builder()
        .node("echo", Echo)
        .input("echo", PortId::default_port(), Bytes::from_static(b"in"))
        .output("echo", PortId::default_port(), Bytes::from_static(b"out"))
        .build()
        .unwrap();

    let mut rx = router.subscribe(Bytes::from_static(b"out"));
    let running = GraphExecutor::new(router.clone()).run(graph);

    tokio::time::sleep(Duration::from_millis(30)).await;
    router.publish(&Bytes::from_static(b"in"), Bytes::from_static(b"hello"));

    let msg = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .unwrap();
    assert_eq!(&msg.payload[..], b"hello");

    running.shutdown();
}

#[tokio::test]
async fn two_node_pipeline_chains_through_topic() {
    let router = Router::new();

    let graph = Graph::builder()
        .node("first", Echo)
        .input("first", PortId::default_port(), Bytes::from_static(b"a"))
        .output("first", PortId::default_port(), Bytes::from_static(b"b"))
        .node("second", Echo)
        .input("second", PortId::default_port(), Bytes::from_static(b"b"))
        .output("second", PortId::default_port(), Bytes::from_static(b"c"))
        .build()
        .unwrap();

    let mut rx = router.subscribe(Bytes::from_static(b"c"));
    let running = GraphExecutor::new(router.clone()).run(graph);

    tokio::time::sleep(Duration::from_millis(30)).await;
    router.publish(&Bytes::from_static(b"a"), Bytes::from_static(b"chained"));

    let msg = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .unwrap();
    assert_eq!(&msg.payload[..], b"chained");

    running.shutdown();
}

#[tokio::test]
async fn event_driven_fan_in_processes_either_input() {
    let router = Router::new();

    let graph = Graph::builder()
        .node("tag", TagPort)
        .input("tag", "a", Bytes::from_static(b"topic_a"))
        .input("tag", "b", Bytes::from_static(b"topic_b"))
        .output("tag", PortId::default_port(), Bytes::from_static(b"tagged"))
        .build()
        .unwrap();

    let mut rx = router.subscribe(Bytes::from_static(b"tagged"));
    let running = GraphExecutor::new(router.clone()).run(graph);

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Publish on the second input port only.
    router.publish(&Bytes::from_static(b"topic_b"), Bytes::from_static(b"hi"));

    let msg = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .unwrap();
    assert_eq!(&msg.payload[..], b"b:hi");

    // Now the first input port.
    router.publish(&Bytes::from_static(b"topic_a"), Bytes::from_static(b"yo"));
    let msg = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .unwrap();
    assert_eq!(&msg.payload[..], b"a:yo");

    running.shutdown();
}

#[tokio::test]
async fn fan_out_publishes_to_all_output_ports() {
    let router = Router::new();

    let graph = Graph::builder()
        .node("dup", Duplicate)
        .input("dup", PortId::default_port(), Bytes::from_static(b"in"))
        .output("dup", "a", Bytes::from_static(b"out_a"))
        .output("dup", "b", Bytes::from_static(b"out_b"))
        .build()
        .unwrap();

    let mut rx_a = router.subscribe(Bytes::from_static(b"out_a"));
    let mut rx_b = router.subscribe(Bytes::from_static(b"out_b"));
    let running = GraphExecutor::new(router.clone()).run(graph);

    tokio::time::sleep(Duration::from_millis(30)).await;
    router.publish(&Bytes::from_static(b"in"), Bytes::from_static(b"split-me"));

    let a = tokio::time::timeout(Duration::from_secs(1), rx_a.recv())
        .await
        .expect("timeout")
        .unwrap();
    let b = tokio::time::timeout(Duration::from_secs(1), rx_b.recv())
        .await
        .expect("timeout")
        .unwrap();

    assert_eq!(&a.payload[..], b"split-me");
    assert_eq!(&b.payload[..], b"split-me");

    running.shutdown();
}

#[test]
fn build_fails_on_unknown_node_reference() {
    let result = Graph::builder()
        .input("ghost", PortId::default_port(), Bytes::from_static(b"in"))
        .build();

    assert!(result.is_err());
}

#[tokio::test]
async fn multiple_subscribers_on_same_output_topic_both_receive() {
    // Sanity check that flow's output publish goes through the *same*
    // zero-copy broadcast path as native Switchboard clients — multiple
    // independent subscribers on the output topic should all get it.
    let router = Router::new();

    let graph = Graph::builder()
        .node("echo", Echo)
        .input("echo", PortId::default_port(), Bytes::from_static(b"in"))
        .output("echo", PortId::default_port(), Bytes::from_static(b"out"))
        .build()
        .unwrap();

    let mut rx1 = router.subscribe(Bytes::from_static(b"out"));
    let mut rx2 = router.subscribe(Bytes::from_static(b"out"));
    let running = GraphExecutor::new(router.clone()).run(graph);

    tokio::time::sleep(Duration::from_millis(30)).await;
    router.publish(&Bytes::from_static(b"in"), Bytes::from_static(b"fan-out-native"));

    let m1 = tokio::time::timeout(Duration::from_secs(1), rx1.recv())
        .await
        .expect("timeout")
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(1), rx2.recv())
        .await
        .expect("timeout")
        .unwrap();

    // Same underlying Bytes allocation — zero-copy, not a re-encode.
    assert_eq!(m1.payload.as_ptr(), m2.payload.as_ptr());

    running.shutdown();
}

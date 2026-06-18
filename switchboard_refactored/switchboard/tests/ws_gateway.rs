use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use switchboard::{connection_ws::run_websocket_connection, router::Router, protocol::Frame};

#[tokio::test]
async fn websocket_gateway_roundtrip() {
    // Bind to an ephemeral port
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let router = Router::new();
    let router_for_server = router.clone();

    // Accept one connection and run the gateway handler
    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.expect("accept");
        run_websocket_connection(stream, peer, router_for_server).await.expect("ws handler");
    });

    // Connect a client
    let url = format!("ws://{}", addr);
    let (ws_stream, _resp) = connect_async(&url).await.expect("connect");
    let (mut write, mut read) = ws_stream.split();

    // Send a Subscribe frame for topic "itest"
    let topic = b"itest".to_vec();
    let mut sub = vec![0x01];
    sub.extend_from_slice(&topic);
    write.send(Message::Binary(sub)).await.expect("send sub");

    // Give the server a moment to register the subscription
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish into the router; the websocket client should receive it
    let payload = Bytes::from_static(b"hello-integration");
    let topic_bytes = Bytes::copy_from_slice(topic.as_slice());
    router.publish(&topic_bytes, payload.clone());

    // Read a message from the WebSocket client
    let got = tokio::time::timeout(Duration::from_secs(2), read.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended")
        .expect("ws error");

    match got {
        Message::Binary(b) => {
            let frame = Frame::parse(Bytes::from(b)).expect("parse frame");
            match frame {
                Frame::Publish { topic: t, payload: p } => {
                    assert_eq!(&p[..], &payload[..]);
                    assert_eq!(&t[..], b"itest");
                }
                other => panic!("expected Publish, got {:?}", other),
            }
        }
        other => panic!("expected binary message, got {:?}", other),
    }

    // Clean up server
    server.abort();
}

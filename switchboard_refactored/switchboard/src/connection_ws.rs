use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::{broadcast, mpsc}};
use tokio_stream::{wrappers::BroadcastStream, StreamMap};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::{protocol::{encode_publish, Frame}, router::{Router, RouterMessage}};

const MAX_WS_FRAME_BYTES: usize = 16 * 1024 * 1024;

struct NewSubscription {
    topic:    Bytes,
    receiver: broadcast::Receiver<RouterMessage>,
}

pub async fn run_websocket_connection(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    router: Router,
) -> Result<()> {
    let ws_stream = accept_async(stream)
        .await
        .context("accepting websocket handshake")?;

    info!(peer = %peer, "websocket connection accepted");

    let (mut ws_write, mut ws_read) = ws_stream.split();
    let (sub_tx, mut sub_rx) = mpsc::channel::<NewSubscription>(64);

    let read_handle = tokio::spawn(async move {
        while let Some(message) = ws_read.next().await {
            let message = message.context("reading websocket message")?;
            if message.is_binary() {
                let raw = message.into_data();
                let raw = Bytes::from(raw);

                debug!(peer = %peer, len = raw.len(), "ws read: binary payload");
                if raw.is_empty() {
                    continue;
                }

                if raw.len() > MAX_WS_FRAME_BYTES {
                    warn!(peer = %peer, len = raw.len(), "ws read: frame exceeds max allowed size, dropping");
                    continue;
                }

                match Frame::parse(raw.clone()) {
                    Ok(frame) => match frame {
                        Frame::Subscribe { topic } => {
                            let receiver = router.subscribe(topic.clone());
                            if let Err(e) = sub_tx.send(NewSubscription { topic, receiver }).await {
                                error!(peer = %peer, error = %e, "ws read: failed to send subscription");
                                // On internal channel send failure, break the read loop
                                break;
                            }
                        }
                        Frame::Publish { topic, payload } => {
                            router.publish(&topic, payload);
                        }
                    },
                    Err(e) => {
                        // Don't tear down the connection for malformed frames — log and continue.
                        warn!(peer = %peer, len = raw.len(), error = %e, "ws read: malformed frame, skipping");
                        continue;
                    }
                }
            } else {
                warn!(peer = %peer, "ws read: ignoring non-binary websocket frame");
            }
        }

        info!(peer = %peer, "ws read: connection closed");
        Ok::<(), anyhow::Error>(())
    });

    let write_handle = tokio::spawn(async move {
        let mut stream_map: StreamMap<Bytes, BroadcastStream<RouterMessage>> = StreamMap::new();

        loop {
            tokio::select! {
                biased;

                maybe_sub = sub_rx.recv() => {
                    match maybe_sub {
                        Some(sub) => {
                            debug!(peer = %peer, topic = %String::from_utf8_lossy(&sub.topic), "ws write_task: registered subscription");
                            stream_map.insert(sub.topic, BroadcastStream::new(sub.receiver));
                        }
                        None => {
                            info!(peer = %peer, "ws write_task: sub_rx closed, exiting");
                            break;
                        }
                    }
                }

                Some((topic, result)) = stream_map.next(), if !stream_map.is_empty() => {
                    match result {
                        Ok(msg) => {
                            let frame = encode_publish(&String::from_utf8_lossy(&topic), &msg.payload);
                            ws_write.send(Message::Binary(frame)).await.context("sending websocket publish")?;
                        }
                        Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                            warn!(peer = %peer, topic = %String::from_utf8_lossy(&topic), dropped = n, "ws write_task: subscriber lagged — messages dropped");
                        }
                    }
                }
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    let (r1, r2) = tokio::try_join!(read_handle, write_handle)?;
    r1?;
    r2?;

    info!(peer = %peer, "websocket connection closed");
    Ok(())
}

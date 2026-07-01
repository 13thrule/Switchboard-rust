use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt, stream::Stream};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_stream::{wrappers::{BroadcastStream, ReceiverStream}, StreamMap};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::{protocol::{encode_publish, Frame}, router::{Router, RouterMessage}};

const MAX_WS_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Unified subscription stream that handles both broadcast and queue modes
enum UnifiedStream {
    Broadcast(BroadcastStream<RouterMessage>),
    Queue(ReceiverStream<RouterMessage>),
}

impl Stream for UnifiedStream {
    type Item = RouterMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            UnifiedStream::Broadcast(stream) => {
                // Handle broadcast stream which returns Result<RouterMessage, Error>
                loop {
                    match Pin::new(&mut *stream).poll_next(cx) {
                        Poll::Ready(Some(Ok(msg))) => return Poll::Ready(Some(msg)),
                        Poll::Ready(Some(Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(_)))) => {
                            // Skip lagged messages and continue polling
                            continue;
                        }
                        Poll::Ready(None) => return Poll::Ready(None),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
            UnifiedStream::Queue(stream) => {
                // Queue stream returns Option<RouterMessage> directly
                Pin::new(&mut *stream).poll_next(cx)
            }
        }
    }
}

struct NewSubscription {
    topic:  Bytes,
    stream: UnifiedStream,
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
                            let stream = UnifiedStream::Broadcast(BroadcastStream::new(receiver));
                            if let Err(e) = sub_tx.send(NewSubscription { topic, stream }).await {
                                error!(peer = %peer, error = %e, "ws read: failed to send subscription");
                                break;
                            }
                        }
                        Frame::Publish { topic, payload } => {
                            router.publish(&topic, payload);
                        }
                        Frame::QueueSubscribe { topic, group } => {
                            let (receiver, _worker_id) = router.queue_subscribe(topic.clone(), group.clone());
                            let stream = UnifiedStream::Queue(ReceiverStream::new(receiver));
                            if let Err(e) = sub_tx.send(NewSubscription { topic, stream }).await {
                                error!(peer = %peer, error = %e, "ws read: failed to send queue subscription");
                                break;
                            }
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
        let mut stream_map: StreamMap<Bytes, UnifiedStream> = StreamMap::new();

        loop {
            tokio::select! {
                biased;

                maybe_sub = sub_rx.recv() => {
                    match maybe_sub {
                        Some(sub) => {
                            debug!(peer = %peer, topic = %String::from_utf8_lossy(&sub.topic), "ws write_task: registered subscription");
                            stream_map.insert(sub.topic, sub.stream);
                        }
                        None => {
                            info!(peer = %peer, "ws write_task: sub_rx closed, exiting");
                            break;
                        }
                    }
                }

                Some((topic, msg)) = stream_map.next(), if !stream_map.is_empty() => {
                    let frame = encode_publish(&String::from_utf8_lossy(&topic), &msg.payload);
                    ws_write.send(Message::Binary(frame)).await.context("sending websocket publish")?;
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

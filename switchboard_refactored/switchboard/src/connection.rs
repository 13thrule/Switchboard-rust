//! Per-connection async task.
//!
//! Driven reactively using StreamMap to eliminate high idle polling CPU burn.

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{broadcast, mpsc},
};
use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};
use tracing::{debug, error, info, warn};

use crate::{
    protocol::Frame,
    router::{Router, RouterMessage},
    state::ConnectionState,
};

const MAX_FRAME_BYTES: u32 = 16 * 1024 * 1024;

struct NewSubscription {
    topic:    Bytes,
    receiver: broadcast::Receiver<RouterMessage>,
}

pub struct Connection {
    stream: TcpStream,
    peer:   std::net::SocketAddr,
    router: Router,
}

impl Connection {
    pub fn new(stream: TcpStream, peer: std::net::SocketAddr, router: Router) -> Self {
        Connection { stream, peer, router }
    }

    pub async fn run(self) -> Result<()> {
        info!(peer = %self.peer, "connection accepted");

        let (read_half, write_half) = self.stream.into_split();
        let (sub_tx, sub_rx) = mpsc::channel::<NewSubscription>(64);

        let peer   = self.peer;
        let router = self.router;

        let read_h  = tokio::spawn(read_task(read_half, peer, router, sub_tx));
        let write_h = tokio::spawn(write_task(write_half, peer, sub_rx));

        let (r1, r2) = tokio::join!(read_h, write_h);
        r1.context("read_task panicked")??;
        r2.context("write_task panicked")??;

        info!(peer = %peer, "connection closed");
        Ok(())
    }
}

async fn read_task(
    mut stream: OwnedReadHalf,
    peer:       std::net::SocketAddr,
    router:     Router,
    sub_tx:     mpsc::Sender<NewSubscription>,
) -> Result<()> {
    let mut state    = ConnectionState::Handshake;
    let mut read_buf = BytesMut::with_capacity(4096);

    loop {
        match state {
            ConnectionState::Closed => {
                info!(peer = %peer, "read_task: closed, exiting");
                break;
            }
            ConnectionState::Handshake | ConnectionState::Ready => {}
        }

        match read_frame(&mut stream, &mut read_buf).await {
            Ok(Some(frame)) => {
                if matches!(state, ConnectionState::Handshake) {
                    state = state.on_first_frame();
                    info!(peer = %peer, "handshake complete → Ready");
                }
                handle_frame(frame, peer, &router, &sub_tx).await?;
            }
            Ok(None) => {
                info!(peer = %peer, "read_task: EOF");
                state = state.close();
            }
            Err(e) => {
                error!(peer = %peer, error = %e, "read_task: frame error");
                state = state.close();
            }
        }
    }

    Ok(())
}

async fn read_frame(
    stream:   &mut OwnedReadHalf,
    read_buf: &mut BytesMut,
) -> Result<Option<Frame>> {
    let mut header = [0u8; 4];
    match stream.read_exact(&mut header).await {
        Ok(_)  => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e).context("reading frame length prefix"),
    }

    let length = u32::from_be_bytes(header);
    if length == 0 {
        return Err(anyhow::anyhow!("zero-length frame rejected"));
    }
    if length > MAX_FRAME_BYTES {
        return Err(anyhow::anyhow!(
            "frame too large: {length} B (max {MAX_FRAME_BYTES} B)"
        ));
    }
    let length = length as usize;

    if read_buf.capacity() < length {
        read_buf.reserve(length - read_buf.capacity());
    }
    read_buf.resize(length, 0);

    stream
        .read_exact(&mut read_buf[..length])
        .await
        .context("reading frame body")?;

    let raw: Bytes = read_buf.split_to(length).freeze();
    Frame::parse(raw).map(Some).context("parsing frame")
}

async fn handle_frame(
    frame:  Frame,
    peer:   std::net::SocketAddr,
    router: &Router,
    sub_tx: &mpsc::Sender<NewSubscription>,
) -> Result<()> {
    match frame {
        Frame::Subscribe { topic } => {
            let receiver = router.subscribe(topic.clone());
            info!(
                peer  = %peer,
                topic = %String::from_utf8_lossy(&topic),
                "subscribed"
            );
            sub_tx
                .send(NewSubscription { topic, receiver })
                .await
                .context("sending subscription to write_task")?;
        }
        Frame::Publish { topic, payload } => {
            let result = router.publish(&topic, payload);
            debug!(
                peer        = %peer,
                topic       = %String::from_utf8_lossy(&topic),
                subscribers = result.subscribers,
                "published"
            );
        }
    }
    Ok(())
}

async fn write_task(
    mut stream: OwnedWriteHalf,
    peer:       std::net::SocketAddr,
    mut sub_rx: mpsc::Receiver<NewSubscription>,
) -> Result<()> {
    let mut stream_map: StreamMap<
        Bytes,
        BroadcastStream<RouterMessage>,
    > = StreamMap::new();

    loop {
        tokio::select! {
            biased;

            maybe_sub = sub_rx.recv() => {
                match maybe_sub {
                    Some(sub) => {
                        debug!(
                            peer  = %peer,
                            topic = %String::from_utf8_lossy(&sub.topic),
                            "write_task: registered subscription"
                        );
                        stream_map.insert(
                            sub.topic,
                            BroadcastStream::new(sub.receiver),
                        );
                    }
                    None => {
                        info!(peer = %peer, "write_task: sub_rx closed, exiting");
                        break;
                    }
                }
            }

            Some((topic, result)) = stream_map.next(), if !stream_map.is_empty() => {
                match result {
                    Ok(msg) => {
                        if let Err(e) = write_message(&mut stream, &topic, &msg.payload).await {
                            error!(peer = %peer, error = %e, "write_task: stream write error");
                            break;
                        }
                    }
                    Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                        warn!(
                            peer    = %peer,
                            topic   = %String::from_utf8_lossy(&topic),
                            dropped = n,
                            "write_task: subscriber lagged — messages dropped"
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

async fn write_message(
    stream:  &mut OwnedWriteHalf,
    topic:   &Bytes,
    payload: &Bytes,
) -> Result<()> {
    let topic_len   = topic.len();
    let payload_len = payload.len();
    let body_len = 1 + 2 + topic_len + payload_len;

    let mut hdr = BytesMut::with_capacity(7 + topic_len);
    hdr.put_u32(body_len as u32);
    hdr.put_u8(0x02);
    hdr.put_u16(topic_len as u16);
    hdr.put_slice(topic);

    stream.write_all(&hdr).await.context("write: header")?;
    stream.write_all(payload).await.context("write: payload")?;

    Ok(())
}

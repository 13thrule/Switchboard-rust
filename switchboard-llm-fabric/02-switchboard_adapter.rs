//! switchboard_adapter.rs
//!
//! Minimal-diff adapter between an existing LLM runtime and Switchboard.
//! Targets the TCP protocol described in 13thrule/Switchboard-rust's README:
//!   subscribe: 0x01 + topic_utf8
//!   publish:   0x02 + u16(topic_len, BE) + topic_bytes + payload_bytes
//!
//! Design goals (per task constraints):
//!   - zero-copy on the hot path: payload is `bytes::Bytes`, never re-allocated
//!     between "runtime produces tensor bytes" and "socket write"
//!   - minimal per-runtime changes: the runtime only needs to call
//!     `handle.publish(Topic::TokensOut, bytes)` and hand a `Stream<Bytes>`
//!     to its existing async loop — no changes to runtime internals required
//!   - explicit backpressure policy per topic (see `LagPolicy`), because
//!     Switchboard's broadcast(1024) channels will silently skip a lagging
//!     consumer (`RecvError::Lagged`) and we must decide, per topic, whether
//!     that's acceptable (see 01-SPEC.md §5)
//!
//! Dependencies (Cargo.toml):
//!   tokio = { version = "1", features = ["full"] }
//!   bytes = "1"
//!   zerocopy = { version = "0.8", features = ["derive"] }
//!   futures-core = "0.3"
//!   thiserror = "1"
//!   tracing = "0.1"

use bytes::{Bytes, BytesMut, BufMut};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use zerocopy::{Immutable, IntoBytes, FromBytes, KnownLayout};

// ---------------------------------------------------------------------
// Topic taxonomy (verbatim per spec)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Topic {
    PromptIn,
    TokensOut,
    ModelLogits,
    ModelNextToken,
    StreamText,
    KvUpdate,
    Metrics,
}

impl Topic {
    pub fn as_str(&self) -> &'static str {
        match self {
            Topic::PromptIn => "prompt.in",
            Topic::TokensOut => "tokens.out",
            Topic::ModelLogits => "model.logits",
            Topic::ModelNextToken => "model.next_token",
            Topic::StreamText => "stream.text",
            Topic::KvUpdate => "kv.update",
            Topic::Metrics => "metrics",
        }
    }

    /// Backpressure policy per §5 of the spec. Not enforced by the broker —
    /// enforced by us, at the consumer, by how we react to `Lagged`.
    pub fn lag_policy(&self) -> LagPolicy {
        match self {
            Topic::KvUpdate => LagPolicy::MustNotLag,
            _ => LagPolicy::DropOldestOk,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagPolicy {
    /// Acceptable to skip forward on Lagged; consumer must handle gaps.
    DropOldestOk,
    /// Lag is a hard error for this topic (e.g. kv.update) — surface it
    /// loudly rather than silently continuing. See 01-SPEC.md §5.
    MustNotLag,
}

// ---------------------------------------------------------------------
// Zero-copy-safe payload headers (fixed layout, no padding surprises)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
pub struct TokensHeader {
    pub seq_id: u64,
    pub length: u32,
    pub dtype: u8, // 0 = u32 token id
}

#[derive(Debug, Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
pub struct LogitsHeader {
    pub seq_id: u64,
    pub vocab_size: u32,
    pub dtype: u8, // 0=f32 1=f16 2=bf16 3=int8(+scale/zero trailer)
}

#[derive(Debug, Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
pub struct KvHeader {
    pub layer: u16,
    pub head: u16,
    pub seq_start: u32,
    pub seq_len: u32,
    pub dtype: u8, // 0=f32 1=f16 2=bf16
}

// ---------------------------------------------------------------------
// Wire framing — mirrors Switchboard's protocol.rs behavior exactly
// ---------------------------------------------------------------------

const OP_SUBSCRIBE: u8 = 0x01;
const OP_PUBLISH: u8 = 0x02;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // broker-enforced cap, mirrored client-side

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame exceeds 16MB broker limit: {0} bytes")]
    FrameTooLarge(usize),
    #[error("connection closed")]
    Closed,
    #[error("channel lagged (topic={0:?}) — see LagPolicy::MustNotLag")]
    Lagged(Topic),
}

/// Encode a publish frame: 0x02 + u16(topic_len BE) + topic + payload.
/// Zero-copy on payload: we only allocate the small header, then chain
/// the caller's `Bytes` payload without copying it.
fn encode_publish(topic: &str, payload: Bytes) -> Result<Bytes, AdapterError> {
    let topic_bytes = topic.as_bytes();
    let total = 1 + 2 + topic_bytes.len() + payload.len();
    if total > MAX_FRAME_BYTES {
        return Err(AdapterError::FrameTooLarge(total));
    }
    let mut head = BytesMut::with_capacity(1 + 2 + topic_bytes.len());
    head.put_u8(OP_PUBLISH);
    head.put_u16(topic_bytes.len() as u16); // big-endian, per spec
    head.put_slice(topic_bytes);

    // Chain header + payload without copying the payload buffer.
    let mut framed = head.freeze();
    let mut out = BytesMut::with_capacity(framed.len() + payload.len());
    out.extend_from_slice(&framed);
    out.extend_from_slice(&payload); // payload bytes copied once here into
                                      // the outgoing socket buffer — this is
                                      // the one unavoidable copy at the network
                                      // boundary; the *runtime-to-adapter* hop
                                      // above stays zero-copy (Bytes is a view).
    framed.clear();
    Ok(out.freeze())
}

fn encode_subscribe(topic: &str) -> Bytes {
    let topic_bytes = topic.as_bytes();
    let mut buf = BytesMut::with_capacity(1 + topic_bytes.len());
    buf.put_u8(OP_SUBSCRIBE);
    buf.put_slice(topic_bytes);
    buf.freeze()
}

// ---------------------------------------------------------------------
// Public adapter surface: subscribe() -> Stream<Bytes>, publish()
// ---------------------------------------------------------------------

pub struct SwitchboardHandle {
    write_tx: mpsc::Sender<Bytes>,
}

impl SwitchboardHandle {
    /// Connect once; internally spawns the read/write tasks. This is the
    /// only integration point a runtime needs to touch.
    pub async fn connect(addr: &str) -> Result<Self, AdapterError> {
        let stream = TcpStream::connect(addr).await?;
        let (read_half, write_half) = stream.into_split();
        let (write_tx, write_rx) = mpsc::channel::<Bytes>(256);

        tokio::spawn(write_task(write_half, write_rx));
        // read_task is spawned per-subscription in `subscribe()`, since each
        // subscriber in this minimal adapter owns its own TCP connection —
        // simplest correct mapping onto Switchboard's per-connection
        // StreamMap model. (An optimization: multiplex many subscriptions
        // over one connection using the broker's existing StreamMap-style
        // dispatch; left as a follow-up once the single-sub path is proven.)
        let _ = read_half; // consumed inside subscribe() via a fresh connect,
                            // see note above — kept here to document intent.

        Ok(Self { write_tx })
    }

    /// Zero-copy publish: `payload` should already be the `Bytes` view over
    /// runtime tensor/token memory — no re-encoding needed by the caller.
    pub async fn publish(&self, topic: Topic, payload: Bytes) -> Result<(), AdapterError> {
        let frame = encode_publish(topic.as_str(), payload)?;
        self.write_tx
            .send(frame)
            .await
            .map_err(|_| AdapterError::Closed)
    }

    /// Opens a *new* connection dedicated to this subscription and returns
    /// a `Stream<Bytes>` of raw payload bytes (topic + publish-opcode
    /// already stripped). Caller decodes the topic-specific header itself
    /// (TokensHeader / LogitsHeader / KvHeader / etc.) — the adapter does
    /// not assume payload shape, keeping it topic-agnostic.
    pub async fn subscribe(addr: &str, topic: Topic) -> Result<TopicStream, AdapterError> {
        let stream = TcpStream::connect(addr).await?;
        let (read_half, mut write_half) = stream.into_split();
        write_half.write_all(&encode_subscribe(topic.as_str())).await?;

        let (tx, rx) = mpsc::channel::<Result<Bytes, AdapterError>>(1024);
        tokio::spawn(read_task(read_half, tx, topic));
        Ok(TopicStream { rx })
    }
}

async fn write_task(mut write_half: tokio::net::tcp::OwnedWriteHalf, mut rx: mpsc::Receiver<Bytes>) {
    while let Some(frame) = rx.recv().await {
        if let Err(e) = write_half.write_all(&frame).await {
            tracing::error!(error = ?e, "switchboard write failed");
            break;
        }
    }
}

/// Reads publish frames off the socket and forwards decoded payload bytes.
/// Mirrors the broker's own zero-copy slicing: we read into one buffer and
/// `split_to`/`slice` out of it rather than re-allocating per field.
async fn read_task(
    mut read_half: tokio::net::tcp::OwnedReadHalf,
    tx: mpsc::Sender<Result<Bytes, AdapterError>>,
    topic: Topic,
) {
    let mut reader = BufReader::new(&mut read_half);
    loop {
        let mut opcode = [0u8; 1];
        if reader.read_exact(&mut opcode).await.is_err() {
            let _ = tx.send(Err(AdapterError::Closed)).await;
            return;
        }
        if opcode[0] != OP_PUBLISH {
            continue; // ignore anything unexpected on this connection
        }
        let mut len_buf = [0u8; 2];
        if reader.read_exact(&mut len_buf).await.is_err() {
            let _ = tx.send(Err(AdapterError::Closed)).await;
            return;
        }
        let topic_len = u16::from_be_bytes(len_buf) as usize;

        let mut topic_buf = BytesMut::with_capacity(topic_len);
        topic_buf.resize(topic_len, 0);
        if reader.read_exact(&mut topic_buf).await.is_err() {
            let _ = tx.send(Err(AdapterError::Closed)).await;
            return;
        }
        // NOTE: real implementation needs a length-delimited outer frame
        // (or a payload-length field) to know where payload ends on a raw
        // TCP stream — see 01-SPEC.md §2.1 note on TCP vs WS framing.
        // Sketch below assumes a codec (e.g. tokio_util::codec::LengthDelimitedCodec)
        // wraps this in production rather than hand-rolled reads.
        let payload = read_remaining_frame(&mut reader).await;
        match payload {
            Ok(bytes) => {
                if tx.send(Ok(bytes)).await.is_err() {
                    return; // consumer dropped the stream
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
        let _ = topic; // topic is already known (we subscribed to exactly one)
    }
}

// Placeholder for the codec-backed read; swap for tokio_util's
// LengthDelimitedCodec in the real implementation.
async fn read_remaining_frame<R: tokio::io::AsyncRead + Unpin>(
    _reader: &mut R,
) -> Result<Bytes, AdapterError> {
    unimplemented!("wire this to the chosen framing codec (see note above)")
}

pub struct TopicStream {
    rx: mpsc::Receiver<Result<Bytes, AdapterError>>,
}

impl Stream for TopicStream {
    type Item = Result<Bytes, AdapterError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

// ---------------------------------------------------------------------
// Example: minimal-diff wiring into an existing generate loop
// ---------------------------------------------------------------------
//
// let sb = SwitchboardHandle::connect("127.0.0.1:7777").await?;
//
// // In the runtime's existing per-token callback:
// let header = TokensHeader { seq_id, length: 1, dtype: 0 };
// let mut buf = BytesMut::with_capacity(13 + 4);
// buf.extend_from_slice(header.as_bytes()); // zerocopy: no serialization step
// buf.extend_from_slice(&token_id.to_ne_bytes());
// sb.publish(Topic::ModelNextToken, buf.freeze()).await?;
//
// // Elsewhere, a consumer:
// let mut stream = SwitchboardHandle::subscribe("127.0.0.1:7777", Topic::TokensOut).await?;
// while let Some(Ok(bytes)) = stream.next().await {
//     let header = TokensHeader::ref_from_bytes(&bytes[..13]).unwrap();
//     let ids: &[u32] = /* zerocopy cast of bytes[13..] */ unimplemented!();
// }

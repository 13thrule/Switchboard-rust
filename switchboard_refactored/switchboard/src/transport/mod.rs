/// Transport abstraction layer for Switchboard
/// 
/// Supports multiple transport backends (TCP, WebSocket, Shared Memory)
/// while maintaining zero-copy semantics and lock-free properties.

pub mod shm;

use std::pin::Pin;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::Stream;

/// Result type for transport operations
pub type TransportResult<T> = Result<T, TransportError>;

/// Transport errors
#[derive(Debug, Clone)]
pub enum TransportError {
    /// Shared memory ring buffer is full
    RingFull,
    /// Transport not available or initialized
    NotAvailable,
    /// IO error
    IoError(String),
    /// Topic not found in transport registry
    TopicNotFound,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::RingFull => write!(f, "shared memory ring is full"),
            TransportError::NotAvailable => write!(f, "transport not available"),
            TransportError::IoError(e) => write!(f, "transport IO error: {}", e),
            TransportError::TopicNotFound => write!(f, "topic not found in transport"),
        }
    }
}

impl std::error::Error for TransportError {}

/// Generic subscription stream that bridges all transport types
pub trait SubscriptionStream: Stream<Item = Bytes> + Send + Unpin {}

impl<S: Stream<Item = Bytes> + Send + Unpin> SubscriptionStream for S {}

/// Transport trait for message passing backends
#[async_trait]
pub trait Transport: Send + Sync {
    /// Subscribe to a topic, returning a stream of messages
    async fn subscribe(&self, topic: Bytes) -> TransportResult<Pin<Box<dyn SubscriptionStream>>>;

    /// Publish a message to a topic
    async fn publish(&self, topic: Bytes, payload: Bytes) -> TransportResult<()>;

    /// Check if transport supports this topic
    fn has_topic(&self, topic: &Bytes) -> bool;
}

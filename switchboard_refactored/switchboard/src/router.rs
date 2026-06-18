//! Lock-free topic registry and zero-copy broadcast routing.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::state::MessageState;

const CHANNEL_CAPACITY: usize = 1024;

struct TopicEntry {
    sender: broadcast::Sender<RouterMessage>,
}

#[derive(Debug, Clone)]
pub struct RouterMessage {
    pub payload: Bytes,
    pub state: MessageState,
}

#[derive(Clone)]
pub struct Router {
    topics: Arc<SkipMap<Bytes, TopicEntry>>,
    create_lock: Arc<Mutex<()>>,
}

impl Router {
    pub fn new() -> Self {
        Router {
            topics:      Arc::new(SkipMap::new()),
            create_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn subscribe(&self, topic: Bytes) -> broadcast::Receiver<RouterMessage> {
        // Fast path: lock-free get
        if let Some(entry) = self.topics.get(&topic) {
            debug!(topic = %topic_display(&topic), "subscriber joined existing topic");
            return entry.value().sender.subscribe();
        }

        // Slow path: structural creation guarded by a Mutex to avoid TOCTOU races
        let _guard = self.create_lock.lock().unwrap();

        // Re-check under the lock boundary
        if let Some(entry) = self.topics.get(&topic) {
            debug!(topic = %topic_display(&topic), "subscriber joined topic (lost create race)");
            return entry.value().sender.subscribe();
        }

        let (sender, _discard) = broadcast::channel(CHANNEL_CAPACITY);
        
        // Subscribe BEFORE inserting to guarantee no dropped messages during parallel publishes
        let receiver = sender.subscribe();

        self.topics.insert(topic.clone(), TopicEntry { sender });

        debug!(topic = %topic_display(&topic), "subscriber created new topic");
        receiver
    }

    pub fn publish(&self, topic: &Bytes, payload: Bytes) -> PublishResult {
        let entry = match self.topics.get(topic) {
            Some(e) => e,
            None => {
                debug!(topic = %topic_display(topic), "publish: no subscribers, dropped");
                return PublishResult { state: MessageState::Routed, subscribers: 0 };
            }
        };

        let msg = RouterMessage { payload, state: MessageState::Routed };

        match entry.value().sender.send(msg) {
            Ok(n) => {
                debug!(topic = %topic_display(topic), subscribers = n, "published");
                PublishResult { state: MessageState::Delivered, subscribers: n }
            }
            Err(_) => {
                warn!(topic = %topic_display(topic), "all subscribers dropped before publish landed");
                PublishResult { state: MessageState::Routed, subscribers: 0 }
            }
        }
    }

    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    pub fn subscriber_count(&self, topic: &Bytes) -> usize {
        self.topics
            .get(topic)
            .map(|e| e.value().sender.receiver_count())
            .unwrap_or(0)
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

pub struct PublishResult {
    pub state:       MessageState,
    pub subscribers: usize,
}

#[inline]
fn topic_display(topic: &Bytes) -> impl std::fmt::Display + '_ {
    String::from_utf8_lossy(topic.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn t(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

    #[tokio::test]
    async fn subscribe_then_publish() {
        let router = Router::new();
        let topic  = t("events");

        let mut rx = router.subscribe(topic.clone());
        let result = router.publish(&topic, Bytes::from_static(b"hello"));

        assert_eq!(result.subscribers, 1);
        assert_eq!(result.state, MessageState::Delivered);

        let msg = rx.recv().await.unwrap();
        assert_eq!(&msg.payload[..], b"hello");
    }

    #[tokio::test]
    async fn multiple_subscribers_zero_copy() {
        let router = Router::new();
        let topic  = t("multi");

        let mut rx1 = router.subscribe(topic.clone());
        let mut rx2 = router.subscribe(topic.clone());

        router.publish(&topic, Bytes::from_static(b"broadcast"));

        let m1 = rx1.recv().await.unwrap();
        let m2 = rx2.recv().await.unwrap();

        assert_eq!(m1.payload.as_ptr(), m2.payload.as_ptr());
        assert_eq!(&m1.payload[..], b"broadcast");
    }

    #[tokio::test]
    async fn publish_no_subscribers_is_ok() {
        let router = Router::new();
        let result = router.publish(&t("ghost"), Bytes::from_static(b"void"));
        assert_eq!(result.subscribers, 0);
    }

    #[tokio::test]
    async fn topic_count_deduplicates() {
        let router = Router::new();
        router.subscribe(t("a"));
        router.subscribe(t("b"));
        router.subscribe(t("a"));
        assert_eq!(router.topic_count(), 2);
    }

    #[tokio::test]
    async fn binary_topic_does_not_panic() {
        let router      = Router::new();
        let binary_topic = Bytes::from_static(&[0xFF, 0xFE, 0x00, 0x80]);

        let mut rx = router.subscribe(binary_topic.clone());
        let result = router.publish(&binary_topic, Bytes::from_static(b"payload"));

        assert_eq!(result.subscribers, 1);
        let msg = rx.recv().await.unwrap();
        assert_eq!(&msg.payload[..], b"payload");
    }

    #[tokio::test]
    async fn concurrent_subscribe_no_orphan() {
        use std::sync::Barrier;
        use std::sync::Arc as StdArc;

        let router  = Router::new();
        let topic   = t("race");
        let barrier = StdArc::new(Barrier::new(2));

        let r1 = router.clone();
        let t1 = topic.clone();
        let b1 = barrier.clone();

        let r2 = router.clone();
        let t2 = topic.clone();
        let b2 = barrier.clone();

        let h1 = std::thread::spawn(move || { b1.wait(); r1.subscribe(t1) });
        let h2 = std::thread::spawn(move || { b2.wait(); r2.subscribe(t2) });

        let mut rx1 = h1.join().unwrap();
        let mut rx2 = h2.join().unwrap();

        assert_eq!(router.topic_count(), 1);

        router.publish(&topic, Bytes::from_static(b"race-msg"));

        let m1 = rx1.recv().await.unwrap();
        let m2 = rx2.recv().await.unwrap();

        assert_eq!(m1.payload.as_ptr(), m2.payload.as_ptr());
    }
}

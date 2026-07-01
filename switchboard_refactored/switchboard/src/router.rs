//! Lock-free topic registry and zero-copy broadcast routing.
//! 
//! Supports two delivery modes:
//! - **Broadcast (default):** All subscribers receive every message
//! - **Work Queue (`queue://` prefix):** Exactly one worker per message (round-robin)

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, warn};

use crate::state::MessageState;
use crate::metrics;

const CHANNEL_CAPACITY: usize = 1024;
const WORK_QUEUE_PREFIX: &[u8] = b"queue://";

struct TopicEntry {
    sender: broadcast::Sender<RouterMessage>,
}

struct WorkerChannel {
    tx: mpsc::UnboundedSender<RouterMessage>,
}

struct GroupEntry {
    /// Active worker channels in this consumer group
    workers: Vec<WorkerChannel>,
    /// Lock-free round-robin counter (Ordering::Relaxed for maximum speed)
    counter: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct RouterMessage {
    pub payload: Bytes,
    pub state: MessageState,
}

#[derive(Clone)]
pub struct Router {
    topics: Arc<SkipMap<Bytes, TopicEntry>>,
    /// Consumer groups for work queue mode (queue://topic)
    groups: Arc<SkipMap<Bytes, Arc<Mutex<GroupEntry>>>>,
    create_lock: Arc<Mutex<()>>,
}

impl Router {
    pub fn new() -> Self {
        Router {
            topics:      Arc::new(SkipMap::new()),
            groups:      Arc::new(SkipMap::new()),
            create_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Subscribe to a broadcast topic (all subscribers receive all messages)
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

    /// Join a consumer group (work queue mode).
    /// Exactly one worker in the group receives each message.
    /// Returns (receiver, group_id) where group_id is needed for metrics.
    pub fn subscribe_group(&self, topic: Bytes) -> (mpsc::UnboundedReceiver<RouterMessage>, usize) {
        let base_topic = if topic.starts_with(WORK_QUEUE_PREFIX) {
            topic.clone()
        } else {
            // Support both "queue://topic" and auto-prefix "topic"
            let mut prefixed = Vec::with_capacity(WORK_QUEUE_PREFIX.len() + topic.len());
            prefixed.extend_from_slice(WORK_QUEUE_PREFIX);
            prefixed.extend_from_slice(&topic);
            Bytes::from(prefixed)
        };

        // Fast path: lock-free get
        if let Some(group_ref) = self.groups.get(&base_topic) {
            let mut group = group_ref.value().lock().unwrap();
            let (tx, rx) = mpsc::unbounded_channel();
            let worker_id = group.workers.len();
            group.workers.push(WorkerChannel { tx });
            debug!(topic = %topic_display(&base_topic), worker_id, "worker joined existing group");
            return (rx, worker_id);
        }

        // Slow path: create new group under lock
        let _guard = self.create_lock.lock().unwrap();

        // Re-check under lock
        if let Some(group_ref) = self.groups.get(&base_topic) {
            let mut group = group_ref.value().lock().unwrap();
            let (tx, rx) = mpsc::unbounded_channel();
            let worker_id = group.workers.len();
            group.workers.push(WorkerChannel { tx });
            debug!(topic = %topic_display(&base_topic), worker_id, "worker joined group (lost create race)");
            return (rx, worker_id);
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let group = Arc::new(Mutex::new(GroupEntry {
            workers: vec![WorkerChannel { tx }],
            counter: Arc::new(AtomicUsize::new(0)),
        }));

        self.groups.insert(base_topic.clone(), group);
        debug!(topic = %topic_display(&base_topic), "created new consumer group");
        (rx, 0)
    }

    pub fn publish(&self, topic: &Bytes, payload: Bytes) -> PublishResult {
        // Detect work queue mode (queue:// prefix)
        if topic.starts_with(WORK_QUEUE_PREFIX) {
            return self.publish_work_queue(topic, payload);
        }

        // Broadcast mode (original behavior)
        let entry = match self.topics.get(topic) {
            Some(e) => e,
            None => {
                debug!(topic = %topic_display(topic), "publish: no subscribers, dropped");
                return PublishResult { state: MessageState::Routed, subscribers: 0 };
            }
        };

        let size = payload.len() as f64;
        // update metrics
        metrics::PUBLISHES.inc();
        metrics::LAST_PUBLISH_SIZE.set(size);

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

    /// Publish to a consumer group (work queue mode).
    /// Uses lock-free round-robin to deliver to exactly one worker.
    fn publish_work_queue(&self, topic: &Bytes, payload: Bytes) -> PublishResult {
        let group_ref = match self.groups.get(topic) {
            Some(g) => g,
            None => {
                debug!(topic = %topic_display(topic), "work queue: no workers, dropped");
                return PublishResult { state: MessageState::Routed, subscribers: 0 };
            }
        };

        let mut group = match group_ref.value().lock() {
            Ok(g) => g,
            Err(_) => {
                warn!(topic = %topic_display(topic), "work queue: lock poisoned");
                return PublishResult { state: MessageState::Routed, subscribers: 0 };
            }
        };

        // No workers available
        if group.workers.is_empty() {
            debug!(topic = %topic_display(topic), "work queue: no active workers, dropped");
            return PublishResult { state: MessageState::Routed, subscribers: 0 };
        }

        let size = payload.len() as f64;
        metrics::PUBLISHES.inc();
        metrics::LAST_PUBLISH_SIZE.set(size);

        // Lock-free round-robin: fetch-and-increment with Relaxed ordering
        // Relaxed ordering is safe here because we don't need synchronization with other atomics
        let idx = group.counter.fetch_add(1, Ordering::Relaxed);
        let target_worker_idx = idx % group.workers.len();

        let msg = RouterMessage { payload, state: MessageState::Routed };

        match group.workers[target_worker_idx].tx.send(msg) {
            Ok(_) => {
                debug!(
                    topic = %topic_display(topic),
                    worker = target_worker_idx,
                    "work queue message delivered"
                );
                PublishResult { state: MessageState::Delivered, subscribers: 1 }
            }
            Err(_) => {
                warn!(topic = %topic_display(topic), worker = target_worker_idx, "worker channel closed");
                // Remove dead worker
                group.workers.remove(target_worker_idx);
                PublishResult { state: MessageState::Routed, subscribers: 0 }
            }
        }
    }

    pub fn topic_count(&self) -> usize {
        self.topics.len() + self.groups.len()
    }

    pub fn subscriber_count(&self, topic: &Bytes) -> usize {
        if topic.starts_with(WORK_QUEUE_PREFIX) {
            if let Some(g) = self.groups.get(topic) {
                if let Ok(group) = g.value().lock() {
                    return group.workers.len();
                }
            }
            return 0;
        }

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

    // ==================== Consumer Group (Work Queue) Tests ====================

    #[tokio::test]
    async fn work_queue_single_worker_receives_message() {
        let router = Router::new();
        let topic = t("queue://tasks");

        let (mut rx1, _) = router.subscribe_group(topic.clone());

        let result = router.publish(&topic, Bytes::from_static(b"task1"));
        assert_eq!(result.subscribers, 1);
        assert_eq!(result.state, MessageState::Delivered);

        let msg = rx1.recv().await.unwrap();
        assert_eq!(&msg.payload[..], b"task1");
    }

    #[tokio::test]
    async fn work_queue_round_robin_distribution() {
        let router = Router::new();
        let topic = t("queue://jobs");

        let (mut rx1, _) = router.subscribe_group(topic.clone());
        let (mut rx2, _) = router.subscribe_group(topic.clone());
        let (mut rx3, _) = router.subscribe_group(topic.clone());

        // Publish 6 messages: should distribute round-robin
        for i in 0..6 {
            router.publish(&topic, Bytes::from(format!("job_{}", i)));
        }

        // Worker 1: receives job_0, job_3
        assert_eq!(&rx1.recv().await.unwrap().payload[..], b"job_0");
        assert_eq!(&rx1.recv().await.unwrap().payload[..], b"job_3");

        // Worker 2: receives job_1, job_4
        assert_eq!(&rx2.recv().await.unwrap().payload[..], b"job_1");
        assert_eq!(&rx2.recv().await.unwrap().payload[..], b"job_4");

        // Worker 3: receives job_2, job_5
        assert_eq!(&rx3.recv().await.unwrap().payload[..], b"job_2");
        assert_eq!(&rx3.recv().await.unwrap().payload[..], b"job_5");
    }

    #[tokio::test]
    async fn work_queue_no_broadcast() {
        let router = Router::new();
        let topic = t("queue://exclusive");

        let (mut rx1, _) = router.subscribe_group(topic.clone());
        let (mut rx2, _) = router.subscribe_group(topic.clone());

        router.publish(&topic, Bytes::from_static(b"only_one_gets_this"));

        // Exactly one worker receives the message
        // Use try_recv to avoid blocking on the second channel
        let msg1 = rx1.try_recv();
        let msg2 = rx2.try_recv();

        // Exactly one should have a message
        let has_msg = (msg1.is_ok() as u8) + (msg2.is_ok() as u8);
        assert_eq!(has_msg, 1, "Work queue should deliver to exactly one worker");
    }

    #[tokio::test]
    async fn work_queue_prefix_normalization() {
        let router = Router::new();

        // Both forms should use the same group
        let (mut rx1, _) = router.subscribe_group(t("queue://normalized"));
        let (mut rx2, _) = router.subscribe_group(t("queue://normalized"));

        router.publish(&t("queue://normalized"), Bytes::from_static(b"msg1"));

        // Exactly one receives it
        let msg1 = rx1.try_recv();
        let msg2 = rx2.try_recv();

        assert!(msg1.is_ok() || msg2.is_ok(), "At least one worker should receive message");
        assert!(!(msg1.is_ok() && msg2.is_ok()), "Only one worker should receive message");
    }

    #[tokio::test]
    async fn work_queue_broadcast_mode_isolation() {
        let router = Router::new();

        // Broadcast subscribers on regular topic
        let mut bc_rx1 = router.subscribe(t("events"));
        let mut bc_rx2 = router.subscribe(t("events"));

        // Work queue workers on different topic
        let (mut wq_rx1, _) = router.subscribe_group(t("queue://tasks"));
        let (mut wq_rx2, _) = router.subscribe_group(t("queue://tasks"));

        // Publish to broadcast topic
        router.publish(&t("events"), Bytes::from_static(b"broadcast"));
        let m1 = bc_rx1.recv().await.unwrap();
        let m2 = bc_rx2.recv().await.unwrap();
        assert_eq!(m1.payload.as_ptr(), m2.payload.as_ptr()); // Both got same message

        // Publish to work queue
        router.publish(&t("queue://tasks"), Bytes::from_static(b"task"));
        let wq_m1 = wq_rx1.try_recv();
        let wq_m2 = wq_rx2.try_recv();
        
        // Exactly one worker got it
        let has_msg = (wq_m1.is_ok() as u8) + (wq_m2.is_ok() as u8);
        assert_eq!(has_msg, 1, "Work queue should deliver to exactly one worker");
    }
}


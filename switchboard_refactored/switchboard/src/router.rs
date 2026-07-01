//! Lock-free topic registry and zero-copy broadcast routing.
//! 
//! Supports two delivery modes:
//! - **Broadcast (default):** All subscribers receive every message
//! - **Consumer Groups (queue_subscribe):** Exactly one worker per message (lock-free round-robin)
//!
//! Consumer groups use a lock-free architecture with SkipMap for worker tracking and
//! AtomicUsize for round-robin distribution, enabling competing consumers patterns for
//! distributed task processing without data copying.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use tokio::sync::{broadcast, mpsc};
use tracing::debug;

use crate::state::MessageState;
use crate::metrics;

const CHANNEL_CAPACITY: usize = 1024;

/// Represents a single worker connected to a consumer group.
/// Uses mpsc for efficient single-sender, single-receiver channels.
struct Worker {
    pub id: usize,
    pub tx: mpsc::Sender<RouterMessage>,
}

/// A consumer group for competing consumers pattern (work queues).
/// Multiple workers subscribe to the same topic and receive messages round-robin.
pub struct ConsumerGroup {
    /// Lock-free collection of workers assigned to this specific group
    /// Key: worker ID, Value: mpsc sender
    pub workers: SkipMap<usize, mpsc::Sender<RouterMessage>>,
    /// Atomic counter for distributing the next worker ID
    pub next_worker_id: AtomicUsize,
    /// The hot-path atomic steering wheel for lock-free round-robin routing
    pub rr_counter: AtomicUsize,
}

impl ConsumerGroup {
    pub fn new() -> Self {
        Self {
            workers: SkipMap::new(),
            next_worker_id: AtomicUsize::new(0),
            rr_counter: AtomicUsize::new(0),
        }
    }

    /// Dispatches a zero-copy message to exactly ONE worker using lock-free round-robin routing.
    /// Returns true if message was successfully sent, false on backpressure or no workers available.
    pub fn dispatch(&self, msg: RouterMessage) -> bool {
        // Collect a quick atomic snapshot of active worker channels
        let active_workers: Vec<_> = self.workers
            .iter()
            .map(|e| e.value().clone())
            .collect();
        
        if active_workers.is_empty() {
            return false; // No workers currently online in this group
        }

        // Lock-free steering: increment the counter atomically (Relaxed is safe here)
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let target_worker = &active_workers[idx % active_workers.len()];
        
        // Push the shared reference into the worker's channel buffer
        match target_worker.try_send(msg) {
            Ok(_) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Backpressure boundary hit for this specific worker.
                // Could implement fallback to next worker here if needed.
                debug!("worker backpressure hit");
                false
            }
            Err(_) => false,
        }
    }
}

impl Default for ConsumerGroup {
    fn default() -> Self {
        Self::new()
    }
}

/// A topic entry holds both the broadcast channel for pub/sub mode
/// and a map of consumer groups for work queue mode.
struct TopicEntry {
    /// Classic Pub/Sub channel (broadcasts to all independent listeners)
    pub broadcast_sender: broadcast::Sender<RouterMessage>,
    /// Competing Consumers map: group_name -> ConsumerGroup
    pub groups: SkipMap<Bytes, ConsumerGroup>,
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
            topics: Arc::new(SkipMap::new()),
            create_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Subscribe to a broadcast topic (all subscribers receive all messages)
    pub fn subscribe(&self, topic: Bytes) -> broadcast::Receiver<RouterMessage> {
        // Fast path: lock-free get
        if let Some(entry) = self.topics.get(&topic) {
            debug!(topic = %topic_display(&topic), "subscriber joined existing topic");
            return entry.value().broadcast_sender.subscribe();
        }

        // Slow path: structural creation guarded by a Mutex to avoid TOCTOU races
        let _guard = self.create_lock.lock().unwrap();

        // Re-check under the lock boundary
        if let Some(entry) = self.topics.get(&topic) {
            debug!(topic = %topic_display(&topic), "subscriber joined topic (lost create race)");
            return entry.value().broadcast_sender.subscribe();
        }

        let (sender, _discard) = broadcast::channel(CHANNEL_CAPACITY);
        
        // Subscribe BEFORE inserting to guarantee no dropped messages during parallel publishes
        let receiver = sender.subscribe();

        let entry = TopicEntry {
            broadcast_sender: sender,
            groups: SkipMap::new(),
        };

        self.topics.insert(topic.clone(), entry);

        debug!(topic = %topic_display(&topic), "created new topic");
        receiver
    }

    /// Join a consumer group (work queue mode).
    /// Exactly one worker in the group receives each message.
    /// Returns (receiver, worker_id) where worker_id is assigned atomically.
    pub fn queue_subscribe(
        &self,
        topic: Bytes,
        group: Bytes,
    ) -> (mpsc::Receiver<RouterMessage>, usize) {
        // Ensure topic exists
        self.ensure_topic_exists(topic.clone());

        if let Some(entry) = self.topics.get(&topic) {
            let topic_entry = entry.value();
            
            // Fast path: group exists, add worker
            if let Some(group_entry) = topic_entry.groups.get(&group) {
                let group_ref = group_entry.value();
                let worker_id = group_ref.next_worker_id.fetch_add(1, Ordering::SeqCst);
                let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                group_ref.workers.insert(worker_id, tx);
                debug!(
                    topic = %topic_display(&topic),
                    group = %topic_display(&group),
                    worker_id,
                    "worker joined existing consumer group"
                );
                return (rx, worker_id);
            }
        }

        // Slow path: create new group
        let _guard = self.create_lock.lock().unwrap();

        if let Some(entry) = self.topics.get(&topic) {
            let topic_entry = entry.value();
            
            // Re-check under lock
            if let Some(group_entry) = topic_entry.groups.get(&group) {
                let group_ref = group_entry.value();
                let worker_id = group_ref.next_worker_id.fetch_add(1, Ordering::SeqCst);
                let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                group_ref.workers.insert(worker_id, tx);
                debug!(
                    topic = %topic_display(&topic),
                    group = %topic_display(&group),
                    worker_id,
                    "worker joined consumer group (lost create race)"
                );
                return (rx, worker_id);
            }

            // Create new group
            let new_group = ConsumerGroup::new();
            let worker_id = new_group.next_worker_id.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            new_group.workers.insert(worker_id, tx);
            
            topic_entry.groups.insert(group.clone(), new_group);
            
            debug!(
                topic = %topic_display(&topic),
                group = %topic_display(&group),
                "created new consumer group with first worker"
            );
            
            (rx, worker_id)
        } else {
            // This shouldn't happen, but handle gracefully
            panic!("Topic should have been created in ensure_topic_exists");
        }
    }

    /// Ensure a topic exists in the registry (creates if needed).
    fn ensure_topic_exists(&self, topic: Bytes) {
        if self.topics.get(&topic).is_some() {
            return;
        }

        let _guard = self.create_lock.lock().unwrap();
        if self.topics.get(&topic).is_some() {
            return;
        }

        let (sender, _discard) = broadcast::channel(CHANNEL_CAPACITY);
        let entry = TopicEntry {
            broadcast_sender: sender,
            groups: SkipMap::new(),
        };
        self.topics.insert(topic, entry);
    }

    /// Publish a message to a topic.
    /// Messages are delivered to:
    /// 1. All broadcast subscribers (via broadcast channel)
    /// 2. Exactly one worker in each consumer group (via lock-free round-robin)
    pub fn publish(&self, topic: &Bytes, payload: Bytes) -> PublishResult {
        if let Some(entry) = self.topics.get(topic) {
            let topic_entry = entry.value();
            
            let msg = RouterMessage {
                payload, // Shared Bytes reference
                state: MessageState::Routed,
            };

            // 1. Deliver to traditional Pub/Sub listeners (broadcast mode)
            let broadcast_result = topic_entry.broadcast_sender.send(msg.clone());
            let broadcast_subscribers = match &broadcast_result {
                Ok(n) => *n,
                Err(_) => 0,
            };

            // Update metrics
            metrics::PUBLISHES.inc();
            let size = msg.payload.len() as f64;
            metrics::LAST_PUBLISH_SIZE.set(size);

            // 2. Deliver to Consumer Groups (competing consumers)
            // Each group gets exactly one copy of the message via lock-free round-robin
            for group_entry in topic_entry.groups.iter() {
                let group = group_entry.value();
                // msg.clone() only increments the atomic reference count of the underlying Bytes
                let _ = group.dispatch(msg.clone());
            }

            // Consider delivery successful if at least one subscriber received it
            let state = if broadcast_subscribers > 0 {
                MessageState::Delivered
            } else {
                MessageState::Routed
            };

            PublishResult {
                state,
                subscribers: broadcast_subscribers,
            }
        } else {
            debug!(topic = %topic_display(topic), "publish: topic not found, dropped");
            PublishResult {
                state: MessageState::Routed,
                subscribers: 0,
            }
        }
    }

    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    pub fn subscriber_count(&self, topic: &Bytes) -> usize {
        if let Some(entry) = self.topics.get(topic) {
            entry.value().broadcast_sender.receiver_count()
        } else {
            0
        }
    }

    pub fn group_count(&self, topic: &Bytes) -> usize {
        if let Some(entry) = self.topics.get(topic) {
            entry.value().groups.len()
        } else {
            0
        }
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

    // ==================== Consumer Group (Queue Subscribe) Tests ====================

    #[tokio::test]
    async fn queue_subscribe_single_worker_receives_message() {
        let router = Router::new();
        let topic = t("tasks");
        let group = t("workers");

        let (mut rx1, worker_id) = router.queue_subscribe(topic.clone(), group.clone());
        assert_eq!(worker_id, 0);

        let result = router.publish(&topic, Bytes::from_static(b"task1"));
        assert_eq!(result.subscribers, 0); // No broadcast subscribers

        let msg = rx1.recv().await.unwrap();
        assert_eq!(&msg.payload[..], b"task1");
    }

    #[tokio::test]
    async fn queue_subscribe_round_robin_distribution() {
        let router = Router::new();
        let topic = t("images");
        let group = t("gpu_cluster");

        let (mut rx1, id1) = router.queue_subscribe(topic.clone(), group.clone());
        let (mut rx2, id2) = router.queue_subscribe(topic.clone(), group.clone());
        let (mut rx3, id3) = router.queue_subscribe(topic.clone(), group.clone());

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);

        // Publish 6 messages: should distribute round-robin
        for i in 0..6 {
            router.publish(&topic, Bytes::from(format!("img_{}", i)));
        }

        // Worker 0: receives img_0, img_3
        assert_eq!(&rx1.recv().await.unwrap().payload[..], b"img_0");
        assert_eq!(&rx1.recv().await.unwrap().payload[..], b"img_3");

        // Worker 1: receives img_1, img_4
        assert_eq!(&rx2.recv().await.unwrap().payload[..], b"img_1");
        assert_eq!(&rx2.recv().await.unwrap().payload[..], b"img_4");

        // Worker 2: receives img_2, img_5
        assert_eq!(&rx3.recv().await.unwrap().payload[..], b"img_2");
        assert_eq!(&rx3.recv().await.unwrap().payload[..], b"img_5");
    }

    #[tokio::test]
    async fn queue_subscribe_multiple_groups_isolated() {
        let router = Router::new();
        let topic = t("orders");

        // Group A: 2 workers
        let (mut rx_a1, _) = router.queue_subscribe(topic.clone(), t("group_a"));
        let (mut rx_a2, _) = router.queue_subscribe(topic.clone(), t("group_a"));

        // Group B: 2 workers  
        let (mut rx_b1, _) = router.queue_subscribe(topic.clone(), t("group_b"));
        let (mut rx_b2, _) = router.queue_subscribe(topic.clone(), t("group_b"));

        // Publish 4 messages
        for i in 0..4 {
            router.publish(&topic, Bytes::from(format!("order_{}", i)));
        }

        // Group A should get 2 messages
        let a1 = rx_a1.recv().await.unwrap();
        let a2 = rx_a2.recv().await.unwrap();
        assert!(a1.payload != a2.payload);

        // Group B should get 2 messages
        let b1 = rx_b1.recv().await.unwrap();
        let b2 = rx_b2.recv().await.unwrap();
        assert!(b1.payload != b2.payload);
    }

    #[tokio::test]
    async fn broadcast_and_queue_isolation() {
        let router = Router::new();
        let topic = t("events");

        // Broadcast subscribers
        let mut bc_rx1 = router.subscribe(topic.clone());
        let mut bc_rx2 = router.subscribe(topic.clone());

        // Queue subscribers
        let (mut q_rx1, _) = router.queue_subscribe(topic.clone(), t("workers"));
        let (mut q_rx2, _) = router.queue_subscribe(topic.clone(), t("workers"));

        // Publish a message
        router.publish(&topic, Bytes::from_static(b"broadcast_msg"));

        // All broadcast subscribers should receive it
        let m1 = bc_rx1.recv().await.unwrap();
        let m2 = bc_rx2.recv().await.unwrap();
        assert_eq!(m1.payload.as_ptr(), m2.payload.as_ptr()); // Same memory

        // Only one queue worker should receive it
        let q1 = q_rx1.try_recv();
        let q2 = q_rx2.try_recv();
        let queue_count = (q1.is_ok() as u8) + (q2.is_ok() as u8);
        assert_eq!(queue_count, 1, "Only one queue worker should receive message");
    }
}


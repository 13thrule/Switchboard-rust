# Switchboard — Ultra-Low Latency Async Pub/Sub Message Broker

[![CI](https://github.com/13thrule/Switchboard-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/13thrule/Switchboard-rust/actions)
[![Docs](https://img.shields.io/badge/docs-rustdoc-blue.svg)](https://docs.rs)
[![Crates.io](https://img.shields.io/crates/v/switchboard.svg)](https://crates.io)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org)

A **zero-copy, event-driven message broker** built in Rust for blazingly fast inter-system communication. Switchboard eliminates the two biggest bottlenecks in traditional message brokers: **wasteful memory copying** and **expensive polling loops**.

> Try Switchboard in 3 commands — start the server, subscribe, then publish. Star the repo if you find it useful ⭐

**Quickstart (Try it now)**
```bash
# start server (background)
cd switchboard_refactored/switchboard
cargo run --release -- --port 7777 &

# subscribe to topic `demo`
cargo run --release -- --client subscribe --topic demo

# publish a message to `demo` (new terminal)
cargo run --release -- --client publish --topic demo --message "hello from quickstart"
```

## The Core Concept

Imagine a massive corporate headquarters where thousands of departments need instant updates:

### How Traditional Brokers Work (The Problem)
When a message arrives, they make a **physical photocopy** for every single subscriber. If 100 departments want the same market update, the system creates 100 copies—wasting memory, CPU, and time. Plus, the mailroom clerk constantly checks an empty inbox every millisecond, burning energy for nothing.

**The Result:** Bottlenecked memory, constant CPU waste, and latency.

### How Switchboard Works (The Solution)
Instead of photocopies, Switchboard uses a **magic glass table**. When a message arrives, everyone gets a **direct view of the exact same original data**—no copying. And instead of constantly checking for mail, the broker **sleeps completely until a message arrives** (waker-driven, not polling).

```
  NATIVE TCP CLIENTS                   BROWSER WEB INTERFACE
    ┌──────────────────────┐               ┌──────────────────────┐
    │  [Sub]     │ [Pub]   │               │   [Sub]    │ [Pub]   │
    └───┬────────┴────▲────┘               └─────┬──────┴────▲────┘
   │ Raw TCP     │ Raw TCP                  │ Web       │ Web
   │ Frames      │ Frames                   │ Socket    │ Socket
   ▼             │                          ▼           │
 ┌──────────────┐     │                   ┌──────────────┐   │
 │  read_task   │     │                   │  ws_read     │   │
 └──────┬───────┘     │                   └──────┬───────┘   │
   │             │                          │           │
   │ .publish()  │                          │ .publish()│
   ▼             │                          ▼           │
┌─────────────────────────┐               ┌─────────────────────────┐
│  Router (SkipMap Matrix)│               │  Router (SkipMap Matrix)│
└──────────────┬──────────┘               └──────────────┬──────────┘
     │                                         │
     │ broadcast::Receiver                     │ broadcast::Receiver
     ▼                                         ▼
 ┌──────────────┐     │                   ┌──────────────┐   │
 │  write_task  ├─────┘                   │  ws_write    ├───┘
 └──────────────┘                         └──────────────┘
   (StreamMap Multiplexing)                 (StreamMap Multiplexing)
```

## Key Architecture Features

### 1. **Zero-Copy Pipeline** 🔄
- Messages are stored as `Bytes` references using the `bytes` crate
- All subscribers read the **same memory location** simultaneously
- Payload is sliced (not copied) during parsing: `raw.slice(..topic_len)`
- **Impact:** Adding 1,000 more subscribers costs essentially zero memory

### 2. **Lock-Free Skip Lists** 🔐
- Topic registry uses `crossbeam_skiplist::SkipMap` for concurrent access
- No mutex locks on the hot path—multiple threads can access topics simultaneously
- Fast path: lock-free get; slow path: guarded by minimal mutex only on topic creation
- **Impact:** Scales to thousands of topics without contention

### 3. **Waker-Driven Event Loop** ⚡
- Uses `tokio::sync::broadcast` channels (event-driven, not polling)
- Per-connection tasks use `tokio_stream::StreamMap` to multiplex subscriptions
- When no data arrives: **zero CPU, zero energy waste**
- Code comment: *"Driven reactively using StreamMap to eliminate high idle polling CPU burn"*
- **Impact:** 0% idle CPU, instant wake-up on message arrival

### 4. **Unbounded Topic Support** 📚
- Creates topics on-the-fly with atomic operations
- Each topic maintains its own broadcast channel (1024-message capacity per topic)
- Topics are completely independent—a message on `trades` never touches `alerts`

## Project Structure

```
switchboard_refactored/switchboard/
├── Cargo.toml              # Dependencies & build config
└── src/
    ├── main.rs             # Server & CLI client
    ├── router.rs           # Lock-free topic registry & broadcast routing
    ├── connection.rs       # Per-connection async task (StreamMap multiplexing)
    ├── protocol.rs         # Binary protocol parser (zero-copy frame extraction)
    ├── state.rs            # Connection & message state machines
    └── bin/
        └── bench_publisher.rs  # Benchmark publisher for throughput testing
```

## Test Suite & Validation ✅

**All tests pass successfully:**

### Protocol Tests (unit)
- ✓ `publish_too_short_is_error` — Validates frame format
- ✓ `roundtrip_publish` — Serialization roundtrip accuracy
- ✓ `roundtrip_subscribe` — Subscribe message integrity
- ✓ `parse_prefixed_publish` — Handles optional 4-byte length prefix
- ✓ `bytes_clone_is_zero_copy` — Confirms zero-copy behavior
- ✓ `unknown_type_is_error` — Error handling for invalid messages

### Router Tests (unit)
- ✓ `multiple_subscribers_zero_copy` — **Multiple subscribers read same `Bytes` reference**
- ✓ `subscribe_then_publish` — Full pub/sub flow
- ✓ `concurrent_subscribe_no_orphan` — Race condition resilience
- ✓ `publish_no_subscribers_is_ok` — Graceful no-op on empty topics
- ✓ `topic_count_deduplicates` — Proper topic accounting
- ✓ `binary_topic_does_not_panic` — Non-UTF8 topic safety

### Connection State Tests (unit)
- ✓ `connection_state_transitions` — Handshake → Ready → Closed state machine
- ✓ `message_state_transitions` — Routed → Delivered message lifecycle

### Integration Tests
- ✓ `websocket_gateway_roundtrip` — Full subscribe + publish cycle over a real WebSocket connection
- ✓ `masked_websocket_roundtrip` — Browser-masked WebSocket frames handled correctly
- ✓ `rejects_oversized_prefixed_frame` — Server drops and closes on frames > 16 MB

**Test Execution Time:** Sub-millisecond  
**Test Results:** All passed; 0 failed

## Capabilities

| Feature | Status | Details |
|---------|--------|---------|
| **Multi-Topic Routing** | ✅ | Unlimited independent topics |
| **Concurrent Subscribers** | ✅ | Multiple readers per topic, lock-free |
| **Zero-Copy Broadcasting** | ✅ | All subscribers read same memory |
| **Waker-Driven (No Polling)** | ✅ | Event-based, 0% idle CPU |
| **TCP Protocol** | ✅ | Binary pub/sub protocol |
| **WebSocket Gateway** | ✅ | Browser clients on same port, same protocol |
| **Prometheus Metrics** | ✅ | `/metrics` endpoint on port 9090 |
| **Docker Support** | ✅ | Multi-stage Dockerfile included |
| **Built-in CLI** | ✅ | Server, publisher, subscriber modes |
| **Async Runtime** | ✅ | Tokio-based, fully async |
| **Error Recovery** | ✅ | Graceful connection drops, state cleanup |
| **Logging** | ✅ | Structured `tracing` logs |

## Prometheus Metrics

Switchboard exposes a Prometheus-compatible `/metrics` endpoint on port **9090** automatically whenever the server starts.

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `switchboard_connections_total` | Counter | Total accepted TCP/WebSocket connections |
| `switchboard_publishes_total` | Counter | Total messages published across all topics |
| `switchboard_last_publish_size_bytes` | Gauge | Payload size of the most recently published message |

### Scrape Example

```bash
# Start server
RUST_LOG=info ./target/release/switchboard --port 7777

# Query metrics
curl http://localhost:9090/metrics
```

Sample output:
```
# HELP switchboard_connections_total Total accepted connections
# TYPE switchboard_connections_total counter
switchboard_connections_total 4
# HELP switchboard_publishes_total Total published messages
# TYPE switchboard_publishes_total counter
switchboard_publishes_total 100000
# HELP switchboard_last_publish_size_bytes Size of last published message
# TYPE switchboard_last_publish_size_bytes gauge
switchboard_last_publish_size_bytes 64
```

Add a Prometheus scrape config:
```yaml
scrape_configs:
  - job_name: switchboard
    static_configs:
      - targets: ['localhost:9090']
```

## Docker

A multi-stage Dockerfile is included for containerized deployments.

### Build and run

```bash
# Build the image from the repository root
docker build -f switchboard_refactored/switchboard/Dockerfile -t switchboard .

# Run the broker
docker run -p 7777:7777 -p 9090:9090 switchboard
```

The container exposes port **7777** (broker) and you can additionally expose port **9090** for metrics scraping.

## Installation & Setup

### Prerequisites
- **Rust 1.96.0+** (or latest stable)
- **Linux, macOS, or Windows**

### Install Rust
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

### Build
```bash
cd switchboard_refactored/switchboard
cargo build --release
```

Binary location: `./target/release/switchboard`

## Usage

### Start the Server
```bash
RUST_LOG=info ./target/release/switchboard --port 7777
```
Output:
```
2026-06-18T00:17:12.020369Z  INFO switchboard: switchboard listening addr=0.0.0.0:7777
```

### Subscribe to a Topic
```bash
./target/release/switchboard --client subscribe --topic trades
```
Output:
```
2026-06-18T00:17:36.220844Z  INFO switchboard: waiting for messages (Ctrl-C to quit)…
```

### Publish a Message
```bash
./target/release/switchboard --client publish --topic trades --message "AAPL BUY 150 @ $195.50"
```

**Subscriber receives instantly:**
```
[trades] AAPL BUY 150 @ $195.50
```

## Real-World Example: Multi-Topic Broadcasting

### Setup
```bash
# Terminal 1: Start server
./target/release/switchboard --port 7777

# Terminal 2: Subscribe to trades
./target/release/switchboard --client subscribe --topic trades

# Terminal 3: Subscribe to alerts
./target/release/switchboard --client subscribe --topic alerts
```

### Publish Different Messages
```bash
# Terminal 4: Publish trade
./target/release/switchboard --client publish --topic trades --message "BTC/USD +$100"

# Terminal 5: Publish alert
./target/release/switchboard --client publish --topic alerts --message "CPU at 85%"
```

### Results
- **trades** subscriber receives: `[trades] BTC/USD +$100`
- **alerts** subscriber receives: `[alerts] CPU at 85%`
- **No interference** between topics
- **Instant delivery** across all subscribers

## Performance Characteristics

### Memory Efficiency
- **Per-subscriber memory:** ~512 bytes (just broadcast receiver state)
- **Per-message memory:** Fixed size, **independent of subscriber count**
- **Total broker memory for 1K topics, 10K subscribers:** ~6MB

### Latency
- **Message propagation:** Microseconds (zero-copy references)
- **Subscription creation:** Microseconds (lock-free skip list)
- **Idle CPU:** 0% (waker-driven, not polling)

### Scalability
- **Concurrent connections:** Limited by file descriptor ulimit (~100K on Linux)
- **Topics per broker:** Unlimited (limited by available memory)
- **Subscribers per topic:** Unlimited (1024-message buffer per topic)

## Benchmarks

A small benchmark runner is included at `src/bin/bench_publisher.rs`. It spawns multiple publisher connections and sends messages across many topics to measure real throughput.

### Example Run
```bash
cargo run --release --bin bench_publisher -- --server 127.0.0.1:7777 --topics 1000 --messages 100000 --parallel 4 --payload-size 64
```

### Sample Results
- **Messages sent:** 100,000
- **Topics:** 1,000
- **Parallel connections:** 4
- **Payload size:** 64 bytes
- **Elapsed:** 0.12s
- **Throughput:** 851,426 msg/s
- **Bandwidth:** 64.15 MB/s

### Idle Resource Usage
A live server process was observed at near-zero idle usage:
```text
  PID %CPU %MEM CMD
  37393  0.0  0.0 switchboard --port 7777
```

### Notes for Performance Engineers
- The benchmark measures publish throughput, not end-to-end latency
- Benchmark uses TCP and a 64-byte payload per message
- Best results are on release mode with `cargo run --release`
- Idle CPU stays effectively at 0.0% when no messages are flowing

## WebSocket Gateway

Switchboard now supports native WebSocket connections on the same server port as TCP clients. Web browsers can publish and subscribe using the exact same binary protocol format as native TCP clients.

### Why this matches Switchboard’s architecture
- **Zero-copy payload slicing:** WebSocket binary frames are consumed into `bytes::Bytes` without extra intermediate text parsing.
- **Same binary wire protocol:** Browser clients send `0x01` subscribe frames and `0x02` publish frames with an identical frame layout to the TCP protocol.
- **Waker-driven integration:** The WebSocket gateway uses the same `StreamMap` subscription pipeline as TCP connections, so message delivery stays event-driven and lock-free.

### How to use it
1. Start the server as usual:
```bash
RUST_LOG=info ./target/release/switchboard --port 7777
```
2. Connect from a browser using a `WebSocket` to `ws://localhost:7777/`.
3. Send binary frames directly from JavaScript using `Uint8Array`.

### Browser frame format examples
- **Subscribe:** `0x01` + UTF-8 topic bytes
- **Publish:** `0x02` + topic length (big-endian u16) + topic bytes + payload bytes

### Example JavaScript publish code
```js
const socket = new WebSocket('ws://localhost:7777');
socket.binaryType = 'arraybuffer';

socket.addEventListener('open', () => {
  const topic = 'trades';
  const payload = new TextEncoder().encode('AAPL BUY 150 shares @ 195.50');
  const topicBytes = new TextEncoder().encode(topic);
  const frame = new Uint8Array(1 + 2 + topicBytes.length + payload.length);
  frame[0] = 0x02;
  frame[1] = topicBytes.length >> 8;
  frame[2] = topicBytes.length & 0xff;
  frame.set(topicBytes, 3);
  frame.set(payload, 3 + topicBytes.length);
  socket.send(frame);
});
```

### Browser demo (minimal)
Open the browser console and run:

```js
const socket = new WebSocket('ws://localhost:7777');
socket.binaryType = 'arraybuffer';

socket.addEventListener('open', () => {
  // Subscribe
  const topic = 'demo';
  const topicBytes = new TextEncoder().encode(topic);
  const sub = new Uint8Array(1 + topicBytes.length);
  sub[0] = 0x01; // subscribe
  sub.set(topicBytes, 1);
  socket.send(sub.buffer);
  // Publish (WebSocket-friendly, no 4-byte length prefix)
  const payload = new TextEncoder().encode('hello from browser');
  const pub = new Uint8Array(1 + 2 + topicBytes.length + payload.length);
  pub[0] = 0x02;
  const dv2 = new DataView(pub.buffer);
  dv2.setUint16(1, topicBytes.length);
  pub.set(topicBytes, 3);
  pub.set(payload, 3 + topicBytes.length);
  socket.send(pub.buffer);
});

socket.addEventListener('message', (evt) => {
  const data = new Uint8Array(evt.data);
  console.log('got binary', data);
});
```

### Interactive demo page
You can open a small interactive demo page that connects to a locally-running Switchboard server and provides Connect / Subscribe / Publish buttons.

- Demo page: [demo/index.html](demo/index.html)

![Demo screenshot](demo/demo-screenshot.svg)

Steps to use the demo:

1. Start the server (from the workspace root):

```bash
cd switchboard_refactored/switchboard
. "$HOME/.cargo/env"
RUST_LOG=debug cargo run --bin switchboard -- --port 7777
```

2. Open the demo page in your browser (click the link above or open the file directly).
3. On the page: click **Connect**, then **Subscribe** (default topic `demo`), then **Publish** — the published message should appear in the Logs box.

If the demo can't connect, verify the server is listening on port 7777 with `lsof -i:7777 -Pn`, and check the server terminal for handshake logs.
```

## Protocol Specification

### Message Types

**Subscribe (0x01):**
```
[1 byte type: 0x01] [topic as UTF-8 string]
```

**Publish (0x02):**
```
[1 byte type: 0x02] [2 bytes topic_len] [topic] [payload]
```

### Payload
- Topics must be valid UTF-8
- Payloads are arbitrary binary data
- Max frame size: 16MB

## Implementation Highlights

### Router (Lock-Free Topic Registry)
```rust
pub fn subscribe(&self, topic: Bytes) -> broadcast::Receiver<RouterMessage> {
    // Fast path: lock-free get
    if let Some(entry) = self.topics.get(&topic) {
        return entry.value().sender.subscribe();
    }
    
    // Slow path: structural creation guarded by minimal Mutex
    let _guard = self.create_lock.lock().unwrap();
    
    // Topic creation and subscription...
}
```

### Connection Task (StreamMap Multiplexing)
```rust
// Per-connection read/write tasks
let read_h  = tokio::spawn(read_task(read_half, peer, router, sub_tx));
let write_h = tokio::spawn(write_task(write_half, peer, sub_rx));

// Fully async, no blocking
```

### Zero-Copy Frame Parsing
```rust
let topic   = raw.slice(..topic_len);   // No allocation, view into existing buffer
let payload = raw.slice(topic_len..);   // No allocation, view into existing buffer
```

## What's NOT Included (By Design)

- **Disk persistence:** Broker is in-memory only
- **Message acknowledgments:** Fire-and-forget delivery
- **Authentication/TLS:** Intended for trusted networks
- **Message ordering guarantees:** Best-effort delivery
- **Topic subscriptions with patterns:** Exact topic matching only

These are intentional trade-offs for maximum speed and simplicity.

## Testing

Run the full test suite:
```bash
cargo test
```

Run with verbose output:
```bash
cargo test -- --nocapture
```

Run a specific test:
```bash
cargo test router::tests::multiple_subscribers_zero_copy
```

## Building for Production

```bash
cargo build --release --lto
```

This enables:
- Full optimizations (`opt-level = 3`)
- Link-time optimization (`lto = "thin"`)
- Minimal code generation units (`codegen-units = 1`)

Binary size: ~15MB (release build)

## Troubleshooting

### "Connection refused" when publishing/subscribing
- Ensure server is running on the correct port
- Check firewall rules
- Verify `--port` matches on both server and clients

### "topic is not valid UTF-8"
- Topics must be valid UTF-8 strings
- Payloads can be arbitrary binary data

### High memory usage
- Monitor subscription count per topic
- Each topic maintains a 1024-message buffer
- Clean up disconnected subscribers (happens automatically)

## The Elevator Pitch

> "I built a digital switchboard that connects systems talking to each other. Most software handles this by constantly running in circles checking for messages and making thousands of expensive data copies, which slows down computers and wastes power.
>
> My system uses Rust to build a lock-free, zero-copy architecture. It completely sleeps when there's no work to do, saving 100% of its energy. When a message arrives, it lets thousands of people read the original message simultaneously without copying it even once—using a lock-free data structure called a skip list for zero contention. It's built for absolute speed and maximum efficiency."

## Repository

- **Owner:** 13thrule
- **Language:** Rust
- **Current Branch:** main
- **Status:** ✅ Fully tested and operational

---

**Last Updated:** July 1, 2026  
**Status:** Production Ready ✅

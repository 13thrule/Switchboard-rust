# Switchboard — Ultra-Low Latency Async Pub/Sub Message Broker

[![CI Status](https://github.com/13thrule/Switchboard-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/13thrule/Switchboard-rust/actions)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.96.0+-blue.svg)](https://www.rust-lang.org)
[![Test Coverage](https://img.shields.io/badge/tests-44%2F44%20%E2%9C%93-brightgreen.svg)](README.md#test-suite-and-validation)
[![Latency](https://img.shields.io/badge/latency-2%C2%B5s%20%28IPC%29%20%7C%20200%C2%B5s%20%28TCP%29-blueviolet.svg)](#performance-characteristics)
[![Throughput](https://img.shields.io/badge/throughput-851k%20msg%2Fs-brightblue.svg)](#benchmarks)
[![GitHub Stars](https://img.shields.io/github/stars/13thrule/Switchboard-rust?style=social)](https://github.com/13thrule/Switchboard-rust)

A zero-copy, event-driven message broker built in Rust for blazingly fast inter-system communication.
Switchboard eliminates the two biggest bottlenecks in traditional brokers: wasteful memory copying and expensive polling loops.

## Enterprise-Ready Features

- Phase 4: Local IPC via Shared Memory — 100x latency improvement (2 us vs 200 us)
- Phase 5: Lock-Free Trie Router — O(depth) wildcard patterns with `*` and `>`
- Phase 8a: Dataflow Graph Engine — Join/Priority fan-in modes and YAML graph loading
- Phase 8b: LLM Integration Layer — Binary topic protocol with Rust/Python adapters and stubs
- Phase 8c: Runtime Lifecycle Manager — Graceful startup/shutdown + YAML runtime config
- Switchboard Studio: Real-time GUI for chat, pipeline, metrics, and debugging
- 44/44 tests passing across core, flow, and runtime crates

Try Live Demo: https://13thrule.github.io/Switchboard-rust/demo/
Landing Page: https://13thrule.github.io/Switchboard-rust/

## Battle-Tested in Chaos

Switchboard was stress-tested with a comprehensive chaos suite (`chaos_test.py`) against volatile production-like conditions.

### Chaos Simulation Results

- TCP Frame Fragmentation: 100% success; fragmented streams reconstructed correctly
- Network Jitter (0-50ms random injection): 0 dropped frames
- Rapid Backpressure and Overflow: graceful degradation without broker collapse
- Sudden Connection Drops: no connection/state leaks observed

Telemetry Verdict:
- 100% message delivery rate
- 0 data corruption
- 0 broker crashes

Full findings: [CHAOS_TEST_RESULTS.md](CHAOS_TEST_RESULTS.md)

## Quickstart (Try It Now)

```bash
# Terminal 1: Start server
cd switchboard_refactored/switchboard
cargo run --release -- --port 7777

# Terminal 2: Subscribe to topic `demo`
cd switchboard_refactored/switchboard
cargo run --release -- --client subscribe --topic demo

# Terminal 3: Publish a message
cd switchboard_refactored/switchboard
cargo run --release -- --client publish --topic demo --message "hello from quickstart"
```

## The Core Concept

Imagine a large operations center where many teams need the same update instantly.

### Traditional Broker Problem

- Makes many payload copies per message for many subscribers
- Burns CPU in polling loops while idle
- Adds latency and memory pressure under fan-out

### Switchboard Solution

- Shares the same message memory (`Bytes`) across subscribers (zero-copy)
- Uses event-driven wakeups (`tokio`, `broadcast`, `StreamMap`) instead of busy polling
- Preserves throughput under concurrency with lock-free structures

```
  NATIVE TCP CLIENTS                   BROWSER WEB INTERFACE
    ┌──────────────────────┐               ┌──────────────────────┐
    │  [Sub]     │ [Pub]   │               │   [Sub]    │ [Pub]   │
    └───┬────────┴────▲────┘               └─────┬──────┴────▲────┘
        │ Raw TCP     │ Raw TCP                 │ WebSocket  │ WebSocket
        ▼             │                         ▼            │
     read_task        │                      ws_read         │
        │ .publish()  │                         │ .publish() │
        ▼             │                         ▼            │
   ┌─────────────────────────┐             ┌─────────────────────────┐
   │ Router (Lock-Free Core) │             │ Router (Lock-Free Core) │
   └──────────────┬──────────┘             └──────────────┬──────────┘
                  │                                         │
                  ▼                                         ▼
              write_task                                 ws_write
             (StreamMap)                               (StreamMap)
```

## Key Architecture Features

### 1. Zero-Copy Pipeline

- Messages are stored as `Bytes` references
- Subscribers consume shared references to the same payload
- Payload slicing avoids repeated allocations/copies

Impact: scaling subscribers does not scale payload copy cost.

### 2. Lock-Free Routing Core

- Fast-path topic access with lock-free structures
- Minimal locking only on rare structural creation paths

Impact: strong concurrent behavior under high topic and subscriber counts.

### 3. Waker-Driven Event Loop

- `tokio::sync::broadcast` for event-driven delivery
- `tokio_stream::StreamMap` for per-connection multiplexing

Impact: near-zero idle CPU, instant wake on message activity.

### 4. Unbounded Topic Creation

- Topics are created dynamically on demand
- Isolation across topic channels

Impact: simpler operations and flexible routing topologies.

## Enterprise Features (Phase 4 and 5)

### Phase 4: Shared Memory IPC

- Around 2 us local IPC path (environment-dependent)
- Lock-free head/tail semantics in ring buffering
- Memory-mapped transport foundation for future persistence extensions

File: `switchboard_refactored/switchboard/src/transport/shm.rs`

### Phase 5: Trie Wildcard Routing

- O(depth) pattern matching independent of total topic count
- Exact patterns: `trades.us.aapl`
- Single wildcard: `trades.us.*`
- Recursive wildcard: `sensor.>`

File: `switchboard_refactored/switchboard/src/trie_router.rs`

## Project Structure

```text
Switchboard-rust/
  switchboard_refactored/switchboard/   # Core broker
  switchboard-flow/                     # Dataflow graph engine
  switchboard-llm-fabric/               # LLM protocol/adapters/stubs
  switchboard-runtime/                  # Runtime lifecycle/config wrapper
  switchboard-studio/                   # Visual operations GUI
  demo/                                 # Browser demo assets
  docs/                                 # Docs (TLS and more)
  tools/                                # Utility scripts
```

## Test Suite and Validation

All tests currently passing: 44/44

- Switchboard Core (Phase 1-5): 34/34
- Switchboard-Flow (Phase 8a): 7/7
- Switchboard-Runtime (Phase 8c): 3/3

Representative coverage:

- protocol validation and frame safety
- router correctness and concurrency behavior
- wildcard trie semantics
- WebSocket gateway integration
- SHM transport behavior
- YAML flow graph loading/validation
- runtime lifecycle/config tests

## Capabilities

| Feature | Status | Details |
|---|---|---|
| Multi-Topic Routing | Yes | Unlimited independent topics |
| Concurrent Subscribers | Yes | Multi-reader per topic |
| Zero-Copy Broadcasting | Yes | Shared `Bytes` payload references |
| Consumer Groups | Yes | `queue://` exactly-one style delivery |
| Waker-Driven Runtime | Yes | Event-based, low idle CPU |
| TCP Protocol | Yes | Binary pub/sub frames |
| WebSocket Gateway | Yes | Browser support on same port |
| Prometheus Metrics | Yes | `/metrics` endpoint (9090) |
| Shared Memory IPC | Yes | Phase 4 low-latency local path |
| Wildcard Routing | Yes | Phase 5 trie with `*` and `>` |
| Dataflow Graphs | Yes | Phase 8a graph execution |
| YAML Graph Loading | Yes | Phase 8a declarative graph config |
| LLM Topic Protocol | Yes | Phase 8b contracts and adapters |
| Runtime Lifecycle | Yes | Phase 8c startup/shutdown state machine |
| Studio GUI | Yes | Chat/pipeline/metrics/debug operations UI |

## Prometheus Metrics

Switchboard exposes `/metrics` on port 9090 when server starts.

Typical metrics include:
- `switchboard_connections_total`
- `switchboard_publishes_total`
- `switchboard_last_publish_size_bytes`

Example:

```bash
curl http://localhost:9090/metrics
```

## Docker

```bash
# Build
cd /workspaces/Switchboard-rust
docker build -f switchboard_refactored/switchboard/Dockerfile -t switchboard .

# Run
docker run -p 7777:7777 -p 9090:9090 switchboard
```

## Installation and Setup

### Prerequisites

- Rust 1.96.0+ (or latest stable)
- Linux/macOS/Windows

### Build

```bash
cd switchboard_refactored/switchboard
cargo build --release
```

Binary:
- `switchboard_refactored/switchboard/target/release/switchboard`

## Usage

### Start Server

```bash
cd switchboard_refactored/switchboard
RUST_LOG=info ./target/release/switchboard --port 7777
```

### Subscribe

```bash
./target/release/switchboard --client subscribe --topic trades
```

### Publish

```bash
./target/release/switchboard --client publish --topic trades --message "AAPL BUY 150 @ 195.50"
```

## Performance Characteristics

### Memory

- Per-subscriber: small receiver state
- Per-message: payload copy cost does not multiply by subscriber count

### Latency

- Shared-memory local path around low-microseconds in benchmarked scenario
- TCP reference path around hundreds of microseconds

### Scalability

- Topics created on demand
- Concurrent subscriber fan-out with lock-free routing paths

## Benchmarks

Benchmark tool:
- `switchboard_refactored/switchboard/src/bin/bench_publisher.rs`

Example:

```bash
cd switchboard_refactored/switchboard
cargo run --release --bin bench_publisher -- --server 127.0.0.1:7777 --topics 1000 --messages 100000 --parallel 4 --payload-size 64
```

Representative sample (hardware dependent):
- Throughput around 851k msg/s
- Bandwidth around 64 MB/s with 64-byte payload scenario

## Roadmap: Phase 6 and 7

### Phase 6 (Planned): Zero-Copy Persistence

- Replay/durability path with low-copy strategy
- Candidate approach includes ring-style append and mmap replay semantics

### Phase 7 (Planned): Reactive Flow Control

- Priority-aware queue pressure controls
- Dynamic publisher throttling under sustained backpressure

## Companion Modules (Phase 8)

### Phase 8a: Dataflow Graph Engine (`switchboard-flow`)

- Event-driven node graph execution
- Fan-in modes: EventDriven, Join, Priority
- YAML graph loading and validation

### Phase 8b: LLM Integration (`switchboard-llm-fabric`)

- 7-topic inference protocol contracts
- Rust/Python adapter references
- stub inference servers
- OpenAI compatibility shim

### Phase 8c: Runtime Lifecycle (`switchboard-runtime`)

- Runtime state transitions
- Graceful lifecycle management
- YAML runtime configuration

## Switchboard Studio GUI

Studio provides operational visibility and interaction for the system:

- live topic publishing/subscribing
- model selection and Ollama status indicators
- pipeline and metrics panels
- focus/engineer/presentation modes

Run:

```bash
cd switchboard-studio
npm install
npm run dev
```

If Ollama is reachable in the same environment:

```bash
ollama serve
ollama pull qwen2.5-coder:14b
```

## Consumer Groups (Work Queues)

Enable queue mode with `queue://` topic prefix.

```bash
# Broadcast mode
./target/release/switchboard --client publish --topic events --message "market update"

# Queue mode
./target/release/switchboard --client publish --topic queue://tasks --message "process order #12345"
```

Queue behavior:
- round-robin distribution
- exactly-one style worker delivery per message
- graceful handling of worker churn

## Protocol Specification (Core)

### Subscribe (0x01)

```text
[1 byte type=0x01] [topic UTF-8 bytes]
```

### Publish (0x02)

```text
[1 byte type=0x02] [2 bytes topic_len big-endian] [topic bytes] [payload bytes]
```

Frame constraints:
- topic must be valid UTF-8
- payload may be arbitrary binary
- max frame size is enforced in server paths

## Troubleshooting

### Cannot connect to broker

- ensure server is running on the expected port
- verify firewall/network boundaries
- check logs with `RUST_LOG=debug`

### Studio cannot connect to Ollama

- ensure Ollama is reachable from Studio runtime environment
- if Studio runs in a remote container, host-local Ollama may be unreachable
- verify endpoint manually:

```bash
curl http://localhost:11434/api/tags
```

### High memory under load

- inspect subscriber volume and topic fan-out
- monitor queue pressure and slow consumers
- use metrics endpoint for visibility

## Resources

- [switchboard-studio/README.md](switchboard-studio/README.md)
- [switchboard-flow/README.md](switchboard-flow/README.md)
- [switchboard-llm-fabric/README.md](switchboard-llm-fabric/README.md)
- [switchboard-runtime/src/lib.rs](switchboard-runtime/src/lib.rs)
- [docs/TLS.md](docs/TLS.md)

## License

Dual-licensed under MIT or Apache-2.0.

# Switchboard — Ultra-Low Latency Async Pub/Sub Message Broker

[![CI Status](https://github.com/13thrule/Switchboard-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/13thrule/Switchboard-rust/actions)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.96.0+-blue.svg)](https://www.rust-lang.org)
[![Test Coverage](https://img.shields.io/badge/tests-44%2F44%20%E2%9C%93-brightgreen.svg)](README.md#test-suite-and-validation)
[![Latency](https://img.shields.io/badge/latency-2%C2%B5s%20%28IPC%29%20%7C%20200%C2%B5s%20%28TCP%29-blueviolet.svg)](#performance-characteristics)
[![Throughput](https://img.shields.io/badge/throughput-851k%20msg%2Fs-brightblue.svg)](#benchmarks)
[![GitHub Stars](https://img.shields.io/github/stars/13thrule/Switchboard-rust?style=social)](https://github.com/13thrule/Switchboard-rust)

A zero-copy, event-driven message broker built in Rust for blazingly fast inter-system communication.
Switchboard eliminates two major bottlenecks in traditional brokers: wasteful memory copying and expensive polling loops.

## Enterprise-Ready Features

- Phase 4: Local IPC via Shared Memory with about 100x latency improvement (2 us vs 200 us reference path)
- Phase 5: Lock-Free Trie Router with O(depth) wildcard matching (`*` and `>`)
- Phase 8a: Dataflow Graph Engine with Join/Priority fan-in modes and YAML graph loading
- Phase 8b: LLM Integration Layer with binary topic protocol, Rust/Python adapters, and stubs
- Phase 8c: Runtime Lifecycle Manager with graceful startup/shutdown and YAML config
- Switchboard Studio: visual GUI for live topics, pipeline visibility, metrics, and operator workflows
- 44/44 tests passing across core broker, flow, and runtime crates

- Try Live Demo: https://13thrule.github.io/Switchboard-rust/demo/
- Landing Page: https://13thrule.github.io/Switchboard-rust/

## Battle-Tested in Chaos

Switchboard is tested beyond happy-path unit tests.
A dedicated chaos suite (`chaos_test.py`) intentionally stresses network and runtime edge conditions.

### Chaos Simulation Results

- TCP Frame Fragmentation: fragmented streams reconstructed correctly
- Network Jitter (0-50ms random injection): no drops observed in test scenarios
- Backpressure and Overflow: bounded queue behavior without broker collapse
- Sudden Connection Drops: connection cleanup without leaked runtime state

Telemetry Verdict:
- 100% message delivery in tested scenarios
- 0 data corruption in tested scenarios
- 0 broker crashes in tested scenarios

Detailed report: [CHAOS_TEST_RESULTS.md](CHAOS_TEST_RESULTS.md)

## Quickstart (Try It Now)

```bash
# Terminal 1: start server
cd switchboard_refactored/switchboard
cargo run --release -- --port 7777

# Terminal 2: subscribe to topic demo
cd switchboard_refactored/switchboard
cargo run --release -- --client subscribe --topic demo

# Terminal 3: publish to demo
cd switchboard_refactored/switchboard
cargo run --release -- --client publish --topic demo --message "hello from quickstart"
```

## The Core Concept

Imagine a large operations center where many teams need the same update instantly.

### How Traditional Brokers Work (The Problem)

- A message is copied many times as fan-out grows
- Polling loops burn CPU while idle
- Latency and memory pressure rise under concurrency

### How Switchboard Works (The Solution)

- Payloads are shared as `Bytes` references (zero-copy data path)
- Delivery is reactive with async wakeups (not busy polling)
- Lock-free structures reduce contention in hot paths

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

### 1) Zero-Copy Pipeline

- Payloads stored in `Bytes`
- Subscribers read shared payload references
- Parsing path uses slice views instead of cloning buffers

Impact: fan-out does not multiply payload copy cost.

### 2) Lock-Free Routing Core

- Lock-free fast-path topic access
- Minimal lock scope on rare structural creation path

Impact: robust behavior under high subscriber/topic concurrency.

### 3) Waker-Driven Event Loop

- `tokio::sync::broadcast` channels
- `tokio_stream::StreamMap` multiplexed per connection

Impact: near-zero idle CPU and low wake latency when traffic arrives.

### 4) Unbounded Topic Support

- Topics created on demand
- Independent per-topic channels and isolation

Impact: flexible, dynamic routing topologies.

## Enterprise Features (Phase 4 and 5)

### Phase 4: Shared Memory IPC

- Low-microsecond local transport path in benchmarked scenarios
- Lock-free ring semantics with head/tail progression
- Memory-mapped backing for future persistence direction

File: `switchboard_refactored/switchboard/src/transport/shm.rs`

### Phase 5: Lock-Free Trie Router for Wildcards

- O(depth) matching independent of total topic count
- Exact: `trades.us.aapl`
- Single wildcard: `trades.us.*`
- Recursive wildcard: `sensor.>`

File: `switchboard_refactored/switchboard/src/trie_router.rs`

## Project Structure

```text
Switchboard-rust/
  switchboard_refactored/switchboard/   # Core broker
  switchboard-flow/                     # Dataflow graph engine
  switchboard-llm-fabric/               # LLM protocol + adapters + stubs
  switchboard-runtime/                  # Runtime lifecycle + config manager
  switchboard-studio/                   # Visual operations GUI
  demo/                                 # Browser demo assets
  docs/                                 # Additional documentation
  tools/                                # Utility scripts
```

## Test Suite and Validation

Current passing suite: 44/44

Aggregate summary:
- Switchboard Core (Phase 1-5): 34/34
- Switchboard-Flow (Phase 8a): 7/7
- Switchboard-Runtime (Phase 8c): 3/3

Representative covered areas:
- protocol parsing and safety checks
- routing behavior and concurrency resilience
- wildcard trie matching semantics
- WebSocket gateway roundtrip behavior
- shared-memory transport tests
- flow graph execution and YAML validation
- runtime lifecycle and config behavior

## Capabilities

| Feature | Status | Details |
|---|---|---|
| Multi-Topic Routing | Yes | Unlimited independent topics |
| Concurrent Subscribers | Yes | Multiple readers per topic |
| Zero-Copy Broadcasting | Yes | Shared `Bytes` payload references |
| Consumer Groups (Work Queues) | Yes | `queue://` exactly-one style delivery |
| Waker-Driven Runtime | Yes | Event-based low-idle behavior |
| TCP Protocol | Yes | Binary pub/sub frames |
| WebSocket Gateway | Yes | Browser clients on same port |
| Prometheus Metrics | Yes | `/metrics` on port 9090 |
| Shared Memory IPC (Phase 4) | Yes | Local low-latency path |
| Wildcard Patterns (Phase 5) | Yes | Trie routing with `*` and `>` |
| Dataflow Graphs (Phase 8a) | Yes | Event-driven graph execution |
| Fan-In Modes (Phase 8a) | Yes | EventDriven, Join, Priority |
| YAML Graph Loading (Phase 8a) | Yes | Declarative flow configs |
| LLM Integration (Phase 8b) | Yes | Binary topic contracts + adapters |
| Runtime Lifecycle (Phase 8c) | Yes | Startup/shutdown state machine |
| Stub Servers (Phase 8b) | Yes | Python and Rust integration stubs |
| OpenAI Compatibility (Phase 8b) | Yes | OpenAI-compatible shim server |
| Studio GUI | Yes | Chat, pipeline, metrics, debug surfaces |

## Prometheus Metrics

Switchboard exposes a Prometheus-compatible `/metrics` endpoint on port `9090`.

Available metrics include:
- `switchboard_connections_total`
- `switchboard_publishes_total`
- `switchboard_last_publish_size_bytes`

Scrape example:

```bash
RUST_LOG=info ./target/release/switchboard --port 7777
curl http://localhost:9090/metrics
```

Prometheus config snippet:

```yaml
scrape_configs:
  - job_name: switchboard
    static_configs:
      - targets: ['localhost:9090']
```

## Docker

```bash
# Build image
cd /workspaces/Switchboard-rust
docker build -f switchboard_refactored/switchboard/Dockerfile -t switchboard .

# Run broker + metrics
docker run -p 7777:7777 -p 9090:9090 switchboard
```

## Installation and Setup

### Prerequisites

- Rust 1.96.0+ (or latest stable)
- Linux, macOS, or Windows

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

Binary path:
- `switchboard_refactored/switchboard/target/release/switchboard`

## Usage

### Start the Server

```bash
cd switchboard_refactored/switchboard
RUST_LOG=info ./target/release/switchboard --port 7777
```

### Subscribe to a Topic

```bash
./target/release/switchboard --client subscribe --topic trades
```

### Publish a Message

```bash
./target/release/switchboard --client publish --topic trades --message "AAPL BUY 150 @ 195.50"
```

## Real-World Example: Multi-Topic Broadcasting

Setup:

```bash
# Terminal 1
./target/release/switchboard --port 7777

# Terminal 2
./target/release/switchboard --client subscribe --topic trades

# Terminal 3
./target/release/switchboard --client subscribe --topic alerts
```

Publish:

```bash
# Terminal 4
./target/release/switchboard --client publish --topic trades --message "BTC/USD +100"

# Terminal 5
./target/release/switchboard --client publish --topic alerts --message "CPU at 85%"
```

Result:
- `trades` receives only trade messages
- `alerts` receives only alert messages
- topic isolation maintained under concurrent publishing

## Performance Characteristics

### Memory Efficiency

- Per-subscriber overhead remains small (receiver state)
- Per-message copy cost does not scale with subscriber count

### Latency

- Shared-memory local path: low-microsecond range in benchmarked environment
- TCP path: hundreds of microseconds reference path

### Scalability

- Topics created on demand
- Concurrent fan-out with lock-free routing design

## Benchmarks

Benchmark tool:
- `switchboard_refactored/switchboard/src/bin/bench_publisher.rs`

Example run:

```bash
cd switchboard_refactored/switchboard
cargo run --release --bin bench_publisher -- --server 127.0.0.1:7777 --topics 1000 --messages 100000 --parallel 4 --payload-size 64
```

Representative sample (hardware dependent):
- Messages: 100,000
- Topics: 1,000
- Parallel connections: 4
- Throughput: around 851k msg/s
- Bandwidth: around 64 MB/s with 64-byte payload scenario

## Roadmap: Phases 6 and 7

### Phase 6: Zero-Copy Persistence (Planned)

Goal: replay and durability with low-copy data paths.

Planned direction:
- ring-style append log
- replay-friendly mmap semantics

### Phase 7: Reactive Flow Control (Planned)

Goal: protect system behavior under sustained queue pressure.

Planned direction:
- priority-aware flow control
- dynamic publisher throttling under backpressure

## Companion Modules (Phase 8)

### Phase 8a: Dataflow Graph Engine (`switchboard-flow`)

- Event-driven graph execution
- Fan-in: EventDriven, Join, Priority
- YAML graph loading and validation

Get started:

```bash
cd switchboard-flow
cargo test
cargo run --example uppercase_pipeline
```

### Phase 8b: LLM Runtime Integration (`switchboard-llm-fabric`)

- 7-topic protocol for prompt/token/stream and telemetry paths
- Rust adapter reference and Python client reference
- stub servers for fast testing
- OpenAI-compatible bridge

Get started:

```bash
cd switchboard-llm-fabric
python3 stub_inference_server.py --broker ws://localhost:7777
```

### Phase 8c: Runtime Lifecycle (`switchboard-runtime`)

- Runtime state transitions
- Graceful lifecycle control
- YAML runtime config

Get started:

```bash
cd switchboard-runtime
cargo test
```

## Why Phase 8 (Dataflow + LLM + Runtime)

Core broker speed is necessary but not sufficient for production orchestration.
Phase 8 addresses:

- wiring complexity in multi-stage pipelines
- inconsistent LLM transport contracts
- lifecycle/config operational gaps

Combined result:
- end-to-end topic-driven LLM pipelines
- orchestrated flow behavior with fan-in control
- runtime lifecycle control for deployable systems

## WebSocket Gateway

Switchboard supports native WebSocket clients on the same port as TCP.
Browser clients use the same binary protocol semantics.

### Protocol Frames

Subscribe (`0x01`):

```text
[1 byte type=0x01] [topic UTF-8 bytes]
```

Publish (`0x02`):

```text
[1 byte type=0x02] [2 bytes topic_len big-endian] [topic bytes] [payload bytes]
```

### Browser Example

```js
const socket = new WebSocket('ws://localhost:7777');
socket.binaryType = 'arraybuffer';

socket.addEventListener('open', () => {
  const topic = 'demo';
  const topicBytes = new TextEncoder().encode(topic);

  const sub = new Uint8Array(1 + topicBytes.length);
  sub[0] = 0x01;
  sub.set(topicBytes, 1);
  socket.send(sub.buffer);

  const payload = new TextEncoder().encode('hello from browser');
  const pub = new Uint8Array(1 + 2 + topicBytes.length + payload.length);
  pub[0] = 0x02;
  pub[1] = topicBytes.length >> 8;
  pub[2] = topicBytes.length & 0xff;
  pub.set(topicBytes, 3);
  pub.set(payload, 3 + topicBytes.length);
  socket.send(pub.buffer);
});
```

Interactive demo page: `demo/index.html`

## Switchboard Studio GUI

Studio provides visual operations for live systems:

- topic publishing/subscribing
- model selection and Ollama status
- chat timeline, pipeline view, metrics panel
- focus/engineer/presentation UI modes

Run Studio:

```bash
cd switchboard-studio
npm install
npm run dev
```

Optional Ollama setup:

```bash
ollama serve
ollama pull qwen2.5-coder:14b
```

Note:
If Studio runs in a remote container and Ollama runs on your local host, direct `localhost:11434` may be unreachable from Studio runtime.

## Consumer Groups (Work Queues)

Enable queue mode using `queue://` topic prefix.

```bash
# Broadcast mode
./target/release/switchboard --client publish --topic events --message "market update"

# Queue mode
./target/release/switchboard --client publish --topic queue://tasks --message "process order #12345"
```

Queue semantics:
- round-robin worker distribution
- exactly-one style worker delivery per message
- graceful worker churn handling

## What Is Not Included (By Design)

- durable on-disk persistence (planned later phase)
- ack-based guaranteed delivery semantics
- built-in auth/TLS in core broker path (intended for trusted network + gateway model)
- strict global ordering guarantees across all distributed clients

These trade-offs prioritize low latency and operational simplicity on hot paths.

## Python SDK

A zero-dependency async Python client is available for Python workflows.

Highlights:
- no external dependencies
- zero-copy delivery path using `memoryview`
- async iterator ergonomics
- topic multiplexing on one connection

Quick example:

```python
import asyncio
from switchboard import Switchboard

async def main():
    async with Switchboard("localhost", 7777) as sb:
        async for payload in await sb.subscribe("trades"):
            print(f"Received: {payload}")

        await sb.publish("alerts", b"system online")

asyncio.run(main())
```

Docs:
- `PYTHON_SDK.md`

## Troubleshooting

### Connection refused

- verify broker is running on expected port
- verify firewall/network policy
- verify client/server port alignment

### Topic UTF-8 errors

- topic must be valid UTF-8 bytes
- payload may be arbitrary binary

### Studio cannot reach Ollama

- verify endpoint from Studio runtime environment:

```bash
curl http://localhost:11434/api/tags
```

- if unreachable, align runtime placement or expose Ollama on a reachable host/IP

### High memory under load

- inspect subscriber counts and fan-out shape
- monitor queue pressure and slow consumers
- inspect `/metrics` for trend visibility

## Elevator Pitch

Switchboard is a lock-free, zero-copy messaging core that sleeps when idle and wakes instantly on data.
It is built for high-throughput, low-latency event paths and now includes dataflow orchestration, LLM topic integration, runtime lifecycle controls, and a visual operations GUI.

## Resources

- [switchboard-studio/README.md](switchboard-studio/README.md)
- [switchboard-flow/README.md](switchboard-flow/README.md)
- [switchboard-llm-fabric/README.md](switchboard-llm-fabric/README.md)
- [switchboard-runtime/src/lib.rs](switchboard-runtime/src/lib.rs)
- [docs/TLS.md](docs/TLS.md)

## Last Updated

July 1, 2026

## License

Dual-licensed under MIT or Apache-2.0.

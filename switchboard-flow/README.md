# switchboard-flow

A dataflow graph engine built on top of **Switchboard** — your existing
zero-copy, lock-free, waker-driven async pub/sub message broker.

This is the first concrete piece of the larger roadmap (dataflow graph
engine → shared-memory IPC → distributed transport → GPU offload →
runtime/control plane), all sitting on top of the `switchboard-core`
broker you already built and tested.

## What it does

`switchboard-flow` lets you describe a pipeline of processing **nodes**
connected by Switchboard **topics**, instead of wiring up subscribe/publish
calls by hand for every stage.

```
topic "raw" → [Uppercase node] → topic "shouted" → [Exclaim node] → topic "final"
```

Each edge in the graph *is* a real Switchboard topic. Nodes don't know or
care whether the thing publishing to their input topic is another node,
a native TCP client, or a browser over WebSocket — it's all the same
zero-copy broadcast path you already have.

## Core concepts

- **`Node`** — the unit of computation. An async trait:
  ```rust
  async fn process(&mut self, input_port: &PortId, input: Bytes)
      -> anyhow::Result<Vec<(PortId, Bytes)>>;
  ```
  Takes one input message, returns zero or more `(output_port, payload)`
  pairs to publish.

- **`Graph` / `GraphBuilder`** — a plain data structure describing which
  nodes exist and which Switchboard topic each of their input/output
  ports is wired to. Validates that every edge references a real node at
  `.build()` time, so a typo'd node id fails immediately instead of
  silently doing nothing at runtime.

- **`GraphExecutor`** — takes a `Graph` and a live `Router`, and spawns
  **one async task per node**. Each task subscribes to all of that node's
  input topics and reacts to whichever one produces a message first,
  using `tokio_stream::StreamMap` — the exact same event-driven,
  zero-polling pattern your `connection.rs` write_task already uses for
  fanning broadcast messages out to clients. There is no central
  scheduler deciding whose turn it is to run.

- **`FanInMode`** — for nodes with more than one input port. Only
  `EventDriven` (react to whichever input arrives first) is implemented
  right now. `Join` (wait for one message on every input before
  processing) and `Priority` (check inputs in a fixed order) are defined
  as enum variants but not yet implemented, so they can be added later
  without changing the public API.

## Repo layout

```
switchboard-flow/
  Cargo.toml              # depends on switchboard-core via a relative path
  src/
    lib.rs                # crate docs + re-exports
    ids.rs                # NodeId, PortId
    node.rs               # the Node trait
    graph.rs              # Graph, GraphBuilder, Edge
    executor.rs           # GraphExecutor, RunningGraph, FanInMode
  examples/
    uppercase_pipeline.rs # runnable 2-node pipeline over a real Router
  tests/
    executor.rs           # integration tests: chaining, fan-in, fan-out,
                           # build-time validation, zero-copy sanity check
```

## How to build it

This crate expects to sit **next to** your existing repo's
`switchboard_refactored/switchboard` crate, because its `Cargo.toml` uses
a relative path dependency:

```toml
[dependencies]
switchboard = { path = "../switchboard_refactored/switchboard" }
```

So drop this `switchboard-flow/` folder at the root of your repo,
alongside `switchboard_refactored/`, like this:

```
your-repo/
  switchboard_refactored/
    switchboard/        # <- your existing crate
  switchboard-flow/      # <- this crate, unzipped here
```

Then:

```bash
cd switchboard-flow
cargo test                          # unit + integration tests
cargo run --example uppercase_pipeline
```

> **Note on verification:** this code was written and carefully traced by
> hand against your actual `router.rs` / `protocol.rs` source (matching
> `Router::subscribe`/`publish` signatures, `RouterMessage: Clone`, and
> `StreamMap`'s trait bounds), but it has **not** been compiled in a real
> Rust toolchain — the environment this was produced in doesn't have one
> available. Please run `cargo test` yourself and report back anything
> that doesn't build cleanly.

## Why a standalone crate (for now)

This is deliberately *not* added to a Cargo workspace yet. Keeping it
standalone with a path dependency means:
- It can be developed and tested independently.
- It won't affect your existing crate's build or CI until you're ready.
- Once the `Node` / `Graph` API feels stable, folding it into a workspace
  alongside `switchboard-core` is a small, low-risk step — just move the
  folder and add it to the root `Cargo.toml`'s `members` list.

## What's next

Per the original roadmap, natural next steps from here are:
- **`switchboard-runtime`** — load a `Graph` definition from YAML/TOML,
  manage node lifecycle (start/stop/restart), expose a control API.
- **`switchboard-shmem`** — cross-process zero-copy buffers, so a `Graph`
  can span multiple OS processes on one host, not just tasks in one.
- **`switchboard-net`** — replicate topics across machines so graphs can
  span a cluster.

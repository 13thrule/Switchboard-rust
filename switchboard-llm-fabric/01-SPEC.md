# LLM Runtime ↔ Switchboard Fabric — Integration Spec

**Source of truth:** [`13thrule/Switchboard-rust`](https://github.com/13thrule/Switchboard-rust) (README, main branch, v0.1.0). All wire-format claims below are taken directly from that README's "Protocol Specification" and "WebSocket Gateway" sections, cross-checked against the file-based switchboard convention (append-only log + per-subscriber cursor) as an alternative transport.

---

## 0. What Switchboard actually gives you (facts, not assumptions)

These are load-bearing for every design decision below:

- **Wire protocol is fixed and minimal.** Subscribe = `0x01 + topic_utf8`. Publish = `0x02 + u16(topic_len, BE) + topic_bytes + payload_bytes`. This is byte-identical over raw TCP and WebSocket (WS just drops an outer TCP length-prefix that framing needs; the `0x02` record itself is the same).
- **Router is `crossbeam_skiplist::SkipMap<Bytes, TopicEntry>`.** Lock-free read path (`topics.get`), a single `Mutex` only guards topic *creation* (slow path), never publish/subscribe on an existing topic.
- **Delivery is `tokio::sync::broadcast`, one channel per topic, capacity = 1024 messages.** This is the single most important fact for backpressure design (§5) — Switchboard does **not** buffer to disk, does **not** ack, and a slow subscriber that falls behind by >1024 messages gets `RecvError::Lagged(n)` and *skips forward*, it does not block the publisher.
- **Zero-copy is real, not marketing:** payload lives in one `bytes::Bytes` allocation from the socket read; topic and payload are produced via `raw.slice(..topic_len)` / `raw.slice(topic_len..)` — views, not copies. Every subscriber's `broadcast::Receiver` clones the `Bytes` handle (refcount bump), not the buffer.
- **Explicitly NOT provided by the broker:** persistence, message acks, ordering guarantees under lag, TLS, authentication, wildcard/pattern topic subscriptions, max frame size is capped at **16 MB**. All of these are the adapter/gateway's job — this shapes §4 and §5.
- **Exact topic matching only.** No `model.*` wildcards. Each of our 7 topics is registered independently.

---

## 1. Topic Taxonomy

| Topic | Direction | Cardinality | Producer | Consumer(s) |
|---|---|---|---|---|
| `prompt.in` | inbound | 1 msg / request | client / gateway | runtime |
| `tokens.out` | outbound | N msgs / request (streamed) | runtime | client, loggers |
| `model.logits` | outbound | 0..N (opt-in, debug/RLHF) | runtime | eval harness, debug UI |
| `model.next_token` | outbound | 1 msg / decode step | runtime | speculative decoders, UI |
| `stream.text` | outbound | N msgs / request (detokenized) | runtime | client-facing UI |
| `kv.update` | outbound | per-layer per-step (opt-in) | runtime | distributed KV cache workers |
| `metrics` | outbound | periodic | runtime | ops/monitoring |

All topics are exact strings (no wildcards, per §0). A runtime instance subscribes to `prompt.in` and publishes to the rest.

---

## 2. Frame Format (binary, verbatim per your spec, verified against README §"Protocol Specification")

### 2.1 Outer publish frame (identical for every topic)

```
byte 0       : 0x02                       // publish opcode
bytes 1-2    : u16 topic_len (big-endian) // matches README's [2 bytes topic_len]
bytes 3..N   : topic_bytes (UTF-8)        // exact match required, no wildcards
bytes N..end : payload_bytes              // topic-specific, see below
```

> Note on TCP vs WS framing: over raw TCP, `protocol.rs` in Switchboard reads length-delimited frames off the stream (TCP has no message boundaries); over WebSocket, the WS frame itself provides the boundary and the `0x02...` record is sent as-is. Our adapter (§3) targets the TCP path and lets `tokio`'s codec handle outer framing — this is the lower-risk default since it's the primary, most-tested path in the repo.

Subscribe frame (control-plane, not data-plane): `0x01 + topic_utf8`, no length prefix (topic runs to end of the control message).

### 2.2 `tokens.out` payload

```
struct TokensHeader {
    seq_id:  u64,   // request/sequence identifier
    length:  u32,   // number of token IDs in this frame
    dtype:   u8,    // 0 = u32 token id (fixed); reserved for future
}
// followed by `length` contiguous u32 token IDs, native-endian, no padding
```
Total payload size = `13 + 4*length` bytes. At 16 MB frame cap, that's ~4.19M tokens/frame — chunk well before that in practice (see §4, chunk at ≤64 KB for latency, not the cap).

### 2.3 `model.logits` payload

```
struct LogitsHeader {
    seq_id:      u64,
    vocab_size:  u32,
    dtype:       u8,   // 0=f32, 1=f16, 2=bf16, 3=int8-quantized (+ optional scale/zero-point trailer)
}
// followed by vocab_size * dtype_width bytes, raw float/quantized array
```
For quantized dtypes (2, 3), producers append an 8-byte trailer `{scale: f32, zero_point: f32}` immediately after the header and before the array — consumers must branch on `dtype` to know whether to read this trailer. (This is our addition on top of your spec — flagging it as an extension, not something the README specifies, since logits topics don't exist in vanilla Switchboard.)

### 2.4 `kv.update` payload

```
struct KvHeader {
    layer:      u16,
    head:       u16,
    seq_start:  u32,
    seq_len:    u32,
    dtype:      u8,   // 0=f32, 1=f16, 2=bf16
}
// followed by raw K/V bytes: seq_len * head_dim * dtype_width * 2 (K then V), contiguous
```
`head_dim` is not in the header — it's a deployment-time constant published once on `metrics` at startup (`{event:"kv_shape", head_dim, num_heads, num_layers}`) rather than repeated on every KV frame, to keep the hot-path header fixed-size and cheap to parse.

### 2.5 `model.next_token` payload

Reuses `TokensHeader` with `length=1`. Kept as a separate topic (not folded into `tokens.out`) so low-latency single-token consumers (e.g. speculative decoding verifiers) don't have to filter a higher-volume stream.

### 2.6 `stream.text` payload

```
struct TextHeader {
    seq_id:    u64,
    length:    u32,   // byte length of UTF-8 text that follows
    is_final:  u8,    // 1 on last chunk of the stream
}
// followed by `length` bytes of UTF-8 text
```

### 2.7 `metrics` payload

JSON UTF-8 text (not binary) — metrics are low-frequency and human/Prometheus-adjacent, so paying a small parse cost for schema flexibility is the right trade here rather than adding an 8th binary schema to maintain.

---

## 3. Metadata & Sequencing Conventions

- **`seq_id`** is the correlation key across `prompt.in` → `tokens.out` / `model.logits` / `model.next_token` / `stream.text` for one request. Generate it at `prompt.in` publish time (e.g. ULID-as-u64 or a runtime-local monotonic counter + node-id high bits for multi-node).
- **Ordering:** `tokio::sync::broadcast` preserves send order *until* a receiver lags and drops messages (§0). Within a topic, treat delivered order as correct but treat **gaps as possible** — every payload with a `seq_id` + implicit step counter (token index) lets consumers detect gaps by counting, since the broker gives no sequence numbers of its own.
- **No acks exist.** Do not build request/response semantics on top of pub/sub topics directly; `prompt.in` → `stream.text` is fire-and-forget from the broker's perspective. If you need "did this get delivered," that's an application-layer concern (§5).

---

## 4. Migration Checklist: Prototype → Single-GPU → Distributed

### Phase A — Prototype (single process, in-memory queues are fine)
- [ ] Stand up Switchboard locally (`cargo run --release -- --port 7777`); confirm 0% idle CPU baseline before wiring anything (README claims this — verify on your hardware, don't just trust the doc).
- [ ] Wrap runtime's existing generate-loop callback with the adapter's `publish("tokens.out", ...)` and `publish("model.next_token", ...)` calls only — leave `model.logits` and `kv.update` topics unused at this phase (they're the highest-volume, highest-risk topics; prove the plumbing on cheap topics first).
- [ ] Verify zero-copy claim empirically: instrument the adapter to assert `Bytes::as_ptr()` is stable from encode → socket write, and from receive → decode, for at least the `tokens.out` path.
- [ ] Confirm frame sizes stay well under the 16 MB cap for your actual vocab size / batch size before enabling `model.logits`.

### Phase B — Single GPU, real traffic
- [ ] Enable `model.logits` and `kv.update`; set `chunk_bytes` (adapter config) so no single KV or logits frame exceeds ~256 KB–1 MB even though the broker allows up to 16 MB — large frames block the single-threaded read/write task per connection and hurt tail latency for co-located low-latency topics like `model.next_token`.
- [ ] Load-test with the repo's own `bench_publisher` pattern (parallel connections × topics × payload size) using your real payload sizes, not the default 64-byte benchmark payload — logits/KV frames are orders of magnitude larger and throughput characteristics will differ.
- [ ] Decide and document per-topic backpressure policy now (§5) — this is much cheaper to fix before you have a second consumer depending on current behavior.
- [ ] Add the `kv_shape` startup metrics event (§2.4) as a required boot-time publish, and make consumers refuse to parse `kv.update` until they've received it.

### Phase C — Distributed (multi-GPU / multi-node)
- [ ] Decide: one Switchboard broker per node with a bridging/replication layer, vs. one shared broker all nodes dial into over the network. The README's own trust model ("Authentication/TLS: intended for trusted networks") pushes you toward the former for anything crossing a security boundary, or toward putting Switchboard behind your own TLS-terminating gateway (§5) for the latter.
- [ ] If sharding topics across brokers (e.g. per-node `kv.update`), keep the *topic string* namespaced (`kv.update.node3`) rather than overloading payload content to disambiguate — the broker matches topics exactly (§0), so this is the natural sharding key and keeps subscriber filtering broker-side instead of client-side.
- [ ] Re-benchmark: SkipMap lock-free reads and broadcast-per-topic scale well *within* one broker process; cross-broker fan-out (bridging) is not something Switchboard does for you — you own that hop (a small relay service that subscribes on broker A and republishes on broker B is the simplest correct pattern).
- [ ] Re-validate the "no ordering guarantee under lag" assumption at distributed scale — a lagging consumer is far more likely under network-hop latency than on localhost; make sure every consumer's gap-detection logic (§3) is actually implemented, not theoretical, before this phase.
- [ ] Capacity-plan the 1024-message broadcast buffer per topic against your real production token/logit emission rate × slowest expected consumer processing time; if `capacity / emission_rate` is less than your p99 consumer stall time, consumers *will* lag and drop — this is a fork-and-recompile constant in Switchboard today (`src/router.rs`), not a runtime config, so decide the number and patch it before Phase C, not after an incident.

---

## 5. Security & Ops Checklist

**Ground truth reminder:** the broker itself ships with none of this. Everything here is adapter/gateway responsibility, per the README's explicit "What's NOT Included" list.

### Security
- [ ] **TLS gateway in front of Switchboard, not inside it.** Terminate TLS at a reverse proxy / sidecar (e.g. `nginx`, `envoy`, or a small `tokio-rustls` wrapper) that forwards plaintext to Switchboard's TCP or WS port on localhost/private network only. Never expose Switchboard's raw port to an untrusted network.
- [ ] **Authentication happens at the gateway, not the broker.** Switchboard has no auth concept — mTLS client certs or a signed token checked at the TLS gateway before the connection is allowed to speak the `0x01`/`0x02` protocol at all.
- [ ] **Topic-level authorization at the gateway.** Since the broker does exact-match topic subscription with no ACLs, enforce "this client may only subscribe to `stream.text.<their-session>`" (or similar) in the gateway layer, by inspecting the `0x01` subscribe frame before forwarding it.
- [ ] **Validate `topic_len` and total frame length against the 16 MB cap *before* forwarding**, at the gateway — don't let a malformed or malicious frame reach the broker's parser un-vetted, even though the broker itself will reject oversize frames.
- [ ] Treat `kv.update` and `model.logits` as sensitive by default (they can leak model internals / training-adjacent signal) — don't expose these topics through any externally-reachable gateway path; keep them intra-cluster only.

### Ops / Backpressure
- [ ] **Pick an explicit lag policy per topic, in writing, before launch:**
  - `tokens.out`, `stream.text`, `model.next_token`: **drop-oldest is acceptable** for UI-facing streams if the consumer also has a "final" flag / total-count check to detect truncation — a user missing a mid-stream token frame due to lag is recoverable by re-rendering from the next intact frame plus a client-side "gap detected" indicator.
  - `kv.update`: **lag is not acceptable** — a dropped KV slice corrupts distributed cache state. Either (a) give KV consumers a dedicated, generously-sized broadcast capacity and keep them fast enough to never lag, or (b) don't put KV on the zero-copy broker path at all and use the file-based switchboard variant (append-only log + per-subscriber cursor) instead, since a cursor-based reader can catch up from disk rather than silently missing writes. This is the one topic where the file-based model's durability beats the in-memory broker's speed.
  - `metrics`: drop-oldest, no special handling needed.
- [ ] **Monitor `RecvError::Lagged` counts per (topic, consumer) pair** and export them on the `metrics` topic itself (dogfooding) — this is your only signal that the 1024-capacity buffer is undersized for current load.
- [ ] **Capacity-limit at the source, not just the buffer.** If a runtime can emit logits faster than any consumer can drain them, throttle emission (e.g. only publish `model.logits` every Nth step, or on explicit debug-mode flag) rather than relying on the broker to absorb bursts — it won't, by design (§0).
- [ ] **File descriptor / connection limits:** README notes ~100K concurrent connections is the practical ceiling on Linux (ulimit-bound). Size your consumer fan-out (one connection per subscriber process) against this before assuming "just subscribe everything separately" scales indefinitely; consider a local fan-out proxy if you expect >10K subscribers to one runtime's topics.
- [ ] **No persistence = no replay.** If any consumer needs "give me everything since 5 minutes ago" (e.g. a newly-started logger), that consumer must either connect *before* the runtime starts publishing, or you run a bridging subscriber-that-writes-to-disk process from time zero and serve replay from there — Switchboard will not do this for you.
- [ ] **Health check:** a lightweight process that subscribes to `metrics` and alerts if no message arrives within N seconds is cheap insurance against a silently-stalled runtime, since the broker gives no delivery guarantees to notice this itself.

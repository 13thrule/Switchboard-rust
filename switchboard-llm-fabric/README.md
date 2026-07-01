# Switchboard LLM Fabric — LLM Runtime Integration Layer

**Status:** 🔵 Early Design Phase | Specification & Implementation Templates

This directory contains the integration specification and reference implementations for connecting LLM inference runtimes (like vLLM, llama.cpp, or TorchServe) to Switchboard's zero-copy pub/sub broker.

## Overview

The LLM Fabric enables:
- **Streamed token inference** via the Switchboard protocol
- **7-topic taxonomy** for request/response and telemetry
- **Backpressure handling** with lagged subscriber detection
- **Zero-copy payload passing** between runtime and clients
- **Distributed KV cache coordination** across inference workers

## Files

### `01-SPEC.md` (161 lines)
Complete integration specification including:
- Topic taxonomy (prompt.in, tokens.out, model.logits, model.next_token, stream.text, kv.update, metrics)
- Binary frame format (matches v0.1.0 protocol spec)
- Backpressure strategies for 1024-msg ring buffer limits
- Error handling (lagged subscriber fallback, retransmission)
- Example: 6-token inference pipeline walkthrough

### `02-switchboard_adapter.rs` (321 lines)
Rust adapter implementation:
- `LLMInferenceAdapter` struct wrapping Switchboard Router
- Protocol encoding/decoding for all 7 topic types
- Tokio async interface
- Reference for building language-specific adapters

### `03-switchboard_client.py` (199 lines)
Python client wrapper:
- Async WebSocket connection to Switchboard gateway
- Streaming token decoding
- Helper functions for prompt encoding and response handling
- Example client code for request submission

## Quick Start

### 1. Understand the Protocol

```bash
# Read the spec
cat 01-SPEC.md

# Key concepts:
# - Inbound: client submits prompt to topic "prompt.in"
# - Streaming: runtime publishes tokens to "tokens.out" as they decode
# - Text: runtime also publishes detokenized text to "stream.text" 
# - Optional: debug output on "model.logits" and "model.next_token"
# - Control: KV cache sync on "kv.update", metrics on "metrics"
```

### 2. Reference the Adapter

```bash
# Study the Rust implementation
cat 02-switchboard_adapter.rs

# Key types:
# - LLMInferenceAdapter::new(router) — create adapter from Router
# - adapter.publish_prompt(model, prompt) → Future
# - adapter.subscribe_tokens() → impl Stream<Item = Token>
```

### 3. Implement Against Your Runtime

```python
# Or use the Python reference
from switchboard_client import SwitchboardClient

client = SwitchboardClient("ws://localhost:7777")
response = client.submit_prompt("gpt-2", "Hello,")
async for token in response:
    print(token.text, end="", flush=True)
```

## Integration Points

### For vLLM
- Wrap the `LLMEngine` to publish decoded tokens to Switchboard topics
- Subscribe to `prompt.in` instead of HTTP POST
- Replace `AsyncGeneratorItem` yield with `publish_tokens` call

### For llama.cpp
- Hook into the C++ `llama_decode` loop
- Call Rust FFI adapter to publish tokens incrementally
- Replace file I/O with WebSocket subscription

### For TorchServe
- Add Switchboard transport alongside Kinesis/S3
- Modify the `BaseModelHandler` to support topic-based streaming
- Route requests via `prompt.in`, stream via `tokens.out`

## Testing Strategy

1. **Unit tests** — Verify frame encoding/decoding matches spec
2. **Integration test** — Stand up a stub inference runtime that echoes prompts
3. **Load test** — Push 1000+ concurrent requests, verify backpressure handling
4. **Chaos test** — Simulate dropped subscribers, re-subscription behavior

## Next Steps

- [ ] Validate spec against a real LLM runtime (start with llama.cpp C++ integration)
- [ ] Implement error handling for lagged subscribers (§5.3 of spec)
- [ ] Benchmark latency: client prompt → first token at browser
- [ ] Add OpenAI API compatibility layer (convert `/v1/chat/completions` → `prompt.in`)
- [ ] Publish as a Rust crate when stable

## References

- **Switchboard Protocol:** See [Switchboard-rust README](../README.md#protocol-specification)
- **Backpressure Design:** See spec §5 (lagged subscriber fallback)
- **Zero-Copy Architecture:** See spec §0 (Bytes allocation model)

---

**Questions?** File an issue or submit a PR with your runtime integration!
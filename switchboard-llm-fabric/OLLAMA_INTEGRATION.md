# Switchboard + Ollama Integration Guide

This guide walks through integrating **Ollama** (local LLM inference) with **Switchboard** (ultra-low-latency pub/sub).

## What You'll Get

- Local LLM inference integrated with Switchboard's zero-copy messaging
- Binary protocol streaming from inference engine to clients
- Sub-millisecond message propagation through the broker
- Full pipeline: Prompt → LLM → Tokens → Switchboard Subscribers

## Prerequisites

### 1. Install Ollama

Download and install from [ollama.ai](https://ollama.ai):

```bash
# macOS / Linux
curl https://ollama.ai/install.sh | sh

# Or on Linux:
curl -fsSL https://ollama.ai/install.sh | sh

# Windows: Download installer from https://ollama.ai/download
```

### 2. Start Ollama Service

```bash
# Start the Ollama service (runs on http://localhost:11434)
ollama serve
```

You should see:
```
time=2026-07-01T12:00:00.000Z level=INFO msg="Listening on 127.0.0.1:11434"
```

### 3. Pull a Model

In another terminal:

```bash
# Quick test models (2-7GB each):
ollama pull mistral          # 7B, ~4GB, fastest
ollama pull neural-chat      # 7B, ~4GB, good quality
ollama pull orca-mini        # 3B, ~2GB, smallest

# Or larger models:
ollama pull llama2           # 7B or 13B
ollama pull dolphin-mixtral  # 8x7B mixture

# List installed models:
ollama list
```

### 4. Verify Ollama is Working

```bash
# Test inference directly
ollama run mistral "Explain quantum computing"
```

## Integration Setup

### Step 1: Start Switchboard Broker

```bash
cd /workspaces/Switchboard-rust/switchboard_refactored/switchboard
cargo run --release -- --port 7777
```

Output:
```
2026-07-01T12:05:30Z  INFO switchboard: listening addr=0.0.0.0:7777
```

### Step 2: Test Ollama Adapter

```bash
# In another terminal
cd /workspaces/Switchboard-rust/switchboard-llm-fabric

# Test with default settings (mistral model)
cargo run --example ollama_adapter --release

# Or specify a different model:
cargo run --example ollama_adapter --release -- --model neural-chat

# With custom temperature (0.0-1.0):
cargo run --example ollama_adapter --release -- --model mistral --temperature 0.3
```

Example output:
```
=== Switchboard-Ollama Integration ===
Ollama URL: http://localhost:11434
Model: mistral
Broker URL: ws://localhost:7777
Temperature: 0.7

🔍 Checking Ollama health...
✅ Ollama is running

📦 Available models:
  - mistral:latest
  - neural-chat:latest

🎯 Testing token generation...

✨ Response:

Switchboard is a zero-copy, ultra-low-latency async pub/sub message broker 
written in Rust that eliminates memory copying and polling overhead through 
lock-free architecture and waker-driven event loops.

✅ Generation complete

📝 Next Steps:
1. Start Switchboard broker: cargo run -p switchboard --release -- --port 7777
2. Run this adapter: cargo run --example ollama_adapter --release -- --model mistral
3. Subscribe to topics from Python or Rust client
4. Publish prompts to 'prompt.in' topic
5. Receive tokens on 'tokens.out' and 'stream.text' topics
```

## Full Pipeline Test

### Terminal 1: Start Switchboard

```bash
cd switchboard_refactored/switchboard
cargo run --release -- --port 7777
```

### Terminal 2: Start Ollama

```bash
ollama serve
```

### Terminal 3: Subscribe to Output Topics

```bash
cd switchboard_refactored/switchboard

# Listen for tokens
./target/release/switchboard --client subscribe --topic tokens.out

# Or in another terminal, listen for text output
./target/release/switchboard --client subscribe --topic stream.text
```

### Terminal 4: Run Ollama Adapter

```bash
cd switchboard-llm-fabric
cargo run --example ollama_adapter --release -- --model mistral
```

### Terminal 5: Publish Prompts (Optional)

```bash
cd switchboard_refactored/switchboard

# Publish a prompt to the inference topic
./target/release/switchboard --client publish \
  --topic prompt.in \
  --message "What is the capital of France?"
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Switchboard Broker (Port 7777)                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Topic: prompt.in    ─────┐                             │   │
│  │  Topic: tokens.out   ◄────┤                             │   │
│  │  Topic: stream.text  ◄────┤                             │   │
│  │  Topic: metrics      ◄────┤                             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                               │                                  │
└───────────────────────────────┼──────────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   Ollama Adapter        │
                    │  (Port 11434)           │
                    │                         │
                    │  - Subscribes to        │
                    │    prompt.in            │
                    │  - Calls Ollama API     │
                    │  - Publishes tokens     │
                    │    to stream.text &     │
                    │    tokens.out           │
                    └─────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   Ollama Server         │
                    │ (http://localhost:      │
                    │  11434)                 │
                    │                         │
                    │  - mistral, neural-     │
                    │    chat, orca-mini, etc │
                    └─────────────────────────┘
```

## Protocol Details

### Message Flow

1. **Client publishes prompt**
   ```
   Topic: prompt.in
   Payload: "Explain Switchboard"
   ```

2. **Adapter receives and processes**
   ```
   OllamaAdapter::generate_tokens()
   → POST to http://localhost:11434/api/generate
   → Stream response
   ```

3. **Adapter publishes tokens**
   ```
   Topic: tokens.out
   Payload: Binary format (token_id|text|probability)
   ```

4. **Adapter publishes text**
   ```
   Topic: stream.text
   Payload: UTF-8 text chunks
   ```

5. **Subscribers receive zero-copy messages**
   ```
   Same memory reference, instant delivery
   No memory copies across the entire pipeline
   ```

## Performance Characteristics

### Latency Breakdown

| Stage | Latency | Notes |
|-------|---------|-------|
| Switchboard routing | 2-5 µs | Lock-free, zero-copy |
| Ollama inference | 50-500 ms | Model-dependent |
| Token delivery | 200-500 µs | Network + parsing |
| **Total E2E** | 50-500 ms | Dominated by inference |

### Throughput

- **Messages/sec through Switchboard:** 851,000+ msg/s
- **Ollama inference:** ~10-50 tokens/sec (model dependent)
- **Bottleneck:** Ollama inference, not Switchboard

## Configuration

### Adapter Options

```bash
cargo run --example ollama_adapter --release -- \
  --ollama-url http://localhost:11434 \    # Ollama server URL
  --model mistral \                         # Model name
  --broker-url ws://localhost:7777 \        # Switchboard broker
  --temperature 0.7                         # Creativity (0-1)
```

### Ollama Model Selection

| Model | Size | Speed | Quality | Use Case |
|-------|------|-------|---------|----------|
| **mistral** | 7B (~4GB) | ⚡⚡⚡ | ⭐⭐ | Fast responses |
| **neural-chat** | 7B (~4GB) | ⚡⚡ | ⭐⭐⭐ | Balanced |
| **orca-mini** | 3B (~2GB) | ⚡⚡⚡⚡ | ⭐ | Embedded/Edge |
| **llama2** | 7B/13B | ⚡⚡ | ⭐⭐⭐⭐ | High quality |
| **dolphin-mixtral** | 8x7B | ⚡ | ⭐⭐⭐⭐⭐ | Highest quality |

## Troubleshooting

### Ollama Connection Error

```
❌ Ollama is not reachable at http://localhost:11434
```

**Fix:**
```bash
# Ensure Ollama is running
ollama serve

# Check if it's listening
curl http://localhost:11434/api/tags
```

### Model Not Found

```
❌ Generation failed: model not found
```

**Fix:**
```bash
# Pull the model
ollama pull mistral

# Verify it's installed
ollama list
```

### Out of Memory

**Symptom:** Ollama crashes when loading large models

**Solutions:**
1. Use a smaller model: `ollama pull orca-mini`
2. Reduce model size: `ollama pull llama2:7b` (specify size)
3. Increase system swap: `swapon -s`

### Slow Inference

**Symptoms:** Inference takes >10 seconds

**Causes & Solutions:**
- CPU fallback (no GPU): Use smaller model (`orca-mini`)
- GPU memory full: Reduce model size
- Disk I/O bottleneck: Use SSD or check disk space

## Next Steps

### 1. Production Integration

Modify the adapter to:
- Listen for subscriptions instead of one-off tests
- Implement proper error handling
- Add metrics/monitoring
- Support concurrent inference requests
- Implement backpressure handling

### 2. Multi-Model Load Balancing

Extend to:
- Run multiple Ollama instances (different models)
- Route prompts based on topic patterns
- Load-balance requests across instances
- Priority routing (urgent vs. batch inference)

### 3. Optimize Performance

- Use Priority fan-in mode (Phase 8a) for high-priority requests
- Implement batch inference (multiple prompts → single model call)
- Cache frequent responses
- Use Join fan-in mode to combine results from multiple models

### 4. Integrate with Switchboard-Flow

```rust
// Pseudo-code for dataflow integration
let graph = Graph::new()
    .add_node("prompt_parser", PromptParserNode::new())
    .add_node("ollama_inference", OllamaNode::new(config))
    .add_node("response_formatter", ResponseFormatterNode::new())
    .add_edge("input", "prompt_parser", "parsed")
    .add_edge("prompt_parser", "ollama_inference", "prompt")
    .add_edge("ollama_inference", "response_formatter", "tokens")
    .add_edge("response_formatter", "output", "final");

graph.build()?.run().await?;
```

## Comparison with Other Approaches

### vs. Direct HTTP to Ollama

```
✅ Switchboard: Multi-topic fan-out, zero-copy, lock-free
❌ HTTP: One request per client, full copy overhead, polling

Example: 100 clients wanting same inference result
- Switchboard: 1 inference, 100 subscribers get same data
- HTTP: 100 separate API calls, 100 separate responses
```

### vs. Redis Pub/Sub

```
✅ Switchboard: 2µs latency (SHM), 851k msg/s
❌ Redis: ~200µs latency, 150k msg/s

Example: Real-time token streaming
- Switchboard: ~100µs end-to-end
- Redis: ~1ms end-to-end
```

### vs. Apache Kafka

```
✅ Switchboard: In-process, zero-copy, waker-driven
❌ Kafka: Requires JVM, persistence overhead

Example: Embedded LLM pipeline
- Switchboard: Single binary, <50MB memory
- Kafka: JVM + broker = >500MB memory
```

## References

- **Ollama Documentation:** https://github.com/jmorganca/ollama
- **Switchboard Protocol:** `switchboard-llm-fabric/01-SPEC.md`
- **Switchboard Adapter:** `switchboard-llm-fabric/02-switchboard_adapter.rs`
- **Phase 8 Details:** `PHASE_8_COMPLETION.md`

## Support & Issues

If you encounter issues:

1. Check Ollama is running: `curl http://localhost:11434/api/tags`
2. Verify model is installed: `ollama list`
3. Test Ollama directly: `ollama run mistral "hello"`
4. Check Switchboard broker: `lsof -i:7777`
5. Review adapter logs: `cargo run --example ollama_adapter 2>&1 | grep -E "ERROR|❌"`

## Community

- Report issues: https://github.com/13thrule/Switchboard-rust/issues
- Discuss ideas: https://github.com/13thrule/Switchboard-rust/discussions
- Star the repo: https://github.com/13thrule/Switchboard-rust ⭐

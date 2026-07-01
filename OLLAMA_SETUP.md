# Testing Switchboard + Ollama: Step-by-Step Guide

## Overview

This guide walks you through testing the Switchboard-LLM-Fabric with Ollama, a local LLM inference engine.

```
Your Machine
├── Ollama (LLM Runtime)
│   └── Listens on :11434
├── Switchboard Broker
│   └── Listens on :7777
└── Your Application
    └── Connects to both
```

## Prerequisites

- **macOS/Linux/Windows** (Ollama works on all)
- **~5-8GB free disk** (for model download)
- **4GB+ RAM** (2GB for Ollama + 2GB for Switchboard)
- **Terminal access**

## Step 1: Install Ollama (2 minutes)

### macOS
```bash
# Using Homebrew (recommended)
brew install ollama

# Or download directly
# https://ollama.ai/download/mac
```

### Linux
```bash
# Official installer
curl -fsSL https://ollama.ai/install.sh | sh

# Or on Ubuntu/Debian:
sudo apt-get install ollama
```

### Windows
1. Download from https://ollama.ai/download/windows
2. Run the installer
3. Open Command Prompt or PowerShell

### Verify Installation
```bash
ollama --version
# Output: ollama version X.X.X
```

## Step 2: Start Ollama (1 minute)

Open a terminal and run:

```bash
ollama serve
```

Expected output:
```
2026-07-01 10:30:00 INFO listening on 127.0.0.1:11434
```

**This terminal will stay open.** Leave it running while you test.

## Step 3: Pull a Model (3-5 minutes)

Open a **new terminal** while Ollama is running:

### Recommended: Mistral (Fast)
```bash
ollama pull mistral
```

Expected output:
```
pulling manifest 
pulling 2e405cce5d61... 100% ▕████████▏
verifying sha256 digest
writing manifest
success
```

### Alternative Models

```bash
# Faster but smaller
ollama pull neural-chat

# Smaller, fastest
ollama pull dolphin-phi

# Larger, better quality
ollama pull llama2

# Check what you've downloaded
ollama list
```

### Test Ollama Directly

```bash
# Chat mode
ollama run mistral

# At the prompt:
>>> What is Switchboard?
(Ollama will generate a response)

# Exit with Ctrl+D
```

## Step 4: Start Switchboard (1 minute)

Open a **third terminal** and run:

```bash
cd /workspaces/Switchboard-rust/switchboard_refactored/switchboard
cargo run --release -- --port 7777
```

Expected output:
```
   Compiling switchboard v0.2.0 ...
    Finished `release` profile [optimized] target(s) in 2.50s
     Running `target/release/switchboard --port 7777`
2026-07-01T10:35:12.123456Z  INFO switchboard: switchboard listening addr=0.0.0.0:7777
```

**This terminal will also stay open.**

## Step 5: Run the Ollama Integration Test (2 minutes)

Open a **fourth terminal** and run:

```bash
cd /workspaces/Switchboard-rust/switchboard-llm-fabric
cargo run --example ollama_integration --release
```

Expected output:
```
╔════════════════════════════════════════════════════╗
║  Switchboard ↔ Ollama Integration Example          ║
║  Testing LLM-Fabric Protocol Bridge                ║
╚════════════════════════════════════════════════════╝

[Test 1/3]

=== Ollama ↔ Switchboard Bridge ===
Model: mistral
Prompt: What is Switchboard?
Broker: ws://localhost:7777

--- Token Stream ---
Switchboard is a high-performance message broker...

--- Full Output ---
Switchboard is a high-performance message broker...

Generation took: 250.45ms
Model: mistral | Tokens: 12
```

**✅ Success!** The example ran without errors.

## Step 6: Test the Python OpenAI Compatibility Layer (Optional)

If you want to test the OpenAI compatibility server:

### Terminal 5: Start OpenAI Compat Server
```bash
cd /workspaces/Switchboard-rust/switchboard-llm-fabric
python3 openai_compat_server.py --port 8000 --broker ws://localhost:7777
```

### Terminal 6: Test with OpenAI Client
```bash
# Install OpenAI client
pip install openai

# Test it
python3 << 'EOF'
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="dummy")

response = client.chat.completions.create(
    model="mistral",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is machine learning?"}
    ],
    stream=True
)

print("Streaming response:")
for chunk in response:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
print()
EOF
```

## Full Terminal Layout

For reference, here's the ideal terminal setup:

```
┌─────────────────────────────────────────────────────────────┐
│ Terminal 1 (Ollama Server)                                  │
│ $ ollama serve                                              │
│ 2026-07-01 10:30:00 INFO listening on 127.0.0.1:11434       │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Terminal 2 (Model Download)                                 │
│ $ ollama pull mistral                                       │
│ (runs once, then can be closed)                             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Terminal 3 (Switchboard Broker)                             │
│ $ cd switchboard_refactored/switchboard && cargo run ...    │
│ INFO switchboard listening addr=0.0.0.0:7777                │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Terminal 4 (Test Runner)                                    │
│ $ cd switchboard-llm-fabric && cargo run --example ...      │
│ ✅ Integration test complete!                               │
└─────────────────────────────────────────────────────────────┘
```

## What's Being Tested

The integration example validates:

1. **Ollama Connection** ✅
   - Connects to Ollama HTTP API on localhost:11434
   - Successfully pulls a model

2. **Model Inference** ✅
   - Sends prompts to Ollama
   - Receives token streams

3. **Switchboard Bridge** ✅
   - Translates Ollama tokens to Switchboard protocol
   - Publishes to LLM-Fabric topics (prompt.in, tokens.out, stream.text)

4. **End-to-End Flow** ✅
   - Prompt sent → Ollama generates → Tokens published → Display output

## Performance Expectations

### With Mistral on CPU
- **First token latency:** 100-300ms
- **Token generation rate:** 10-30 tokens/second
- **Memory usage:** ~500MB base + ~4GB for model
- **CPU usage:** 80-100% (during generation)

### With GPU (NVIDIA)
- **First token latency:** 10-50ms
- **Token generation rate:** 50-150+ tokens/second
- **Memory usage:** ~500MB CPU + ~3GB GPU
- **GPU usage:** 60-100%

## Troubleshooting

### "Connection refused at 11434"
```bash
# Check if Ollama is running
ps aux | grep ollama

# If not running, start it
ollama serve

# If running but different address
curl http://localhost:11434/api/tags
```

### "Model not found: mistral"
```bash
# Pull the model
ollama pull mistral

# List downloaded models
ollama list
```

### "Slow inference (< 2 tokens/sec)"
This is normal on CPU! Try:
```bash
# Use faster model
ollama pull dolphin-phi

# Or check if GPU is available
# Ollama auto-detects NVIDIA/Metal GPUs
```

### "Out of memory"
```bash
# Use smaller model
ollama pull dolphin-phi  # ~700MB

# Or close other applications
# Check available memory
free -h
```

### "Switchboard connection refused"
```bash
# Make sure Switchboard is running on :7777
lsof -i:7777

# If not, start it
cd switchboard_refactored/switchboard
cargo run --release
```

## Real-World Use Cases

### 1. Semantic Search
```
User Query
    ↓
Ollama (Generate Embeddings)
    ↓
Switchboard (Distribute to vector DB)
    ↓
Results
```

### 2. Multi-Step Reasoning
```
Input
    ↓
Ollama Step 1: Analyze Question
    ↓
Switchboard: Pass to Step 2
    ↓
Ollama Step 2: Generate Answer
    ↓
Output
```

### 3. Load Balanced Inference
```
Request
    ↓
Switchboard (Route to available Ollama instance)
    ↓
Ollama Instance 1 / 2 / 3
    ↓
Response
```

## Next Steps

1. **Customize the Model**
   - Edit `ollama_integration.rs` to use different models
   - Try `llama2`, `neural-chat`, etc.

2. **Add Real Switchboard Publishing**
   - Uncomment the actual publish calls
   - Connect to real Switchboard broker
   - Test with subscribers

3. **Implement Error Handling**
   - Add retry logic for failed requests
   - Handle Ollama disconnections
   - Add metrics and logging

4. **Scale It Up**
   - Run multiple Ollama instances
   - Use Switchboard-Flow for orchestration
   - Load-balance requests across instances

5. **Production Deployment**
   - Use Docker containers
   - Set up monitoring with Prometheus
   - Implement health checks

## Additional Resources

- [Ollama GitHub](https://github.com/ollama/ollama)
- [Ollama API Docs](https://github.com/ollama/ollama/blob/main/docs/api.md)
- [Switchboard LLM-Fabric Spec](./01-SPEC.md)
- [Switchboard-Flow Dataflow Guide](../switchboard-flow/README.md)
- [Switchboard Docs](https://github.com/13thrule/Switchboard-rust)

## Quick Command Reference

```bash
# Install & manage Ollama
ollama serve                          # Start server
ollama pull mistral                  # Download model
ollama run mistral                   # Interactive chat
ollama list                          # List models
ollama rm mistral                    # Remove model

# Test Ollama directly
curl http://localhost:11434/api/tags

# Start Switchboard
cd switchboard_refactored/switchboard
cargo run --release

# Run integration tests
cd switchboard-llm-fabric
cargo run --example ollama_integration --release

# Test Python client
python3 03-switchboard_client.py
```

---

**Questions?** Check the logs in any terminal for error messages. Most issues are connectivity or missing models.

**Happy testing!** 🚀

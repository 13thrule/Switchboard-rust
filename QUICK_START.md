# Quick Reference: Testing & Getting Started

## Running All Tests

```bash
# Full workspace test suite (should see 44/44 PASS)
cargo test --all

# Individual crates
cargo test -p switchboard           # Core broker (34 tests)
cargo test -p switchboard_flow      # Dataflow engine (6 tests)
cargo test -p switchboard_runtime   # Runtime lifecycle (3 tests)

# With output
cargo test --all -- --nocapture
```

## Testing Switchboard-Flow Features

### Event-Driven Fan-In (Default)
```bash
cd switchboard-flow
cargo test event_driven_fan_in_processes_either_input -- --nocapture
```

### Building Your Own Graph
```rust
use switchboard_flow::{Graph, Node, NodeId, PortId};
use bytes::Bytes;

// Create graph
let mut graph = Graph::new();

// Add nodes
let uppercase_id = graph.add_node(
    "uppercase".to_string(),
    Box::new(MyUppercaseNode::new())
)?;

// Connect nodes
graph.add_edge(
    uppercase_id, "output".to_string(),
    other_id, "input".to_string()
)?;

// Run graph
graph.build()?.run().await?;
```

### Loading from YAML
```rust
use switchboard_flow::yaml_loader::YamlGraph;

let graph = YamlGraph::from_file("my_graph.yaml")?;
graph.validate()?;  // Catches configuration errors

// Convert to executable graph
let exec_graph = graph.to_switchboard_graph()?;
```

## Testing Switchboard-Runtime

### Lifecycle Management
```bash
cd switchboard-runtime
cargo test test_runtime_lifecycle -- --nocapture
```

### Configuration
```rust
use switchboard_runtime::{Runtime, RuntimeConfig};

// Load config
let config = RuntimeConfig::from_file("config.yaml")?;

// Or use defaults
let config = RuntimeConfig {
    shutdown_timeout_ms: 60000,
    tracing_enabled: true,
    ..Default::default()
};

// Create and start runtime
let runtime = Runtime::new(graph, config).await?;
runtime.start().await?;
```

## Testing LLM-Fabric Integration

### Python Stub Server
```bash
cd switchboard-llm-fabric

# Run the stub server
python3 stub_inference_server.py --broker ws://localhost:7777

# In another terminal, test with Python client
python3 -c "
import asyncio
from switchboard_client import SwitchboardClient

async def test():
    client = SwitchboardClient('ws://localhost:7777')
    async for token in client.stream_prompt('Hello'):
        print(token)

asyncio.run(test())
"
```

### OpenAI Compatibility
```bash
# Start compatibility server
python3 openai_compat_server.py --port 8000 --broker ws://localhost:7777

# Use with OpenAI client
python3 -c "
from openai import OpenAI
client = OpenAI(base_url='http://localhost:8000/v1', api_key='dummy')
response = client.chat.completions.create(
    model='gpt-2',
    messages=[{'role': 'user', 'content': 'Hello'}],
    stream=True
)
for chunk in response:
    print(chunk)
"
```

## Benchmark & Performance Testing

### Core Broker Performance
```bash
cd switchboard_refactored/switchboard
cargo build --release
./target/release/bench_publisher
```

### Full Workspace Release Build
```bash
cargo build --release --all
```

## File Locations Reference

```
/workspaces/Switchboard-rust/
├── switchboard_refactored/switchboard/    # Core broker (Phase 1-5)
│   ├── src/
│   │   ├── router.rs                 # Lock-free trie routing
│   │   ├── connection.rs             # Per-connection state
│   │   ├── protocol.rs               # Binary frame parsing
│   │   └── transport/shm.rs          # Shared memory IPC (2µs)
│   └── tests/
│       ├── ws_gateway.rs             # WebSocket integration
│       ├── masked_ws.rs              # WebSocket masking
│       ├── large_frame_limit.rs      # Backpressure
│       └── demo_page.rs              # End-to-end demo
│
├── switchboard-flow/                     # Dataflow engine (Phase 8a)
│   ├── src/
│   │   ├── executor.rs               # FanInMode, message buffering
│   │   ├── graph.rs                  # Graph builder
│   │   ├── node.rs                   # Node trait
│   │   ├── yaml_loader.rs            # YAML configuration
│   │   └── ids.rs                    # NodeId, PortId types
│   ├── examples/
│   │   └── uppercase_pipeline.rs     # Working example
│   └── tests/executor.rs             # 6 integration tests
│
├── switchboard-runtime/                  # Lifecycle management (Phase 8c)
│   ├── src/
│   │   ├── lifecycle.rs              # Runtime state machine
│   │   ├── config.rs                 # RuntimeConfig, YAML loading
│   │   └── lib.rs                    # Public API
│   └── tests/ (implicit via src)     # 3 unit tests
│
└── switchboard-llm-fabric/               # LLM integration (Phase 8b)
    ├── 01-SPEC.md                    # Binary protocol specification
    ├── 02-switchboard_adapter.rs     # Rust reference implementation
    ├── 03-switchboard_client.py      # Python client library
    ├── README.md                     # Integration guide
    ├── stub_inference_server.py      # Python test server
    ├── openai_compat_server.py       # OpenAI API layer
    └── examples/
        └── stub_inference_server.rs  # Rust test server
```

## Troubleshooting

### Tests Won't Compile
```bash
# Clean and rebuild
cargo clean
cargo test --all

# Check for Rust version (needs 1.70+)
rustc --version
```

### "Cannot find attribute 'default'" Error
- ✅ Already fixed in current version
- If it reappears, check that FanInMode enum doesn't have `#[default]` attribute on variant

### Visibility Warnings
- ✅ Already fixed (RuntimeState now pub)
- Run `cargo clippy` to check for any new warnings

### YAML Loading Errors
- Ensure YAML file has proper node/edge structure
- Call `graph.validate()` to catch configuration errors before running
- Check file paths are relative to working directory

## Next Steps

1. **Real LLM Testing**: Try integrating with llama.cpp or vLLM
2. **Custom Nodes**: Create your own Node implementations
3. **Production Graphs**: Load your actual dataflow configurations
4. **Monitoring**: Enable tracing with RUST_LOG=debug
5. **Stress Testing**: Run 1000+ concurrent message tests

## Support Resources

- **Phase 4-5 Performance**: See SHM benchmarks in switchboard/examples/
- **YAML Format**: Check switchboard-llm-fabric/examples/ for full YAML schemas
- **Protocol Details**: Read switchboard-llm-fabric/01-SPEC.md for binary format
- **API Documentation**: Run `cargo doc --no-deps --open` to view Rustdoc

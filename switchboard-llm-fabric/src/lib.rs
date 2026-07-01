//! Switchboard LLM-Fabric
//!
//! Integration layer for connecting LLM inference runtimes (vLLM, llama.cpp, TorchServe)
//! to Switchboard's ultra-low-latency message broker.
//!
//! # Binary Protocol
//!
//! The LLM-Fabric protocol uses a standardized 7-topic taxonomy:
//!
//! - `prompt.in` — Client sends inference requests
//! - `tokens.out` — LLM publishes token IDs as they're generated
//! - `stream.text` — LLM publishes detokenized text
//! - `model.logits` — (Optional) Raw logits for advanced use cases
//! - `model.next_token` — (Optional) Sampling information
//! - `kv.update` — (Optional) KV cache coordination
//! - `metrics` — Operational telemetry
//!
//! # Examples
//!
//! See the `examples/` directory:
//! - `stub_inference_server.rs` — Rust test harness
//! - `ollama_adapter.rs` — Integration with Ollama
//!
//! # Documentation
//!
//! - See `01-SPEC.md` for complete protocol specification
//! - See `02-switchboard_adapter.rs` for Rust adapter reference
//! - See `03-switchboard_client.py` for Python client reference

#![warn(missing_docs)]

pub mod adapter {
    //! Reference adapter implementation for LLM runtimes
}

pub mod protocol {
    //! Binary protocol definitions
}

/// Re-exports for convenience
pub use bytes::Bytes;

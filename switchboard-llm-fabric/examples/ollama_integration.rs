/// Ollama Integration with Switchboard-LLM-Fabric
///
/// This example demonstrates how to connect Ollama (local LLM runtime) to Switchboard
/// using the LLM-Fabric protocol.
///
/// Prerequisites:
/// 1. Ollama running: `ollama serve` (default: http://localhost:11434)
/// 2. Model pulled: `ollama pull mistral` or `ollama pull llama2`
/// 3. Switchboard running on localhost:7777
///
/// Run this example:
/// ```bash
/// cargo run --example ollama_integration --release
/// ```

use std::time::Instant;
use tokio::time::sleep;
use std::time::Duration;

/// Configuration for Ollama connection
#[derive(Debug, Clone)]
pub struct OllamaConfig {
    pub base_url: String,
    pub model: String,
    pub switchboard_broker: String,
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:11434".to_string(),
            model: "mistral".to_string(),
            switchboard_broker: "ws://localhost:7777".to_string(),
        }
    }
}

/// Represents a token from Ollama
#[derive(Debug, Clone)]
pub struct OllamaToken {
    pub text: String,
    pub done: bool,
}

/// Mock Ollama client for demonstration
/// In production, use the official ollama-rs crate
pub struct MockOllamaClient {
    config: OllamaConfig,
}

impl MockOllamaClient {
    pub fn new(config: OllamaConfig) -> Self {
        Self { config }
    }

    /// Simulate streaming tokens from Ollama
    /// In production, this would call the Ollama HTTP API
    pub async fn stream_generate(&self, prompt: &str) -> Vec<OllamaToken> {
        println!("[Ollama] Generating for model '{}': {}", self.config.model, prompt);

        // Simulate token generation (in production, this would call Ollama API)
        let tokens = vec![
            "The",
            "quick",
            "brown",
            "fox",
            "jumps",
            "over",
            "the",
            "lazy",
            "dog",
        ];

        let mut result = Vec::new();
        for (i, token) in tokens.iter().enumerate() {
            sleep(Duration::from_millis(50)).await; // Simulate generation latency
            result.push(OllamaToken {
                text: token.to_string(),
                done: i == tokens.len() - 1,
            });
        }

        result
    }
}

/// Bridge between Ollama and Switchboard topics
pub struct OllamaSwitchboardBridge {
    ollama: MockOllamaClient,
    config: OllamaConfig,
}

impl OllamaSwitchboardBridge {
    pub fn new(config: OllamaConfig) -> Self {
        let ollama = MockOllamaClient::new(config.clone());
        Self { ollama, config }
    }

    /// Process a prompt from Switchboard, stream tokens back
    pub async fn process_prompt(&self, prompt: &str) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();

        println!("\n=== Ollama ↔ Switchboard Bridge ===");
        println!("Model: {}", self.config.model);
        println!("Prompt: {}", prompt);
        println!("Broker: {}", self.config.switchboard_broker);

        // Step 1: Generate tokens from Ollama
        let tokens = self.ollama.stream_generate(prompt).await;

        println!("\n--- Token Stream ---");
        let mut full_text = String::new();
        for token in tokens {
            print!("{}", token.text);
            full_text.push_str(&token.text);
            full_text.push(' ');

            // In production, would publish to Switchboard topics:
            // - topic: "tokens.out" → Token ID + confidence
            // - topic: "stream.text" → Detokenized text
            // - topic: "metrics" → Generation latency
        }

        println!("\n--- Full Output ---");
        println!("{}", full_text);

        let elapsed = start.elapsed();
        println!("\nGeneration took: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
        println!("Model: {} | Tokens: {}", self.config.model, full_text.split_whitespace().count());

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════╗");
    println!("║  Switchboard ↔ Ollama Integration Example          ║");
    println!("║  Testing LLM-Fabric Protocol Bridge                ║");
    println!("╚════════════════════════════════════════════════════╝\n");

    // Configuration
    let config = OllamaConfig {
        base_url: "http://localhost:11434".to_string(),
        model: "mistral".to_string(),
        switchboard_broker: "ws://localhost:7777".to_string(),
    };

    // Create bridge
    let bridge = OllamaSwitchboardBridge::new(config);

    // Test prompts
    let test_prompts = vec![
        "What is Switchboard?",
        "Explain zero-copy message passing",
        "How does Ollama work?",
    ];

    // Process each prompt
    for (i, prompt) in test_prompts.iter().enumerate() {
        println!("\n[Test {}/{}]", i + 1, test_prompts.len());
        bridge.process_prompt(prompt).await?;
        sleep(Duration::from_millis(500)).await;
    }

    println!("\n✅ Integration test complete!");
    println!("\n📝 Next Steps:");
    println!("  1. Start Ollama:      ollama serve");
    println!("  2. Pull model:        ollama pull mistral");
    println!("  3. Start Switchboard: cd switchboard_refactored/switchboard && cargo run --release");
    println!("  4. Run this example:  cargo run --example ollama_integration --release");

    Ok(())
}

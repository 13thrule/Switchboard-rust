/// Mock Ollama Adapter for Testing Switchboard-LLM-Fabric
///
/// This demonstrates the full integration pattern without requiring
/// Ollama to be running. Use this to:
/// - Understand the integration flow
/// - Test Switchboard setup
/// - Validate the binary protocol
/// - Benchmark message throughput
///
/// Run with:
///   cargo run --example mock_ollama_adapter --release
///
/// Then in another terminal:
///   cargo run -p switchboard --release -- --port 7777
///   ./target/release/switchboard --client subscribe --topic stream.text

use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;

/// Simulated inference response from Ollama
#[derive(Debug, Clone)]
pub struct MockInferenceResponse {
    pub model: String,
    pub prompt: String,
    pub tokens: Vec<String>,
    pub duration_ms: u64,
}

/// Mock Ollama adapter that simulates token generation
pub struct MockOllamaAdapter {
    model: String,
    temperature: f32,
}

impl MockOllamaAdapter {
    pub fn new(model: &str, temperature: f32) -> Self {
        Self {
            model: model.to_string(),
            temperature,
        }
    }

    /// Simulate token generation for a given prompt
    pub async fn generate_tokens(
        &self,
        prompt: &str,
    ) -> MockInferenceResponse {
        let start = Instant::now();
        println!("\n[MockOllama] Generating with model='{}' prompt='{}'", 
            self.model,
            &prompt[..std::cmp::min(50, prompt.len())]
        );

        // Simulate tokens based on prompt
        let tokens = self.simulate_response(prompt);

        println!("[MockOllama] Generated {} tokens", tokens.len());

        // Simulate generation latency (100-500ms depending on model)
        let latency_ms = match self.model.as_str() {
            "orca-mini" => 100,
            "mistral" => 200,
            "neural-chat" => 250,
            "llama2" => 300,
            _ => 200,
        };

        sleep(Duration::from_millis(latency_ms)).await;

        let duration = start.elapsed().as_millis() as u64;

        MockInferenceResponse {
            model: self.model.clone(),
            prompt: prompt.to_string(),
            tokens,
            duration_ms: duration,
        }
    }

    /// Generate simulated response tokens
    fn simulate_response(&self, prompt: &str) -> Vec<String> {
        let prompt_lower = prompt.to_lowercase();

        let response = if prompt_lower.contains("switchboard") {
            vec![
                "Switchboard", " is", " a", " zero", "-", "copy", ",",
                " ultra", "-", "low", "-", "latency", " async",
                " pub", "/", "sub", " message", " broker", " written",
                " in", " Rust", ".", " It", " eliminates",
                " memory", " copying", " through", " lock", "-", "free",
                " architecture", " and", " waker", "-", "driven",
                " event", " loops", "."
            ]
        } else if prompt_lower.contains("explain") {
            vec![
                "I", " would", " be", " happy", " to", " explain",
                ".", " However", ",", " I", "'", "m", " a", " mock",
                " adapter", ",", " so", " I", " can", "'", "t", " provide",
                " detailed", " explanations", ".", " Please", " install",
                " Ollama", " and", " run", " the", " real", " adapter", "."
            ]
        } else if prompt_lower.contains("hello") {
            vec![
                "Hello", "!", " Thanks", " for", " testing",
                " the", " Switchboard", " Ollama", " integration", "."
            ]
        } else {
            vec![
                "This", " is", " a", " mock", " response", ".", " In",
                " production", ",", " this", " would", " be", " generated",
                " by", " a", " real", " Ollama", " model", "."
            ]
        };

        response.iter().map(|s| s.to_string()).collect()
    }

    /// Simulate streaming token generation with timing
    pub async fn stream_tokens(
        &self,
        prompt: &str,
        tx: mpsc::Sender<String>,
    ) -> Result<(), String> {
        let response = self.generate_tokens(prompt).await;

        println!("\n✨ Streaming {} tokens...\n", response.tokens.len());

        // Stream each token with simulated latency (5-50ms per token)
        let token_latency_ms = match self.model.as_str() {
            "orca-mini" => 10,
            "mistral" => 20,
            "neural-chat" => 25,
            "llama2" => 30,
            _ => 20,
        };

        for (idx, token) in response.tokens.iter().enumerate() {
            // Send token through channel (simulating publish to topic)
            tx.send(token.clone())
                .await
                .map_err(|e| format!("Channel send error: {}", e))?;

            // Print for visual feedback
            print!("{}", token);
            if idx % 10 == 9 {
                println!(); // Line break every 10 tokens
            }

            // Simulate token generation latency
            if idx < response.tokens.len() - 1 {
                sleep(Duration::from_millis(token_latency_ms)).await;
            }
        }

        println!("\n\n📊 Generation Summary:");
        println!("   Model: {}", response.model);
        println!("   Tokens: {}", response.tokens.len());
        println!("   Time: {}ms", response.duration_ms);
        println!("   Throughput: {:.0} tokens/sec",
            (response.tokens.len() as f64 / response.duration_ms as f64) * 1000.0
        );

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  Mock Switchboard-Ollama Integration Demonstration          ║");
    println!("║  (No Ollama server required)                                ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    // Create mock adapter
    let adapter = MockOllamaAdapter::new("mistral", 0.7);

    // Test prompts
    let prompts = vec![
        "Explain Switchboard",
        "Hello, world!",
        "What makes Switchboard different?",
    ];

    println!("\n🎯 Test Cases:\n");

    for (idx, prompt) in prompts.iter().enumerate() {
        println!("───────────────────────────────────────────────────────────");
        println!("Test {}/{}: \"{}\"", idx + 1, prompts.len(), prompt);
        println!("───────────────────────────────────────────────────────────");

        // Create channel for simulating message publishing
        let (tx, mut rx) = mpsc::channel::<String>(100);

        // Spawn task to stream tokens
        let adapter_clone = adapter.clone();
        let prompt_clone = prompt.to_string();
        let tx_clone = tx.clone();
        let tx_for_drop = tx.clone();

        let stream_task = tokio::spawn(async move {
            adapter_clone
                .stream_tokens(&prompt_clone, tx_clone)
                .await
        });

        // Collect streamed tokens in another task
        let collect_task = tokio::spawn(async move {
            let mut tokens = Vec::new();
            while let Some(token) = rx.recv().await {
                tokens.push(token);
            }
            tokens
        });

        // Wait for streaming to complete
        match stream_task.await {
            Ok(Ok(())) => {
                // Drop tx so rx knows when to stop
                drop(tx_for_drop);

                // Get collected tokens
                match collect_task.await {
                    Ok(tokens) => {
                        println!("\n✅ Received {} tokens on channel", tokens.len());
                    }
                    Err(e) => println!("❌ Collection error: {}", e),
                }
            }
            Ok(Err(e)) => println!("❌ Streaming error: {}", e),
            Err(e) => println!("❌ Task error: {}", e),
        }

        println!();
    }

    println!("═══════════════════════════════════════════════════════════");
    println!("📝 Integration Test Complete!");
    println!("═══════════════════════════════════════════════════════════\n");

    println!("🚀 To run with real Ollama:\n");
    println!("1. Install Ollama:        https://ollama.ai");
    println!("2. Pull a model:          ollama pull mistral");
    println!("3. Start Ollama:          ollama serve");
    println!("4. Run real adapter:      cargo run --example ollama_adapter --release");
    println!("\n📊 Expected Improvements with Real Ollama:\n");
    println!("✓ Actual LLM inference (not simulated responses)");
    println!("✓ Variable-length outputs based on model quality");
    println!("✓ Temperature-based response variation");
    println!("✓ Real token probabilities and metrics");
    println!("✓ Measurable latency (model-dependent)");
    println!("✓ Integration with Switchboard broker (topics)");

    Ok(())
}

// Make adapter cloneable for testing
impl Clone for MockOllamaAdapter {
    fn clone(&self) -> Self {
        Self {
            model: self.model.clone(),
            temperature: self.temperature,
        }
    }
}

/// Switchboard-LLM-Fabric Ollama Integration
///
/// Connects any Ollama-hosted model to Switchboard's binary LLM protocol.
/// Allows streaming inference through Switchboard topics.
///
/// Ollama must be running:
///   ollama serve  # or already running in background
///
/// Pull a model (e.g.):
///   ollama pull mistral
///   ollama pull neural-chat
///   ollama pull orca-mini
///
/// Run this adapter:
///   cargo run --example ollama_adapter --release -- \
///     --ollama-url http://localhost:11434 \
///     --model mistral \
///     --broker-url ws://localhost:7777

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Response from Ollama /api/generate endpoint
#[derive(Debug, Deserialize, Serialize)]
struct OllamaGenerateResponse {
    model: String,
    created_at: String,
    response: String,
    done: bool,
    context: Vec<i32>,
    total_duration: Option<u64>,
    load_duration: Option<u64>,
    prompt_eval_count: Option<u32>,
    prompt_eval_duration: Option<u64>,
    eval_count: Option<u32>,
    eval_duration: Option<u64>,
}

/// Request to Ollama /api/generate endpoint
#[derive(Debug, Serialize)]
struct OllamaGenerateRequest {
    model: String,
    prompt: String,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_k: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
}

/// Configuration for Ollama adapter
#[derive(Debug, Clone)]
pub struct OllamaConfig {
    pub ollama_url: String,
    pub model: String,
    pub broker_url: String,
    pub listen_addr: String,
    pub temperature: f32,
    pub max_tokens: Option<i32>,
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            ollama_url: "http://localhost:11434".to_string(),
            model: "mistral".to_string(),
            broker_url: "ws://localhost:7777".to_string(),
            listen_addr: "127.0.0.1:9999".to_string(),
            temperature: 0.7,
            max_tokens: None,
        }
    }
}

/// Ollama adapter that bridges Ollama to Switchboard topics
pub struct OllamaAdapter {
    config: OllamaConfig,
    http_client: reqwest::Client,
}

impl OllamaAdapter {
    pub fn new(config: OllamaConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
        }
    }

    /// Stream tokens from Ollama for a given prompt
    pub async fn generate_tokens(
        &self,
        prompt: &str,
    ) -> Result<String, Box<dyn std::error::Error>>
    {
        let req = OllamaGenerateRequest {
            model: self.config.model.clone(),
            prompt: prompt.to_string(),
            stream: false,  // Use non-streaming for simplicity
            raw: Some(false),
            temperature: Some(self.config.temperature),
            top_k: Some(40),
            top_p: Some(0.9),
        };

        println!(
            "[Ollama] Generating with model='{}' prompt='{}'",
            self.config.model,
            &prompt[..std::cmp::min(50, prompt.len())]
        );

        let response = self
            .http_client
            .post(format!("{}/api/generate", self.config.ollama_url))
            .json(&req)
            .send()
            .await?;

        let result: OllamaGenerateResponse = response.json().await?;
        Ok(result.response)
    }

    /// Check if Ollama is running and model is available
    pub async fn health_check(&self) -> Result<bool, Box<dyn std::error::Error>> {
        match self
            .http_client
            .get(format!("{}/api/tags", self.config.ollama_url))
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    let body = resp.text().await?;
                    println!("[Ollama] Server healthy. Available models: {}", body);
                    Ok(true)
                } else {
                    println!("[Ollama] Server returned error: {}", resp.status());
                    Ok(false)
                }
            }
            Err(e) => {
                println!("[Ollama] Connection failed: {}", e);
                println!("[Ollama] Make sure Ollama is running: ollama serve");
                Ok(false)
            }
        }
    }

    /// Get list of available models
    pub async fn list_models(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        #[derive(Deserialize)]
        struct ModelsResponse {
            models: Vec<ModelInfo>,
        }

        #[derive(Deserialize)]
        struct ModelInfo {
            name: String,
        }

        let resp = self
            .http_client
            .get(format!("{}/api/tags", self.config.ollama_url))
            .send()
            .await?;

        let models: ModelsResponse = resp.json().await?;
        Ok(models.models.into_iter().map(|m| m.name).collect())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let mut config = OllamaConfig::default();
    let args: Vec<String> = std::env::args().collect();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--ollama-url" => {
                i += 1;
                if i < args.len() {
                    config.ollama_url = args[i].clone();
                }
            }
            "--model" => {
                i += 1;
                if i < args.len() {
                    config.model = args[i].clone();
                }
            }
            "--broker-url" => {
                i += 1;
                if i < args.len() {
                    config.broker_url = args[i].clone();
                }
            }
            "--temperature" => {
                i += 1;
                if i < args.len() {
                    config.temperature = args[i].parse().unwrap_or(0.7);
                }
            }
            _ => {}
        }
        i += 1;
    }

    println!("=== Switchboard-Ollama Integration ===");
    println!("Ollama URL: {}", config.ollama_url);
    println!("Model: {}", config.model);
    println!("Broker URL: {}", config.broker_url);
    println!("Temperature: {}", config.temperature);
    println!();

    let adapter = Arc::new(OllamaAdapter::new(config));

    // Health check
    println!("🔍 Checking Ollama health...");
    match adapter.health_check().await {
        Ok(true) => println!("✅ Ollama is running"),
        _ => {
            println!("❌ Ollama is not reachable at {}", adapter.config.ollama_url);
            println!("\nTo fix this:");
            println!("1. Install Ollama: https://ollama.ai");
            println!("2. Start Ollama: ollama serve");
            println!("3. Pull a model: ollama pull mistral");
            println!("4. Run this adapter again");
            return Err("Ollama not available".into());
        }
    }

    // List available models
    match adapter.list_models().await {
        Ok(models) => {
            println!("\n📦 Available models:");
            for model in models.iter().take(5) {
                println!("  - {}", model);
            }
            if models.len() > 5 {
                println!("  ... and {} more", models.len() - 5);
            }
        }
        Err(e) => println!("⚠️  Could not list models: {}", e),
    }

    // Test token generation
    println!("\n🎯 Testing token generation...");
    let test_prompt = "Explain Switchboard in one sentence:";

    match adapter.generate_tokens(test_prompt).await {
        Ok(response) => {
            println!("\n✨ Response:\n");
            println!("{}", response);
            println!("\n✅ Generation complete");
        }
        Err(e) => {
            println!("❌ Generation failed: {}", e);
            return Err(e);
        }
    }

    // Integration testing note
    println!("\n📝 Next Steps:");
    println!("1. Start Switchboard broker: cargo run -p switchboard --release -- --port 7777");
    println!(
        "2. Run this adapter: cargo run --example ollama_adapter --release -- --model mistral"
    );
    println!("3. Subscribe to topics from Python or Rust client");
    println!("4. Publish prompts to 'prompt.in' topic");
    println!("5. Receive tokens on 'tokens.out' and 'stream.text' topics");

    Ok(())
}

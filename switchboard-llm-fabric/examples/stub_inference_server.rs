/// Stub LLM Inference Server for Switchboard LLM-Fabric Testing
/// 
/// This server simulates an LLM inference runtime that:
/// - Subscribes to the 'prompt.in' topic for inference requests
/// - Generates tokens in response
/// - Publishes token IDs to 'tokens.out'
/// - Publishes detokenized text to 'stream.text'
/// - Optionally publishes debug data to 'model.logits' and 'model.next_token'
/// - Reports metrics to 'metrics'
///
/// Run with:
/// ```sh
/// cargo run --example stub_inference_server --release -- --broker ws://localhost:7777
/// ```

use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;
use switchboard::router::Router;
use switchboard::connection::Connection;
use tokio::sync::Mutex;

/// Configuration for the stub server
#[derive(Debug, Clone)]
pub struct StubConfig {
    pub broker_url: String,
    pub model_name: String,
    pub max_tokens: usize,
    pub simulated_latency_ms: u64,
}

impl Default for StubConfig {
    fn default() -> Self {
        Self {
            broker_url: "ws://localhost:7777".to_string(),
            model_name: "stub-gpt-2".to_string(),
            max_tokens: 100,
            simulated_latency_ms: 50,
        }
    }
}

/// Represents a single token generation request
#[derive(Debug, Clone)]
pub struct InferenceRequest {
    pub id: String,
    pub prompt: String,
    pub model: String,
    pub max_tokens: usize,
}

/// A simulated token from the model
#[derive(Debug, Clone)]
pub struct GeneratedToken {
    pub token_id: u32,
    pub text: String,
    pub log_prob: f32,
}

pub struct StubInferenceServer {
    config: StubConfig,
    router: Arc<Router>,
}

impl StubInferenceServer {
    /// Create a new stub inference server
    pub async fn new(config: StubConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let router = Arc::new(Router::new());
        
        Ok(Self { config, router })
    }

    /// Generate simulated tokens from a prompt
    fn generate_tokens(&self, prompt: &str, max_tokens: usize) -> Vec<GeneratedToken> {
        let words = prompt.split_whitespace().collect::<Vec<_>>();
        let base_tokens = vec!["hello", "world", "from", "switchboard", "!"];
        
        let mut tokens = Vec::new();
        let mut token_id = 1000u32;
        
        // Echo the prompt words as tokens
        for word in words.iter().take(max_tokens) {
            tokens.push(GeneratedToken {
                token_id,
                text: word.to_string(),
                log_prob: -0.5,
            });
            token_id += 1;
        }
        
        // Add base tokens if we haven't hit the limit
        for word in base_tokens.iter() {
            if tokens.len() >= max_tokens {
                break;
            }
            tokens.push(GeneratedToken {
                token_id,
                text: word.to_string(),
                log_prob: -0.5,
            });
            token_id += 1;
        }
        
        tokens
    }

    /// Process a single inference request
    async fn process_request(
        &self,
        request: InferenceRequest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        println!(
            "[{}] Processing inference request: model='{}', prompt='{}'",
            request.id,
            request.model,
            request.prompt.chars().take(50).collect::<String>()
        );

        // Generate tokens
        let tokens = self.generate_tokens(&request.prompt, request.max_tokens);
        
        println!(
            "[{}] Generated {} tokens",
            request.id,
            tokens.len()
        );

        // Simulate generation with latency
        for (idx, token) in tokens.iter().enumerate() {
            // Publish to tokens.out topic (token ID + log probability)
            let token_data = format!(
                "{}|{}|{}",
                token.token_id,
                token.text,
                token.log_prob
            );
            
            // TODO: Publish to Switchboard topic 'tokens.out'
            // connection.publish("tokens.out", Bytes::from(token_data)).await?;
            
            // Publish to stream.text topic (decodedtext)
            // TODO: Publish to Switchboard topic 'stream.text'
            // connection.publish("stream.text", Bytes::from(token.text.clone())).await?;
            
            if idx > 0 {
                tokio::time::sleep(
                    tokio::time::Duration::from_millis(self.config.simulated_latency_ms)
                ).await;
            }
        }

        let elapsed = start.elapsed();
        println!(
            "[{}] Inference complete in {:.2}ms",
            request.id,
            elapsed.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Main server loop
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!(
            "Starting stub inference server: model='{}' broker='{}'",
            self.config.model_name, self.config.broker_url
        );

        // TODO: Integrate with actual Switchboard WebSocket connection
        // For now, just process some test requests to demonstrate the flow

        let test_requests = vec![
            InferenceRequest {
                id: "test-1".to_string(),
                prompt: "Hello, world!".to_string(),
                model: self.config.model_name.clone(),
                max_tokens: self.config.max_tokens,
            },
            InferenceRequest {
                id: "test-2".to_string(),
                prompt: "Tell me a story about Switchboard".to_string(),
                model: self.config.model_name.clone(),
                max_tokens: self.config.max_tokens,
            },
        ];

        for request in test_requests {
            self.process_request(request).await?;
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        println!("Stub inference server finished");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = StubConfig {
        broker_url: "ws://localhost:7777".to_string(),
        model_name: "stub-gpt-2".to_string(),
        max_tokens: 50,
        simulated_latency_ms: 25,
    };

    let server = StubInferenceServer::new(config).await?;
    server.run().await?;

    Ok(())
}

#!/usr/bin/env python3
"""
Stub LLM Inference Server for testing Switchboard LLM-Fabric integration

Connects to a Switchboard broker and simulates token generation for testing purposes.
Subscribes to 'prompt.in' and publishes simulated tokens to 'tokens.out' and 'stream.text'.

Usage:
    python3 stub_inference_server.py --broker ws://localhost:7777
"""

import asyncio
import json
import argparse
import logging
from typing import AsyncIterator
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StubInferenceServer:
    """Simulates an LLM inference runtime using Switchboard topics"""

    def __init__(self, broker_url: str = "ws://localhost:7777"):
        """
        Initialize the stub server
        
        Args:
            broker_url: URL to the Switchboard broker (e.g., ws://localhost:7777)
        """
        self.broker_url = broker_url
        self.model_name = "stub-gpt-2"
        self.running = False
        logger.info(f"Initialized stub server for broker: {broker_url}")

    async def generate_tokens(self, prompt: str) -> AsyncIterator[tuple[int, str]]:
        """
        Simulate token generation from a prompt
        
        Yields:
            (token_id, token_text) tuples
        """
        # Simple simulation: split prompt into words and repeat
        words = prompt.split()
        base_tokens = ["hello", "world", "from", "switchboard", "!"]
        
        token_id = 1000
        for word in words:
            yield token_id, word
            token_id += 1
        
        # Add some extra tokens
        for word in base_tokens:
            yield token_id, word
            token_id += 1

    async def process_prompt(self, prompt: str) -> None:
        """
        Process a single inference request
        
        This would normally:
        1. Receive prompt from topic 'prompt.in'
        2. Run inference
        3. Publish tokens to 'tokens.out'
        4. Publish detokenized text to 'stream.text'
        5. Optionally publish debug info to 'model.logits' and 'model.next_token'
        6. Publish metrics to 'metrics'
        """
        logger.info(f"Processing prompt: {prompt[:50]}...")
        
        # Simulate token generation
        full_text = ""
        async for token_id, token_text in self.generate_tokens(prompt):
            # In real implementation, these would be published to Switchboard topics
            full_text += token_text + " "
            logger.debug(f"Generated token {token_id}: {token_text}")
            await asyncio.sleep(0.05)  # Simulate generation latency
        
        logger.info(f"Completed inference. Generated text: {full_text}")

    async def run(self) -> None:
        """
        Main server loop - subscribe to prompts and generate responses
        
        TODO: Integrate with actual Switchboard WebSocket connection
        """
        self.running = True
        logger.info("Stub server running (awaiting WebSocket integration)")
        
        # Simulate receiving some prompts for testing
        test_prompts = [
            "Hello, world!",
            "Tell me a story",
            "What is Switchboard?"
        ]
        
        for prompt in test_prompts:
            if not self.running:
                break
            await self.process_prompt(prompt)
            await asyncio.sleep(1)  # Delay between prompts
        
        logger.info("Stub server finished")

    async def shutdown(self) -> None:
        """Gracefully shutdown the server"""
        logger.info("Shutting down stub server")
        self.running = False


async def main():
    parser = argparse.ArgumentParser(
        description="Stub LLM inference server for Switchboard testing"
    )
    parser.add_argument(
        "--broker",
        default="ws://localhost:7777",
        help="Switchboard broker URL (default: ws://localhost:7777)"
    )
    parser.add_argument(
        "--model",
        default="stub-gpt-2",
        help="Model name to advertise (default: stub-gpt-2)"
    )
    
    args = parser.parse_args()
    
    server = StubInferenceServer(broker_url=args.broker)
    server.model_name = args.model
    
    try:
        await server.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        await server.shutdown()
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

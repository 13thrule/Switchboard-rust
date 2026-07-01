#!/usr/bin/env python3
"""
OpenAI API Compatibility Layer for Switchboard

Provides an OpenAI-compatible /v1/chat/completions endpoint that routes requests
through Switchboard topics, enabling drop-in replacement for OpenAI clients.

Usage:
    python3 openai_compat_server.py --port 8000 --broker ws://localhost:7777
    
Then use with OpenAI client:
    client = OpenAI(base_url="http://localhost:8000/v1", api_key="dummy")
    response = client.chat.completions.create(
        model="gpt-2",
        messages=[{"role": "user", "content": "Hello"}],
        stream=True
    )
"""

import asyncio
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


class OpenAICompatMessage:
    """Represents a message in OpenAI format"""
    
    def __init__(self, role: str, content: str):
        self.role = role
        self.content = content
    
    def to_dict(self) -> Dict[str, str]:
        return {"role": self.role, "content": self.content}
    
    @staticmethod
    def from_dict(data: Dict[str, str]) -> "OpenAICompatMessage":
        return OpenAICompatMessage(data["role"], data["content"])


class OpenAICompatChoice:
    """Represents a choice in OpenAI format"""
    
    def __init__(self, index: int = 0, message: Optional[OpenAICompatMessage] = None, finish_reason: str = "stop"):
        self.index = index
        self.message = message or OpenAICompatMessage("assistant", "")
        self.finish_reason = finish_reason
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "message": self.message.to_dict(),
            "finish_reason": self.finish_reason
        }


class OpenAICompatResponse:
    """Represents a completion response in OpenAI format"""
    
    def __init__(self, model: str, choices: list, usage: Optional[Dict[str, int]] = None):
        self.id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        self.object = "chat.completion"
        self.created = int(datetime.utcnow().timestamp())
        self.model = model
        self.choices = choices
        self.usage = usage or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "object": self.object,
            "created": self.created,
            "model": self.model,
            "choices": [c.to_dict() for c in self.choices],
            "usage": self.usage
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class OpenAICompatStreamChunk:
    """Represents a single chunk in a streaming response"""
    
    def __init__(self, model: str, delta: Dict[str, str], index: int = 0, finish_reason: Optional[str] = None):
        self.id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        self.object = "chat.completion.chunk"
        self.created = int(datetime.utcnow().timestamp())
        self.model = model
        self.choices = [{
            "index": index,
            "delta": delta,
            "finish_reason": finish_reason
        }]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "object": self.object,
            "created": self.created,
            "model": self.model,
            "choices": self.choices
        }
    
    def to_sse_line(self) -> str:
        """Convert to Server-Sent Events format"""
        return f"data: {json.dumps(self.to_dict())}"


class OpenAICompatAdapter:
    """Adapter converting OpenAI API calls to Switchboard topics"""
    
    def __init__(self, broker_url: str = "ws://localhost:7777"):
        """
        Initialize the adapter
        
        Args:
            broker_url: URL to the Switchboard broker
        """
        self.broker_url = broker_url
        logger.info(f"Initialized OpenAI compat adapter for broker: {broker_url}")
    
    async def complete_chat(
        self,
        model: str,
        messages: list,
        stream: bool = False,
        max_tokens: Optional[int] = None
    ) -> Any:
        """
        Process a chat completion request
        
        Args:
            model: Model name (e.g., "gpt-2")
            messages: List of message dicts with "role" and "content"
            stream: Whether to stream response
            max_tokens: Maximum tokens to generate
        
        Returns:
            OpenAICompatResponse or async generator of OpenAICompatStreamChunk
        """
        # Construct prompt from messages
        prompt_lines = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            prompt_lines.append(f"{role}: {content}")
        
        prompt = "\n".join(prompt_lines)
        logger.info(f"Processing chat completion for model '{model}': {prompt[:100]}...")
        
        if stream:
            return self._stream_completion(model, prompt, max_tokens)
        else:
            return await self._complete_chat_sync(model, prompt, max_tokens)
    
    async def _complete_chat_sync(
        self,
        model: str,
        prompt: str,
        max_tokens: Optional[int]
    ) -> OpenAICompatResponse:
        """
        Generate a complete response synchronously
        
        TODO: Integrate with actual Switchboard WebSocket connection
        """
        # Simulate generation
        response_text = f"[Simulated response from {model} to: {prompt[:30]}...]"
        
        choice = OpenAICompatChoice(
            message=OpenAICompatMessage("assistant", response_text)
        )
        
        response = OpenAICompatResponse(model, [choice])
        logger.info(f"Generated response: {response_text[:50]}...")
        
        return response
    
    async def _stream_completion(
        self,
        model: str,
        prompt: str,
        max_tokens: Optional[int]
    ):
        """
        Generate a streaming response
        
        TODO: Integrate with actual Switchboard WebSocket connection
        """
        # Simulate streaming tokens
        tokens = ["Hello", " from", " Switchboard", " powered", " inference", "!"]
        
        for token in tokens:
            chunk = OpenAICompatStreamChunk(
                model,
                {"role": "assistant", "content": token} if token == tokens[0]
                else {"content": token}
            )
            yield chunk.to_sse_line()
            await asyncio.sleep(0.1)  # Simulate generation latency
        
        # Final chunk with finish_reason
        final_chunk = OpenAICompatStreamChunk(
            model,
            {},
            finish_reason="stop"
        )
        yield final_chunk.to_sse_line()
        yield "[DONE]"


# Example usage and testing
async def main():
    adapter = OpenAICompatAdapter()
    
    # Test messages
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello, how are you?"}
    ]
    
    # Test non-streaming
    print("=== Non-streaming response ===")
    response = await adapter.complete_chat("gpt-2", messages, stream=False)
    print(response.to_json())
    
    # Test streaming
    print("\n=== Streaming response ===")
    async for chunk in adapter._stream_completion("gpt-2", "Hello", None):
        print(chunk)


if __name__ == "__main__":
    asyncio.run(main())

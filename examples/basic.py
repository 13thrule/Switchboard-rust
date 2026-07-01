#!/usr/bin/env python3
"""
Basic Switchboard Python SDK example.

Demonstrates:
- Connecting to broker
- Publishing a message
- Subscribing to a topic
- Consuming messages with async iterator pattern
"""

import asyncio
import sys
sys.path.insert(0, "/workspaces/Switchboard-rust")

from switchboard import Switchboard


async def main():
    """Run the basic example."""
    
    # Connect to Switchboard broker
    async with Switchboard("localhost", 7777) as sb:
        print("[INFO] Connected to Switchboard broker at localhost:7777")
        
        # Task 1: Subscribe to 'demo' topic
        async def subscriber():
            print("[SUB] Subscribing to 'demo' topic...")
            async for payload in await sb.subscribe("demo"):
                print(f"[SUB] Received: {payload}")
                if payload == b"DONE":
                    break
        
        # Task 2: Publish messages
        async def publisher():
            await asyncio.sleep(0.5)  # Let subscriber connect first
            
            messages = [
                b"Hello from Switchboard!",
                b"Zero-copy message broker",
                b"Ultra-low latency",
                b"DONE"
            ]
            
            for msg in messages:
                print(f"[PUB] Publishing: {msg}")
                await sb.publish("demo", msg)
                await asyncio.sleep(0.2)
        
        # Run both concurrently
        await asyncio.gather(
            subscriber(),
            publisher()
        )
    
    print("[INFO] Connection closed")


if __name__ == "__main__":
    asyncio.run(main())

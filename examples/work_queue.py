#!/usr/bin/env python3
"""
Switchboard Work Queue Example
===============================

Demonstrates consumer groups (work queues) for distributed task processing.
Multiple workers join a queue:// topic and receive tasks round-robin.

Exactly one worker receives each message (unlike broadcast mode where all receive).

Usage:
    python3 work_queue.py

Expected output shows round-robin distribution:
    Worker 0 received: task_0
    Worker 1 received: task_1
    Worker 2 received: task_2
    Worker 0 received: task_3
    ... (pattern repeats)
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for local import
sys.path.insert(0, str(Path(__file__).parent.parent))

from switchboard import Switchboard


async def worker(worker_id: int, switchboard: Switchboard, queue_topic: str):
    """Join work queue and process messages."""
    print(f"Worker {worker_id}: joining queue '{queue_topic}'")
    
    # Subscribe to work queue (queue:// prefix activates work queue mode)
    subscription = await switchboard.subscribe(queue_topic)
    
    # Receive up to 6 messages
    async for message in subscription:
        payload = message.decode() if isinstance(message, bytes) else message
        print(f"Worker {worker_id} received: {payload}")
        # Could do actual work here
        await asyncio.sleep(0.01)  # Simulate processing


async def main():
    """Publish tasks to work queue and have workers process them."""
    switchboard = Switchboard("ws://127.0.0.1:3000")
    
    try:
        await switchboard.connect()
        print("Connected to Switchboard\n")
        
        # Start 3 worker tasks
        num_workers = 3
        queue_topic = "queue://tasks"  # queue:// prefix = work queue mode
        
        workers = [
            asyncio.create_task(worker(i, switchboard, queue_topic))
            for i in range(num_workers)
        ]
        
        # Give workers time to subscribe
        await asyncio.sleep(0.1)
        
        # Publish 9 tasks - should distribute round-robin among 3 workers
        # Worker 0: tasks 0, 3, 6
        # Worker 1: tasks 1, 4, 7
        # Worker 2: tasks 2, 5, 8
        print(f"Publishing 9 tasks to {queue_topic}...\n")
        for i in range(9):
            await switchboard.publish(queue_topic, f"task_{i}".encode())
            await asyncio.sleep(0.05)  # Small delay between publishes
        
        print()
        
        # Let workers finish processing
        await asyncio.sleep(0.5)
        
        # Cancel workers
        for worker_task in workers:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
    
    finally:
        await switchboard.close()
        print("Disconnected from Switchboard")


if __name__ == "__main__":
    asyncio.run(main())

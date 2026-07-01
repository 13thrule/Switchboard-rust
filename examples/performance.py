#!/usr/bin/env python3
"""
Performance stress test for Switchboard Python SDK.

Demonstrates:
- High-throughput message publishing
- Multiple concurrent subscribers
- Zero-copy message delivery
- Memory efficiency under load

Run this against a live Switchboard broker to measure throughput.
"""

import asyncio
import sys
import time
sys.path.insert(0, "/workspaces/Switchboard-rust")

from switchboard import Switchboard


async def main():
    """Run stress test."""
    
    # Configuration
    NUM_PUBLISHERS = 4
    MESSAGES_PER_PUBLISHER = 10000
    PAYLOAD_SIZE = 64  # bytes
    NUM_SUBSCRIBERS = 2
    
    total_messages = NUM_PUBLISHERS * MESSAGES_PER_PUBLISHER
    payload = b"x" * PAYLOAD_SIZE
    
    print(f"""
[STRESS TEST] Configuration:
  - Publishers: {NUM_PUBLISHERS}
  - Messages per publisher: {MESSAGES_PER_PUBLISHER}
  - Total messages: {total_messages}
  - Payload size: {PAYLOAD_SIZE} bytes
  - Total data: {(total_messages * PAYLOAD_SIZE) / (1024 * 1024):.2f} MB
  - Subscribers: {NUM_SUBSCRIBERS}
    """)
    
    async with Switchboard("localhost", 7777) as sb:
        print("[INFO] Connected to broker")
        
        # Message counters
        counters = {f"sub_{i}": 0 for i in range(NUM_SUBSCRIBERS)}
        lock = asyncio.Lock()
        
        async def subscriber(sub_id: int):
            """Consume messages and count."""
            print(f"[SUB-{sub_id}] Starting subscriber...")
            async for payload in await sb.subscribe("perf_test"):
                async with lock:
                    counters[f"sub_{sub_id}"] += 1
                
                if counters[f"sub_{sub_id}"] % 5000 == 0:
                    print(f"[SUB-{sub_id}] Received {counters[f'sub_{sub_id}']} messages")
                
                if counters[f"sub_{sub_id}"] >= total_messages:
                    break
            
            print(f"[SUB-{sub_id}] Done. Total received: {counters[f'sub_{sub_id}']}")
        
        async def publisher(pub_id: int):
            """Publish messages as fast as possible."""
            print(f"[PUB-{pub_id}] Starting publisher...")
            for i in range(MESSAGES_PER_PUBLISHER):
                await sb.publish("perf_test", payload)
                if (i + 1) % 2500 == 0:
                    print(f"[PUB-{pub_id}] Published {i + 1}/{MESSAGES_PER_PUBLISHER}")
            print(f"[PUB-{pub_id}] Done. Total published: {MESSAGES_PER_PUBLISHER}")
        
        # Start subscribers first
        subscriber_tasks = [
            subscriber(i) for i in range(NUM_SUBSCRIBERS)
        ]
        
        # Give subscribers time to connect
        await asyncio.sleep(0.5)
        
        # Start publishers
        publisher_tasks = [
            publisher(i) for i in range(NUM_PUBLISHERS)
        ]
        
        # Measure time
        start_time = time.time()
        
        # Wait for all to complete
        await asyncio.gather(*publisher_tasks, *subscriber_tasks)
        
        elapsed = time.time() - start_time
        
        # Calculate stats
        print("\n[RESULTS]")
        print(f"  Total time: {elapsed:.3f} seconds")
        print(f"  Throughput: {total_messages / elapsed:,.0f} msg/sec")
        print(f"  Bandwidth: {(total_messages * PAYLOAD_SIZE) / elapsed / (1024 * 1024):.2f} MB/sec")
        print(f"  Latency: {(elapsed * 1_000_000) / total_messages:.2f} µs/msg (avg)")
        
        for sub_id in range(NUM_SUBSCRIBERS):
            received = counters[f"sub_{sub_id}"]
            print(f"  Subscriber {sub_id}: {received} messages")
        
        # Verify all messages delivered
        total_received = sum(counters.values())
        if total_received == total_messages * NUM_SUBSCRIBERS:
            print(f"\n✓ SUCCESS: All {total_received} messages delivered (perfect distribution)")
        else:
            print(f"\n✗ MISMATCH: Expected {total_messages * NUM_SUBSCRIBERS}, got {total_received}")


if __name__ == "__main__":
    asyncio.run(main())

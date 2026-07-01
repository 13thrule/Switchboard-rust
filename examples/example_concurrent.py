"""
Example 3: Concurrent Multi-Topic Pub/Sub
Demonstrates:
- Multiple concurrent subscriptions
- Concurrent publish while receiving
- Waker-driven event loop
"""

import asyncio
from switchboard import Switchboard


async def subscriber(topic: str):
    """Task that subscribes to a topic and prints messages."""
    async with Switchboard("localhost", 7777) as sb:
        sub = await sb.subscribe(topic)
        print(f"[{topic}] Subscriber started")

        try:
            # Receive 10 messages then exit
            count = 0
            async for msg in sub:
                print(f"[{topic}] Received: {msg.payload.decode()}")
                count += 1
                if count >= 10:
                    break
        except KeyboardInterrupt:
            print(f"[{topic}] Subscriber stopped")


async def publisher():
    """Task that publishes to multiple topics."""
    async with Switchboard("localhost", 7777) as sb:
        for i in range(20):
            await sb.publish("trades", f"Trade {i}".encode())
            await sb.publish("alerts", f"Alert {i}".encode())
            print(f"Published trade and alert {i}")
            await asyncio.sleep(0.2)


async def main():
    print("Starting concurrent pub/sub demo...")
    print("- Publisher: sends to 'trades' and 'alerts' topics")
    print("- Subscriber 1: listening on 'trades'")
    print("- Subscriber 2: listening on 'alerts'")
    print()

    # Run 3 concurrent tasks
    await asyncio.gather(
        publisher(),
        subscriber("trades"),
        subscriber("alerts"),
    )

    print("\nDemo complete!")


if __name__ == "__main__":
    asyncio.run(main())

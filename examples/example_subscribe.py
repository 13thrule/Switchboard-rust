"""
Example 2: Simple Subscriber
Demonstrates receiving messages from a topic.
"""

import asyncio
from switchboard import Switchboard


async def main():
    # Connect to Switchboard broker (running on localhost:7777)
    async with Switchboard("localhost", 7777) as sb:
        # Subscribe to 'demo' topic
        sub = await sb.subscribe("demo")

        print("Waiting for messages on 'demo' topic... (Ctrl-C to quit)")

        # Async iterator pattern: pull messages as they arrive (waker-driven)
        try:
            async for msg in sub:
                print(f"[{msg.topic.decode()}] {msg.payload.decode()}")
        except KeyboardInterrupt:
            print("Unsubscribed.")


if __name__ == "__main__":
    asyncio.run(main())

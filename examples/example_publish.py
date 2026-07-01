"""
Example 1: Simple Publisher
Demonstrates basic publish to a topic.
"""

import asyncio
from switchboard import Switchboard


async def main():
    # Connect to Switchboard broker (running on localhost:7777)
    async with Switchboard("localhost", 7777) as sb:
        # Publish 5 messages
        for i in range(5):
            message = f"Message {i}: Hello from Python!".encode()
            await sb.publish("demo", message)
            print(f"Published: {message.decode()}")
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())

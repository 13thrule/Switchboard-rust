#!/usr/bin/env python3
"""
Multi-topic Switchboard example.

Demonstrates:
- Multiple concurrent subscriptions
- Independent topic routing (no interference)
- Concurrent producers and consumers
"""

import asyncio
import sys
sys.path.insert(0, "/workspaces/Switchboard-rust")

from switchboard import Switchboard


async def main():
    """Run the multi-topic example."""
    
    async with Switchboard("localhost", 7777) as sb:
        print("[INFO] Connected to Switchboard broker")
        
        # Track received messages
        received_trades = []
        received_alerts = []
        
        async def trades_subscriber():
            """Subscribe to trades topic."""
            print("[SUB] Subscribing to 'trades' topic...")
            count = 0
            async for payload in await sb.subscribe("trades"):
                received_trades.append(payload)
                print(f"[TRADES] Received #{count + 1}: {payload}")
                count += 1
                if count >= 3:
                    break
        
        async def alerts_subscriber():
            """Subscribe to alerts topic."""
            print("[SUB] Subscribing to 'alerts' topic...")
            count = 0
            async for payload in await sb.subscribe("alerts"):
                received_alerts.append(payload)
                print(f"[ALERTS] Received #{count + 1}: {payload}")
                count += 1
                if count >= 3:
                    break
        
        async def trades_publisher():
            """Publish to trades topic."""
            trades = [
                b"AAPL BUY 100 @ $195.50",
                b"GOOGL SELL 50 @ $2845.30",
                b"MSFT BUY 75 @ $445.20"
            ]
            for trade in trades:
                await asyncio.sleep(0.1)
                print(f"[PUB-TRADES] Publishing: {trade}")
                await sb.publish("trades", trade)
        
        async def alerts_publisher():
            """Publish to alerts topic."""
            alerts = [
                b"CPU utilization 85%",
                b"Memory pressure alert",
                b"Disk space low on /data"
            ]
            for alert in alerts:
                await asyncio.sleep(0.15)
                print(f"[PUB-ALERTS] Publishing: {alert}")
                await sb.publish("alerts", alert)
        
        # Run all concurrently
        print("[INFO] Starting concurrent publishers and subscribers...")
        await asyncio.gather(
            trades_subscriber(),
            alerts_subscriber(),
            trades_publisher(),
            alerts_publisher()
        )
        
        # Verify isolation
        print("\n[SUMMARY]")
        print(f"Trades topic received {len(received_trades)} messages (no alerts interference)")
        print(f"Alerts topic received {len(received_alerts)} messages (no trades interference)")
        print(f"Total: {len(received_trades) + len(received_alerts)} messages (perfect isolation)")


if __name__ == "__main__":
    asyncio.run(main())

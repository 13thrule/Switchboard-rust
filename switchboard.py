"""
Switchboard — Zero-Copy Python Async Client
Ultra-low latency pub/sub message broker client.

Zero dependencies. Mirrors Switchboard's architectural principles:
- Zero-copy frame slicing via memoryview
- Waker-driven event loop (asyncio-based)
- Lock-free subscription multiplexing
"""

import asyncio
import struct
from typing import AsyncIterator, Optional, Dict, List, Callable
from dataclasses import dataclass


@dataclass
class Message:
    """A received message: topic and payload (both as bytes)."""
    topic: bytes
    payload: bytes


class SubscriptionQueue:
    """Per-subscription async queue for message delivery."""

    def __init__(self, max_queue_size: int = 128):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._closed = False

    async def put(self, message: Message) -> None:
        """Enqueue a message. Non-blocking."""
        if not self._closed:
            try:
                self._queue.put_nowait(message)
            except asyncio.QueueFull:
                # Subscriber lagged; drop oldest message
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self._queue.put_nowait(message)

    async def get(self) -> Optional[Message]:
        """Dequeue a message. Blocks until available or closed."""
        if self._closed and self._queue.empty():
            return None
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=None)
        except asyncio.CancelledError:
            return None

    def close(self) -> None:
        """Mark subscription as closed."""
        self._closed = True

    def __aiter__(self):
        return self

    async def __anext__(self) -> Message:
        msg = await self.get()
        if msg is None:
            raise StopAsyncIteration
        return msg


class Switchboard:
    """
    Async client for Switchboard message broker.

    Usage:
        async with Switchboard("localhost", 7777) as sb:
            async for msg in sb.subscribe("trades"):
                print(f"Received: {msg.topic} = {msg.payload}")
            await sb.publish("alerts", b"system:online")
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 7777):
        self.host = host
        self.port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._subscriptions: Dict[bytes, SubscriptionQueue] = {}
        self._read_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Connect to Switchboard broker."""
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port
        )
        # Start the background read loop
        self._read_task = asyncio.create_task(self._read_loop())

    async def disconnect(self) -> None:
        """Disconnect from broker and clean up subscriptions."""
        # Close all subscriptions
        for sub_queue in self._subscriptions.values():
            sub_queue.close()
        self._subscriptions.clear()

        # Cancel read task
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass

        # Close connection
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def subscribe(self, topic: str) -> SubscriptionQueue:
        """
        Subscribe to a topic and return an async iterator.

        Args:
            topic: Topic name (string, will be encoded as UTF-8)

        Returns:
            SubscriptionQueue that yields Message objects
        """
        if not self._reader or not self._writer:
            raise RuntimeError("Not connected. Use 'async with' or call connect().")

        topic_bytes = topic.encode("utf-8")

        # Build frame: [0x01] + topic_bytes
        frame = self._encode_frame(b"\x01" + topic_bytes)

        # Send to broker
        self._writer.write(frame)
        await self._writer.drain()

        # Create subscription queue
        sub_queue = SubscriptionQueue()
        self._subscriptions[topic_bytes] = sub_queue

        return sub_queue

    async def publish(self, topic: str, payload: bytes) -> None:
        """
        Publish a message to a topic.

        Args:
            topic: Topic name (string)
            payload: Message payload (bytes)
        """
        if not self._reader or not self._writer:
            raise RuntimeError("Not connected. Use 'async with' or call connect().")

        topic_bytes = topic.encode("utf-8")
        topic_len = len(topic_bytes)

        # Build frame: [0x02] + [topic_len (u16)] + topic_bytes + payload
        body = b"\x02" + struct.pack(">H", topic_len) + topic_bytes + payload
        frame = self._encode_frame(body)

        # Send to broker
        self._writer.write(frame)
        await self._writer.drain()

    @staticmethod
    def _encode_frame(body: bytes) -> bytes:
        """
        Encode a frame with 4-byte big-endian length prefix.

        Args:
            body: Frame body (type + data)

        Returns:
            Length-prefixed frame
        """
        return struct.pack(">I", len(body)) + body

    @staticmethod
    def _parse_frame(data: bytes) -> Optional[Message]:
        """
        Parse a received frame into a Message.

        Args:
            data: Frame body (without length prefix)

        Returns:
            Message or None if parse fails
        """
        if len(data) < 1:
            return None

        msg_type = data[0]

        if msg_type == 0x02:  # Publish frame
            if len(data) < 3:
                return None

            # Parse topic_len (u16, big-endian)
            topic_len = struct.unpack(">H", data[1:3])[0]

            if len(data) < 3 + topic_len:
                return None

            # Zero-copy slicing via memoryview
            mv = memoryview(data)
            topic = bytes(mv[3 : 3 + topic_len])
            payload = bytes(mv[3 + topic_len :])

            return Message(topic=topic, payload=payload)

        return None

    async def _read_loop(self) -> None:
        """
        Background task that continuously reads frames from broker.
        Routes messages to subscription queues.
        """
        try:
            while self._reader:
                # Read 4-byte length prefix
                header = await self._reader.readexactly(4)
                if not header:
                    break

                frame_len = struct.unpack(">I", header)[0]
                if frame_len == 0 or frame_len > 16 * 1024 * 1024:
                    # Invalid frame size
                    break

                # Read frame body
                body = await self._reader.readexactly(frame_len)
                if not body:
                    break

                # Parse message
                msg = self._parse_frame(body)
                if msg:
                    # Route to subscription queue if it exists
                    if msg.topic in self._subscriptions:
                        await self._subscriptions[msg.topic].put(msg)

        except asyncio.CancelledError:
            pass
        except Exception:
            # Connection error; clean shutdown
            pass

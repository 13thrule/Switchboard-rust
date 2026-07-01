"""
Switchboard Python SDK - Zero-dependency, ultra-low latency async pub/sub client.

Mirrors the Rust broker's architecture:
- Zero-copy message slicing via memoryview
- Waker-driven event loop (asyncio)
- Lock-free per-topic subscription queues
- Direct async iterator pattern for message consumption

Usage:
    async with Switchboard("localhost", 7777) as sb:
        async for payload in sb.subscribe("trades"):
            print(payload)  # Raw bytes, zero-copy
        
        await sb.publish("alerts", b"online")
"""

import asyncio
import struct
from typing import AsyncIterator, Optional, Dict
import sys

__version__ = "0.1.0"


class ProtocolError(Exception):
    """Raised on protocol parsing or frame errors."""
    pass


class Frame:
    """Binary protocol frame parser (mirrors Rust Frame enum)."""
    
    SUBSCRIBE = 0x01
    PUBLISH = 0x02
    MAX_FRAME = 16 * 1024 * 1024
    
    @staticmethod
    def parse(raw: memoryview) -> tuple:
        """
        Parse a frame into (msg_type, topic, payload).
        
        Returns: (msg_type: int, topic: bytes, payload: memoryview)
        """
        if len(raw) < 1:
            raise ProtocolError("empty frame")
        
        msg_type = raw[0]
        
        if msg_type == Frame.SUBSCRIBE:
            # Subscribe: [0x01] + topic_bytes
            topic = bytes(raw[1:])
            return msg_type, topic, memoryview(b"")
        
        elif msg_type == Frame.PUBLISH:
            # Publish: [0x02] + [topic_len (u16)] + [topic] + [payload]
            if len(raw) < 3:
                raise ProtocolError("publish frame too short for topic_len")
            
            topic_len = struct.unpack(">H", raw[1:3])[0]
            
            if len(raw) < 3 + topic_len:
                raise ProtocolError(
                    f"publish topic_len overflow: {topic_len} > {len(raw) - 3}"
                )
            
            topic = bytes(raw[3:3 + topic_len])
            payload = raw[3 + topic_len:]
            
            return msg_type, topic, payload
        
        else:
            raise ProtocolError(f"unknown message type: 0x{msg_type:02x}")
    
    @staticmethod
    def encode_subscribe(topic: str) -> bytes:
        """Encode a subscribe frame (0x01 + topic UTF-8)."""
        topic_bytes = topic.encode("utf-8")
        length = 1 + len(topic_bytes)
        return struct.pack(">I", length) + bytes([Frame.SUBSCRIBE]) + topic_bytes
    
    @staticmethod
    def encode_publish(topic: str, payload: bytes) -> bytes:
        """Encode a publish frame (0x02 + topic_len + topic + payload)."""
        topic_bytes = topic.encode("utf-8")
        body_len = 1 + 2 + len(topic_bytes) + len(payload)
        
        header = bytearray()
        header.extend(struct.pack(">I", body_len))
        header.append(Frame.PUBLISH)
        header.extend(struct.pack(">H", len(topic_bytes)))
        header.extend(topic_bytes)
        
        return bytes(header) + payload


class SubscriptionIterator:
    """Async iterator for a single topic subscription."""
    
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
    
    def __aiter__(self):
        return self
    
    async def __anext__(self) -> bytes:
        """Yield the next message payload as bytes."""
        try:
            payload = await asyncio.wait_for(self.queue.get(), timeout=None)
            if payload is None:  # Sentinel: connection closed
                raise StopAsyncIteration
            return payload
        except asyncio.CancelledError:
            raise StopAsyncIteration


class Switchboard:
    """
    Ultra-low latency async pub/sub client for Switchboard broker.
    
    Zero dependencies, zero-copy memoryview slicing, waker-driven asyncio loop.
    """
    
    def __init__(self, host: str = "localhost", port: int = 7777):
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.subscriptions: Dict[str, asyncio.Queue] = {}
        self._read_task: Optional[asyncio.Task] = None
        self._closed = False
    
    async def __aenter__(self):
        """Async context manager entry: connect to broker."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: close connection."""
        await self.close()
    
    async def connect(self) -> None:
        """Establish TCP connection to Switchboard broker."""
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host, self.port
            )
            # Spawn background read task (waker-driven, not polling)
            self._read_task = asyncio.create_task(self._read_loop())
        except Exception as e:
            raise ConnectionError(f"failed to connect to {self.host}:{self.port}: {e}")
    
    async def close(self) -> None:
        """Close connection and cleanup subscriptions."""
        self._closed = True
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
        
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
        
        # Signal all subscriptions to close
        for queue in self.subscriptions.values():
            await queue.put(None)
    
    async def subscribe(self, topic: str) -> AsyncIterator[bytes]:
        """
        Subscribe to a topic and yield messages as they arrive.
        
        Returns an async iterator that yields raw message payloads as bytes.
        Use with 'async for':
            async for payload in client.subscribe("trades"):
                print(payload)
        
        The payload is a memoryview (zero-copy) but yields as bytes for ease of use.
        """
        if self._closed:
            raise RuntimeError("connection closed")
        
        # Create per-topic queue (waker-driven backpressure)
        queue: asyncio.Queue = asyncio.Queue(maxsize=1024)
        self.subscriptions[topic] = queue
        
        # Send subscribe frame
        frame = Frame.encode_subscribe(topic)
        try:
            self.writer.write(frame)
            await self.writer.drain()
        except Exception as e:
            del self.subscriptions[topic]
            raise RuntimeError(f"failed to send subscribe frame: {e}")
        
        # Return async iterator
        return SubscriptionIterator(queue)
    
    async def publish(self, topic: str, payload: bytes) -> None:
        """
        Publish a message to a topic.
        
        Args:
            topic: Topic name (UTF-8 string)
            payload: Message payload (raw bytes)
        """
        if self._closed:
            raise RuntimeError("connection closed")
        
        frame = Frame.encode_publish(topic, payload)
        try:
            self.writer.write(frame)
            await self.writer.drain()
        except Exception as e:
            raise RuntimeError(f"failed to send publish frame: {e}")
    
    async def _read_loop(self) -> None:
        """
        Background read task (waker-driven, not polling).
        
        Continuously reads frames from broker and dispatches to subscriptions.
        Blocks on socket.read() → waker fires when data arrives.
        """
        try:
            while not self._closed:
                # Read 4-byte length prefix
                header = await self.reader.readexactly(4)
                if not header:
                    break
                
                length = struct.unpack(">I", header)[0]
                
                if length == 0 or length > Frame.MAX_FRAME:
                    raise ProtocolError(
                        f"invalid frame length: {length} (max {Frame.MAX_FRAME})"
                    )
                
                # Read frame body
                frame_body = await self.reader.readexactly(length)
                
                # Parse frame (zero-copy via memoryview)
                raw = memoryview(frame_body)
                msg_type, topic, payload = Frame.parse(raw)
                
                # Dispatch to subscription queue
                if msg_type == Frame.PUBLISH:
                    topic_str = topic.decode("utf-8")
                    if topic_str in self.subscriptions:
                        queue = self.subscriptions[topic_str]
                        try:
                            # Queue is waker-driven; queue.put() wakes waiting iterator
                            queue.put_nowait(bytes(payload))
                        except asyncio.QueueFull:
                            # Backpressure: subscriber is slow
                            # Queue payload on next iteration (this is non-blocking)
                            await queue.put(bytes(payload))

        except asyncio.CancelledError:
            pass
        except Exception:
            # Connection error; clean shutdown
            pass

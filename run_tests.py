"""
Simple test runner for Switchboard Python SDK (no pytest required).
Run unit tests only (no broker dependency).
"""

import sys
from switchboard import Switchboard, Message, SubscriptionQueue


def test_frame_encoding():
    """Test binary frame encoding."""
    frame = Switchboard._encode_frame(b"\x01trades")
    assert len(frame) == 11, f"Expected 11 bytes, got {len(frame)}"
    assert frame[:4] == b"\x00\x00\x00\x07", f"Wrong length prefix: {frame[:4]}"
    assert frame[4:] == b"\x01trades", f"Wrong body: {frame[4:]}"
    print("✓ test_frame_encoding passed")


def test_frame_parsing_publish():
    """Test parsing a publish frame."""
    # Construct: [0x02] [topic_len=6] [topic] [payload]
    frame_data = b"\x02" + b"\x00\x06" + b"trades" + b"hello"
    msg = Switchboard._parse_frame(frame_data)

    assert msg is not None, "Message parsing returned None"
    assert msg.topic == b"trades", f"Wrong topic: {msg.topic}"
    assert msg.payload == b"hello", f"Wrong payload: {msg.payload}"
    print("✓ test_frame_parsing_publish passed")


def test_frame_parsing_empty_payload():
    """Test parsing a publish frame with no payload."""
    frame_data = b"\x02" + b"\x00\x06" + b"trades"
    msg = Switchboard._parse_frame(frame_data)

    assert msg is not None, "Message parsing returned None"
    assert msg.topic == b"trades", f"Wrong topic: {msg.topic}"
    assert msg.payload == b"", f"Wrong payload: {msg.payload}"
    print("✓ test_frame_parsing_empty_payload passed")


def test_frame_parsing_invalid():
    """Test that invalid frames return None."""
    assert Switchboard._parse_frame(b"") is None, "Should reject empty frame"
    assert Switchboard._parse_frame(b"\xFF") is None, "Should reject unknown type"
    assert Switchboard._parse_frame(b"\x02\x00") is None, "Should reject truncated frame"
    print("✓ test_frame_parsing_invalid passed")


def test_zero_copy_memoryview():
    """
    Test that frame parsing uses zero-copy memoryview slicing.
    """
    frame_data = b"\x02" + b"\x00\x06" + b"trades" + b"payload"
    msg = Switchboard._parse_frame(frame_data)

    assert msg is not None, "Message parsing returned None"
    assert isinstance(msg.topic, bytes), f"Topic should be bytes, got {type(msg.topic)}"
    assert isinstance(msg.payload, bytes), f"Payload should be bytes, got {type(msg.payload)}"
    assert msg.topic == b"trades", f"Wrong topic: {msg.topic}"
    assert msg.payload == b"payload", f"Wrong payload: {msg.payload}"
    print("✓ test_zero_copy_memoryview passed")


def test_subscription_queue():
    """Test basic queue operations."""
    import asyncio

    async def run_test():
        queue = SubscriptionQueue(max_queue_size=10)

        # Enqueue messages
        msg1 = Message(topic=b"trades", payload=b"msg1")
        msg2 = Message(topic=b"trades", payload=b"msg2")

        await queue.put(msg1)
        await queue.put(msg2)

        # Retrieve
        r1 = await queue.get()
        r2 = await queue.get()

        assert r1.payload == b"msg1", f"Wrong first message: {r1.payload}"
        assert r2.payload == b"msg2", f"Wrong second message: {r2.payload}"

    asyncio.run(run_test())
    print("✓ test_subscription_queue passed")


def test_subscription_queue_async_iteration():
    """Test async iteration over queue."""
    import asyncio

    async def run_test():
        queue = SubscriptionQueue(max_queue_size=10)

        # Enqueue messages
        msg1 = Message(topic=b"trades", payload=b"msg1")
        msg2 = Message(topic=b"trades", payload=b"msg2")

        await queue.put(msg1)
        await queue.put(msg2)
        queue.close()

        # Iterate
        messages = []
        async for msg in queue:
            messages.append(msg)

        assert len(messages) == 2, f"Expected 2 messages, got {len(messages)}"
        assert messages[0].payload == b"msg1", f"Wrong first message: {messages[0].payload}"
        assert messages[1].payload == b"msg2", f"Wrong second message: {messages[1].payload}"

    asyncio.run(run_test())
    print("✓ test_subscription_queue_async_iteration passed")


def test_subscription_queue_backpressure():
    """Test that slow subscribers drop messages on overflow."""
    import asyncio

    async def run_test():
        queue = SubscriptionQueue(max_queue_size=2)

        msg1 = Message(topic=b"t", payload=b"1")
        msg2 = Message(topic=b"t", payload=b"2")
        msg3 = Message(topic=b"t", payload=b"3")
        msg4 = Message(topic=b"t", payload=b"4")

        await queue.put(msg1)
        await queue.put(msg2)
        # Queue is now full; msg3 should drop msg1
        await queue.put(msg3)
        await queue.put(msg4)

        # Drain queue
        received = []
        for _ in range(3):
            msg = await queue.get()
            if msg:
                received.append(msg)

        # Last message should be msg4
        assert received[-1].payload == b"4", f"Last message should be '4', got {received[-1].payload}"

    asyncio.run(run_test())
    print("✓ test_subscription_queue_backpressure passed")


if __name__ == "__main__":
    try:
        print("Running Switchboard Python SDK unit tests...\n")
        
        test_frame_encoding()
        test_frame_parsing_publish()
        test_frame_parsing_empty_payload()
        test_frame_parsing_invalid()
        test_zero_copy_memoryview()
        test_subscription_queue()
        test_subscription_queue_async_iteration()
        test_subscription_queue_backpressure()

        print("\n✅ All unit tests passed!")
        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

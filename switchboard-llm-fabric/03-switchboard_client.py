"""
switchboard_client.py

Minimal Python client for Switchboard, matching the real wire protocol from
13thrule/Switchboard-rust's README:

    subscribe: 0x01 + topic_utf8
    publish:   0x02 + u16(topic_len, BE) + topic_bytes + payload_bytes

Goal: consume `tokens.out` / `model.logits` / `kv.update` frames with a
single `memoryview` over the socket-read buffer, so numpy/torch can view
directly into that buffer instead of Python copying bytes into a new
`bytes` object before array construction. This mirrors the adapter's
zero-copy guarantee on the Rust side (bytes::Bytes -> broadcast -> socket)
as closely as Python's memory model allows.

No external deps beyond numpy (optional, only used for the zero-copy
np.frombuffer examples).
"""

import asyncio
import struct
from dataclasses import dataclass
from typing import AsyncIterator, Optional

OP_SUBSCRIBE = 0x01
OP_PUBLISH = 0x02
MAX_FRAME_BYTES = 16 * 1024 * 1024  # broker-enforced cap, see 01-SPEC.md

# ---------------------------------------------------------------------
# Header layouts (must match the Rust `#[repr(C, packed)]` structs exactly)
# ---------------------------------------------------------------------

# TokensHeader: seq_id:u64, length:u32, dtype:u8  -> 13 bytes, native-endian
_TOKENS_HDR = struct.Struct("<QIB")
# LogitsHeader: seq_id:u64, vocab_size:u32, dtype:u8 -> 13 bytes
_LOGITS_HDR = struct.Struct("<QIB")
# KvHeader: layer:u16, head:u16, seq_start:u32, seq_len:u32, dtype:u8 -> 13 bytes
_KV_HDR = struct.Struct("<HHIIB")


@dataclass(frozen=True)
class TokensFrame:
    seq_id: int
    dtype: int
    # zero-copy: a memoryview slice of the *original* read buffer, cast to
    # u32 — no `bytes(...)` copy, no list() unpacking.
    token_ids: memoryview  # format 'I' (u32), read-only view


@dataclass(frozen=True)
class LogitsFrame:
    seq_id: int
    vocab_size: int
    dtype: int
    data: memoryview  # raw view; caller casts per dtype (see decode helpers)


@dataclass(frozen=True)
class KvFrame:
    layer: int
    head: int
    seq_start: int
    seq_len: int
    dtype: int
    data: memoryview  # raw K||V bytes, uncast — shape known by caller from dtype+seq_len+head_dim


class SwitchboardClient:
    """One TCP connection per subscription, matching the adapter's model
    (see 02-switchboard_adapter.rs). A production client could multiplex
    several topics over one socket; kept 1:1 here for clarity and to match
    the minimal-diff goal — fewer moving parts to reason about first."""

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)

    async def publish(self, topic: str, payload: bytes | bytearray | memoryview) -> None:
        """Publish raw payload bytes under `topic`. Caller is responsible for
        having already packed the topic-specific header (TokensHeader etc.)
        at the front of `payload` — this function does no schema validation,
        mirroring the broker's own topic-agnostic stance on payload content."""
        assert self._writer is not None, "call connect() first"
        topic_bytes = topic.encode("utf-8")
        total = 1 + 2 + len(topic_bytes) + len(payload)
        if total > MAX_FRAME_BYTES:
            raise ValueError(f"frame of {total} bytes exceeds 16MB broker limit")
        header = struct.pack(">BH", OP_PUBLISH, len(topic_bytes))
        # writev-style: write header, topic, and payload as separate chunks
        # rather than concatenating into one new buffer — avoids a Python-side
        # copy of `payload` before it even reaches the socket.
        self._writer.write(header)
        self._writer.write(topic_bytes)
        self._writer.write(payload)
        await self._writer.drain()

    async def subscribe(self, topic: str) -> None:
        assert self._writer is not None, "call connect() first"
        topic_bytes = topic.encode("utf-8")
        self._writer.write(struct.pack(">B", OP_SUBSCRIBE) + topic_bytes)
        await self._writer.drain()

    async def _read_frame(self) -> tuple[str, memoryview]:
        """Reads one publish frame and returns (topic, payload_view), where
        payload_view is a memoryview over a freshly-read bytearray — the one
        copy that's unavoidable at the OS socket boundary in Python (asyncio
        StreamReader always hands back owned bytes). Everything downstream
        of this point (header parsing, array casting) is view-only."""
        assert self._reader is not None
        opcode = await self._reader.readexactly(1)
        if opcode[0] != OP_PUBLISH:
            raise ValueError(f"unexpected opcode {opcode[0]:#x}, expected publish (0x02)")
        topic_len = struct.unpack(">H", await self._reader.readexactly(2))[0]
        topic = (await self._reader.readexactly(topic_len)).decode("utf-8")

        # NOTE: as in the Rust adapter, raw TCP has no inherent message
        # boundary — production code needs a length-delimited outer frame
        # (e.g. a u32 total-length prefix) so `readexactly` here knows how
        # much payload to pull. Assumed present via `self._reader.readexactly(n)`
        # below with `n` sourced from that outer prefix; wire it to match
        # whatever framing the deployed broker build uses (see 01-SPEC.md §2.1).
        payload_len = struct.unpack(">I", await self._reader.readexactly(4))[0]
        raw = bytearray(await self._reader.readexactly(payload_len))
        return topic, memoryview(raw)

    async def tokens_stream(self) -> AsyncIterator[TokensFrame]:
        while True:
            _topic, view = await self._read_frame()
            seq_id, length, dtype = _TOKENS_HDR.unpack_from(view, 0)
            # Zero-copy cast: memoryview.cast() re-interprets the same
            # underlying buffer as u32 elements — no allocation, no copy.
            ids_view = view[_TOKENS_HDR.size:].cast("I")
            assert len(ids_view) == length, "declared length must match payload"
            yield TokensFrame(seq_id=seq_id, dtype=dtype, token_ids=ids_view)

    async def logits_stream(self) -> AsyncIterator[LogitsFrame]:
        while True:
            _topic, view = await self._read_frame()
            seq_id, vocab_size, dtype = _LOGITS_HDR.unpack_from(view, 0)
            data_view = view[_LOGITS_HDR.size:]
            yield LogitsFrame(seq_id=seq_id, vocab_size=vocab_size, dtype=dtype, data=data_view)

    async def kv_stream(self) -> AsyncIterator[KvFrame]:
        while True:
            _topic, view = await self._read_frame()
            layer, head, seq_start, seq_len, dtype = _KV_HDR.unpack_from(view, 0)
            data_view = view[_KV_HDR.size:]
            yield KvFrame(
                layer=layer, head=head, seq_start=seq_start,
                seq_len=seq_len, dtype=dtype, data=data_view,
            )

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()


# ---------------------------------------------------------------------
# Example usage: zero-copy numpy view over a logits frame
# ---------------------------------------------------------------------

async def example_consume_logits(host: str, port: int) -> None:
    import numpy as np  # optional dependency, only needed for this example

    client = SwitchboardClient(host, port)
    await client.connect()
    await client.subscribe("model.logits")

    _DTYPE_MAP = {0: np.float32, 1: np.float16}  # 2=bf16, 3=int8 need custom handling

    async for frame in client.logits_stream():
        if frame.dtype not in _DTYPE_MAP:
            continue  # bf16 / quantized handling omitted for brevity
        # np.frombuffer over a memoryview does NOT copy — it's a real view
        # into the same bytearray that came off the socket.
        logits = np.frombuffer(frame.data, dtype=_DTYPE_MAP[frame.dtype], count=frame.vocab_size)
        top_token = int(logits.argmax())
        print(f"seq={frame.seq_id} vocab={frame.vocab_size} argmax={top_token}")

    await client.close()


async def example_publish_prompt(host: str, port: int, seq_id: int, text: str) -> None:
    client = SwitchboardClient(host, port)
    await client.connect()
    payload = struct.pack("<QIB", seq_id, len(text.encode("utf-8")), 1) + text.encode("utf-8")
    await client.publish("prompt.in", payload)
    await client.close()


if __name__ == "__main__":
    asyncio.run(example_consume_logits("127.0.0.1", 7777))

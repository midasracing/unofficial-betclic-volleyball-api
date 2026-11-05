import struct

import pytest

from app.streaming import GrpcWebFrames, FrameError

def packet(payload, flags=0):
    return bytes([flags]) + struct.pack(">I", len(payload)) + payload

def test_split_chunks_and_multiple_frames():
    reader = GrpcWebFrames()
    data = packet(b"first") + packet(b"next") + packet(b"grpc-status: 0", flags=0x80)
    assert reader.feed(data[:3]) == []
    assert reader.feed(data[3:8]) == []
    frames = reader.feed(data[8:])
    assert [x["data"] for x in frames] == [b"first", b"next", b"grpc-status: 0"]
    assert frames[-1]["flags"] == 0x80

def test_reject_excessive_frame():
    reader = GrpcWebFrames()
    with pytest.raises(FrameError):
        reader.feed(b"\x00" + struct.pack(">I", 2_000_001))

def test_fragmented_frame_roundtrip():
    reader = GrpcWebFrames()
    payload = b"\x00\x01\xff" * 50
    frames = []
    for byte in packet(payload):
        frames.extend(reader.feed(bytes([byte])))
    assert frames == [{"flags": 0, "data": payload}]

def test_reconnect_delay_is_bounded():
    from app.streaming import reconnect_delay
    assert reconnect_delay(0) == 90
    assert reconnect_delay(1) == 90
    assert reconnect_delay(2) == 180
    assert reconnect_delay(3) == 360
    assert reconnect_delay(6) == 900

def test_single_http_connection_receives_multiple_updates(monkeypatch):
    import threading
    from app import streaming

    class FakeResponse:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def raise_for_status(self):
            pass
        def iter_content(self, chunk_size):
            payload = packet(b"score1") + packet(b"odds1") + packet(b"score2")
            for block in (payload[:3], payload[3:11], payload[11:]):
                yield block
            yield packet(b"grpc-status: 0", flags=0x80)

    class FakeSession:
        calls = 0
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post(self, *args, **kwargs):
            FakeSession.calls += 1
            assert kwargs["stream"] is True
            return FakeResponse()

    monkeypatch.setattr(streaming.requests, "Session", FakeSession)
    received = []
    streaming.stream_http(123, threading.Event(), received.append)
    assert FakeSession.calls == 1
    assert [f["data"] for f in received] == [b"score1", b"odds1", b"score2"]

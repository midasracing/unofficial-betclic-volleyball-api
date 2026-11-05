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

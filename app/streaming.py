"""Experimental one-connection-per-match Betclic gRPC-web streaming.

This code does not create one request per received update. The server can keep
the HTTP response open and send multiple framed protobuf messages. If the
server closes the stream, a conservative cooldown applies before reconnection.

Enable only after checking data-provider terms and testing on real matches.
"""
import asyncio
import logging
import struct
import threading
import time
from dataclasses import dataclass

import requests
from betclic_api.client import _BASE_URL, _HEADERS
from betclic_api.proto import encode_field_string, encode_field_varint, grpc_frame

log = logging.getLogger(__name__)
SERVICE = "offering.access.api.MatchService/GetMatchWithNotification"
MAX_FRAME = 2_000_000

class FrameError(ValueError):
    pass

class GrpcWebFrames:
    """Incremental binary gRPC-web framer; supports arbitrary HTTP chunks."""
    def __init__(self):
        self.pending = bytearray()

    def feed(self, chunk):
        self.pending.extend(chunk)
        output = []
        while len(self.pending) >= 5:
            flags = self.pending[0]
            length = struct.unpack(">I", self.pending[1:5])[0]
            if length > MAX_FRAME:
                raise FrameError(f"gRPC frame exceeds {MAX_FRAME} bytes")
            if len(self.pending) < length + 5:
                break
            payload = bytes(self.pending[5:5 + length])
            del self.pending[:5 + length]
            output.append({"flags": flags, "data": payload})
        return output

@dataclass
class StreamStats:
    opens: int = 0
    updates: int = 0
    last_frame_at: float | None = None
    last_error: str | None = None
    state: str = "idle"

def stream_http(match_id, stop, on_frame, locale="fr"):
    """Read one server stream until stop, trailer, network error or timeout.

    Read timeout is a liveness bound, NOT a reason to poll fast. Caller backs
    off after every close, including clean EOF.
    """
    data = encode_field_varint(1, match_id) + encode_field_string(2, locale)
    headers = dict(_HEADERS)
    with requests.Session() as session:
        with session.post(
            f"{_BASE_URL}/{SERVICE}",
            data=grpc_frame(data),
            headers=headers,
            stream=True,
            timeout=(5, 35),
        ) as response:
            response.raise_for_status()
            framer = GrpcWebFrames()
            for chunk in response.iter_content(chunk_size=4096):
                if stop.is_set():
                    return
                if not chunk:
                    continue
                for frame in framer.feed(chunk):
                    if frame["flags"] & 0x80:
                        return
                    # Protobuf messages are stored unmodified.
                    on_frame(frame)

class StreamWorker:
    """Manage one HTTP stream on a worker thread and reconnect conservatively."""
    def __init__(self, match_id, loop, callback, cooldown=90):
        self.match_id = match_id
        self.loop = loop
        self.callback = callback
        self.cooldown = max(60, cooldown)
        self.stop = threading.Event()
        self.stats = StreamStats()
        self.thread = None

    def start(self):
        if self.thread is not None:
            return
        self.thread = threading.Thread(target=self._run, daemon=True, name=f"betclic-stream-{self.match_id}")
        self.thread.start()

    def _run(self):
        consecutive_failures = 0
        while not self.stop.is_set():
            self.stats.state = "connected_or_connecting"
            self.stats.opens += 1
            started = time.monotonic()
            try:
                def received(frame):
                    self.stats.updates += 1
                    self.stats.last_frame_at = time.time()
                    self.loop.call_soon_threadsafe(self.callback, self.match_id, frame)
                stream_http(self.match_id, self.stop, received)
                self.stats.last_error = None
                consecutive_failures = 0 if time.monotonic() - started > 60 else consecutive_failures + 1
            except (requests.RequestException, FrameError) as exc:
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                consecutive_failures += 1
                log.warning("Betclic stream %s disconnected: %s", self.match_id, exc)
            except Exception:
                self.stats.last_error = "unexpected_stream_error"
                consecutive_failures += 1
                log.exception("Unexpected Betclic stream error for %s", self.match_id)
            if self.stop.is_set():
                break
            self.stats.state = "cooldown"
            # Avoid rapid retries; increase up to 15 minutes, no bypass logic.
            delay = min(900, self.cooldown * (2 ** min(4, max(0, consecutive_failures - 1))))
            self.stop.wait(delay)
        self.stats.state = "stopped"

    def close(self):
        self.stop.set()

    async def shutdown(self):
        self.close()
        if self.thread:
            await asyncio.to_thread(self.thread.join, 38)

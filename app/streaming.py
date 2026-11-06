"""Experimental one-connection-per-match Betclic gRPC-web streaming.

This code does not create one request per received update. The server can keep
the HTTP response open and send multiple framed protobuf messages. If the
server closes the stream, a conservative cooldown applies before reconnection.

Enable only after checking data-provider terms and testing on real matches.
"""
import asyncio
import logging
import math
import random
import struct
import threading
import time
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import unquote

import requests
from betclic_api.client import _BASE_URL, _HEADERS
from betclic_api.proto import encode_field_string, encode_field_varint, grpc_frame

log = logging.getLogger(__name__)
SERVICE = "offering.access.api.MatchService/GetMatchWithNotification"
MAX_FRAME = 2_000_000
STREAM_READ_TIMEOUT = 90
STREAM_SHUTDOWN_GRACE = STREAM_READ_TIMEOUT + 5
STREAM_MIN_OPEN_INTERVAL = 5
FAST_RECOVERY_ATTEMPTS = 3
FAST_RECOVERY_WINDOW = 30
TRANSIENT_RETRY_BASE = 5
TRANSIENT_RETRY_CAP = 30

class FrameError(ValueError):
    pass


class IncompleteFrameError(FrameError):
    """The response ended partway through a frame; retry on a fresh stream."""


class GrpcStatusError(requests.ConnectionError):
    def __init__(self, code, message=""):
        self.code = code
        self.message = message
        super().__init__(f"gRPC status {code}: {message}")


@dataclass
class StreamResult:
    end_reason: str
    grpc_status: int | None = None
    frames: int = 0


def grpc_status(metadata):
    value = metadata.get("grpc-status")
    if value is None:
        raise FrameError("gRPC trailers have no status")
    try:
        code = int(value)
    except (TypeError, ValueError) as error:
        raise FrameError("Invalid gRPC status") from error
    if code < 0:
        raise FrameError("Invalid negative gRPC status")
    if code:
        raise GrpcStatusError(code, unquote(metadata.get("grpc-message", "")))
    return code


def retry_after_seconds(error, now=None):
    response = getattr(error, "response", None)
    if response is None or response.status_code not in (429, 503):
        return 0
    value = response.headers.get("Retry-After", "")
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            seconds = date.timestamp() - (time.time() if now is None else now)
        except (TypeError, ValueError, OverflowError):
            return 0
    return min(threading.TIMEOUT_MAX, max(0, seconds)) if math.isfinite(seconds) else 0

class GrpcWebFrames:
    """Incremental binary gRPC-web framer; supports arbitrary HTTP chunks."""
    def __init__(self):
        self.pending = bytearray()

    def feed(self, chunk):
        self.pending.extend(chunk)
        output = []
        while len(self.pending) >= 5:
            flags = self.pending[0]
            if flags not in (0, 0x80):
                raise FrameError(f"Unsupported gRPC frame flags: {flags}")
            length = struct.unpack(">I", self.pending[1:5])[0]
            if length > MAX_FRAME:
                raise FrameError(f"gRPC frame exceeds {MAX_FRAME} bytes")
            if len(self.pending) < length + 5:
                break
            payload = bytes(self.pending[5:5 + length])
            del self.pending[:5 + length]
            output.append({"flags": flags, "data": payload})
        return output

    def finish(self):
        if self.pending:
            raise IncompleteFrameError("Stream ended with an incomplete gRPC frame")

@dataclass
class StreamStats:
    opens: int = 0
    updates: int = 0
    last_frame_at: float | None = None
    last_error: str | None = None
    state: str = "idle"
    last_end_reason: str | None = None
    last_grpc_status: int | None = None
    consecutive_failures: int = 0
    last_retry_delay: float | None = None
    next_retry_at: float | None = None
    last_connection_seconds: float | None = None
    disconnects: int = 0
    last_open_at: float | None = None
    last_disconnect_at: float | None = None
    last_recovery_seconds: float | None = None
    longest_recovery_seconds: float | None = None
    recovery_attempts_left: int = FAST_RECOVERY_ATTEMPTS

def stream_http(match_id, stop, on_frame, locale="fr"):
    """Read one server stream until stop, trailer, network error or timeout.

    Read timeout is a liveness bound, NOT a reason to poll fast. Caller backs
    off after every close, including clean EOF.
    """
    if stop.is_set():
        return StreamResult("stopped")
    data = encode_field_varint(1, match_id) + encode_field_string(2, locale)
    headers = dict(_HEADERS)
    with requests.Session() as session:
        with session.post(
            f"{_BASE_URL}/{SERVICE}",
            data=grpc_frame(data),
            headers=headers,
            stream=True,
            timeout=(5, STREAM_READ_TIMEOUT),
        ) as response:
            response.raise_for_status()
            response_headers = getattr(response, "headers", {})
            if response_headers.get("grpc-status") is not None:
                grpc_status(response_headers)
            framer = GrpcWebFrames()
            frames = 0
            for chunk in response.iter_content(chunk_size=4096):
                if stop.is_set():
                    return StreamResult("stopped", frames=frames)
                if not chunk:
                    continue
                for frame in framer.feed(chunk):
                    if stop.is_set():
                        return StreamResult("stopped", frames=frames)
                    if frame["flags"] == 0x80:
                        try:
                            lines = frame["data"].decode("ascii").splitlines()
                        except UnicodeDecodeError as error:
                            raise FrameError("Non-ASCII gRPC trailers") from error
                        metadata = {}
                        for line in lines:
                            key, separator, value = line.partition(":")
                            if separator:
                                metadata[key.strip().lower()] = value.strip()
                        return StreamResult("grpc_complete", grpc_status(metadata), frames)
                    # Protobuf messages are stored unmodified.
                    frames += 1
                    on_frame(frame)
                    if stop.is_set():
                        return StreamResult("stopped", frames=frames)
            framer.finish()
            if response_headers.get("grpc-status") is not None:
                return StreamResult("grpc_complete", grpc_status(response_headers), frames)
            return StreamResult("eof_without_trailers", frames=frames)

def reconnect_delay(consecutive_failures, cooldown=90):
    """Bounded exponential cooldown after a stream closes."""
    return min(900, max(60, cooldown) * (2 ** min(4, max(0, consecutive_failures - 1))))


class StreamWorker:
    """Manage one HTTP stream on a worker thread and reconnect conservatively."""
    def __init__(self, match_id, loop, callback, cooldown=90, recovery_seconds=1):
        self.match_id = match_id
        self.loop = loop
        self.callback = callback
        self.cooldown = max(60, cooldown)
        self.recovery_seconds = max(1, min(self.cooldown, recovery_seconds))
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
        backoff_failures = 0
        recovery_attempts_left = FAST_RECOVERY_ATTEMPTS
        recovery_deadline = None
        recovery_started = None
        while not self.stop.is_set():
            self.stats.state = "opening"
            self.stats.next_retry_at = None
            self.stats.opens += 1
            started = time.monotonic()
            self.stats.last_open_at = time.time()
            if recovery_deadline is None:
                recovery_deadline = started + FAST_RECOVERY_WINDOW
            first_frame = last_frame = None
            frame_count = 0
            failure = None
            result = None
            try:
                def received(frame):
                    nonlocal first_frame, last_frame, frame_count, recovery_started
                    frame_at = time.monotonic()
                    if frame["data"]:
                        last_frame = frame_at
                        if first_frame is None:
                            first_frame = frame_at
                            if recovery_started is not None:
                                gap = round(frame_at - recovery_started, 3)
                                self.stats.last_recovery_seconds = gap
                                self.stats.longest_recovery_seconds = max(
                                    self.stats.longest_recovery_seconds or 0, gap)
                                recovery_started = None
                        frame_count += 1
                    self.stats.updates += 1
                    self.stats.last_frame_at = time.time()
                    self.stats.state = "streaming"
                    self.stats.last_error = None
                    self.loop.call_soon_threadsafe(self.callback, self.match_id, frame)
                result = stream_http(self.match_id, self.stop, received)
                self.stats.last_error = None
            except (requests.RequestException, FrameError) as exc:
                failure = exc
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("Betclic stream %s disconnected: %s", self.match_id, exc)
            except Exception as exc:
                failure = exc
                self.stats.last_error = "unexpected_stream_error"
                log.exception("Unexpected Betclic stream error for %s", self.match_id)
            ended = time.monotonic()
            elapsed = ended - started
            self.stats.last_connection_seconds = round(elapsed, 3)
            self.stats.last_grpc_status = failure.code if isinstance(failure, GrpcStatusError) else getattr(result, "grpc_status", None)
            self.stats.last_end_reason = (
                f"grpc_status_{failure.code}" if isinstance(failure, GrpcStatusError)
                else type(failure).__name__ if failure is not None
                else getattr(result, "end_reason", "closed"))
            if self.stop.is_set():
                break
            self.stats.disconnects += 1
            self.stats.last_disconnect_at = time.time()
            if recovery_started is None:
                recovery_started = ended
            response = getattr(failure, "response", None)
            if (isinstance(failure, GrpcStatusError) and failure.code in (7, 16)) or (
                    response is not None and response.status_code in (401, 403)):
                self.stats.state = "blocked"
                return
            productive = frame_count >= 2 and last_frame - first_frame >= 2
            consecutive_failures = 0 if productive else consecutive_failures + 1
            self.stats.consecutive_failures = consecutive_failures
            if productive:
                backoff_failures = 0
                recovery_attempts_left = FAST_RECOVERY_ATTEMPTS
                recovery_deadline = ended + FAST_RECOVERY_WINDOW
            elif ended >= recovery_deadline:
                recovery_attempts_left = 0
            recoverable = (
                failure.code in (4, 14) if isinstance(failure, GrpcStatusError)
                else isinstance(failure, (requests.ConnectionError, requests.ReadTimeout,
                                          requests.exceptions.ChunkedEncodingError,
                                          IncompleteFrameError))
                or getattr(result, "end_reason", None) in ("eof_without_trailers", "grpc_complete")
                or (response is not None and response.status_code in (500, 502, 503, 504)))
            if recoverable and recovery_attempts_left and ended < recovery_deadline:
                delay = self.recovery_seconds
                recovery_attempts_left -= 1
            else:
                backoff_failures += 1
                delay = (min(TRANSIENT_RETRY_CAP, TRANSIENT_RETRY_BASE * 2 ** min(4, backoff_failures - 1))
                         if recoverable else reconnect_delay(backoff_failures, self.cooldown))
            self.stats.recovery_attempts_left = recovery_attempts_left
            ceiling = max(TRANSIENT_RETRY_CAP, self.recovery_seconds) if recoverable else 900
            delay = min(ceiling, delay + random.uniform(0, min(5, delay * 0.1)))
            # Short failed openings cannot spin: only three fast retries in a
            # 30-second recovery window, at least five seconds between starts.
            # Provider Retry-After always overrides the local recovery policy.
            delay = max(delay, STREAM_MIN_OPEN_INTERVAL - elapsed, retry_after_seconds(failure))
            self.stats.last_retry_delay = delay
            self.stats.next_retry_at = time.time() + delay
            self.stats.state = "cooldown"
            self.stop.wait(delay)
        self.stats.state = "stopped"
        self.stats.next_retry_at = None

    def close(self):
        self.stop.set()

    async def shutdown(self):
        self.close()
        if self.thread:
            await asyncio.to_thread(self.thread.join, STREAM_SHUTDOWN_GRACE)

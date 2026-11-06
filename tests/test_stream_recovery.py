"""Transport failures and reconnect policy, without network calls or sleeping."""
import struct
import threading
from unittest.mock import Mock

import pytest
import requests

from app import streaming


def packet(payload, flags=0):
    return bytes([flags]) + struct.pack(">I", len(payload)) + payload


def transport(monkeypatch, chunks, headers=None):
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.headers = headers or {}
    response.iter_content.side_effect = lambda **kwargs: iter(chunks)
    session = Mock()
    session.__enter__ = Mock(return_value=session)
    session.__exit__ = Mock(return_value=False)
    session.post.return_value = response
    monkeypatch.setattr(streaming.requests, "Session", lambda: session)
    return session


def test_server_cancellation_is_reported_after_delivering_updates(monkeypatch):
    session = transport(monkeypatch, [packet(b"score"), packet(
        b"grpc-status: 14\r\ngrpc-message: A%20task%20was%20canceled.\r\n", 0x80)])
    frames = []
    with pytest.raises(streaming.GrpcStatusError) as caught:
        streaming.stream_http(123, threading.Event(), frames.append)
    assert caught.value.code == 14
    assert caught.value.message == "A task was canceled."
    assert [frame["data"] for frame in frames] == [b"score"]
    assert session.post.call_args.kwargs["timeout"] == (5, 90)
    assert session.post.call_count == 1


def test_clean_completion_and_missing_trailers_are_distinguished(monkeypatch):
    transport(monkeypatch, [packet(b"score"), packet(b"grpc-status: 0\r\n", 0x80)])
    result = streaming.stream_http(123, threading.Event(), lambda frame: None)
    assert result == streaming.StreamResult("grpc_complete", 0, 1)
    transport(monkeypatch, [packet(b"score")])
    result = streaming.stream_http(123, threading.Event(), lambda frame: None)
    assert result == streaming.StreamResult("eof_without_trailers", frames=1)


@pytest.mark.parametrize("chunk", [
    packet(b"grpc-message: missing status", 0x80),
    packet(b"grpc-status: invalid", 0x80),
    packet(b"grpc-status: -1", 0x80),
    packet(b"grpc-status: 0\r\ngrpc-message: \xff", 0x80),
    packet(b"truncated")[:-1],
    packet(b"unsupported compression", 1),
])
def test_malformed_transport_never_counts_as_clean_completion(monkeypatch, chunk):
    transport(monkeypatch, [chunk])
    with pytest.raises(streaming.FrameError):
        streaming.stream_http(123, threading.Event(), lambda frame: None)


def test_header_only_permission_error_is_not_an_empty_success(monkeypatch):
    transport(monkeypatch, [], {"grpc-status": "7", "grpc-message": "Denied"})
    with pytest.raises(streaming.GrpcStatusError) as caught:
        streaming.stream_http(123, threading.Event(), lambda frame: None)
    assert caught.value.code == 7


def test_callback_can_stop_before_another_blocking_read(monkeypatch):
    def chunks():
        yield packet(b"score")
        raise AssertionError("Reading again could block for 90 seconds")
    transport(monkeypatch, chunks())
    stop = threading.Event()
    result = streaming.stream_http(123, stop, lambda frame: stop.set())
    assert result == streaming.StreamResult("stopped", frames=1)


def http_error(status, retry_after=None):
    response = requests.Response()
    response.status_code = status
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    return requests.HTTPError(response=response)


@pytest.mark.parametrize("value, expected", [
    ("120", 120), ("-10", 0), ("nonsense", 0), ("nan", 0),
    ("Fri, 06 Nov 2026 12:02:00 GMT", 120),
])
def test_retry_after_supports_duration_and_http_date(value, expected):
    assert streaming.retry_after_seconds(http_error(429, value), now=1793966400) == expected


class StopAfterWaits:
    def __init__(self, count=1):
        self.count = count
        self.delays = []
        self.stopped = False

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, delay):
        self.delays.append(delay)
        self.stopped = len(self.delays) >= self.count
        return self.stopped


def worker(monkeypatch, stream, clock, attempts=1):
    result = streaming.StreamWorker(123, Mock(), Mock())
    result.stop = StopAfterWaits(attempts)
    monkeypatch.setattr(streaming, "stream_http", stream)
    monkeypatch.setattr(streaming.time, "monotonic", Mock(side_effect=clock))
    monkeypatch.setattr(streaming.random, "uniform", lambda low, high: 0)
    result._run()
    return result


@pytest.mark.parametrize("end", [
    streaming.GrpcStatusError(14, "A task was canceled."),
    requests.ConnectionError("Disconnected"),
    requests.exceptions.ChunkedEncodingError("Response ended prematurely"),
    streaming.StreamResult("eof_without_trailers", frames=2),
])
def test_productive_transport_failure_recovers_quickly_but_spaces_openings(monkeypatch, end):
    def stream(match_id, stop, callback):
        callback({"data": b"initial", "flags": 0})
        callback({"data": b"update", "flags": 0})
        if isinstance(end, Exception):
            raise end
        return end
    result = worker(monkeypatch, stream, [0, 0.1, 4, 5])
    assert result.stop.delays == [25]  # Never open twice within 30 seconds.
    assert result.stats.consecutive_failures == 0
    assert result.stats.updates == 2
    assert result.loop.call_soon_threadsafe.call_count == 2
    if isinstance(end, streaming.GrpcStatusError):
        assert result.stats.last_grpc_status == 14
        assert result.stats.last_end_reason == "grpc_status_14"


def test_unproductive_failures_back_off_and_productive_stream_resets_streak(monkeypatch):
    calls = 0

    def stream(match_id, stop, callback):
        nonlocal calls
        calls += 1
        if calls == 3:
            callback({"data": b"initial", "flags": 0})
            callback({"data": b"update", "flags": 0})
        raise streaming.GrpcStatusError(14)
    result = worker(monkeypatch, stream, [0, 1, 91, 92, 272, 273, 276, 312], attempts=3)
    assert result.stop.delays == [90, 180, 15]
    assert result.stats.consecutive_failures == 0


@pytest.mark.parametrize("error", [streaming.GrpcStatusError(7), streaming.GrpcStatusError(16),
                                       http_error(401), http_error(403)])
def test_denied_access_stops_without_retry(monkeypatch, error):
    result = worker(monkeypatch, Mock(side_effect=error), [0, 1])
    assert result.stats.state == "blocked"
    assert result.stop.delays == []


def test_rate_limit_respects_provider_retry_after(monkeypatch):
    result = worker(monkeypatch, Mock(side_effect=http_error(429, "120")), [0, 1])
    assert result.stop.delays == [120]


def test_clean_completion_does_not_use_fast_error_recovery(monkeypatch):
    def stream(match_id, stop, callback):
        callback({"data": b"initial", "flags": 0})
        callback({"data": b"update", "flags": 0})
        return streaming.StreamResult("grpc_complete", 0, 2)
    result = worker(monkeypatch, stream, [0, 0.1, 4, 40])
    assert result.stop.delays == [90]
    assert result.stats.last_grpc_status == 0

"""Bounded snapshot transport for the pinned Betclic offering client."""
import time
from datetime import datetime, timezone

from betclic_api import BetclicClient
from betclic_api.client import _BASE_URL, SPORTS
from betclic_api.parser import parse_match_detail_response, parse_matches_response
from betclic_api.proto import decode_protobuf, grpc_frame, encode_field_string, encode_field_varint

from app.betclic_scores import _first, _messages, decode_score_root

MAX_FRAME_BYTES = 2_000_000
MAX_STREAM_BYTES = 4_000_000


def apply_listing_score_evidence(matches, frames, now=None):
    """Recover a missing live flag only from a started volleyball score.

    The listing's verified score lives at root.1.3.13. The pinned upstream
    parser reads a legacy field 6 that is absent in the captured PlusLiga feed.
    Do not promote future fixtures, empty scores or decided best-of-five games.
    """
    now = now or datetime.now(timezone.utc)
    scores = {}
    for frame in frames:
        if frame.get("flags", 0) & 0x80:
            continue
        for wrapper in _messages(decode_protobuf(frame["data"]), 1):
            for fields in _messages(wrapper, 3):
                match_id = _first(fields, 1)
                score = decode_score_root({}, match_id, snapshots=[fields])
                if score:
                    scores[match_id] = score
    for match in matches:
        score = scores.get(match.id)
        if not score:
            continue
        try:
            start = datetime.fromisoformat(match.date.replace("Z", "+00:00"))
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
        except (AttributeError, ValueError, TypeError):
            continue
        if not 0 <= (now - start).total_seconds() <= 6 * 3600:
            continue
        won = score.get("sets_won_derived") or {}
        if max(won.values(), default=0) >= 3:
            match.is_live = False
            continue
        points = score.get("current_points") or {}
        played_sets = any(period["home"] or period["away"] for period in score.get("completed_sets", []))
        if (score.get("current_period") and any(points.values())) or played_sets:
            match.is_live = True
    return matches


class SnapshotError(RuntimeError):
    """The endpoint did not supply a usable, complete snapshot."""


class BetclicSnapshotClient(BetclicClient):
    """Keep upstream protobuf parsing, close notification streams after the snapshot.

    Private upstream imports are intentional and covered by the pinned requirement.
    Network/protocol errors propagate instead of becoming an empty successful feed.
    """

    def __init__(self, locale="fr", timeout=(5, 8)):
        super().__init__(locale=locale, timeout=timeout)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._session.close()

    def get_matches(self, sport="football", offset=0):
        payload = encode_field_string(1, SPORTS.get(sport, sport))
        payload += encode_field_string(2, self.locale)
        payload += encode_field_varint(4, offset) + encode_field_varint(5, 40)
        frames = self._post("offering.access.api.MatchService", "GetMatchesBySportWithNotifications", payload)
        result = parse_matches_response(frames)
        if SPORTS.get(sport, sport) == "volleyball":
            apply_listing_score_evidence(result["matches"], frames)
        return result

    def _post(self, service, method, payload):
        started = time.monotonic()
        response = self._session.post(
            f"{_BASE_URL}/{service}/{method}",
            data=grpc_frame(payload), timeout=self.timeout, stream=True,
        )
        try:
            response.raise_for_status()
            status = response.headers.get("grpc-status", "0")
            if status != "0":
                raise SnapshotError(f"gRPC status {status}")
            buffer = bytearray()
            frames = []
            received = 0
            for chunk in response.iter_content(chunk_size=1024):
                if time.monotonic() - started > 20:
                    raise SnapshotError("Snapshot deadline exceeded")
                received += len(chunk)
                if received > MAX_STREAM_BYTES:
                    raise SnapshotError("Snapshot exceeds the byte limit")
                buffer.extend(chunk)
                while len(buffer) >= 5:
                    flags = buffer[0]
                    size = int.from_bytes(buffer[1:5], "big")
                    if size > MAX_FRAME_BYTES:
                        raise SnapshotError("gRPC frame exceeds the byte limit")
                    if len(buffer) < 5 + size:
                        break
                    data = bytes(buffer[5:5 + size])
                    del buffer[:5 + size]
                    if flags == 0x80:
                        trailers = {}
                        for line in data.decode("ascii").splitlines():
                            key, separator, value = line.partition(":")
                            if separator:
                                trailers[key.strip().lower()] = value.strip()
                        if trailers.get("grpc-status") != "0":
                            raise SnapshotError(f"gRPC status {trailers.get('grpc-status', 'missing')}")
                        if not frames:
                            raise SnapshotError("No snapshot before gRPC trailers")
                        return frames  # An explicitly empty, successfully completed snapshot.
                    if flags != 0:
                        raise SnapshotError(f"Unsupported gRPC frame flags: {flags}")
                    frames.append({"flags": flags, "data": data})
                    if len(frames) > 64:
                        raise SnapshotError("Too many frames without a snapshot")
                    if method == "GetMatchesBySportWithNotifications":
                        # Field 1 is the initial listing response, including a valid
                        # empty last page. Field 2 carries later notifications.
                        # Waiting for a match on an empty page keeps the stream open.
                        ready = any(isinstance(wrapper, bytes)
                                    for wrapper in decode_protobuf(data).get(1, []))
                    elif method == "GetMatchWithNotification":
                        ready = parse_match_detail_response(frames).id is not None
                    else:
                        raise SnapshotError(f"Unsupported snapshot method: {method}")
                    if ready:
                        return frames
            raise SnapshotError("Stream ended before a complete snapshot")
        finally:
            response.close()

"""Decode verified volleyball score notifications from Betclic gRPC-web frames.

Reverse engineered from a captured 120-byte score notification, event
1271196261842944 (2026-11-05). Only parses the verified message shape.
All decoded points are observations, not betting market outcomes.
"""
import re

from betclic_api.proto import decode_protobuf


def _first(fields, number, default=None):
    values = fields.get(number, [])
    return values[0] if values else default


def _message(fields, number):
    value = _first(fields, number)
    if not isinstance(value, bytes):
        return {}
    try:
        return decode_protobuf(value)
    except (ValueError, IndexError, TypeError, OverflowError):
        return {}


def _utf8(value):
    if not isinstance(value, bytes):
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _score_pair(fields):
    left = _utf8(_first(fields, 1))
    right = _utf8(_first(fields, 2))
    if left is None or right is None:
        return None
    if not left.isascii() or not right.isascii():
        return None
    if not (left.isdecimal() and right.isdecimal()):
        return None
    return {"home": int(left), "away": int(right)}


def _score_notification(data):
    """Read score update at root.2.1.2.1; otherwise ignore the frame."""
    try:
        root = decode_protobuf(data)
        wrapper = _message(_message(_message(root, 2), 1), 2)
        entry = _message(wrapper, 1)
    except (ValueError, IndexError, TypeError, OverflowError):
        return None
    if _utf8(_first(entry, 1)) != "volleyball":
        return None
    match_id = _first(entry, 10)
    if not isinstance(match_id, int):
        return None
    points = _score_pair(_message(entry, 7))
    # field 9 is a repeated completed period entry, with a code and points
    finished = []
    for raw in entry.get(9, []):
        if not isinstance(raw, bytes):
            continue
        try:
            period = decode_protobuf(raw)
        except (ValueError, IndexError, TypeError, OverflowError):
            continue
        code = _utf8(_first(period, 2))
        result = _score_pair(_message(period, 3))
        if code and re.fullmatch(r"vlb_(?:first|second|third|fourth|fifth)_set", code) and result:
            finished.append({"period": code, **result})
    current_info = _message(entry, 13)
    current_period = _utf8(_first(current_info, 3))
    set_indices = {"first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5}
    current_match = re.fullmatch(r"vlb_(first|second|third|fourth|fifth)_set", current_period or "")
    current_set_number = set_indices[current_match.group(1)] if current_match else None
    if not current_match:
        current_period = None

    if not points and not finished:
        return None
    sets_won = {
        "home": sum(p["home"] > p["away"] for p in finished),
        "away": sum(p["away"] > p["home"] for p in finished),
    } if finished else None
    return {
        "match_id": match_id,
        "sport": "volleyball",
        "current_period": current_period,
        "current_set_number": current_set_number,
        "current_points": points,
        "completed_sets": finished,
        "sets_won_derived": sets_won,
        "source": "betclic_grpc_web_score_notification",
    }


def decode_score_frames(frames, expected_match_id=None):
    """Return latest verified score notification; skip odds and unknown frames."""
    latest = None
    for frame in frames:
        flags = frame.get("flags", 0)
        if flags & 0x80:
            continue
        payload = frame.get("data")
        if not isinstance(payload, bytes):
            continue
        score = _score_notification(payload)
        if score and (expected_match_id is None or score["match_id"] == expected_match_id):
            latest = score
    return latest

"""Inspect raw gRPC-web fields from actual Betclic event details.

Diagnostic only, NOT a score parser. Prints bounded field-path summaries and
never infers a live score from the names of betting markets.

Run: python -m tools.probe_raw_live
"""
import json
from datetime import datetime, timezone, timedelta
from app.client import BetclicSnapshotClient as BetclicClient
from betclic_api.proto import encode_field_varint, encode_field_string, decode_protobuf, try_decode_string

SPORTS = ("volleyball", "football")
MAX_DEPTH = 6
MAX_NODES = 160

def summarize(data, path="root", depth=0, count=None, output=None):
    if count is None:
        count = [0]
    if output is None:
        output = []
    if depth > MAX_DEPTH or count[0] >= MAX_NODES:
        return output
    try:
        fields = decode_protobuf(data)
    except (ValueError, TypeError, IndexError, OverflowError):
        return output
    for field, values in fields.items():
        for i, value in enumerate(values[:3]):
            if count[0] >= MAX_NODES:
                return output
            count[0] += 1
            key = f"{path}.{field}[{i}]"
            if isinstance(value, int):
                output.append({"path": key, "wire": "varint", "value": value})
            elif isinstance(value, bytes):
                s = try_decode_string(value)
                if s and len(s) < 100:
                    output.append({"path": key, "wire": "string", "value": s})
                else:
                    output.append({"path": key, "wire": "bytes", "length": len(value)})
                    if depth < MAX_DEPTH and len(value) < 20000:
                        summarize(value, key, depth + 1, count, output)
    return output

def parse_datetime(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        return None

def select_match(matches, now):
    # is_live alone is not trustworthy in the upstream parser: future matches
    # were marked live in the first actual probe.
    candidates = [m for m in matches if m.id is not None and m.is_live]
    started = [m for m in candidates if (parse_datetime(m.date) or now + timedelta(days=1)) <= now]
    if started:
        return started[0], "live_flag_and_start_in_past"
    return (candidates[0], "live_flag_only_unverified") if candidates else (None, "no_candidate")

def inspect(client, match_id):
    payload = encode_field_varint(1, match_id) + encode_field_string(2, client.locale)
    frames = client._post("offering.access.api.MatchService", "GetMatchWithNotification", payload)
    results = []
    for frame in frames:
        if frame["flags"] & 0x80:
            continue
        results.append({"bytes": len(frame["data"]), "fields": summarize(frame["data"])})
    return results

def main():
    now = datetime.now(timezone.utc)
    result = {"timestamp_utc": now.isoformat(), "source": "Betclic gRPC-web offering",
              "notes": "unknown field numbers are NOT scores without external validation",
              "sports": {}}
    successes = 0
    for sport in SPORTS:
        entry = {}
        result["sports"][sport] = entry
        try:
            matches = client.get_matches(sport)["matches"]
            entry["listing_count"] = len(matches)
            m, reason = select_match(matches, now)
            entry["selection_reason"] = reason
            if m is None:
                continue
            entry["match"] = {"id": m.id, "name": m.name, "competition": m.competition,
                              "starts_at": m.date, "upstream_is_live": m.is_live}
            entry["raw_frames"] = inspect(client, m.id)
            successes += 1
        except Exception as exc:
            entry["error"] = f"{type(exc).__name__}: {exc}"
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if successes == 0:
        raise SystemExit("No raw Betclic detail frames captured")

client = BetclicClient(timeout=(5, 12))
if __name__ == "__main__":
    main()

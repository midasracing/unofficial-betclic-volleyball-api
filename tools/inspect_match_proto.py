"""Inspect one Betclic volleyball match's raw protobuf field tree.

Run locally with a real Betclic event ID:
    python -m tools.inspect_match_proto 123456789

This makes ONE detail request. It does not infer or label unknown fields as
scores. Inspect output against the Betclic live scoreboard before mapping.
"""
import argparse
from app.client import BetclicSnapshotClient as BetclicClient
from betclic_api.proto import encode_field_varint, encode_field_string, decode_protobuf, try_decode_string

def walk(payload, path="root", depth=0, max_depth=5, budget=None):
    if budget is None:
        budget = [0]
    if depth > max_depth or budget[0] >= 250:
        return
    fields = decode_protobuf(payload)
    for number, values in fields.items():
        for index, value in enumerate(values[:5]):
            if budget[0] >= 250:
                return
            budget[0] += 1
            key = f"{path}.{number}[{index}]"
            if isinstance(value, int):
                print(f"{key} = {value}")
                continue
            if not isinstance(value, bytes):
                continue
            label = try_decode_string(value)
            if label and len(label) <= 100:
                print(f"{key} = {label!r}")
            elif len(value) < 30000:
                print(f"{key} bytes={len(value)}")
                # Unknown protobuf blobs may not decode cleanly; remain heuristic.
                if depth < max_depth:
                    try:
                        nested = decode_protobuf(value)
                        if nested and all(0 < n < 1000 for n in nested):
                            walk(value, key, depth + 1, max_depth, budget)
                    except (ValueError, IndexError, TypeError, OverflowError):
                        pass
            else:
                print(f"{key} large_blob_bytes={len(value)} (not expanded)")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("match_id", type=int, help="Real Betclic match ID")
    args = parser.parse_args()
    client = BetclicClient(timeout=(5, 12))
    payload = encode_field_varint(1, args.match_id)
    payload += encode_field_string(2, client.locale)
    frames = client._post("offering.access.api.MatchService", "GetMatchWithNotification", payload)
    print(f"Data frames received: {sum(not (f['flags'] & 0x80) for f in frames)}")
    for number, frame in enumerate(frames):
        if frame["flags"] & 0x80:
            continue
        print(f"FRAME {number} / {len(frame['data'])} bytes")
        walk(frame["data"])

if __name__ == "__main__":
    main()

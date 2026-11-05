"""Capture a bounded, original gRPC-web protobuf payload for one sports event.

The upstream Betclic client _post returns the original frame payload bytes,
not interpreted Match objects. This script stores those untouched bytes, plus
frame metadata, as GitHub Actions artifacts for offline reverse engineering.
No scores are inferred.
"""
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from betclic_api import BetclicClient
from betclic_api.proto import encode_field_string, encode_field_varint

OUT = Path(os.environ.get("BETCLIC_CAPTURE_DIR", "captures"))
SPORTS = ("volleyball", "football")
MAX_FRAME_BYTES = 1024 * 1024

def parse_date(date):
    try:
        return datetime.fromisoformat(date.replace("Z", "+00:00"))
    except (ValueError, AttributeError, TypeError):
        return None

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    client = BetclicClient(timeout=(5, 12))
    now = datetime.now(timezone.utc)
    manifest = {"captured_at_utc": now.isoformat(), "events": [],
                "note": "Raw response message payloads; not decoded scores"}
    for sport in SPORTS:
        item = {"sport": sport}
        manifest["events"].append(item)
        try:
            matches = client.get_matches(sport)["matches"]
            # A live flag is unreliable on its own. Prefer a started event.
            candidates = [m for m in matches if m.id and m.is_live]
            started = [m for m in candidates if (parse_date(m.date) or now + timedelta(days=1)) <= now]
            match = (started or candidates or [None])[0]
            if match is None:
                item["status"] = "no_candidate"
                continue
            item.update({"id": match.id, "name": match.name,
                         "competition": match.competition,
                         "upstream_is_live": match.is_live,
                         "start_date": match.date,
                         "selection_basis": "started_and_live_flag" if started else "live_flag_only"})
            payload = encode_field_varint(1, match.id)
            payload += encode_field_string(2, client.locale)
            frames = client._post("offering.access.api.MatchService", "GetMatchWithNotification", payload)
            records = []
            for index, frame in enumerate(frames):
                flags, data = frame["flags"], frame["data"]
                if flags & 0x80:
                    # Trailer metadata is not required for the protobuf investigation.
                    continue
                if len(data) > MAX_FRAME_BYTES:
                    records.append({"frame": index, "skipped": "size_limit", "bytes": len(data)})
                    continue
                filename = f"{sport}_{match.id}_frame_{index}.protobuf.bin"
                (OUT / filename).write_bytes(data)
                records.append({"frame": index, "filename": filename, "bytes": len(data),
                                "flags": flags})
            item["frames"] = records
            item["status"] = "captured" if any("filename" in r for r in records) else "no_data_frames"
        except Exception as error:
            item["status"] = "error"
            item["error"] = f"{type(error).__name__}: {error}"
    (OUT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    if not any(e.get("status") == "captured" for e in manifest["events"]):
        raise SystemExit("No original protobuf payload captured")

if __name__ == "__main__":
    main()

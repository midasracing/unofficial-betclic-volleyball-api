"""Bounded single-connection Betclic gRPC-web streaming probe.

One sports discovery request, at most one HTTP stream, no automatic reconnect.
The report distinguishes: no event, one-shot response, and actual multiple
updates arriving at different times on the same HTTP connection.
"""
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from app.client import BetclicSnapshotClient as BetclicClient
from app.betclic_scores import decode_score_frames
from app.filters import category
from app.scheduling import eligible_for_live_detail
from app.streaming import stream_http

RUN_SECONDS = 70
OUT = Path("captures/streaming")
OUT.mkdir(parents=True, exist_ok=True)

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def main():
    report = {
        "started_at": utc_now(),
        "kind": "one_connection_no_retries",
        "max_run_seconds": RUN_SECONDS,
        "discovery_requests": 0,
        "stream_requests": 0,
        "frames": [],
        "outcome": "not_started",
    }
    try:
        # Diagnostic only: any started volleyball match, irrespective of gender/league.
        # Production API remains men-only.
        client = BetclicClient(timeout=(5, 12))
        listing = client.get_matches("volleyball")
        report["discovery_requests"] = 1
        events = [
            m for m in listing.get("matches", [])
            if m.id is not None
            and eligible_for_live_detail(m)
        ]
        report["eligible_events_count"] = len(events)
        if not events:
            report["outcome"] = "no_started_live_volleyball_match"
            return
        # Prefer a target male competition if present, otherwise test any volleyball event.
        events.sort(key=lambda m: category(m.competition, m.name, m.competition_id) is None)
        match = events[0]
        report["event"] = {
            "diagnostic_only": True,
            "production_allowlisted": category(match.competition, match.name, match.competition_id) is not None,
            "id": match.id,
            "name": match.name,
            "competition": match.competition,
            "starts_at": match.date,
            "upstream_is_live": match.is_live,
        }
        stop = threading.Event()
        observations = []
        errors = []
        def on_frame(frame):
            score = decode_score_frames([frame], expected_match_id=match.id)
            observations.append(time.monotonic())
            report["frames"].append({
                "time_utc": utc_now(),
                "elapsed_seconds": round(observations[-1] - start, 3),
                "bytes": len(frame["data"]),
                "score": score,
            })
        def runner():
            try:
                stream_http(match.id, stop, on_frame)
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")

        report["stream_requests"] = 1
        start = time.monotonic()
        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        thread.join(RUN_SECONDS)
        stop.set()
        thread.join(40)  # read timeout in transport = 35 sec
        report["thread_stopped"] = not thread.is_alive()
        report["connection_errors"] = errors
        report["frame_count"] = len(observations)
        report["frames_spread_seconds"] = (
            round(observations[-1] - observations[0], 3)
            if len(observations) > 1 else 0
        )
        if errors:
            report["outcome"] = "connection_error"
        elif len(observations) >= 2 and observations[-1] - observations[0] >= 2:
            report["outcome"] = "streaming_multiple_temporally_separated_frames"
        elif observations:
            report["outcome"] = "only_initial_frames_confirmed"
        else:
            report["outcome"] = "no_frames_received"
    except Exception as exc:
        report["outcome"] = "discovery_error"
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        report["finished_at"] = utc_now()
        (OUT / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        print("OUTCOME:", report["outcome"])

if __name__ == "__main__":
    main()

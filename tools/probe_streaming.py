"""Bounded diagnostics of live Betclic streams across currently active sports.

One listing per sport, at most three concurrent streams, one connection per event.
Production competition filtering is used only as a report label.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import threading
import time
from datetime import datetime, timezone

import requests
from betclic_api.client import SPORTS
from betclic_api.parser import parse_match_detail_response

from app.betclic_scores import decode_score_frames
from app.client import BetclicSnapshotClient
from app.filters import category
from app.streaming import stream_http

DEFAULT_SPORTS = ("tennis", "table_tennis", "football", "basketball",
                  "hockey", "handball", "volleyball")
MAX_FRAMES = 200
MAX_BYTES = 5_000_000


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def started_at(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result
    except (AttributeError, TypeError, ValueError):
        return None


def select_live_candidate(matches, now):
    eligible = []
    for match in matches:
        start = started_at(match.date)
        if (match.id is not None and match.is_live and len(match.teams) == 2
                and start is not None and 0 <= (now - start).total_seconds() <= 6 * 3600):
            eligible.append((start, match))
    # Latest known kickoff reduces selection of old/completed events with stale flags.
    return max(eligible, key=lambda item: item[0])[1] if eligible else None


def transport_error(error):
    if isinstance(error, requests.exceptions.ConnectTimeout):
        return "connect_timeout"
    if isinstance(error, requests.exceptions.ReadTimeout) or (
        isinstance(error, requests.exceptions.ConnectionError)
        and "read timed out" in str(error).casefold()
    ):
        return "read_timeout"
    if isinstance(error, requests.exceptions.HTTPError):
        return "http_error"
    if isinstance(error, requests.RequestException):
        return "request_error"
    return "unexpected_error"


def market_state(match):
    markets = []
    for market in match.markets:
        selections = sorted(
            (selection.name, selection.odds) for selection in market.selections
            if isinstance(selection.odds, (int, float)) and not isinstance(selection.odds, bool)
            and math.isfinite(selection.odds)
        )
        markets.append((market.name, bool(market.suspended), selections))
    return json.dumps(sorted(markets), ensure_ascii=False, allow_nan=False)


class Observation:
    def __init__(self, sport, match):
        self.sport = sport
        self.match = match
        self.stop = threading.Event()
        self.started = time.monotonic()
        self.records = []
        self.hashes = set()
        self.bytes_received = 0
        self.limit_reached = False
        self.end = "not_started"
        self.error = None
        self.previous_markets = None
        self.previous_score = None
        self.market_snapshots = 0
        self.late_market_updates = 0
        self.late_score_updates = 0

    def record(self, frame, elapsed=None):
        if self.stop.is_set():
            return
        data = frame["data"]
        self.bytes_received += len(data)
        if len(self.records) >= MAX_FRAMES or self.bytes_received > MAX_BYTES:
            self.limit_reached = True
            self.stop.set()
            return
        elapsed = time.monotonic() - self.started if elapsed is None else elapsed
        digest = hashlib.sha256(data).hexdigest()
        self.hashes.add(digest)
        row = {"elapsed_seconds": round(elapsed, 3), "bytes": len(data), "sha256": digest}
        late = bool(self.records) and elapsed - self.records[0]["elapsed_seconds"] >= 2
        try:
            detail = parse_match_detail_response([frame])
        except (ValueError, TypeError, IndexError, OverflowError, RecursionError):
            detail = None
        if detail is not None and detail.id == self.match.id:
            state = market_state(detail)
            self.market_snapshots += 1
            if late and self.previous_markets is not None and state != self.previous_markets:
                self.late_market_updates += 1
            self.previous_markets = state
            row["decoded_market_count"] = len(detail.markets)
        # This decoder has evidence for volleyball only; never reinterpret other sports.
        if self.sport == "volleyball":
            try:
                score = decode_score_frames([frame], expected_match_id=self.match.id)
            except (ValueError, TypeError, IndexError, OverflowError, RecursionError):
                score = None
            if score:
                state = json.dumps(score, sort_keys=True)
                if late and self.previous_score is not None and state != self.previous_score:
                    self.late_score_updates += 1
                self.previous_score = state
                row["volleyball_score"] = score
        self.records.append(row)

    def result(self):
        spread = self.records[-1]["elapsed_seconds"] - self.records[0]["elapsed_seconds"] if self.records else 0
        if not self.records:
            evidence = "no_frames_received"
        elif spread < 2:
            evidence = "initial_data_only"
        elif self.late_market_updates:
            evidence = "market_updates_observed"
        elif self.late_score_updates:
            evidence = "volleyball_score_updates_observed"
        elif len(self.hashes) > 1:
            evidence = "different_payloads_observed"
        else:
            evidence = "repeated_payloads_only"
        match = self.match
        return {
            "sport": self.sport,
            "event": {"id": match.id, "name": match.name, "competition": match.competition,
                      "starts_at": match.date, "upstream_is_live": match.is_live,
                      "diagnostic_only": True,
                      "production_allowlisted": self.sport == "volleyball" and
                          category(match.competition, match.name, match.competition_id) is not None},
            "evidence": evidence, "transport_end": self.end, "transport_error": self.error,
            "frame_count": len(self.records), "distinct_payloads": len(self.hashes),
            "spread_seconds": round(spread, 3), "bytes_received": self.bytes_received,
            "market_snapshots": self.market_snapshots,
            "late_market_updates": self.late_market_updates,
            "late_volleyball_score_updates": self.late_score_updates,
            "sample_limit_reached": self.limit_reached, "frames": list(self.records),
        }


def run_probe(sports=DEFAULT_SPORTS, max_streams=3, seconds=90, client=None, streamer=stream_http):
    sports = tuple(dict.fromkeys(sports))
    if not 1 <= max_streams <= 3 or not 10 <= seconds <= 180 or not 1 <= len(sports) <= 8:
        raise ValueError("Limits: 1-3 streams, 10-180 seconds, 1-8 sports")
    report = {"started_at": utc_now(), "kind": "active_sports_one_connection_per_event",
              "seconds_per_stream": seconds, "max_concurrent_streams": max_streams,
              "discovery_requests": 0, "stream_requests": 0, "discovery": [], "streams": []}
    own_client = client is None
    client = client if client is not None else BetclicSnapshotClient(timeout=(5, 8))
    candidates = []
    now = datetime.now(timezone.utc)
    try:
        for sport in sports:
            item = {"sport": sport}
            report["discovery"].append(item)
            report["discovery_requests"] += 1
            try:
                matches = client.get_matches(sport).get("matches", [])
                candidate = select_live_candidate(matches, now)
                item.update({"listing_count": len(matches), "live_flag_count": sum(bool(m.is_live) for m in matches),
                             "candidate_found": candidate is not None})
                if candidate is not None:
                    candidates.append((sport, candidate))
                    item["candidate_id"] = candidate.id
            except Exception as error:
                item["error"] = f"{type(error).__name__}: {error}"
    finally:
        if own_client:
            client.close()

    observations = [Observation(sport, match) for sport, match in candidates[:max_streams]]
    threads = []
    selected = {(sample.sport, sample.match.id) for sample in observations}
    for item in report["discovery"]:
        item["stream_selected"] = (item["sport"], item.get("candidate_id")) in selected

    def receive(sample):
        sample.started = time.monotonic()
        sample.end = "connected_or_connecting"
        try:
            streamer(sample.match.id, sample.stop, sample.record)
            sample.end = "sample_limit" if sample.limit_reached else "time_limit" if sample.stop.is_set() else "closed"
        except Exception as error:
            sample.end = transport_error(error)
            sample.error = f"{type(error).__name__}: {error}"

    for sample in observations:
        thread = threading.Thread(target=receive, args=(sample,), daemon=True)
        threads.append(thread)
        report["stream_requests"] += 1
        thread.start()
    deadline = time.monotonic() + seconds
    for thread in threads:
        thread.join(max(0, deadline - time.monotonic()))
    for sample in observations:
        sample.stop.set()
    # The shared transport has a 35-second read timeout. One global grace period.
    shutdown_deadline = time.monotonic() + 40
    for thread in threads:
        thread.join(max(0, shutdown_deadline - time.monotonic()))
    for sample, thread in zip(observations, threads):
        result = sample.result()
        result["thread_stopped"] = not thread.is_alive()
        if thread.is_alive():
            result["transport_end"] = "shutdown_timeout"
        report["streams"].append(result)
    if not observations:
        report["outcome"] = "discovery_failed" if all("error" in item for item in report["discovery"]) else "no_started_live_candidates"
    else:
        report["outcome"] = "stream_data_received" if any(s["frame_count"] for s in report["streams"]) else "no_stream_frames"
    report["finished_at"] = utc_now()
    return report


def markdown_summary(report):
    def cell(value):
        return str(value).replace("|", "\\|").replace("\n", " ")
    lines = ["## Betclic active-sports streaming probe", "",
             "One discovery per sport; one connection per selected event; no reconnection.", "",
             "| Sport | Event | Frames | Distinct payloads | Spread (s) | Market updates | Evidence | Connection end |",
             "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |"]
    for stream in report["streams"]:
        lines.append("| " + " | ".join(map(cell, [
            stream["sport"], stream["event"]["name"], stream["frame_count"],
            stream["distinct_payloads"], stream["spread_seconds"], stream["late_market_updates"],
            stream["evidence"], stream["transport_end"],
        ])) + " |")
    lines += ["", "Outcome: **" + report["outcome"] + "**.",
              "Different payload bytes alone do not prove score/price changes. A read timeout after data does not erase the observed evidence.", ""]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sports", default=",".join(DEFAULT_SPORTS))
    parser.add_argument("--max-streams", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=90)
    parser.add_argument("--output", type=Path, default=Path("captures/streaming/report.json"))
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args(argv)
    sports = tuple(dict.fromkeys(s.strip() for s in args.sports.split(",") if s.strip()))
    if not sports or len(sports) > 8 or any(s not in SPORTS for s in sports):
        parser.error("--sports must contain 1-8 supported Betclic sports")
    if not 1 <= args.max_streams <= 3 or not 10 <= args.seconds <= 180:
        parser.error("--max-streams: 1-3; --seconds: 10-180")
    report = run_probe(sports, args.max_streams, args.seconds)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.summary:
        with args.summary.open("a", encoding="utf-8") as output:
            output.write(markdown_summary(report))
    compact = {**report, "streams": [{k: v for k, v in item.items() if k != "frames"} for item in report["streams"]]}
    print(json.dumps(compact, ensure_ascii=False, indent=2))
    print("OUTCOME:", report["outcome"])
    return int(report["outcome"] in ("discovery_failed", "no_stream_frames"))


if __name__ == "__main__":
    raise SystemExit(main())

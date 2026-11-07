"""Offline comparison of the previous decoder and winner-only live parsing.

Input is a local capture from tools.probe_long_streaming; no network is used.
The baseline source is loaded from this repository's trusted Git history.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess
import time

from betclic_api.parser import parse_match_detail_response

from app.live_parser import LiveMatchParser

BASELINE = "4152798ece2dd7d1b75b0bed9245a5c5e000de06"
FILENAME = re.compile(r"_(\d+)_s(\d+)_f(\d+)\.protobuf\.bin$")


def baseline_module(path):
    source = subprocess.check_output(["git", "show", f"{BASELINE}:{path}"], text=True)
    namespace = {"__name__": "pipeline_baseline"}
    exec(compile(source, f"{BASELINE}:{path}", "exec"), namespace)
    return namespace


def benchmark(directory, repeats=20, runs=5):
    samples = []
    for path in Path(directory).glob("*.protobuf.bin"):
        match = FILENAME.search(path.name)
        if match:
            samples.append((tuple(map(int, match.groups())), path))
    samples.sort()
    if not samples or len(samples) > 2000:
        raise ValueError("Use a capture with 1–2000 frames")
    if sum(path.stat().st_size for _, path in samples) > 60_000_000:
        raise ValueError("Capture exceeds the 60 MB benchmark limit")
    frames = [(key, {"flags": 0, "data": path.read_bytes()}) for key, path in samples]
    old_scores = baseline_module("app/betclic_scores.py")["decode_score_frames"]
    old_winner = baseline_module("app/winner_odds.py")["winner_market"]
    old_runs, new_runs = [], []
    for _ in range(runs):
        started = time.perf_counter()
        for _ in range(repeats):
            for (match_id, _, _), frame in frames:
                old_scores([frame], expected_match_id=match_id)
                detail = parse_match_detail_response([frame])
                if detail.id == match_id:
                    old_winner(detail.markets)
        old_runs.append(time.perf_counter() - started)
        decoders = {(match_id, session): LiveMatchParser(match_id)
                    for (match_id, session, _), _ in frames}
        started = time.perf_counter()
        for _ in range(repeats):
            for (match_id, session, _), frame in frames:
                decoders[(match_id, session)].feed(frame)
        new_runs.append(time.perf_counter() - started)
    compared = 0
    for (match_id, _, _), frame in frames:
        detail = parse_match_detail_response([frame])
        old = old_winner(detail.markets) if detail.id == match_id else None
        if old is not None:
            new = LiveMatchParser(match_id).feed(frame).market
            if new != old:
                raise AssertionError(f"Winner snapshot differs for match {match_id}")
            compared += 1
    old_median, new_median = statistics.median(old_runs), statistics.median(new_runs)
    return {"baseline_commit": BASELINE, "frames": len(frames), "repeats": repeats, "runs": runs,
            "old_seconds": old_runs, "new_seconds": new_runs,
            "old_median_seconds": old_median, "new_median_seconds": new_median,
            "speedup": old_median / new_median,
            "winner_snapshots_compared": compared,
            "inputs": [{"file": path.name, "sha256": hashlib.sha256(frame["data"]).hexdigest()}
                       for (_, path), (_, frame) in zip(samples, frames)]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("captures/pipeline_benchmark.json"))
    args = parser.parse_args()
    if not 1 <= args.repeats <= 100 or not 1 <= args.runs <= 9:
        parser.error("--repeats: 1–100; --runs: 1–9")
    result = benchmark(args.directory, args.repeats, args.runs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "inputs"}))


if __name__ == "__main__":
    main()

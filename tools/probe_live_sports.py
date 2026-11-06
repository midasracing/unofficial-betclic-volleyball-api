"""One-shot diagnostic of live offering across multiple sports.

Uses original Betclic API client without a browser. It probes only 3 sports,
one listing request each, and at most one detail request for a live event.
No infinite polling, credentials or secrets.
"""
import json
import sys
from datetime import datetime, timezone

from app.client import BetclicSnapshotClient as BetclicClient

SPORTS = ("volleyball", "tennis", "football")

def main():
    client = BetclicClient(timeout=(5, 10))
    report = {"timestamp_utc": datetime.now(timezone.utc).isoformat(),
              "sports": {}, "probe": "public offering API", "successes": 0}
    for sport in SPORTS:
        item = {"count_returned": 0, "live_count": 0, "live_examples": []}
        try:
            data = client.get_matches(sport)
            matches = data.get("matches", [])
            live = [m for m in matches if m.is_live]
            item["count_returned"] = len(matches)
            item["total_reported"] = data.get("total")
            item["live_count"] = len(live)
            item["live_examples"] = [
                {"id": m.id, "name": m.name, "competition": m.competition,
                 "competition_id": m.competition_id, "date": m.date}
                for m in live[:5]
            ]
            report["successes"] += 1
            if live and live[0].id is not None:
                try:
                    detail = client.get_match(live[0].id)
                    item["detail"] = {
                        "id": detail.id, "name": detail.name,
                        "markets": [{"name": market.name,
                                     "suspended": market.suspended,
                                     "selections": [
                                         {"name": selection.name, "odds": selection.odds}
                                         for selection in market.selections[:10]]}
                                    for market in detail.markets[:5]],
                        "score_supported_by_parser": False,
                    }
                except Exception as exc:
                    item["detail_error"] = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            item["error"] = f"{type(exc).__name__}: {exc}"
        report["sports"][sport] = item
    print(json.dumps(report, indent=2, ensure_ascii=False))
    sys.exit(0 if report["successes"] else 1)

if __name__ == "__main__":
    main()

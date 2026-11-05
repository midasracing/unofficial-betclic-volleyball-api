# Unofficial Betclic men's volleyball API — MVP

Unofficial, not affiliated with Betclic. This API filters men's volleyball:
national teams, international club tournaments, Poland PlusLiga, Italy SuperLega,
and Japan men's SV.League. Unknown competitions are excluded by default.

## Run

Python 3.10+:

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Docs: http://127.0.0.1:8000/docs

- `GET /health`
- `GET /api/v1/matches`
- `GET /api/v1/matches/live`
- `GET /api/v1/matches/{id}`
- `GET /api/v1/matches/{id}/odds`
- `GET /api/v1/matches/{id}/score` — current set and points, completed sets, set wins
- `GET /api/v1/competitions`
- `GET /api/v1/catalog` — discovered competition names and inclusion status
- `GET /api/v1/schema` — available fields and unsupported score fields
- `WS /api/v1/live` (snapshot and changes)

## Important limitations

- **Volleyball score notifications ARE present in captured raw gRPC-web frames.**
  `app/betclic_scores.py` now decodes a verified notification shape with
  current points, completed sets and the current period. For events with no
  decoded score notification these fields stay null. Serving team is still
  unknown. This schema requires validation on additional men's matches. Never infer score from betting odds.
- **Not upstream push streaming**: the MVP polls Betclic every 30 seconds by
  default (minimum 15 seconds); downstream WebSocket emits detected changes.
- Betclic's original offering client uses the French regulatory zone. Its
  listing may differ from the Polish site and some tournaments may be absent.
- Competition names and gender filtering are **conservative heuristics**,
  not verified Betclic IDs; review actual data before production. Women's
  events with ambiguous names may require additional metadata and an ID
  allowlist for certainty.
- Results may be missing or stale during upstream failure; inspect /health.
- Respect terms of service and data-provider restrictions. No automated betting.

## Configuration

`POLL_SECONDS` (default 30, minimum 15); `PAGE_LIMIT` (default 4,
max 10 pages of ~40 matches).

## Output conventions

Each normalized match includes `sport`, `gender`, `provider`, `status`, `market_count`, and `source_data`. The latter preserves **every field already decoded by the upstream Python dataclass** and is not a dump of unknown raw protobuf tags. The normalized `score`, `sets`, `current_set`, and `current_points` fields are populated when a verified score-notification frame is observed; otherwise null. `serving_team` remains null. The `status` value `scheduled_or_unknown` does **not** mean Betclic confirmed a match is scheduled rather than completed.

`/api/v1/catalog` helps identify competition aliases and which were excluded by conservative filters. It shows names and counts, not excluded events. It is intended for diagnostics and may reveal women's competition names; the /matches endpoints still filter them. Periodic polling is not equivalent to Betclic source streaming.

## Captured-score evidence (2026-11-05)

The user-provided GitHub Actions protobuf artifact contained six data frames for
Fenerbahce F. vs Afyon Bld F. The 120-byte fifth frame was a separate
volleyball scoring notification with match ID 1271196261842944, current points
15:10, first set 25:10, and current period `vlb_second_set`. It does **not**
prove serving team or all game status codes. A test uses the exact captured
120-byte notification. The upstream client still lacks score support.

This example is women's volleyball and exists **only as a protocol regression
fixture**. Event output remains limited to allowed men's competitions.

## Score API

`GET /api/v1/matches/{id}/score` exposes a single decoded score snapshot:

```json
{
  "match_id": 1271196261842944,
  "updated_at": "2026-11-05T11:33:00+00:00",
  "status": "decoded_from_grpc_notification",
  "sets_won": {"home": 1, "away": 0},
  "completed_sets": [{"period": "vlb_first_set", "home": 25, "away": 10}],
  "current_set": 2,
  "current_points": {"home": 15, "away": 10},
  "source": "betclic_grpc_web_score_notification"
}
```

**Illustrative historical fixture** from a women's match, used only to validate the parser.
The production competition filter remains men's-only. `updated_at` is our
refresh time, not a server-origin timestamp. When the source does not send a
supported score notification, the score fields are **null**, not 0:0.
The source of points is the same gRPC-web detail response as the betting markets.
The source currently uses **polling** and has not been proven to have continuous
low-latency push updates in production. Values should be treated as stale if
`/health` reports a refresh error.

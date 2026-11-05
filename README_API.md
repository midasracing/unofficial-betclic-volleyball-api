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
- `GET /api/v1/competitions`
- `WS /api/v1/live` (snapshot and changes)

## Important limitations

- **Scores and set-by-set points are NOT available** from the existing
  `TapsHTS/betclic-api` Match parser. The `score` field is explicitly null.
  Integrate a separate licensed scores feed, or first verify the raw
  gRPC-web payload contains score fields. Never infer score from betting odds.
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

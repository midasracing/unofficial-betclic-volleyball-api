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
- `GET /api/v1/matches/{id}/odds` — **only match winner** (two-way team victory odds), no exact-score or set-winner betting markets
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
- **Experimental upstream push streaming is now implemented.** In streaming
  mode (default), one persistent gRPC-web HTTP connection per monitored
  fixture is maintained (default: at most one fixture per target league,
  three total). Sustained multi-frame score updates over one connection were
  confirmed in a short probe on 2026-11-05, but full-day stability and live
  price-change parsing still require verification.
- Match discovery is checked no more often than every 10 minutes by default;
  REST/WebSocket clients never trigger Betclic requests. Do not run multiple
  server workers/instances without external leader election.
- After disconnect, reconnection waits at least 90 seconds and backs off to
  15 minutes. This intentionally favors server friendliness over guaranteed
  low latency. If streaming is not supported, the feed becomes stale.
- Betclic's original offering client uses the French regulatory zone. Its
  listing may differ from the Polish site and some tournaments may be absent.
- Competition names and gender filtering are **conservative heuristics**,
  not verified Betclic IDs; review actual data before production. Women's
  events with ambiguous names may require additional metadata and an ID
  allowlist for certainty.
- Results may be missing or stale during upstream failure; inspect /health.
- Respect terms of service and data-provider restrictions. No automated betting.

## Configuration

Streaming is enabled by default with `BETCLIC_STREAM=1`. Use
`BETCLIC_STREAM=0` only for the bounded fallback polling mode.
`DISCOVERY_SECONDS` defaults to 600 (minimum 600) for the match directory;
PAGE_LIMIT` defaults to 4. By default streaming selects up to **three
concurrent matches**: one each from Poland's PlusLiga, Italy's men's SuperLega,
and Japan's men's SV.League. A new connection starts no more often than every
30 seconds, even if several live matches are discovered at once. Optional
`STREAMS_PER_LEAGUE=2` and `MAX_STREAMS=6` allow up to two per league
and six overall; do not raise these until provider limits have been assessed.
`STREAM_START_GAP_SECONDS` defaults to 30, minimum 20. The
fallback mode uses `POLL_SECONDS` (default 30, minimum 15) and
`MAX_DETAILS_PER_CYCLE` (default 3, max 5). Start uvicorn with exactly one
worker and do not deploy several app instances yet.

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
The source now has an **experimental streaming collector**, but sustained
low-latency push updates have not been verified in production. Values should be treated as stale if
`/health` reports a refresh error.

## End-to-end streaming data path

```text
Betclic gRPC-web MatchService stream (one HTTP POST held open)
  -> app.streaming.StreamWorker (incremental gRPC-web frame reader)
  -> app.betclic_scores (decode score notifications)
  -> app.main.Store (in-memory latest state)
  -> GET /api/v1/matches/{id}/score, /odds (cached; zero Betclic calls)
  -> WS /api/v1/live (push from the cache to any connected web clients)
```

Frontend clients must only talk to this API. They should initialize from REST
or the WebSocket `snapshot` event and subscribe to `match_updated` events.
No browser traffic must be forwarded to Betclic. A WebSocket heartbeat is not
proof of upstream activity: inspect `GET /health` for stream state and
`last_frame_at`. When no fresh upstream frame has arrived, the frontend must
not label old values as fresh.

The score decoder is validated against a single captured notification. The
price parser may require additional adaptation if Betclic sends incremental
odds deltas instead of full detail snapshots. Unknown delta messages are not
silently converted into odds updates. Do not increase upstream concurrency
until the actual behavior has been inspected and authorized.

## League-specific streaming coverage

The live upstream streaming scheduler targets **only** the following men's
national leagues, even though the REST match catalog may include eligible
international tournaments:

| League | Internal category | Stream cap by default |
| --- | --- | --- |
| Poland PlusLiga | `league:poland` | 1 |
| Italy SuperLega | `league:italy` | 1 |
| Japan SV.League Men | `league:japan` | 1 |

Match discovery uses the upstream **French Betclic offering**. That catalog
might omit some or all fixtures or use competition aliases not yet recognized.
No stream is opened if a fixture is absent or its start/live flags do not pass
the conservative eligibility check. The API cannot guarantee that all
simultaneous fixtures are streamed under the default safety limit.

`GET /health` lists currently connected match IDs, last frame times and
stream counts. More matches can be enabled deliberately after the live
catalog and provider load have been measured.

### Winner-odds example

`GET /api/v1/matches/{id}/odds`:

```json
{
  "match_id": 123,
  "updated_at": "2026-11-05T12:01:59+00:00",
  "type": "match_winner_only",
  "market": {
    "name": "Vainqueur du match",
    "suspended": false,
    "selections": [
      {"name": "Home team", "odds": 1.52},
      {"name": "Away team", "odds": 2.48}
    ]
  },
  "markets": [
    {
      "name": "Vainqueur du match",
      "suspended": false,
      "selections": [
        {"name": "Home team", "odds": 1.52},
        {"name": "Away team", "odds": 2.48}
      ]
    }
  ]
}
```

The numeric values above are illustrative, not live Betclic quotes.
The `markets` compatibility array contains the same sole winner market
when present (the example omits it for brevity). Match scores and points
remain separately available at `/score`.

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

For deployment, use the included Dockerfile with exactly one worker. The
`render.yaml` blueprint defines a free service. `CORS_ORIGINS` defaults to
`https://midasracing.github.io`; set a comma-separated list for other frontend
origins. A frontend needs the deployed HTTPS API base URL and its WSS
`/api/v1/live` endpoint. Committing this repository does not create a hosted
service. See [the PlusLiga verification](docs/PLUSLIGA_LIVE_2026-11-07.md).

- `GET /health`
- `GET /api/v1/matches`
- `GET /api/v1/matches/live`
- `GET /api/v1/matches/{id}`
- `GET /api/v1/matches/{id}/odds` — **only match winner** (two-way team victory odds), no exact-score or set-winner betting markets
- `GET /api/v1/matches/{id}/score` — current set and points, completed sets, set wins
- `GET /api/v1/leagues` — coverage and stream IDs for Poland, Italy, Japan
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
  confirmed in short probes, including score and winner-price changes on
  2026-11-06. Full-day reliability still requires verification.
- Match discovery is checked no more often than every 10 minutes by default;
  REST/WebSocket clients never trigger Betclic requests. Do not run multiple
  server workers/instances without external leader election.
- Startup and productive streams (two nonempty frames separated by two seconds)
  receive a bounded recovery window: up to **three fast retries within 30 seconds**.
  Transient failures, gRPC 4/14 and clean gRPC completion retry after **one
  second plus jitter**. Openings for the same event remain at least five seconds
  apart, with one connection per event. A single failed reconnect can use the
  remaining fast attempts instead of immediately waiting 90 seconds. When the
  window or retry budget expires, transient failures back off through 5, 10,
  20 and at most 30 seconds, including jitter. HTTP 500/502/503/504 use this
  policy too. Rate-limit/protocol failures retain conservative 90–900-second
  backoff. HTTP `Retry-After` on 429/503 takes precedence over all local
  limits; HTTP 401/403 and gRPC 7/16 stop the worker.
  Betclic can cancel streams: recovery reduces the interruption but cannot
  guarantee an uninterrupted feed. See [the original recovery test](docs/STREAM_RECOVERY_2026-11-06.md)
  and [the continuity follow-up](docs/LIVE_CONTINUITY_2026-11-06.md).
- Betclic's original offering client uses the French regulatory zone. Its
  listing may differ from the Polish site and some tournaments may be absent.
- Competition-name filtering remains conservative. Betclic competition ID
  **36538** is explicitly mapped to Japan's men's SV.League after observing
  `Japon SV Ligue` with Voreas Hokkaido in the offering probe on 2026-11-06.
  Explicit women's markers still take precedence over that ID. Other unknown
  or ambiguous competition names remain excluded.
- Results may be missing or stale during upstream failure; inspect /health.
- Missing legacy live flags are recovered from verified nonzero volleyball scores
  in the listing, only after a recent scheduled start. Future/unknown starts,
  zero placeholders and decided best-of-five matches are not promoted.
- Respect terms of service and data-provider restrictions. No automated betting.

## Configuration

Streaming is enabled by default with `BETCLIC_STREAM=1`. Use
`BETCLIC_STREAM=0` only for the bounded fallback polling mode.
`DISCOVERY_SECONDS` defaults to 600 (minimum 600) for the match directory;
`PAGE_LIMIT` defaults to 4. By default streaming selects up to **three
concurrent matches**: one each from Poland's PlusLiga, Italy's men's SuperLega,
and Japan's men's SV.League. Newly selected matches start no more often than
every 30 seconds, even if several are discovered at once. Reconnecting an
existing match uses the separate recovery policy above. Optional
`STREAMS_PER_LEAGUE=2` and `MAX_STREAMS=6` allow up to two per league
and six overall; do not raise these until provider limits have been assessed.
`STREAM_START_GAP_SECONDS` defaults to 30, minimum 20. The
`STREAM_RECOVERY_SECONDS` setting defaults to 1 (range 1–90) before jitter and
the minimum interval between openings of the same event. The
fallback mode uses `POLL_SECONDS` (default 30, minimum 15) and
`MAX_DETAILS_PER_CYCLE` (default 3, max 5). Start uvicorn with exactly one
worker and do not deploy several app instances yet.

Streaming reads allow **90 seconds of upstream silence** (previously 35);
this does not delay frames that arrive earlier. A connection still blocked in
a read may take up to 90 seconds to stop; shutdown allows a 95-second grace
period. The recovery delay is separate from this idle-read timeout.

Per-event `/health.streams` entries expose `state`, `last_frame_at`,
`last_end_reason`, `last_grpc_status`, `consecutive_failures`,
`last_retry_delay`, `next_retry_at`, `last_connection_seconds`, `disconnects`,
`last_open_at`, `last_disconnect_at`, `last_recovery_seconds`,
`longest_recovery_seconds`, `recovery_attempts_left`, `last_frame_age_seconds`
and `receiving`. The recovery metrics measure from disconnection to the first
nonempty payload after reconnection, including intervening failed attempts.
They measure client-observed recovery, not latency from the sporting event.
The `*_at` values in stream stats are Unix timestamps.

The top-level health status becomes `degraded` if an expected live stream is
missing, opening, cooling down, blocked or has no frame within the read timeout,
even if catalog refresh succeeded. `expected_live_streams`,
`receiving_live_streams` and `live_stream_status` summarize this check for the
configured stream limits. Receiving a transport frame does not by itself prove
a fresh score or price; their observation timestamps remain separate.

Catalog refresh merges metadata into the latest pushed state on the event loop,
so it cannot replace a newer pushed score or odds with its older cached copy.

## Output conventions

Each normalized match includes `sport`, `gender`, `provider`, `status`, `market_count`, and `source_data`. The latter preserves dataclass fields after this client's live-flag correction; it is not a dump of raw protobuf tags. The normalized `score`, `sets`, `current_set`, and `current_points` fields are populated when a verified score-notification frame is observed; otherwise null. `serving_team` remains null. The `status` value `scheduled_or_unknown` does **not** mean Betclic confirmed a match is scheduled rather than completed.

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

`app.live_parser.LiveMatchParser` reads score entries from both initial snapshots
and score notifications. Multiple notifications in one envelope are processed
in order. Snapshot recovery can therefore restore points before the next rally
notification arrives. The legacy `score_status` value is retained; `score_source`
identifies a snapshot or notification.

Winner-price deltas are applied immediately only after a two-selection winner
snapshot establishes the match, market and selection IDs. The mapping resets
for every new upstream connection. Other matches/markets and unknown IDs are
ignored. Numeric delta availability codes are not interpreted as suspension or
resumption. An explicit invalid/unit-price winner snapshot or a known-selection
invalid delta withdraws the quote (`market: null`) until valid prices return.
Prices must be finite, greater than 1 and less than 10000 after rounding.

The live path parses only winner markets and supported score entries. It does
not construct all unused markets. Identical confirmations refresh observation
timestamps without publishing another full match update. The collector also
skips catalog copying and worker-thread work between actual discovery cycles.
`/matches.updated_at` reflects cached data observations and `/score.updated_at`
uses the score observation time; `/catalog.updated_at` remains catalog discovery.

Each WebSocket client has a bounded queue with one pending latest state per
match. A slow client receives current state instead of replaying every
intermediate point/price snapshot. If too many distinct matches overflow that
queue, the next message is a fresh `snapshot`; clients must treat **every**
snapshot as authoritative, including snapshots received after the first one.
Heartbeat messages include per-active-match `observations` with
`score_observed_at` and `odds_observed_at`, so quiet-but-confirmed values can
refresh their freshness indicators. This stream serves latest state, not a
complete event history.

See [the pipeline benchmark and live parser verification](docs/PIPELINE_OPTIMIZATION_2026-11-06.md).

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
the conservative eligibility check (including a six-hour age limit on the start time). The API cannot guarantee that all
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

### Japanese league identifier evidence

The [offering probe](https://github.com/midasracing/unofficial-betclic-volleyball-api/actions/runs/42206411474) returned competition ID `36538`, named `Japon SV Ligue`, with Voreas Hokkaido. The team appears in the [official SV.League men\'s team directory](https://www.svleague.jp/en/sv_men/team/detail/479). The production filter uses this verified ID rather than accepting all competitions with an ambiguous Japanese name.

## Active-sports streaming diagnostics

The diagnostic probe can test currently started matches outside production
volleyball coverage, including women's events. Production filters are unchanged.

```bash
python -m tools.probe_streaming --sports tennis,table_tennis,football --max-streams 3 --seconds 90
```

By default it discovers tennis, table tennis, football, basketball, hockey,
handball and volleyball, with one listing per sport and at most three simultaneous
streams. Each selected event must have a live flag, two sides and a known kickoff
in the past six hours. The newest started event per sport is preferred. A live
flag alone never admits a future event. This is a bounded diagnostic of the first
listing page per sport, not a complete catalog scan.

GitHub Actions **Probe Betclic active sports streaming** runs the probe automatically
when its code changes and can also be started manually with sport/time inputs.
Each selected match uses one connection with no automatic reconnect. The report
and step summary are saved to `captures/streaming/report.json` and uploaded as an artifact.

The report separates **evidence** from **transport_end**:
- `initial_data_only`: no temporally separated delivery was demonstrated.
- `repeated_payloads_only`: later deliveries contained identical bytes.
- `different_payloads_observed`: bytes changed, without claiming decoded game/price changes.
- `market_updates_observed`: a later decoded market snapshot changed.
- `volleyball_score_updates_observed`: a later recognized volleyball score changed.

A `read_timeout` after receiving frames does not erase that evidence or prove an
unusable stream. No candidate is reported as `no_started_live_candidates`, rather
than a connection failure. Other sports are never passed to the volleyball score
decoder. Samples are bounded by time, frame count and bytes. A completed diagnostic
workflow means the report was produced; inspect the evidence and connection-end
columns for the actual result.

## Longer score and odds capture

Use `python -m tools.probe_long_streaming --seconds 600` for a bounded long capture (maximum 900 seconds, three events and four connections per event, with 90-second reconnection cooldown). It may finish early when every connection budget is exhausted. Each run uses a fresh capture directory. `tools.analyze_long_capture` inspects captured payloads offline, including initial score snapshots, score notifications and raw match-winner prices.

See [the ten-minute test report from 2026-11-06](docs/LONG_STREAM_TEST_2026-11-06.md) for observed point and winner-price changes on the same connection. Diagnostic raw values, including 1.00 and uninterpreted source codes, do not establish that a selection is currently available to place.

# Research: are live volleyball scores carried by Betclic?

Status: **plausible, not yet independently verified from a raw volleyball frame**.
Updated 2026-11-05.

## Evidence

1. Upstream TapsHTS/betclic-api `models.Match` has no score, set or
   current points fields. Its parser handles fields 1,2,3,6,8,12 on a match
   and markets at field 11. Unmapped fields may exist.
2. PulseScore documents a Betclic-derived in-play service exposing
   `score.home`, `score.away` (sets won), `score.info`,
   `matchClock.periodId`, `statistics.sets.home` and `.away`
   (set point arrays), plus `homeServe` for volleyball.
   https://pulsescore.net/betclic-api
   https://api.pulsescore.net/api/betclic/doc
3. PulseScore says it consumes a Betclic push stream, but that does NOT
   establish which offering gRPC method/field contains the scoreboard.
   Its feed is a third-party normalized product, not an official Betclic spec.
4. This environment could not resolve `offering.begmedia.com` for an actual
   match probe. Therefore no live frame has been inspected or validated.

## Safe local verification procedure

1. Identify an actual **men's volleyball** match ID from `get_live("volleyball")`
   while the match is active. Use a test fixture only if licensing allows.
2. Run `python -m tools.inspect_match_proto MATCH_ID`; it makes a **single**
   detail request and prints a bounded field-tree summary.
3. Capture one more sample *only if needed*, later in the same match. Compare
   changed small fields with the displayed official scoreboard.
4. Determine whether scores are attached to this detail response, another
   gRPC service, or separate endpoints; verify set boundaries and suspended
   moments. Do **not** guess field numbers from coincidental integer values.
5. Add fixture-based decoding tests before changing `score: null` in the API.

## Fallback

Keep score models decoupled from Betclic so a separately licensed scoring
feed can provide point-by-point results. Join only using validated event,
competition, home/away team and scheduled-start metadata, with confidence
checks and freshness timeouts.

## Important scope note

The present app is a **polling MVP**, not verified upstream push streaming.
A downstream WebSocket does not itself make data more real-time.

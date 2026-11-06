# Live continuity follow-up — 2026-11-06

## Changes

The original recovery test measured 16.337 seconds from a server cancellation
to the next score/odds snapshot. This follow-up targets that interruption and
the long cooldown caused by one failed reconnect.

- Default recovery wait is one second plus up to 0.1 seconds of jitter.
- Up to three fast retries are allowed within a 30-second window at startup
  and after productive delivery (two nonempty payloads separated by two seconds).
  A failed reconnect consumes that budget rather than immediately imposing
  90 seconds of silence. Openings for one event remain at least five seconds apart.
- gRPC 4/14, connection/read failures, broken chunks, truncated frames, EOF
  without trailers and clean gRPC completion can use this recovery policy.
- Empty heartbeats do not replenish the retry budget. Exhausted/expired
  recovery windows back off through 5, 10, 20 and at most 30 seconds for
  transient failures, including HTTP 500/502/503/504. Rate limits and protocol
  errors retain conservative 90–900-second backoff. HTTP `Retry-After` on
  429/503 overrides local caps; denied access still stops the worker.
- Per-worker metrics record disconnect count and time to the first nonempty
  payload after recovery, including intervening failed attempts.
- `/health` includes the actual state of expected live streams, instead of
  declaring success solely because catalog refresh worked.
- Catalog refresh preserves points/odds pushed while its worker thread was
  preparing a result. A regression reproduces the old 11:10 → 10:10 rollback
  and verifies the newer state survives.

The configured men's volleyball leagues and concurrency limits are unchanged.
REST/WebSocket clients continue to read cached data, without triggering upstream
requests. The delay can be increased using `STREAM_RECOVERY_SECONDS` (1–90).

## HTTP/2 check

A separate unmodified-upstream probe negotiated **HTTP/2** on Palencia – Granada,
event `1271837715623936`, starting at 21:41:02 UTC. It still ended with gRPC 14,
`A task was canceled.`, after about 122 seconds. HTTP/2 alone therefore does not
eliminate the observed cancellation. The production transport and dependencies
were retained; this is one observation, not a protocol performance comparison.

## Continuous observation

A bounded 600-second probe uses the production `StreamWorker` with one stream
per selected currently started event, three simultaneous events, at most ten
connections per event and no injected failures. It records frames, decoded
score/market changes and time from an actual disconnect to a new payload.
The diagnostic permits other sports to exercise the shared transport.

The run lasted **600.132 seconds after discovery**, from approximately
21:45:46 to 21:55:46 UTC, and collected **411 frames**. It confirmed **14
resumptions with newly decoded data** after unmodified upstream cancellations:

| Event | Connections | Frames | Completed recoveries | Decoded market-state changes | Decoded score changes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Taylor Fritz – Carlos Alcaraz (tennis) | 6 | 36 | 2 | 12 | 7 |
| Palencia – Granada (basketball) | 8 | 126 | 5 | 15 | 0 |
| Famalicao – Arouca (football) | 8 | 249 | 7 | 82 | 0 |

Recovery time, measured from stream closure to the next nonempty payload:

- Minimum: **1.366 seconds**.
- Median: **1.4475 seconds**.
- Maximum across the 14 completed recoveries: **1.752 seconds**.

Every measured recovery contained decoded markets; the tennis recoveries also
contained scores. Market-state changes count all decoded markets in this
diagnostic, not exclusively the production match-winner market. Zero score
changes for other sports does not imply no scoring: the diagnostic score
decoder supports tennis/table tennis/volleyball only.

The tennis and basketball events subsequently became unavailable: the source
returned gRPC 5, `Match not found`. Those missing events are not counted as
successful reconnections. Football's last cancellation arrived at 599.843
seconds; the observation deadline stopped the next retry. All worker threads
then stopped cleanly. No failure was injected and no final score was inferred.

The live observation verifies the one-second resubscription path. The capped
5–30-second transient backoff, denied/rate-limited access, catalog merge and
health reporting are covered by the offline suite: **112 tests plus six
subtests pass** on Python 3.12. CI runs the same suite on Python 3.11.

Connection timings and per-event evidence are preserved in the
[compact machine-readable report](fixtures/live_recovery_2026-11-06.json).

## Practical limits

This reduces recovery gaps; it does not prevent server cancellations, replay
missed notifications or guarantee zero-delay/full-day reliability. A fresh
snapshot after recovery resynchronizes state; unknown message shapes are not
invented into scores or odds. A healthy transport and fresh sporting data
remain separate observations.

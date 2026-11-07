# Live data pipeline optimization — 2026-11-06

The live path previously decoded scores separately, constructed every market
and selection through the upstream general parser, and then retained only the
match-winner market. Price-only notifications were ignored. Every received
score confirmation could also enqueue another full WebSocket match payload.

## Resulting behavior

- Decode supported score entries and winner data through a single live parser,
  skipping selections for unused markets. Only the winner's alternate layouts
  can use the existing pinned parser as a compatibility fallback.
- Apply verified-shape price deltas immediately after matching all three wire
  IDs: event, winner market and selection. Reset identity mappings on reconnect.
  Unknown delta availability codes do not reopen suspended markets.
- Restore score entries from initial snapshots and process all score
  notifications in batched envelopes, retaining the latest matching entry.
- Confirmed identical values refresh REST observation timestamps without
  another full WebSocket match payload. Score-message source changes alone
  also do not trigger a duplicate value update.
- Coalesce each client's pending events by match, preserving fairness across
  matches. Distinct-match overflow requests a current full snapshot instead of
  silently losing a match update/deletion. Buffers are bounded.
- Avoid copying all discarded markets when serializing match metadata. Skip
  no-op catalog rebuilding between discovery cycles. Keep data/score response
  timestamps tied to observations rather than idle collector iterations.

These changes operate on received data and local delivery; they do not add
upstream requests. Production remains filtered to the configured men's
volleyball competitions, with the existing stream limits and recovery policy.

## Offline benchmark

Baseline: commit `4152798ece2dd7d1b75b0bed9245a5c5e000de06`.
Input: 99 genuine frames from the earlier ten-minute capture (tennis/table
tennis), with 20 passes per run and five runs on the same execution environment.
Both old and new paths process the same payloads. Medians:

| Parsing path | Seconds per run |
| --- | ---: |
| Previous score + general match/market parser | 1.079358321 |
| Winner-only stateful live parser | 0.123767586 |

**8.72× faster parsing in this sample**, or about 88.5% less parser CPU time.
All **23 valid winner snapshots** in the capture matched the old normalized
quotes. This measures parser work, not end-to-end network latency, full API
throughput or delay from the actual sporting event.

The benchmark loads the old functions from the fixed Git commit and performs
no network requests. Run against a locally generated capture:

```bash
python -m tools.benchmark_pipeline captures/long/CAPTURE_DIRECTORY
```

Per-run times and input hashes are in the
[benchmark report](fixtures/pipeline_benchmark_2026-11-06.json).

## Live verification

A separate bounded probe ran from 22:15:19 to 22:17:06 UTC on
Unilife Maringa F. – SESC-RJ F., event `1269745541206016`. This women's match
was used **only as a protocol diagnostic**; production filters remain men's-only.
The probe used one upstream connection and received 17 frames. Its 90-second
sampling limit was followed by a blocked-read shutdown, for 106.057 seconds
total after discovery.

The first snapshot already supplied second-set points **11:9**, completed first
set **20:25**, and winner quotes **4.80 / 1.07**. Later score notifications
and snapshots supplied **11:10 → 11:11** and winner snapshots changed to
**5.00 / 1.06 → 5.80 / 1.04**. Median parser time per received frame was
**208.986 microseconds** in this probe. These are historical observations.

This verifies live score snapshots, notifications and full winner updates.
Winner-price-only deltas were not observed in this short probe: their behavior
is covered by offline ID/isolation regressions using the captured delta shape.
Captured real non-winner deltas are explicitly checked to leave winner odds
untouched. The [probe report](fixtures/optimized_live_2026-11-06.json) preserves
the observations, including repeated unchanged confirmations.

## Validation

The suite covers batched/snapshot scores, captured winner snapshots, unrelated
IDs, invalid/unit prices, suspension preservation, immutable published values,
connection identity reset, immediate delta publication, duplicate suppression,
latest-state delivery, cancellation, overflow resynchronization and listener
cleanup. Existing recovery and competition-filter regressions also pass.

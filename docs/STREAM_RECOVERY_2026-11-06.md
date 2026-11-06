# Stream cancellation and recovery — 2026-11-06

Betclic can terminate an active gRPC-web response with:

```text
grpc-status: 14
grpc-message: A task was canceled.
```

This is a server-reported `UNAVAILABLE` status, not successful completion.
The previous reader discarded trailers, so this failure appeared to be an
ordinary close. It then waited at least 90 seconds before opening another
connection. A separate 35-second read timeout could also end quiet streams.

## Implemented behavior

- Parse gRPC status from response headers and trailer frames. Preserve status
  and reason in the per-event `/health` entry. Reject incomplete frames and
  unsupported compression instead of reporting a successful close.
- Allow 90 seconds of idle reading. Frames arriving sooner are processed
  immediately; this does not add a 90-second batching interval.
- After at least two data frames separated by two seconds, recover from gRPC
  14, connection/read failures, broken chunked responses or EOF without trailers
  after 15 seconds plus 0–1.5 seconds of jitter. Connection openings for the same
  event remain at least 30 seconds apart. Close the old response before retrying.
- Reset the failure streak after productive delivery. Initial-only/empty
  failures retain exponential backoff: 90, 180, 360, 720, up to 900 seconds,
  with small jitter within that cap. Clean gRPC completion retains normal cooldown.
- Honor HTTP `Retry-After` on 429/503, including HTTP-date values. Stop after
  HTTP 401/403 or gRPC 7/16. Keep existing league and concurrency limits.
- Make cancellation interrupt the cooldown. Check cancellation immediately
  after delivering a frame; a blocked read still has a 90-second timeout and
  shutdown allows a 95-second grace period.

## Verification

The offline suite passes **93 tests plus six subtests** on Python 3.12.
New regressions exercise the observed gRPC 14 trailer, malformed/incomplete
responses, cancellation before another blocking read, fast recovery after
productive delivery, exponential backoff and reset, denied access, clean
completion and `Retry-After`. The existing API and men's competition-filter
regressions remain green. GitHub Actions also runs the suite on Python 3.11.

The live probe ran **21:30:22–21:31:57 UTC** on Taylor Fritz – Carlos Alcaraz
(tennis, Open de Paris), event `1272719886798848`. It made one discovery
request and at most two sequential stream connections, stopping immediately
after receiving data on the second connection. No failure was injected.

| Observation | Seconds from first opening |
| --- | ---: |
| First data frame | 0.430 |
| Last of 31 frames on connection 1 | 75.534 |
| Server cancellation, gRPC 14: `A task was canceled.` | 78.063 |
| Connection 2 opened | 94.028 |
| Connection 2 delivered score and winner odds | 94.400 |
| Probe stopped and response closed | 94.401 |

**Measured reconnect delay: 15.965 seconds**, versus the old minimum of
90 seconds. Data resumed 16.337 seconds after the server cancellation.
The second connection contained third-set points **40:40**, games **1:4**,
completed sets **7:6, 5:7**, and match-winner odds **25.00 / 1.01**. These
are historical observations, not current quotes. The first stream included
score notifications and full market snapshots across 75 seconds; the second
provided a fresh snapshot. Receiving that snapshot proves recovery, not
continuous delivery on the intentionally stopped second connection.

The worker recorded `last_grpc_status: 14`, `consecutive_failures: 0` and
`last_retry_delay: 15.964678538834667` during cooldown. After the second
connection delivered data, `last_error` cleared and the probe stopped cleanly.

## Limits

This improves recovery time and avoids premature idle timeouts. It cannot
prevent cancellations originating at Betclic, recover updates missed during a
gap, or establish full-day reliability from a short sample. Tests on tennis
exercise the shared transport only; production still serves the configured
men's volleyball competitions. A WebSocket heartbeat or an initial snapshot
alone is not proof of continuously fresh scores or odds.

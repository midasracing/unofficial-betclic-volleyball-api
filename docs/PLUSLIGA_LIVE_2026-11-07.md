# PlusLiga live check — 2026-11-07

Event: Inpost ChKS Chełm – Norwid Czestochowa, ID `1269825448992768`.

At 17:25 UTC the French Betclic offering listed this fixture with a 17:00 UTC
start and a decoded `is_live: false`. Field 6 used by the pinned parser was
absent. A verified volleyball score was already present at listing path
`root.1.3.13`; a direct detail stream delivered first-set points 15:23 and
winner quotes 2.40 / 1.42. The previous scheduler therefore missed an available
live fixture.

The snapshot client now recovers missing live eligibility from actual scoring
evidence in an already-requested listing. It rejects future/unknown/old starts,
zero placeholders and decided best-of-five games. No extra detail request is
needed to establish eligibility. A captured real listing is a regression fixture.

An end-to-end run used automatic discovery/selection, the actual StreamWorker,
a local Uvicorn server, a real WebSocket client and HTTP API routes. It received
16:25 in the first set, then the transition to 0:0 in the second set. A natural
gRPC 14 cancellation recovered data after 1.683 seconds. Field 9 also exposed
future 0:0 periods; these are now excluded from completed-set counts.

A second run with that correction received second-set points 3:3 → 3:4 and
winner quotes 2.80 / 1.32 → 2.95 / 1.27 through both WebSocket and REST.
These are historical observations, not a current scoreboard. The check validates
delivery from Betclic, not delay relative to television or an official feed.

The API is configured to allow reads from https://midasracing.github.io through
CORS. Dockerfile and a free Render blueprint are included; production hosting
must actually be created before GitHub Pages can connect to a public API URL.
GitHub Pages itself does not execute this Python collector.

152 offline tests and six subtests pass locally. The configured men's league
filters and one collector per running service remain in force.

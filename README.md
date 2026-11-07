# Unofficial Betclic Volleyball API

**An independent, unofficial API for men's volleyball events and odds, built on top of the original [TapsHTS/betclic-api](https://github.com/TapsHTS/betclic-api) project.**

> This repository is an extension/integration based on the original Betclic API client, not an official Betclic product. It is not affiliated with or endorsed by Betclic or the author of the upstream library.

## Samodzielne repozytorium projektu

**Główna, aktualna wersja projektu jest w tym repozytorium, na gałęzi `main`.**
Repozytoria stron `kiedysiatka` i `Volley-hub` nie są potrzebne do jego
instalacji, testowania ani uruchomienia. Kopie klienta Betclic znajdujące się
w repozytoriach stron nie są źródłem tej wersji API.

Tutaj są kompletne moduły API i strumienia (`app/`), testy i przechwycone
przypadki regresyjne (`tests/`), narzędzia diagnostyczne (`tools/`),
zależności (`requirements.txt`), dokumentacja (`docs/`) oraz konfiguracja
wdrożenia (`Dockerfile`, `render.yaml`). Usunięcie kopii plików Betclic
z repozytorium strony nie usuwa ani nie zmienia tego projektu.

Strona może korzystać z uruchomionego API jako osobny klient przez REST lub
WebSocket. Samo repozytorium zawiera kod i konfigurację; publiczny hosting API
pozostaje oddzielnym krokiem.

## Attribution / Podziękowania

This project uses **[TapsHTS/betclic-api](https://github.com/TapsHTS/betclic-api)** as its upstream Python client for the Betclic gRPC-web offering API.

- **Original author/project:** [TapsHTS — betclic-api](https://github.com/TapsHTS/betclic-api)
- **Upstream role:** Requests, gRPC-web framing, Protocol Buffers parsing, initial event and odds models.
- **This project's role:** Men's volleyball competition filtering, normalized REST API, downstream WebSocket updates, diagnostics, live-score decoding, winner-price updates, and experimental upstream streaming.
- **Upstream license:** The original project's `pyproject.toml` declares MIT. At the time of checking, a standalone upstream `LICENSE` file was not present; confirm upstream licensing terms before vendoring or copying its source.

**This repository is not a GitHub fork.** The original package is currently installed as an external dependency. If we later copy or modify its source code directly, we will preserve the original copyright and license notices and document the changes.

## Current capabilities

- Fetch men's volleyball matches from the unofficial Betclic offering client
- Include selected men's competitions: national-team events, international club competitions, Poland's PlusLiga, Italy's SuperLega, and Japan's men's SV.League
- Normalize event IDs, team names, competition metadata, and available betting markets
- REST API with automatic Swagger documentation
- Upstream gRPC-web streaming with reconnect recovery and downstream WebSocket updates
- Score snapshots/notifications, identified winner-price deltas, and latest-state client delivery
- Competition discovery catalog for reviewing Betclic's real competition names

**Limitations:** Streaming and score decoding remain experimental; see [README_API.md](README_API.md) for captured evidence and limits. The serving team is not decoded. Missing score notifications remain null. Actual availability of Poland, Italy and Japan fixtures depends on the upstream French offering.

## Installation

Requires Python 3.10+.

```bash
git clone https://github.com/midasracing/unofficial-betclic-volleyball-api.git
cd unofficial-betclic-volleyball-api
python -m pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000/docs` for the API documentation.

See [README_API.md](README_API.md) for all endpoints, configuration, caveats and diagnostics.

## Maintained version

Use the `main` branch. The upstream dependency is pinned to the source commit validated by the snapshot regression tests. One-shot reads close the notification connection after a complete initial response, including an empty final page. The verified French men's league alias `Pologne Tauron Liga H.` is recognized; women's fixtures and season markets without two teams are excluded from match output.

Run offline checks with `python -m pytest -q tests`. `python -m tools.probe_volleyball` is a bounded manual offering check. The existing experimental live-score and streaming modules remain available.

## Upstream and independence

The original client is available at https://github.com/TapsHTS/betclic-api. This project is not endorsed by the original author or Betclic. Access to the service and reuse of its data may be subject to Betclic terms and applicable law. No automated betting features are implemented.

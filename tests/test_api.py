"""API contract checks using cached, synthetic fixtures and no upstream requests."""
import asyncio
from unittest.mock import Mock

from betclic_api.models import Match, Market, Selection, Team
from fastapi.testclient import TestClient

from app import main


def fixture(match_id=101, competition="Pologne Tauron Liga H.", teams=True):
    return Match(
        id=match_id, name="Projekt Warszawa - Slepsk Suwalki",
        competition=competition, date="2026-11-05T19:30:00Z",
        teams=[Team("Projekt Warszawa"), Team("Slepsk Suwalki")] if teams else [],
        markets=[Market("Vainqueur du match", selections=[
            Selection("Projekt Warszawa", 1.18), Selection("Slepsk Suwalki", 4.0),
        ])],
    )


def test_discovery_pages_by_received_count_and_excludes_women_and_season_markets(monkeypatch):
    monkeypatch.setattr(main, "STREAM_ENABLED", True)
    client = Mock()
    client.get_matches.side_effect = [
        {"matches": [fixture(), fixture(102, "Pologne Tauron Liga F."),
                     fixture(103, "Japon SV.League H.", teams=False)], "total": 0},
        {"matches": [], "total": 0},
    ]
    cache = main.Store(client)
    cache.reconcile_streams = Mock()
    asyncio.run(cache.refresh())
    assert list(cache.matches) == [101]
    assert [call.kwargs["offset"] for call in client.get_matches.call_args_list] == [0, 3]
    client.get_match.assert_not_called()


def test_rest_reads_cached_match_winner_odds_without_upstream_requests(monkeypatch):
    upstream = Mock()
    cache = main.Store(upstream)
    cache.matches = {101: main.serialize(fixture())}
    monkeypatch.setattr(main, "store", cache)
    # No context manager: lifespan/background collector deliberately stays off.
    api = TestClient(main.app)
    response = api.get("/api/v1/matches")
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["matches"][0]["gender"] == "men"
    response = api.get("/api/v1/matches/101/odds")
    assert response.status_code == 200
    assert response.json()["type"] == "match_winner_only"
    assert [selection["odds"] for selection in response.json()["market"]["selections"]] == [1.18, 4.0]
    upstream.get_matches.assert_not_called()
    upstream.get_match.assert_not_called()

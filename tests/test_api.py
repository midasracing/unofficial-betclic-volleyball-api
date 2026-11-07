"""API contract checks using cached, synthetic fixtures and no upstream requests."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from betclic_api.models import Match, Market, Selection, Team
from fastapi.testclient import TestClient

from app import main
from app.streaming import StreamStats


def test_github_pages_can_read_cached_api_without_credentials():
    api=TestClient(main.app)
    response=api.get('/health',headers={'Origin':'https://midasracing.github.io'})
    assert response.headers['access-control-allow-origin']=='https://midasracing.github.io'
    assert 'access-control-allow-credentials' not in response.headers


def fixture(match_id=101, competition="Pologne Tauron Liga H.", teams=True, competition_id=None):
    return Match(
        id=match_id, name="Projekt Warszawa - Slepsk Suwalki",
        competition=competition, competition_id=competition_id, date="2026-11-05T19:30:00Z",
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
                     fixture(103, "Japon SV.League H.", teams=False),
                     fixture(104, "Japon SV Ligue", competition_id=36538)], "total": 0},
        {"matches": [], "total": 0},
    ]
    cache = main.Store(client)
    cache.reconcile_streams = Mock()
    asyncio.run(cache.refresh())
    assert list(cache.matches) == [101, 104]
    assert cache.matches[104]["category"] == "league:japan"
    assert [call.kwargs["offset"] for call in client.get_matches.call_args_list] == [0, 4]
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


def test_catalog_refresh_preserves_a_newer_pushed_score_and_odds(monkeypatch):
    monkeypatch.setattr(main, "STREAM_ENABLED", True)
    cache = main.Store(Mock())
    cache.listed = {101: fixture()}
    cache.matches = {101: main.serialize(fixture())}
    cache.matches[101]["current_points"] = {"home": 10, "away": 10}
    cache.client.get_matches.side_effect = [{"matches": [fixture()]}, {"matches": []}]
    cache.reconcile_streams = Mock()

    async def interleaved(function):
        snapshot = function()
        updated = dict(cache.matches[101])
        updated["current_points"] = {"home": 11, "away": 10}
        updated["winner_odds"] = {"name": "Match winner", "suspended": True, "selections": []}
        cache.publish(101, updated)
        return snapshot

    monkeypatch.setattr(main.asyncio, "to_thread", interleaved)
    asyncio.run(cache.refresh())
    assert cache.matches[101]["current_points"] == {"home": 11, "away": 10}
    assert cache.matches[101]["winner_odds"]["suspended"] is True
    assert cache.client.get_matches.call_count == 2


def test_health_degrades_when_an_expected_live_match_has_no_worker(monkeypatch):
    monkeypatch.setattr(main, "STREAM_ENABLED", True)
    event = fixture()
    event.is_live = True
    event.date = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    cache = main.Store(Mock())
    cache.listed = {101: event}
    cache.refreshed_at = main.timestamp()
    monkeypatch.setattr(main, "store", cache)
    result = main.health()
    assert result["status"] == "degraded"
    assert result["expected_live_streams"] == 1
    assert result["receiving_live_streams"] == 0


@pytest.mark.parametrize("state,age,status", [
    ("opening", None, "degraded"), ("cooldown", 2, "degraded"),
    ("blocked", 2, "degraded"), ("streaming", 20, "ok"),
    ("streaming", 100, "degraded"),
])
def test_health_accounts_for_expected_live_streams(monkeypatch, state, age, status):
    monkeypatch.setattr(main, "STREAM_ENABLED", True)
    event = fixture()
    event.is_live = True
    event.date = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    cache = main.Store(Mock())
    cache.listed = {101: event}
    cache.refreshed_at = main.timestamp()
    cache.streams = {101: SimpleNamespace(stats=StreamStats(
        state=state, last_frame_at=None if age is None else 1000-age))}
    monkeypatch.setattr(main, "store", cache)
    monkeypatch.setattr(main.time, "time", lambda: 1000)
    result = main.health()
    assert result["status"] == status
    assert result["expected_live_streams"] == 1
    assert result["receiving_live_streams"] == (1 if status == "ok" else 0)
    assert result["streams"]["101"]["last_frame_age_seconds"] == age
    cache.client.get_matches.assert_not_called()

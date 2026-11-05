from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from app.scheduling import eligible_for_live_detail, choose_due

@dataclass
class Match:
    id: int
    is_live: bool
    date: str | None = None

def test_future_flagged_as_live_is_not_probed():
    future = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
    assert not eligible_for_live_detail(Match(1, True, future))

def test_started_live_is_eligible():
    past = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    assert eligible_for_live_detail(Match(1, True, past))

def test_request_budget_and_cooldown():
    matches = [Match(i, True, None) for i in range(1, 5)]
    assert [m.id for m in choose_due(matches, {1: 90}, 100, 30, 2)] == [2, 3]
    assert choose_due(matches, {i: 100 for i in range(1,5)}, 110, 30, 3) == []

def test_select_one_stream_in_each_target_league():
    from app.scheduling import select_league_streams
    now = datetime.now(timezone.utc)
    past = (now - timedelta(minutes=15)).isoformat()
    @dataclass
    class LeagueMatch:
        id: int
        competition: str
        name: str
        is_live: bool = True
        date: str | None = past
    fixtures = [
        LeagueMatch(101, "PlusLiga", "Lublin - Warsaw"),
        LeagueMatch(102, "PlusLiga", "Gdansk - Olsztyn"),
        LeagueMatch(201, "SuperLega", "Perugia - Trento"),
        LeagueMatch(301, "SV.League Men", "Osaka - Tokyo"),
        LeagueMatch(401, "Ligue des Champions H.", "X - Y"),
        LeagueMatch(501, "PlusLiga Women", "A F. - B F."),
        LeagueMatch(601, "SuperLega", "Future - Match",
                    date=(now + timedelta(hours=2)).isoformat()),
    ]
    assert select_league_streams(fixtures, now=now) == [101, 201, 301]
    assert select_league_streams(fixtures, current_ids=[102], now=now) == [102, 201, 301]
    assert select_league_streams(fixtures, per_league=2, max_total=4, now=now) == [101, 201, 301, 102]
    assert select_league_streams(fixtures, max_total=2, now=now) == [101, 201]

def test_no_cross_league_or_women_streams():
    from app.scheduling import select_league_streams
    @dataclass
    class Fixture:
        id: int
        competition: str
        name: str
        is_live: bool = True
        date: str | None = None
    assert select_league_streams([
        Fixture(1, "Ligue des Champions H.", "A - B"),
        Fixture(2, "SuperLega Women", "C - D"),
        Fixture(3, "SV.League Women", "E - F"),
    ]) == []

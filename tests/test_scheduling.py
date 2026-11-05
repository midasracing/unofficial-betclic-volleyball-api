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

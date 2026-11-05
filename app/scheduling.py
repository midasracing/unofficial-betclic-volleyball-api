"""Conservative scheduling helpers for public Betclic offering requests.

One coordinator per process should call Betclic; downstream API clients always
read from the local cache. This is polling-only until upstream push is verified.
"""
from datetime import datetime, timezone

TARGET_LEAGUES = ("league:poland", "league:italy", "league:japan")

def eligible_for_live_detail(match, now=None):
    """Do not trust upstream is_live flag alone (future fixtures can be flagged).

    A kickoff later than 2 minutes in the future is never probed as live.
    Unknown kickoff times can be checked but remain unverified.
    """
    if not match.is_live:
        return False
    if not match.date:
        return True
    now = now or datetime.now(timezone.utc)
    try:
        starts = datetime.fromisoformat(match.date.replace("Z", "+00:00"))
        if starts.tzinfo is None:
            starts = starts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, AttributeError):
        return True
    return starts <= now.replace(microsecond=0) or (starts - now).total_seconds() <= 120


def choose_due(matches, last_attempt, now_monotonic, interval, limit):
    """Round-robin priority by oldest request; a global request budget."""
    candidates = [
        m for m in matches
        if m.id is not None and eligible_for_live_detail(m)
        and now_monotonic - last_attempt.get(m.id, float("-inf")) >= interval
    ]
    candidates.sort(key=lambda m: last_attempt.get(m.id, float("-inf")))
    return candidates[:limit]


def select_league_streams(matches, current_ids=(), per_league=1, max_total=3, now=None):
    """Select live eligible fixtures across PL/IT/JP without starving leagues.

    Preserve existing streams to avoid needless disconnects. Maximum one event
    per league by default; configurable up to two per league and six overall.
    Returns selected event IDs only; never performs network requests.
    """
    from app.filters import category
    per_league = max(1, min(2, int(per_league)))
    max_total = max(1, min(6, int(max_total)))
    present = set(current_ids)
    groups = {league: [] for league in TARGET_LEAGUES}
    for match in matches:
        if match.id is None or not eligible_for_live_detail(match, now=now):
            continue
        league = category(match.competition, match.name)
        if league in groups:
            groups[league].append(match)
    for league in groups:
        # Existing streams first; then earliest scheduled match, then ID.
        groups[league].sort(key=lambda m: (m.id not in present, m.date or "", m.id))
    selected = []
    # Fill one stream per league first, then additional streams if configured.
    for slot in range(per_league):
        for league in TARGET_LEAGUES:
            events = groups[league]
            if len(events) > slot and len(selected) < max_total:
                selected.append(events[slot].id)
    return selected

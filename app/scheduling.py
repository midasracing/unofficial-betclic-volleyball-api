"""Conservative scheduling helpers for public Betclic offering requests.

One coordinator per process should call Betclic; downstream API clients always
read from the local cache. This is polling-only until upstream push is verified.
"""
from datetime import datetime, timezone

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

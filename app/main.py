"""REST + WebSocket API for the Betclic men's volleyball offering.

Important: The upstream client does not expose match or set scores. score=null
until a real, separately verified scores provider is integrated.
"""
import asyncio
import logging
import os
from dataclasses import asdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from betclic_api import BetclicClient
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from app.filters import category

log = logging.getLogger(__name__)
POLL_SECONDS = max(15, int(os.getenv("POLL_SECONDS", "30")))
PAGE_LIMIT = max(1, min(10, int(os.getenv("PAGE_LIMIT", "4"))))

def timestamp():
    return datetime.now(timezone.utc).isoformat()

def serialize(match):
    return {
        "id": match.id,
        "sport": "volleyball",
        "gender": "men",
        "provider": "betclic",
        "name": match.name,
        "competition": match.competition,
        "competition_id": match.competition_id,
        "category": category(match.competition, match.name),
        "starts_at": match.date,
        "is_live": match.is_live,
        "status": "live" if match.is_live else "scheduled_or_unknown",
        "teams": [{"name": t.name, "short": t.short} for t in match.teams],
        "score": None,
        "sets": None,
        "current_set": None,
        "current_points": None,
        "serving_team": None,
        "score_status": "not_available_in_upstream_client",
        "market_count": len(match.markets),
        "source_data": asdict(match),
        "markets": [
            {
                "name": market.name,
                "suspended": market.suspended,
                "selections": [{"name": s.name, "odds": s.odds} for s in market.selections],
            }
            for market in match.markets
        ],
    }

class Store:
    def __init__(self, client=None):
        self.client = client or BetclicClient(timeout=(5, 8))
        self.matches = {}
        self.discovered_competitions = {}
        self.upstream_total = None
        self.refreshed_at = None
        self.last_error = None
        self.listeners = set()
        self.task = None

    async def refresh(self):
        def retrieve():
            collected = {}
            discovered_competitions = {}
            upstream_total = None
            for page in range(PAGE_LIMIT):
                result = self.client.get_matches("volleyball", offset=page * 40)
                upstream_total = result.get("total", upstream_total)
                for match in result.get("matches", []):
                    classification = category(match.competition, match.name)
                    key = str(match.competition_id or match.competition or "unknown")
                    entry = discovered_competitions.setdefault(key, {
                        "competition_id": match.competition_id,
                        "name": match.competition,
                        "category": classification,
                        "events_count": 0,
                        "included": bool(classification),
                    })
                    entry["events_count"] += 1
                    if match.id is not None and classification:
                        collected[match.id] = match
                if len(result.get("matches", [])) < 40:
                    break
            # Detail request only for live games. Rate-limit by sequential requests.
            for match_id, match in list(collected.items()):
                if match.is_live:
                    try:
                        detailed = self.client.get_match(match_id)
                        if detailed.id == match_id:
                            detailed.name = detailed.name or match.name
                            detailed.competition = detailed.competition or match.competition
                            detailed.competition_id = detailed.competition_id or match.competition_id
                            detailed.teams = detailed.teams or match.teams
                            detailed.date = detailed.date or match.date
                            detailed.is_live = True
                            collected[match_id] = detailed
                    except Exception as exc:
                        log.warning("Detail fetch failed for %s: %s", match_id, exc)
            return ({key: serialize(m) for key, m in collected.items()}, discovered_competitions, upstream_total)

        new, catalog, total = await asyncio.to_thread(retrieve)
        previous = self.matches
        self.matches = new
        self.discovered_competitions = catalog
        self.upstream_total = total
        self.refreshed_at = timestamp()
        self.last_error = None
        for match_id in set(previous) | set(new):
            before, after = previous.get(match_id), new.get(match_id)
            if before == after:
                continue
            event = {
                "type": "match_deleted" if after is None else "match_updated",
                "timestamp": self.refreshed_at,
                "match_id": match_id,
                "match": after,
            }
            for queue in tuple(self.listeners):
                if queue.full():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                queue.put_nowait(event)

    async def loop(self):
        while True:
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                log.exception("Refresh failed")
            await asyncio.sleep(POLL_SECONDS)

store = Store()

@asynccontextmanager
async def lifespan(app: FastAPI):
    store.task = asyncio.create_task(store.loop())
    yield
    store.task.cancel()
    try:
        await store.task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="Unofficial Betclic Men's Volleyball API", version="0.1.0", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok" if store.refreshed_at and not store.last_error else "degraded",
            "refreshed_at": store.refreshed_at, "last_error": store.last_error,
            "matches_cached": len(store.matches),
            "source": "betclic-api", "update_mode": "polling",
            "poll_interval_seconds": POLL_SECONDS, "upstream_reported_total": store.upstream_total,
            "score_source": "not_implemented"}

@app.get("/api/v1/matches")
def matches(live: bool | None = None):
    items = list(store.matches.values())
    if live is not None:
        items = [m for m in items if m["is_live"] is live]
    return {"updated_at": store.refreshed_at, "count": len(items), "matches": items}

@app.get("/api/v1/matches/live")
def live_matches():
    return matches(live=True)

@app.get("/api/v1/matches/{match_id}")
def match(match_id: int):
    item = store.matches.get(match_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Match not found in monitored competitions")
    return item

@app.get("/api/v1/matches/{match_id}/odds")
def odds(match_id: int):
    item = match(match_id)
    return {"match_id": match_id, "updated_at": store.refreshed_at, "markets": item["markets"]}

@app.get("/api/v1/competitions")
def competitions():
    unique = {}
    for item in store.matches.values():
        key = item["competition_id"] or item["competition"]
        unique[str(key)] = {"id": item["competition_id"], "name": item["competition"],
                            "category": item["category"]}
    return {"competitions": list(unique.values())}


@app.get("/api/v1/catalog")
def catalog():
    """Discovery catalog, including unmatched names, for tuning the allowlist.

    Never returns events from excluded competitions.
    """
    return {
        "updated_at": store.refreshed_at,
        "source": "betclic",
        "volleyball_events_reported": store.upstream_total,
        "competitions": sorted(
            store.discovered_competitions.values(),
            key=lambda x: (not x["included"], str(x["name"])),
        ),
    }

@app.get("/api/v1/schema")
def schema():
    """Explicitly distinguish available fields from unverified score data."""
    return {
        "sport": "volleyball",
        "gender": "men",
        "source": "betclic-api",
        "update_mode": "polling",
        "available": ["id", "name", "date", "competition", "competition_id",
                      "is_live", "teams", "markets", "selections", "odds",
                      "market_suspended"],
        "not_available": ["score", "sets", "current_set", "current_points",
                          "serving_team"],
        "caution": "Original client does not decode score fields, and the "
                   "upstream method's default market bundle might be incomplete.",
    }

@app.websocket("/api/v1/live")
async def stream(socket: WebSocket):
    await socket.accept()
    queue = asyncio.Queue(maxsize=100)
    store.listeners.add(queue)
    try:
        await socket.send_json({"type": "snapshot", "timestamp": store.refreshed_at,
                                "matches": list(store.matches.values())})
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=25)
                await socket.send_json(event)
            except asyncio.TimeoutError:
                await socket.send_json({"type": "heartbeat", "timestamp": timestamp()})
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        store.listeners.discard(queue)

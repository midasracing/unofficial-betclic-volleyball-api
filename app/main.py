"""REST + WebSocket API for the Betclic men's volleyball offering.

The upstream client provides the match and odds; our parser additionally
decodes point and set notifications from the same gRPC-web response.
"""
import asyncio
import logging
import os
import time
from dataclasses import asdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from app.client import BetclicSnapshotClient
from betclic_api.proto import encode_field_varint, encode_field_string
from app.parser import parse_match_frames
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from app.filters import category
from app.scheduling import choose_due, select_league_streams, TARGET_LEAGUES
from app.streaming import StreamWorker
from app.winner_odds import winner_market
from app.betclic_scores import decode_score_frames
from betclic_api.parser import parse_match_detail_response

log = logging.getLogger(__name__)
POLL_SECONDS = max(15, int(os.getenv("POLL_SECONDS", "30")))
DISCOVERY_SECONDS = max(600, int(os.getenv("DISCOVERY_SECONDS", "600")))
STREAM_ENABLED = os.getenv("BETCLIC_STREAM", "1") == "1"
# Default: one connection per league, at most three concurrent upstream streams.
# Optional: two per league / six total after provider/usage validation.
STREAMS_PER_LEAGUE = max(1, min(2, int(os.getenv("STREAMS_PER_LEAGUE", "1"))))
MAX_STREAMS = max(1, min(6, int(os.getenv("MAX_STREAMS", "3"))))
STREAM_START_GAP_SECONDS = max(20, int(os.getenv("STREAM_START_GAP_SECONDS", "30")))
MAX_DETAILS_PER_CYCLE = max(1, min(5, int(os.getenv("MAX_DETAILS_PER_CYCLE", "3"))))
PAGE_LIMIT = max(1, min(10, int(os.getenv("PAGE_LIMIT", "4"))))

def timestamp():
    return datetime.now(timezone.utc).isoformat()

def serialize(match, decoded_score=None):
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
        "score": decoded_score.get("sets_won_derived") if decoded_score else None,
        "sets": decoded_score.get("completed_sets") if decoded_score else None,
        "current_set": decoded_score.get("current_set_number") if decoded_score else None,
        "current_period": decoded_score.get("current_period") if decoded_score else None,
        "current_points": decoded_score.get("current_points") if decoded_score else None,
        "serving_team": None,
        "score_status": "decoded_from_grpc_notification" if decoded_score else "not_observed",
        "score_source": decoded_score.get("source") if decoded_score else None,
        "market_count": 1 if winner_market(match.markets) else 0,
        "winner_odds": winner_market(match.markets),
        "odds_observed_at": None,
        "source_data": {key: value for key, value in asdict(match).items() if key != "markets"},
        "markets": [winner_market(match.markets)] if winner_market(match.markets) else [],
    }

class Store:
    def __init__(self, client=None):
        self.client = client or BetclicSnapshotClient(timeout=(5, 8))
        self.matches = {}
        self.listed = {}
        self.last_discovery = float("-inf")
        self.last_attempt = {}
        self.detail_request_count = 0
        self.discovery_request_count = 0
        self.discovered_competitions = {}
        self.upstream_total = None
        self.refreshed_at = None
        self.last_error = None
        self.listeners = set()
        self.task = None
        self.streams = {}
        self.retiring_streams = []
        self.last_stream_start = float('-inf')

    async def refresh(self):
        """Single centralized Betclic collector; REST/WS never call upstream."""
        now_mono = time.monotonic()
        discovery_due = now_mono - self.last_discovery >= DISCOVERY_SECONDS

        def retrieve():
            listed = dict(self.listed)
            catalog = dict(self.discovered_competitions)
            upstream_total = self.upstream_total
            discovery_count = 0
            if discovery_due:
                listed = {}
                catalog = {}
                offset = 0
                seen = set()
                for page in range(PAGE_LIMIT):
                    result = self.client.get_matches("volleyball", offset=offset)
                    discovery_count += 1
                    upstream_total = result.get("total", upstream_total)
                    batch = result.get("matches", [])
                    if not batch:
                        break
                    fresh = [event for event in batch if event.id is not None and event.id not in seen]
                    if not fresh:
                        break
                    seen.update(event.id for event in fresh)
                    for event in fresh:
                        classification = category(event.competition, event.name)
                        key = str(event.competition_id or event.competition or "unknown")
                        entry = catalog.setdefault(key, {
                            "competition_id": event.competition_id,
                            "name": event.competition,
                            "category": classification,
                            "events_count": 0,
                            "included": bool(classification),
                        })
                        entry["events_count"] += 1
                        if classification and len(event.teams) == 2:
                            listed[event.id] = event
                    offset += len(batch)

            # Keep cached markets and scores, but only for events still in the listing.
            new = {}
            for match_id, event in listed.items():
                cached = self.matches.get(match_id)
                if cached:
                    item = dict(cached)
                    item.update({
                        "name": event.name, "competition": event.competition,
                        "competition_id": event.competition_id,
                        "starts_at": event.date, "is_live": event.is_live,
                        "status": "live_flag_unverified" if event.is_live else "scheduled_or_unknown",
                    })
                    new[match_id] = item
                else:
                    new[match_id] = serialize(event)

            # One small, fair request budget, independent of connected WS clients.
            due = [] if STREAM_ENABLED else choose_due(list(listed.values()), self.last_attempt, now_mono,
                             POLL_SECONDS, MAX_DETAILS_PER_CYCLE)
            attempt_updates = {}
            detail_count = 0
            for event in due:
                match_id = event.id
                attempt_updates[match_id] = now_mono
                detail_count += 1
                try:
                    payload = encode_field_varint(1, match_id)
                    payload += encode_field_string(2, self.client.locale)
                    frames = self.client._post(
                        "offering.access.api.MatchService",
                        "GetMatchWithNotification", payload,
                    )
                    parsed = parse_match_frames(frames, match_id)
                    detailed = parsed.match
                    if detailed.id != match_id:
                        log.warning("Detail mismatch for %s", match_id)
                        continue
                    detailed.name = detailed.name or event.name
                    detailed.competition = detailed.competition or event.competition
                    detailed.competition_id = detailed.competition_id or event.competition_id
                    detailed.teams = detailed.teams or event.teams
                    detailed.date = detailed.date or event.date
                    detailed.is_live = event.is_live
                    item = serialize(detailed, parsed.score)
                    item["status"] = "live_flag_unverified"
                    item["detail_observed_at"] = timestamp()
                    item["odds_observed_at"] = item["detail_observed_at"] if item["winner_odds"] else None
                    item["score_observed_at"] = item["detail_observed_at"] if parsed.score else None
                    new[match_id] = item
                except Exception as exc:
                    log.warning("Detail fetch failed for %s: %s", match_id, exc)

            return new, listed, catalog, upstream_total, discovery_count, detail_count, attempt_updates

        new, listed, catalog, total, dc, mc, attempts = await asyncio.to_thread(retrieve)
        previous = self.matches
        self.matches = new
        self.listed = listed
        self.discovered_competitions = catalog
        self.upstream_total = total
        self.last_attempt.update(attempts)
        if discovery_due:
            self.last_discovery = now_mono
        self.discovery_request_count += dc
        self.detail_request_count += mc
        self.refreshed_at = timestamp()
        self.last_error = None
        if STREAM_ENABLED:
            self.reconcile_streams()
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

    def publish(self, match_id, item):
        previous = self.matches.get(match_id)
        if previous == item:
            return
        self.matches[match_id] = item
        event = {"type": "match_updated", "timestamp": timestamp(),
                 "match_id": match_id, "match": item}
        for queue in tuple(self.listeners):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(event)

    def on_stream_frame(self, match_id, frame):
        """Decode pushed frames; called on event loop, NEVER sends HTTP requests."""
        event = self.listed.get(match_id)
        if event is None or (STREAM_ENABLED and match_id not in self.streams):
            return
        existing = dict(self.matches.get(match_id) or serialize(event))
        updated = dict(existing)
        # Match/market snapshot and score notifications can be independent frames.
        decoded = decode_score_frames([frame], expected_match_id=match_id)
        if decoded:
            updated.update({
                "score": decoded.get("sets_won_derived"),
                "sets": decoded.get("completed_sets"),
                "current_set": decoded.get("current_set_number"),
                "current_period": decoded.get("current_period"),
                "current_points": decoded.get("current_points"),
                "score_status": "decoded_from_grpc_notification",
                "score_source": decoded.get("source"),
                "score_observed_at": timestamp(),
            })
        try:
            detail = parse_match_detail_response([frame])
        except (ValueError, TypeError, IndexError) as exc:
            log.debug("Unrecognized match frame: %s", exc)
            detail = None
        if detail is not None and detail.id == match_id:
            # Only publish match-winner market. Ignore all set/exact score odds.
            market = winner_market(detail.markets)
            if market and market != existing.get("winner_odds"):
                updated["markets"] = [market]
                updated["winner_odds"] = market
                updated["market_count"] = 1
                updated["detail_observed_at"] = timestamp()
                updated["odds_observed_at"] = updated["detail_observed_at"]
        self.publish(match_id, updated)

    def reconcile_streams(self):
        if not STREAM_ENABLED:
            return
        # Round-robin across three men's leagues; preserve existing connections.
        desired = set(select_league_streams(
            self.listed.values(), current_ids=self.streams,
            per_league=STREAMS_PER_LEAGUE, max_total=MAX_STREAMS,
        ))
        for match_id, worker in list(self.streams.items()):
            if match_id not in desired:
                worker.close()
                self.retiring_streams.append(worker)
                del self.streams[match_id]
        # A thread can remain in an HTTP read for up to its read timeout.
        # Never open replacement connections while any old stream is retiring.
        self.retiring_streams = [
            worker for worker in self.retiring_streams
            if worker.thread and worker.thread.is_alive()
        ]
        if self.retiring_streams:
            return
        # Stagger new upstream connections to avoid opening all of them at once.
        now = time.monotonic()
        if now - self.last_stream_start < STREAM_START_GAP_SECONDS:
            return
        loop = asyncio.get_running_loop()
        for match_id in sorted(desired):
            if match_id not in self.streams:
                worker = StreamWorker(match_id, loop, self.on_stream_frame, cooldown=90)
                self.streams[match_id] = worker
                self.last_stream_start = now
                worker.start()
                break

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
    for worker in list(store.streams.values()) + store.retiring_streams:
        worker.close()
    await asyncio.gather(*(
        worker.shutdown() for worker in list(store.streams.values()) + store.retiring_streams
    ))
    try:
        await store.task
    except asyncio.CancelledError:
        pass
    store.client.close()

app = FastAPI(title="Unofficial Betclic Men's Volleyball API", version="0.1.0", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok" if store.refreshed_at and not store.last_error else "degraded",
            "refreshed_at": store.refreshed_at, "last_error": store.last_error,
            "matches_cached": len(store.matches),
            "source": "betclic-api", "update_mode": "streaming_experimental" if STREAM_ENABLED else "polling",
            "streams": {str(i): vars(w.stats) for i, w in store.streams.items()},
            "target_leagues": list(TARGET_LEAGUES),
            "max_streams": MAX_STREAMS, "streams_per_league": STREAMS_PER_LEAGUE,
            "poll_interval_seconds": POLL_SECONDS, "discovery_interval_seconds": DISCOVERY_SECONDS,
            "max_details_per_cycle": MAX_DETAILS_PER_CYCLE,
            "upstream_discovery_requests": store.discovery_request_count,
            "upstream_detail_requests": store.detail_request_count,
            "upstream_reported_total": store.upstream_total,
            "score_source": "betclic_grpc_web_when_observed"}

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

@app.get("/api/v1/matches/{match_id}/score")
def match_score(match_id: int):
    item = match(match_id)
    return {
        "match_id": match_id,
        "updated_at": store.refreshed_at,
        "status": item["score_status"],
        "sets_won": item["score"],
        "completed_sets": item["sets"],
        "current_set": item["current_set"],
        "current_points": item["current_points"],
        "source": item["score_source"],
        "score_observed_at": item.get("score_observed_at"),
        "detail_observed_at": item.get("detail_observed_at"),
    }

@app.get("/api/v1/matches/{match_id}/odds")
def odds(match_id: int):
    item = match(match_id)
    return {"match_id": match_id, "updated_at": item.get("odds_observed_at"),
            "market": item.get("winner_odds"), "markets": item["markets"],
            "type": "match_winner_only"}

@app.get("/api/v1/competitions")
def competitions():
    unique = {}
    for item in store.matches.values():
        key = item["competition_id"] or item["competition"]
        unique[str(key)] = {"id": item["competition_id"], "name": item["competition"],
                            "category": item["category"]}
    return {"competitions": list(unique.values())}


@app.get("/api/v1/leagues")
def leagues():
    """Coverage from the last discovered catalog; no upstream request."""
    result = []
    for league, label in (
        ("league:poland", "PlusLiga (Poland)"),
        ("league:italy", "SuperLega (Italy)"),
        ("league:japan", "SV.League Men (Japan)"),
    ):
        events = [
            e for e in store.matches.values()
            if e.get("category") == league
        ]
        active = [e for e in events if e["id"] in store.streams]
        result.append({
            "category": league, "name": label,
            "discovered_matches": len(events),
            "streamed_matches": len(active),
            "streamed_match_ids": [e["id"] for e in active],
            "matches_with_score": sum(bool(e.get("score_observed_at")) for e in events),
            "matches_with_winner_odds": sum(bool(e.get("winner_odds")) for e in events),
        })
    return {"updated_at": store.refreshed_at,
            "discovery_source": "Betclic FR offering",
            "streams_per_league_limit": STREAMS_PER_LEAGUE,
            "total_stream_limit": MAX_STREAMS,
            "leagues": result}

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
        "streamed_leagues": list(TARGET_LEAGUES),
        "source": "betclic-api",
        "update_mode": "streaming_experimental" if STREAM_ENABLED else "polling",
        "available": ["id", "name", "date", "competition", "competition_id",
                      "is_live", "teams", "markets", "selections", "odds",
                      "market_suspended", "winner_odds"],
        "conditionally_available": ["score", "sets", "current_set", "current_points", "current_period"],
        "not_available": ["serving_team"],
        "caution": "Scores decode from separately emitted gRPC-web notifications. "
                   "Absent notifications mean null values, not 0:0. "
                   "The upstream default market bundle might be incomplete.",
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

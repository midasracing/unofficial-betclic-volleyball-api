import asyncio
from unittest.mock import Mock

import pytest
from fastapi import WebSocketDisconnect

from app import main
from app.updates import MatchUpdates


def event(match_id, value):
    return {"type": "match_updated", "match_id": match_id, "match": {"value": value}}


def test_slow_client_gets_latest_per_match_without_starving_other_matches():
    async def check():
        buffer = MatchUpdates()
        buffer.put_nowait(event(1, 0))
        buffer.put_nowait(event(2, 1))
        for value in range(10000):
            buffer.put_nowait(event(1, value))
        assert len(buffer.pending) == 2
        assert (await buffer.get())["match"] == {"value": 9999}
        assert (await buffer.get())["match_id"] == 2
        assert not buffer.ready.is_set()
    asyncio.run(check())


def test_deletion_replaces_a_pending_update():
    async def check():
        buffer = MatchUpdates()
        buffer.put_nowait(event(1, 1))
        buffer.put_nowait({"type": "match_deleted", "match_id": 1, "match": None})
        assert (await buffer.get())["type"] == "match_deleted"
    asyncio.run(check())


def test_overflow_requests_one_snapshot_and_normal_delivery_resumes():
    async def check():
        buffer = MatchUpdates(limit=2)
        for match_id in range(100):
            buffer.put_nowait(event(match_id, 1))
        assert not buffer.pending
        assert await buffer.get() is None
        buffer.put_nowait(event(101, 2))
        assert (await buffer.get())["match_id"] == 101
    asyncio.run(check())


def test_cancelled_wait_does_not_lose_the_next_update():
    async def check():
        buffer = MatchUpdates()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(buffer.get(), timeout=0.001)
        buffer.put_nowait(event(1, 2))
        assert (await buffer.get())["match"]["value"] == 2
    asyncio.run(check())


def test_websocket_sends_current_snapshot_after_overflow_and_cleans_up(monkeypatch):
    store = main.Store(Mock())
    monkeypatch.setattr(main, "store", store)
    sent = []

    class Socket:
        async def accept(self):
            pass

        async def send_json(self, value):
            sent.append(value)
            if len(sent) == 1:
                for match_id in range(110):
                    store.publish(match_id, {"id": match_id})
            else:
                raise WebSocketDisconnect()

    asyncio.run(main.stream(Socket()))
    assert [item["type"] for item in sent] == ["snapshot", "snapshot"]
    assert len(sent[1]["matches"]) == 110
    assert not store.listeners
    store.client.get_matches.assert_not_called()

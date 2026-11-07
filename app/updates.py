"""Bounded per-client delivery of the latest state of each match."""
import asyncio
from collections import OrderedDict


class MatchUpdates:
    def __init__(self, limit=100):
        self.limit = max(1, limit)
        self.pending = OrderedDict()
        self.ready = asyncio.Event()
        self.resync = False

    def put_nowait(self, event):
        if not self.resync:
            self.pending[event["match_id"]] = event
            if len(self.pending) > self.limit:
                self.pending.clear()
                self.resync = True
        self.ready.set()

    async def get(self):
        await self.ready.wait()
        if self.resync:
            self.resync = False
            self.ready.clear()
            return None  # The caller sends a current full snapshot.
        _, event = self.pending.popitem(last=False)
        if not self.pending:
            self.ready.clear()
        return event

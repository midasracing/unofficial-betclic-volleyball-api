"""Decode only verified scores, winner snapshots and known winner-price deltas.

Keep wire identities inside the decoder so a price-only notification cannot
update another match, market or selection. Published dictionaries are detached
from the mutable decoder state. Unknown availability codes are not inferred.
"""
import math
from dataclasses import dataclass

from betclic_api.parser import _extract_selections
from betclic_api.proto import decode_protobuf, decode_fixed64_as_double, try_decode_string

from app.betclic_scores import _first, _messages, decode_score_root, match_snapshots
from app.winner_odds import is_match_winner, winner_market


def _price(value):
    if not isinstance(value, bytes) or len(value) != 8:
        return None
    number = decode_fixed64_as_double(value)
    if number is None or not math.isfinite(number) or not 1 < number < 10000:
        return None
    rounded = round(number, 2)
    return rounded if 1 < rounded < 10000 else None


def _name(fields, numbers):
    for number in numbers:
        value = try_decode_string(_first(fields, number))
        if value:
            return value
    return None


@dataclass
class LiveUpdate:
    score: dict | None = None
    market: dict | None = None
    market_observed: bool = False


class LiveMatchParser:
    def __init__(self, match_id):
        self.match_id = match_id
        self.market_id = None
        self.market_name = None
        self.suspended = False
        self.selections = []
        self.fallback_market = None

    def _snapshot(self, raw, fields, name):
        self.market_id = _first(fields, 1)
        self.market_name = name
        # Preserve the existing pinned parser's explicit suspension mapping.
        self.suspended = _first(fields, 9) == 3
        self.selections = []
        self.fallback_market = None
        if 16 not in fields and (10 in fields or 13 in fields):
            # Compatibility for less common layouts, only for the winner market.
            self.fallback_market = winner_market([{
                "name": name, "suspended": self.suspended,
                "selections": _extract_selections(raw),
            }])
            self.market_id = None  # No verified wire selection identities.
            return
        for selection in _messages(fields, 16):
            self.selections.append({"id": _first(selection, 1),
                "name": _name(selection, (10, 11, 2, 3)),
                "odds": _price(_first(selection, 12))})

    def _market(self):
        if self.fallback_market is not None:
            return {**self.fallback_market,
                    "selections": [dict(item) for item in self.fallback_market["selections"]]}
        if len(self.selections) != 2 or any(
                not selection["name"] or selection["odds"] is None for selection in self.selections):
            return None
        return {"name": self.market_name, "suspended": self.suspended,
                "selections": [{"name": item["name"], "odds": item["odds"]} for item in self.selections]}

    def feed(self, frame):
        if frame.get("flags", 0) & 0x80 or not frame.get("data"):
            return LiveUpdate()
        root = decode_protobuf(frame["data"])
        snapshots = list(match_snapshots(root, self.match_id))
        score = decode_score_root(root, self.match_id, snapshots=snapshots)
        observed = False
        for match in snapshots:
            found = False
            for bundle in _messages(match, 11):
                for raw in bundle.get(3, []) + bundle.get(1, []):
                    if not isinstance(raw, bytes):
                        continue
                    fields = decode_protobuf(raw)
                    name = _name(fields, (2, 3))
                    if name and is_match_winner(name):
                        self._snapshot(raw, fields, name)
                        observed = found = True
                        break
                if found:
                    break
        # The captured delta shape is root.2.1.1. All three IDs must match a
        # known two-selection winner snapshot; never bind by selection order.
        identities = [item["id"] for item in self.selections]
        can_apply = (isinstance(self.market_id, int) and len(identities) == 2
                     and all(isinstance(value, int) for value in identities)
                     and len(set(identities)) == 2)
        if can_apply:
            for envelope in _messages(root, 2):
                for notification in _messages(envelope, 1):
                    for delta in _messages(notification, 1):
                        if (_first(delta, 5) != self.match_id or _first(delta, 1) != self.market_id
                                or 3 not in delta):
                            continue
                        for selection in self.selections:
                            if selection["id"] == _first(delta, 2):
                                selection["odds"] = _price(_first(delta, 3))
                                observed = True
                                break
        return LiveUpdate(score, self._market() if observed else None, observed)

"""Winner-only delta decoding, identity checks and real captured snapshots."""
import asyncio
import json
import struct
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from betclic_api.models import Match, Team
from betclic_api.proto import encode_field_string as blob, encode_field_varint as integer, encode_varint

from app import main
from app.live_parser import LiveMatchParser
from app.streaming import StreamStats
from app.updates import MatchUpdates


def double(number, value):
    return encode_varint((number << 3) | 1) + struct.pack('<d', value)


def frame(data):
    return {"flags": 0, "data": data}


def snapshot(match_id=101, market_id=9, prices=(1.5, 2.8), name="Vainqueur du match", status=8):
    selections = b"".join(blob(16, integer(1, 10+i) + blob(10, chr(65+i)) + double(12, price))
                          for i, price in enumerate(prices))
    market = integer(1, market_id) + blob(2, name) + integer(9, status) + selections
    return frame(blob(1, blob(1, integer(1, match_id) + blob(11, blob(3, market)))))


def delta(match_id=101, market_id=9, selection_id=10, price=1.7):
    data = integer(1, market_id) + integer(2, selection_id) + double(3, price)
    data += integer(4, 1) + integer(5, match_id)
    return frame(blob(2, blob(1, blob(1, data))))


def test_price_only_update_uses_known_winner_ids_and_keeps_prior_event_immutable():
    parser = LiveMatchParser(101)
    first = parser.feed(snapshot())
    update = parser.feed(delta())
    assert update.market_observed
    assert update.market["selections"] == [{"name": "A", "odds": 1.7}, {"name": "B", "odds": 2.8}]
    assert first.market["selections"][0]["odds"] == 1.5


@pytest.mark.parametrize("kwargs", [{"match_id": 102}, {"market_id": 999}, {"selection_id": 999}])
def test_wrong_event_market_or_selection_cannot_change_a_quote(kwargs):
    parser = LiveMatchParser(101)
    parser.feed(snapshot())
    assert not parser.feed(delta(**kwargs)).market_observed


def test_unseeded_delta_and_old_market_after_replacement_are_ignored():
    parser = LiveMatchParser(101)
    assert not parser.feed(delta()).market_observed
    parser.feed(snapshot())
    parser.feed(snapshot(market_id=19))
    assert not parser.feed(delta()).market_observed


@pytest.mark.parametrize("prices,name", [((1.5, 2.8), "Vainqueur du set"),
                                          ((1.5, 3.1, 2.8), "Vainqueur du match")])
def test_set_and_three_way_markets_never_become_two_way_winner_odds(prices, name):
    parsed = LiveMatchParser(101).feed(snapshot(prices=prices, name=name))
    assert parsed.market is None


@pytest.mark.parametrize("price", [float('nan'), float('inf'), -1, 0, 1.0, 10000])
def test_invalid_or_unit_prices_withdraw_the_quote_and_a_valid_delta_restores_it(price):
    parser = LiveMatchParser(101)
    parser.feed(snapshot())
    unavailable = parser.feed(delta(price=price))
    assert unavailable.market_observed and unavailable.market is None
    assert parser.feed(delta(price=1.8)).market["selections"][0]["odds"] == 1.8


def test_explicit_invalid_snapshot_clears_prices_but_an_unrelated_frame_does_not():
    parser = LiveMatchParser(101)
    parser.feed(snapshot())
    assert not parser.feed(frame(blob(5, b"unknown"))).market_observed
    unavailable = parser.feed(snapshot(prices=(1.0, 2.8)))
    assert unavailable.market_observed and unavailable.market is None


def test_price_delta_cannot_infer_that_a_suspended_market_has_reopened():
    parser = LiveMatchParser(101)
    assert parser.feed(snapshot(status=3)).market["suspended"]
    assert parser.feed(delta()).market["suspended"]


def test_genuine_winner_snapshots_match_the_original_quotes_and_ignore_a_real_other_market_delta():
    data = json.loads((Path(__file__).parent / 'fixtures/live_wire_2026-11-06.json').read_text())
    parser = LiveMatchParser(data["match_id"])
    for sample in data["snapshots"]:
        assert parser.feed(frame(bytes.fromhex(sample["hex"]))).market == sample["expected"]
    assert not parser.feed(frame(bytes.fromhex(data["unrelated_delta"]["hex"]))).market_observed


def cache():
    item = Match(id=101, name="A - B", competition="Pologne Tauron Liga H.", teams=[Team("A"), Team("B")])
    result = main.Store(Mock())
    result.listed = {101: item}
    result.matches = {101: main.serialize(item)}
    result.streams = {101: SimpleNamespace(stats=StreamStats(opens=1))}
    return result


def test_store_publishes_price_delta_immediately_without_another_upstream_request(monkeypatch):
    monkeypatch.setattr(main, "STREAM_ENABLED", True)
    store = cache()
    listener = MatchUpdates()
    store.listeners.add(listener)
    store.on_stream_frame(101, snapshot())
    asyncio.run(listener.get())
    store.on_stream_frame(101, delta())
    event = asyncio.run(listener.get())
    assert event["match"]["winner_odds"]["selections"][0]["odds"] == 1.7
    store.client.get_match.assert_not_called()
    store.client.get_matches.assert_not_called()


def test_identical_confirmations_refresh_freshness_without_more_websocket_payloads(monkeypatch):
    store = cache()
    listener = MatchUpdates()
    store.listeners.add(listener)
    times = iter(["first", "publication", "confirmation"])
    monkeypatch.setattr(main, "timestamp", lambda: next(times))
    store.on_stream_frame(101, snapshot())
    asyncio.run(listener.get())
    store.on_stream_frame(101, snapshot())
    assert store.matches[101]["odds_observed_at"] == "confirmation"
    assert store.data_updated_at == "confirmation"
    assert not listener.pending and not listener.ready.is_set()


def test_reconnect_discards_wire_id_mapping_until_another_snapshot():
    store = cache()
    store.on_stream_frame(101, snapshot())
    store.streams[101].stats.opens = 2
    store.on_stream_frame(101, delta())
    assert store.matches[101]["winner_odds"]["selections"][0]["odds"] == 1.5
    store.on_stream_frame(101, snapshot())
    store.on_stream_frame(101, delta())
    assert store.matches[101]["winner_odds"]["selections"][0]["odds"] == 1.7


def test_same_points_in_a_snapshot_after_a_notification_refresh_without_duplicate_broadcast(monkeypatch):
    store = cache()
    listener = MatchUpdates()
    store.listeners.add(listener)
    times = iter(["first", "publication", "confirmation"])
    monkeypatch.setattr(main, "timestamp", lambda: next(times))
    entry = blob(1, "volleyball") + integer(10, 101)
    entry += blob(7, blob(1, "11") + blob(2, "10"))
    entry += blob(13, blob(3, "vlb_second_set"))
    store.on_stream_frame(101, frame(blob(2, blob(1, blob(2, blob(1, entry))))))
    asyncio.run(listener.get())
    store.on_stream_frame(101, frame(blob(1, blob(1, integer(1, 101) + blob(13, entry)))))
    assert store.matches[101]["score_observed_at"] == "confirmation"
    assert not listener.pending
    monkeypatch.setattr(main, "store", store)
    assert main.match_score(101)["updated_at"] == "confirmation"
    assert main.matches()["updated_at"] == "confirmation"

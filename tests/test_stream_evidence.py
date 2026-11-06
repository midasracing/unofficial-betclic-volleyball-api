"""Regressions from the 2026-11-06 live capture; all tests run without a network."""
import json
import struct

import pytest
import requests
from betclic_api.models import Match
from betclic_api.proto import encode_field_string as blob, encode_field_varint as integer, encode_varint

from tools.stream_evidence import inspect_frame, score_entry
from tools.probe_long_streaming import capture_events, select_events

MATCH_ID = 1271618119696384
# Exact captured score notification: score pair 40:A, second set games 2:4.
SCORE_NOTIFICATION = bytes.fromhex(
    '1292010a8f01128c010a89010a0674656e6e6973180220003a070a023430120141421a0802120e74656e5f7365636f6e645f7365741a060a01321201344a190801120d74656e5f66697273745f7365741a060a0132120136508080c2d2fa90a1025a042a0030026a2c1a0e74656e5f7365636f6e645f73657420013000420932c3a86d65207365744a055365742032500058016001')
SCORE_SNAPSHOT = bytes.fromhex(
    '0a0674656e6e6973180220003a070a014112023430421a0802120e74656e5f7365636f6e645f7365741a060a01321201344a190801120d74656e5f66697273745f7365741a060a0132120136508080c2d2fa90a1025a042a0030026a2c1a0e74656e5f7365636f6e645f73657420013000420932c3a86d65207365744a055365742032500058016001')


def double(number, value):
    return encode_varint((number << 3) | 1) + struct.pack('<d', value)


def test_captured_tennis_score_notification_preserves_advantage_and_games():
    result = inspect_frame(SCORE_NOTIFICATION, 'tennis', MATCH_ID)
    score = result['scores'][0]
    assert score['points'] == {'home': '40', 'away': 'A'}
    assert score['current_period'] == 'ten_second_set'
    assert score['period_scores'][0]['score'] == {'home': 2, 'away': 4}
    assert score['period_scores'][1]['score'] == {'home': 2, 'away': 6}
    assert result['winner_markets'] == []


def test_score_is_also_read_from_initial_match_snapshot():
    data = blob(1, blob(1, integer(1, MATCH_ID) + blob(13, SCORE_SNAPSHOT)))
    score = inspect_frame(data, 'tennis', MATCH_ID)['scores'][0]
    assert score['points'] == {'home': 'A', 'away': '40'}
    assert score['source_path'] == 'root.1.1.13'


def test_wrong_event_or_sport_never_receives_the_captured_score():
    assert inspect_frame(SCORE_NOTIFICATION, 'tennis', 1)['scores'] == []
    assert inspect_frame(SCORE_NOTIFICATION, 'volleyball', MATCH_ID)['scores'] == []
    assert inspect_frame(SCORE_NOTIFICATION, 'football', MATCH_ID)['scores'] == []


def test_empty_table_tennis_score_does_not_become_zero_zero():
    raw = blob(1, 'table_tennis') + integer(10, 101) + blob(13, blob(3, ''))
    assert score_entry(raw, 'table_tennis', 101, 'fixture') is None


def winner_fixture(price=1.0, name='Vainqueur du match'):
    selections = b''
    for selection_id, label, value in ((10, 'A', price), (11, 'B', 8.75)):
        selection = integer(1, selection_id) + blob(10, label) + double(12, value) + integer(14, 2)
        selections += blob(16, selection)
    market = integer(1, 9) + blob(2, name) + integer(9, 8) + selections
    match = integer(1, 101) + blob(11, blob(3, market))
    return blob(1, blob(1, match))


def test_unit_price_is_preserved_as_raw_data_without_claiming_availability():
    market = inspect_frame(winner_fixture(), 'tennis', 101)['winner_markets'][0]
    assert [selection['decimal_odds_raw'] for selection in market['selections']] == [1.0, 8.75]
    assert market['source_field_9'] == 8
    assert market['selections'][0]['source_field_14'] == 2
    assert 'suspended' not in market


@pytest.mark.parametrize('value', [float('nan'), float('inf'), 0, -1])
def test_invalid_price_is_not_invented_or_published_as_a_quote(value):
    market = inspect_frame(winner_fixture(value), 'tennis', 101)['winner_markets'][0]
    assert [selection['name'] for selection in market['selections']] == ['B']


def test_set_winner_market_is_not_mislabeled_as_match_winner():
    assert inspect_frame(winner_fixture(name='Vainqueur du set'), 'tennis', 101)['winner_markets'] == []


def test_price_delta_requires_matching_event_and_keeps_selection_identity():
    delta = integer(1, 9) + integer(2, 10) + double(3, 1.04) + integer(4, 1) + integer(5, 101)
    raw = blob(2, blob(1, blob(1, delta)))
    assert inspect_frame(raw, 'tennis', 101)['price_deltas'] == [
        {'market_id': 9, 'selection_id': 10, 'decimal_odds_raw': 1.04, 'source_field_4': 1}]
    assert inspect_frame(raw, 'tennis', 102)['price_deltas'] == []


def test_selection_prefers_sport_diversity_then_fills_remaining_slots():
    first, second, third = Match(id=1), Match(id=2), Match(id=3)
    assert select_events([('tennis', first), ('tennis', second), ('football', third)]) == [
        ('tennis', first), ('football', third), ('tennis', second)]


def test_long_capture_uses_one_connection_when_the_budget_is_one(tmp_path):
    calls = []
    def stream(match_id, stop, receive):
        calls.append(match_id)
        receive({'flags': 0, 'data': b'captured bytes'})
    report = capture_events([('tennis', Match(id=101, name='A - B'))], tmp_path / 'capture',
                            seconds=10, max_connections=1, streamer=stream)
    assert calls == [101]
    assert report['threads_stopped']
    assert len(report['events'][0]['frames']) == 1
    assert json.loads((tmp_path / 'capture' / 'report.json').read_text())['events'][0]['id'] == 101


def test_http_refusal_stops_without_retry_even_with_a_larger_budget(tmp_path):
    calls = []
    def refused(match_id, stop, receive):
        calls.append(match_id)
        response = requests.Response()
        response.status_code = 403
        raise requests.HTTPError('denied', response=response)
    report = capture_events([('tennis', Match(id=101))], tmp_path / 'capture',
                            seconds=10, max_connections=4, streamer=refused)
    assert calls == [101]
    assert report['events'][0]['sessions'][0]['stop_after_refusal']

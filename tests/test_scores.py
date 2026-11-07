"""Regression tests using a real 120-byte Betclic volleyball notification.

The payload was captured for match 1271196261842944 on 2026-11-05.
"""
from app.betclic_scores import decode_score_frames
import pytest

SCORE_FRAME = bytes.fromhex(
    "12760a7412720a700a0a766f6c6c657962616c6c180220003a080a02313512023130"
    "4a1b0801120d766c625f66697273745f7365741a080a02323512023130508080828d"
    "d784a1025a006a2c1a0e766c625f7365636f6e645f73657420013000420932c3a8"
    "6d65205365744a055365742032500058016001"
)

def test_live_score_frame():
    actual = decode_score_frames([{"flags": 0, "data": SCORE_FRAME}], 1271196261842944)
    assert actual["current_points"] == {"home": 15, "away": 10}
    assert actual["completed_sets"] == [
        {"period": "vlb_first_set", "home": 25, "away": 10}
    ]
    assert actual["current_period"] == "vlb_second_set"
    assert actual["current_set_number"] == 2
    assert actual["sets_won_derived"] == {"home": 1, "away": 0}

def test_wrong_match_is_ignored():
    assert decode_score_frames([{"flags": 0, "data": SCORE_FRAME}], 111) is None

def test_non_score_frames_ignored():
    assert decode_score_frames([{"flags": 0x80, "data": SCORE_FRAME}]) is None
    assert decode_score_frames([{"flags": 0, "data": b"abc"}]) is None

def test_unified_parser_reuses_frames(monkeypatch):
    from app import parser
    marker = object()
    monkeypatch.setattr(parser, "parse_match_detail_response", lambda frames: marker)
    frames = [{"flags": 0, "data": SCORE_FRAME}]
    parsed = parser.parse_match_frames(frames, 1271196261842944)
    assert parsed.match is marker
    assert parsed.score["current_set_number"] == 2
    assert parsed.score["current_points"] == {"home": 15, "away": 10}


def test_reconnection_snapshot_can_restore_the_captured_score_entry():
    from betclic_api.proto import decode_protobuf, encode_field_string as blob, encode_field_varint as integer
    entry = decode_protobuf(decode_protobuf(decode_protobuf(decode_protobuf(SCORE_FRAME)[2][0])[1][0])[2][0])[1][0]
    payload = blob(1, blob(1, integer(1, 1271196261842944) + blob(13, entry)))
    actual = decode_score_frames([{"flags": 0, "data": payload}], 1271196261842944)
    assert actual["current_points"] == {"home": 15, "away": 10}
    assert actual["source"] == "betclic_grpc_web_score_snapshot"
    wrong_parent = blob(1, blob(1, integer(1, 999) + blob(13, entry)))
    assert decode_score_frames([{"flags": 0, "data": wrong_parent}]) is None


def test_latest_score_notification_in_a_batched_envelope_is_not_lost():
    from betclic_api.proto import decode_protobuf, encode_field_string as blob
    notification = decode_protobuf(decode_protobuf(SCORE_FRAME)[2][0])[1][0]
    later = notification.replace(b'\x0a\x0215', b'\x0a\x0216')
    payload = blob(2, blob(1, notification) + blob(1, later))
    assert decode_score_frames([{"flags": 0, "data": payload}])["current_points"] == {"home": 16, "away": 10}


@pytest.mark.parametrize('period,home,away,finished',[
    ('vlb_second_set',0,0,False), ('vlb_first_set',24,23,False),
    ('vlb_first_set',25,24,False), ('vlb_first_set',26,24,True), ('vlb_fifth_set',15,13,True),
])
def test_future_zero_periods_and_unfinished_scores_do_not_count_as_completed_sets(period,home,away,finished):
    from betclic_api.proto import encode_field_string as blob, encode_field_varint as integer
    pair=lambda a,b:blob(1,str(a))+blob(2,str(b))
    entry=blob(1,'volleyball')+integer(10,101)+blob(7,pair(2,1))
    entry+=blob(13,blob(3,'vlb_second_set'))
    entry+=blob(9,blob(2,period)+blob(3,pair(home,away)))
    payload=blob(2,blob(1,blob(2,blob(1,entry))))
    score=decode_score_frames([{'flags':0,'data':payload}],101)
    assert bool(score['completed_sets']) is finished

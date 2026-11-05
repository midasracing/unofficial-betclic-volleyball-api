"""Regression tests using a real 120-byte Betclic volleyball notification.

The payload was captured for match 1271196261842944 on 2026-11-05.
"""
from app.betclic_scores import decode_score_frames

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
    assert actual["sets_won_derived"] == {"home": 1, "away": 0}

def test_wrong_match_is_ignored():
    assert decode_score_frames([{"flags": 0, "data": SCORE_FRAME}], 111) is None

def test_non_score_frames_ignored():
    assert decode_score_frames([{"flags": 0x80, "data": SCORE_FRAME}]) is None
    assert decode_score_frames([{"flags": 0, "data": b"abc"}]) is None

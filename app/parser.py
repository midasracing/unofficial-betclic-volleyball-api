"""Unified parsing of Betclic match details, odds and volleyball score frames.

The upstream betclic-api parser knows the odds; our independent parser adds
volleyball score notifications that the upstream package currently ignores.
No second HTTP call is made: one gRPC-web response is decoded both ways.
"""
from dataclasses import dataclass

from betclic_api.parser import parse_match_detail_response

from app.betclic_scores import decode_score_frames


@dataclass
class ParsedMatch:
    match: object
    score: dict | None


def parse_match_frames(frames: list[dict], match_id: int) -> ParsedMatch:
    """Parse complete gRPC-web result: match/markets plus most recent score.

    Score is None when no supported scoring notification was delivered.
    """
    match = parse_match_detail_response(frames)
    score = decode_score_frames(frames, expected_match_id=match_id)
    return ParsedMatch(match=match, score=score)

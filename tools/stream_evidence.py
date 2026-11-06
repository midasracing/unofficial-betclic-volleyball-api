"""Offline diagnostics of captured score and match-winner price messages.

Parses only observed wire layouts. Raw 1.00 values and numeric source fields
are preserved; this is not a claim that a selection can currently be placed.
Production API filters and score decoders are not changed.
"""
import math

from betclic_api.proto import decode_protobuf, decode_fixed64_as_double


def fields(data):
    if not isinstance(data, bytes):
        return {}
    try:
        return decode_protobuf(data)
    except (ValueError, TypeError, IndexError, OverflowError):
        return {}


def first(message, number):
    return message.get(number, [None])[0]


def text(value):
    if not isinstance(value, bytes):
        return None
    try:
        return value.decode('utf-8')
    except UnicodeDecodeError:
        return None


def pair(message, number, numeric=False):
    entry = fields(first(message, number))
    left, right = text(first(entry, 1)), text(first(entry, 2))
    if left is None or right is None:
        return None
    def allowed(value):
        return bool(value) and value.isascii() and (
            value.isdecimal() or (not numeric and value in ('A', 'AD')))
    if not allowed(left) or not allowed(right):
        return None
    return {'home': int(left) if numeric else left,
            'away': int(right) if numeric else right}


def score_entry(data, sport, match_id, source_path):
    entry = fields(data)
    if sport not in ('tennis', 'table_tennis', 'volleyball'):
        return None
    if text(first(entry, 1)) != sport or first(entry, 10) != match_id:
        return None
    points = pair(entry, 7)
    periods = []
    for key in (8, 9):
        for raw in entry.get(key, []):
            period = fields(raw)
            value = pair(period, 3, numeric=True)
            code = text(first(period, 2))
            if code and value is not None:
                periods.append({'number': first(period, 1), 'period': code,
                                'complete': key == 9, 'score': value})
    if points is None and not periods:
        return None
    return {'match_id': match_id, 'sport': sport, 'points': points,
            'current_period': text(first(fields(first(entry, 13)), 3)),
            'period_scores': periods, 'source_path': source_path}


def winner_markets(match):
    output = []
    for raw_bundle in match.get(11, []):
        bundle = fields(raw_bundle)
        for raw_market in bundle.get(3, []) + bundle.get(1, []):
            market = fields(raw_market)
            name = text(first(market, 2)) or text(first(market, 3))
            if not name or ' '.join(name.casefold().split()) != 'vainqueur du match':
                continue
            selections = []
            for raw_selection in market.get(16, []):
                selection = fields(raw_selection)
                value = first(selection, 12)
                label = text(first(selection, 10)) or text(first(selection, 11))
                if not label or not isinstance(value, bytes) or len(value) != 8:
                    continue
                odds = decode_fixed64_as_double(value)
                if odds is None or not math.isfinite(odds) or odds < 1:
                    continue
                selections.append({'id': first(selection, 1), 'name': label,
                                   'decimal_odds_raw': odds,
                                   'source_field_14': first(selection, 14)})
            if selections:
                output.append({'id': first(market, 1), 'name': name,
                               'source_field_9': first(market, 9), 'selections': selections})
    return output


def inspect_frame(data, sport, match_id):
    root = fields(data)
    result = {'scores': [], 'winner_markets': [], 'price_deltas': []}
    for raw_wrapper in root.get(1, []):
        wrapper = fields(raw_wrapper)
        for raw_match in wrapper.get(1, []):
            match = fields(raw_match)
            if first(match, 1) != match_id:
                continue
            score = score_entry(first(match, 13), sport, match_id, 'root.1.1.13')
            if score:
                result['scores'].append(score)
            result['winner_markets'].extend(winner_markets(match))
    for raw_envelope in root.get(2, []):
        for raw_notification in fields(raw_envelope).get(1, []):
            notification = fields(raw_notification)
            for raw_wrapper in notification.get(2, []):
                for raw_score in fields(raw_wrapper).get(1, []):
                    score = score_entry(raw_score, sport, match_id, 'root.2.1.2.1')
                    if score:
                        result['scores'].append(score)
            for raw_delta in notification.get(1, []):
                delta = fields(raw_delta)
                value = first(delta, 3)
                if (first(delta, 5) != match_id or not isinstance(first(delta, 1), int)
                        or not isinstance(first(delta, 2), int)
                        or not isinstance(value, bytes) or len(value) != 8):
                    continue
                odds = decode_fixed64_as_double(value)
                if odds is not None and math.isfinite(odds) and odds >= 1:
                    result['price_deltas'].append({'market_id': first(delta, 1),
                        'selection_id': first(delta, 2), 'decimal_odds_raw': odds,
                        'source_field_4': first(delta, 4)})
    return result

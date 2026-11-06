"""Analyze locally captured protobuf files without making any network requests."""
import argparse
import copy
import json
from pathlib import Path

from tools.stream_evidence import inspect_frame


def price_signature(market):
    return sorted((selection['name'], selection['decimal_odds_raw'])
                  for selection in market['selections'])


def analyze_event(item, directory):
    states = {}
    scores, winners = [], []
    score_updates = winner_updates = complete_updates = 0
    for record in item['frames']:
        path = (directory / record['file']).resolve()
        if directory.resolve() not in path.parents:
            raise ValueError('Capture path leaves the selected directory')
        parsed = inspect_frame(path.read_bytes(), item['sport'], item['id'])
        session = record['session']
        state = states.setdefault(session, {'score': None, 'winner': None})
        for score in parsed['scores']:
            signature = {k: v for k, v in score.items() if k != 'source_path'}
            changed = state['score'] is not None and state['score'] != signature
            if state['score'] != signature:
                scores.append({'seconds': record['seconds'], 'session': session,
                               'changed_within_connection': changed, 'score': score})
            score_updates += int(changed)
            state['score'] = signature
        winner = None
        if parsed['winner_markets']:
            winner = parsed['winner_markets'][0]
        elif state['winner'] is not None:
            candidate = copy.deepcopy(state['winner'])
            applied = False
            for delta in parsed['price_deltas']:
                if delta['market_id'] != candidate['id']:
                    continue
                for selection in candidate['selections']:
                    if selection['id'] == delta['selection_id']:
                        selection['decimal_odds_raw'] = delta['decimal_odds_raw']
                        selection['delta_source_field_4'] = delta['source_field_4']
                        applied = True
            if applied:
                winner = candidate
        if winner is not None:
            changed = state['winner'] is not None and price_signature(state['winner']) != price_signature(winner)
            if state['winner'] is None or changed:
                winners.append({'seconds': record['seconds'], 'session': session,
                                'changed_within_connection': changed, 'market': copy.deepcopy(winner)})
            winner_updates += int(changed)
            complete_updates += int(changed and len(winner['selections']) == 2
                                    and all(s['decimal_odds_raw'] > 1 for s in winner['selections']))
            state['winner'] = copy.deepcopy(winner)
    return {'sport': item['sport'], 'id': item['id'], 'name': item['name'],
            'frames': len(item['frames']), 'connections': len(item['sessions']),
            'sessions': item['sessions'],
            'score_updates_within_connection': score_updates,
            'winner_price_updates_within_connection_raw': winner_updates,
            'winner_updates_with_two_non_unit_prices': complete_updates,
            'score_history': scores, 'winner_history_raw': winners}


def analyze_directory(directory):
    directory = Path(directory)
    results = []
    for path in sorted(directory.glob('*.json')):
        item = json.loads(path.read_text(encoding='utf-8'))
        if all(key in item for key in ('sport', 'id', 'name', 'frames', 'sessions')):
            results.append(analyze_event(item, directory))
    root = directory / 'report.json'
    run = json.loads(root.read_text(encoding='utf-8')) if root.exists() else {}
    return {'started_at': run.get('started_at'), 'finished_at': run.get('finished_at'),
            'elapsed_seconds': run.get('elapsed_seconds'), 'events': results,
            'notes': ['Connection gaps are real; the observation window is not an uninterrupted stream.',
                      'Raw 1.00 values are preserved, with source codes left uninterpreted.',
                      'Only changes observed after a baseline on the same connection count as push updates.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    report = analyze_directory(args.directory)
    target = args.output or args.directory / 'analysis.json'
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    for item in report['events']:
        print(item['name'], 'frames=', item['frames'], 'connections=', item['connections'],
              'score_updates=', item['score_updates_within_connection'],
              'winner_price_updates_raw=', item['winner_price_updates_within_connection_raw'],
              'two_non_unit_prices_updates=', item['winner_updates_with_two_non_unit_prices'])


if __name__ == '__main__':
    main()

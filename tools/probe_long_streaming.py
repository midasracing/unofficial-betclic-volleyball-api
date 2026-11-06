"""Bounded long capture for offline score/odds diagnostics; no production changes."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import threading
import time

from betclic_api.client import SPORTS

from app.client import BetclicSnapshotClient
from app.streaming import stream_http
from tools.probe_streaming import DEFAULT_SPORTS, select_live_candidate, started_at


def select_events(candidates, limit=3):
    selected = []
    for candidate in candidates:
        if len(selected) == limit:
            break
        if not any(sport == candidate[0] for sport, _ in selected):
            selected.append(candidate)
    for candidate in candidates:
        if len(selected) == limit:
            break
        if candidate not in selected:
            selected.append(candidate)
    return selected


def capture_events(events, directory, seconds=600, max_connections=4, streamer=stream_http):
    if not 10 <= seconds <= 900 or not 1 <= max_connections <= 4 or len(events) > 3:
        raise ValueError('Limits: 10-900 seconds, 1-4 connections per event, at most 3 events')
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError('Use an empty capture directory')
    began, stop, lock = time.monotonic(), threading.Event(), threading.Lock()
    deadline = began + seconds
    report = {'started_at': datetime.now(timezone.utc).isoformat(), 'duration_limit': seconds,
              'connection_limit_per_event': max_connections, 'reconnect_cooldown': 90, 'events': []}

    def emit(value):
        with lock:
            print(json.dumps(value, ensure_ascii=False), flush=True)

    def save(item):
        path = directory / f'{item["sport"]}_{item["id"]}.json'
        temporary = path.with_suffix('.json.tmp')
        temporary.write_text(json.dumps(item, ensure_ascii=False, indent=2), encoding='utf-8')
        temporary.replace(path)

    def observe(sport, match):
        item = {'sport': sport, 'id': match.id, 'name': match.name, 'competition': match.competition,
                'starts_at': match.date, 'sessions': [], 'frames': [], 'bytes_received': 0}
        report['events'].append(item)
        for number in range(1, max_connections + 1):
            if stop.is_set() or time.monotonic() >= deadline:
                break
            session = {'number': number, 'opened_at_seconds': round(time.monotonic()-began, 3)}
            item['sessions'].append(session)
            def received(frame):
                if stop.is_set():
                    return
                data = frame['data']
                item['bytes_received'] += len(data)
                if item['bytes_received'] > 20_000_000 or len(item['frames']) >= 2000:
                    item['sample_limit_reached'] = True
                    stop.set()
                    return
                filename = f'{sport}_{match.id}_s{number}_f{len(item["frames"])}.protobuf.bin'
                (directory / filename).write_bytes(data)
                item['frames'].append({'seconds': round(time.monotonic()-began, 3),
                    'session': number, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                    'file': filename})
                save(item)
            try:
                emit({'open': match.name, 'session': number})
                streamer(match.id, stop, received)
                session['end'] = 'stopped' if stop.is_set() else 'closed'
            except Exception as error:
                session['end'] = f'{type(error).__name__}: {error}'
                response = getattr(error, 'response', None)
                if response is not None and response.status_code in (401, 403, 429):
                    session['stop_after_refusal'] = True
            session['closed_at_seconds'] = round(time.monotonic()-began, 3)
            save(item)
            emit({'closed': match.name, 'session': number, 'end': session['end'], 'frames': len(item['frames'])})
            if number == max_connections or session.get('stop_after_refusal'):
                break
            if stop.wait(min(90, max(0, deadline-time.monotonic()))):
                break
    threads = [threading.Thread(target=observe, args=event, daemon=True) for event in events]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(max(0, deadline-time.monotonic()))
    stop.set()
    shutdown_deadline = time.monotonic() + 40
    for thread in threads:
        thread.join(max(0, shutdown_deadline-time.monotonic()))
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    report['elapsed_seconds'] = round(time.monotonic()-began, 3)
    report['threads_stopped'] = all(not thread.is_alive() for thread in threads)
    (directory / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sports', default=','.join(DEFAULT_SPORTS))
    parser.add_argument('--seconds', type=int, default=600)
    parser.add_argument('--max-connections', type=int, default=4)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    sports = tuple(dict.fromkeys(s.strip() for s in args.sports.split(',') if s.strip()))
    if not 1 <= len(sports) <= 8 or any(s not in SPORTS for s in sports):
        parser.error('Select 1-8 supported sports')
    if not 10 <= args.seconds <= 900 or not 1 <= args.max_connections <= 4:
        parser.error('seconds: 10-900; max-connections: 1-4')
    directory = args.output or Path('captures/long') / datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    candidates = []
    with BetclicSnapshotClient() as client:
        for sport in sports:
            now = datetime.now(timezone.utc)
            listing = client.get_matches(sport)['matches']
            eligible = [match for match in listing if select_live_candidate([match], now) is not None]
            eligible.sort(key=lambda match: started_at(match.date), reverse=True)
            candidates.extend((sport, match) for match in eligible)
    report = capture_events(select_events(candidates), directory, args.seconds, args.max_connections)
    print(json.dumps({'directory': str(directory), 'elapsed': report['elapsed_seconds'],
                      'events': [(event['name'], len(event['frames'])) for event in report['events']]}, ensure_ascii=False))


if __name__ == '__main__':
    main()

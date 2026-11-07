"""Regression for a real PlusLiga game whose legacy live flag was absent."""
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from betclic_api.parser import parse_matches_response
from betclic_api.proto import encode_field_string as blob, encode_field_varint as integer

from app import client
from app.scheduling import select_league_streams


def fixture():
    data = json.loads((Path(__file__).parent/'fixtures/plusliga_listing_2026-11-07.json').read_text())
    frames = [{"flags": 0, "data": bytes.fromhex(data['hex'])}]
    return data, frames, parse_matches_response(frames)['matches'], datetime.fromisoformat(data['captured_at'])


def test_real_started_plusliga_score_restores_missing_live_flag_and_automatic_selection():
    data, frames, matches, now = fixture()
    assert matches[0].is_live is False
    assert select_league_streams(matches, now=now) == []
    client.apply_listing_score_evidence(matches, frames, now)
    assert matches[0].is_live is True
    assert select_league_streams(matches, now=now) == [data['match_id']]


def test_evidence_never_promotes_future_old_or_unknown_start_times():
    _, frames, matches, now = fixture()
    client.apply_listing_score_evidence(matches, frames, now-timedelta(hours=2))
    assert not matches[0].is_live
    client.apply_listing_score_evidence(matches, frames, now+timedelta(hours=12))
    assert not matches[0].is_live
    matches[0].date = None
    client.apply_listing_score_evidence(matches, frames, now)
    assert not matches[0].is_live


def test_scheduled_time_without_a_score_does_not_claim_live():
    _, _, matches, now = fixture()
    client.apply_listing_score_evidence(matches, [], now)
    assert not matches[0].is_live


def test_decided_best_of_five_match_is_not_promoted_or_kept_live():
    data, _, matches, now = fixture()
    entry = blob(1,'volleyball')+integer(10,data['match_id'])
    entry += blob(7,blob(1,'25')+blob(2,'10'))
    entry += blob(13,blob(3,'vlb_third_set'))
    for period in ('first','second','third'):
        entry += blob(9,blob(2,f'vlb_{period}_set')+blob(3,blob(1,'25')+blob(2,'10')))
    frames=[{'flags':0,'data':blob(1,blob(3,integer(1,data['match_id'])+blob(13,entry)))}]
    matches[0].is_live=True
    client.apply_listing_score_evidence(matches,frames,now)
    assert matches[0].is_live is False


def test_client_uses_listing_evidence_without_an_extra_detail_request(monkeypatch):
    _, frames, _, now=fixture()
    real_apply=client.apply_listing_score_evidence
    monkeypatch.setattr(client,'apply_listing_score_evidence',lambda matches, frames:real_apply(matches,frames,now))
    with client.BetclicSnapshotClient() as instance:
        instance._post=Mock(return_value=frames)
        result=instance.get_matches('volleyball')
        assert result['matches'][0].is_live
        assert instance._post.call_count==1


def test_zero_score_placeholder_does_not_promote_a_match_at_scheduled_time():
    data, _, matches, now=fixture()
    entry=blob(1,'volleyball')+integer(10,data['match_id'])
    pair=blob(1,'0')+blob(2,'0')
    entry+=blob(7,pair)+blob(13,blob(3,'vlb_first_set'))
    entry+=blob(9,blob(2,'vlb_first_set')+blob(3,pair))
    frames=[{'flags':0,'data':blob(1,blob(3,integer(1,data['match_id'])+blob(13,entry)))}]
    client.apply_listing_score_evidence(matches,frames,now)
    assert matches[0].is_live is False

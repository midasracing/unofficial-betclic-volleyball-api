"""Offline checks of active-event selection, request budgets and stream evidence."""
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
import requests
from betclic_api.models import Match, Team, Market, Selection

from tools import probe_streaming as probe

NOW = datetime.now(timezone.utc)


def event(match_id=101, minutes=-5, **fields):
    values = dict(id=match_id, name="A - B", competition="Test tournament",
                  date=(NOW + timedelta(minutes=minutes)).isoformat(), is_live=True,
                  teams=[Team("A"), Team("B")])
    values.update(fields)
    return Match(**values)


def packet(data=b"initial"):
    return {"flags": 0, "data": data}


@pytest.fixture
def no_market_parser(monkeypatch):
    monkeypatch.setattr(probe, "parse_match_detail_response", lambda frames: Match())


def test_selects_latest_started_match_and_rejects_future_old_unknown_and_season_events():
    matches = [event(1, -10), event(2, -1), event(3, 5), event(4, -500),
               event(5, is_live=False), event(6, date=None), event(7, teams=[])]
    assert probe.select_live_candidate(matches, NOW).id == 2
    assert probe.select_live_candidate(matches[2:], NOW) is None


def test_diagnostics_do_not_apply_production_gender_or_league_filters():
    selected = probe.select_live_candidate([event(name="A F. - B F.", competition="Other sport women")], NOW)
    assert selected is not None


def test_one_discovery_per_sport_and_one_connection_per_selected_event(no_market_parser):
    client = Mock()
    ids = {"tennis": 1, "football": 2, "volleyball": 3}
    client.get_matches.side_effect = lambda sport: {"matches": [event(ids[sport])]}
    received = []

    def stream(match_id, stop, callback):
        received.append(match_id)
        callback(packet())

    report = probe.run_probe(("tennis", "football", "volleyball", "tennis"),
                             max_streams=2, seconds=10, client=client, streamer=stream)
    assert [call.args[0] for call in client.get_matches.call_args_list] == ["tennis", "football", "volleyball"]
    assert report["discovery_requests"] == 3
    assert report["stream_requests"] == 2
    assert sorted(received) == [1, 2]
    assert len(report["streams"]) == 2
    assert report["discovery"][-1]["stream_selected"] is False
    assert all(row["thread_stopped"] for row in report["streams"])


def test_idle_timeout_keeps_the_evidence_of_repeated_payloads(no_market_parser):
    sample = probe.Observation("tennis", event())
    sample.record(packet(), elapsed=0)
    sample.record(packet(), elapsed=32)
    sample.end = "read_timeout"
    result = sample.result()
    assert result["evidence"] == "repeated_payloads_only"
    assert result["transport_end"] == "read_timeout"
    assert result["frame_count"] == 2
    assert result["distinct_payloads"] == 1


def test_changed_bytes_are_not_automatically_price_or_score_updates(no_market_parser):
    sample = probe.Observation("football", event())
    sample.record(packet(b"first"), elapsed=0)
    sample.record(packet(b"next"), elapsed=5)
    result = sample.result()
    assert result["evidence"] == "different_payloads_observed"
    assert result["late_market_updates"] == 0
    assert result["late_volleyball_score_updates"] == 0


def test_initial_multi_frame_burst_is_not_proof_of_push_updates(no_market_parser):
    sample = probe.Observation("tennis", event())
    sample.record(packet(b"one"), elapsed=0)
    sample.record(packet(b"two"), elapsed=0.1)
    assert sample.result()["evidence"] == "initial_data_only"


def test_only_later_decoded_market_changes_count_as_live_market_updates(monkeypatch):
    def detail(odds):
        return event(markets=[Market("Match winner", selections=[Selection("A", odds), Selection("B", 2.4)])])
    snapshots = iter([detail(1.5), detail(1.6), detail(1.6), detail(1.7)])
    monkeypatch.setattr(probe, "parse_match_detail_response", lambda frames: next(snapshots))
    sample = probe.Observation("football", event())
    sample.record(packet(b"a"), elapsed=0)
    sample.record(packet(b"b"), elapsed=0.1)
    sample.record(packet(b"c"), elapsed=5)
    assert sample.result()["late_market_updates"] == 0
    sample.record(packet(b"d"), elapsed=10)
    assert sample.result()["late_market_updates"] == 1
    assert sample.result()["evidence"] == "market_updates_observed"


def test_other_sports_never_use_the_volleyball_score_decoder(monkeypatch, no_market_parser):
    decoder = Mock(side_effect=AssertionError("Wrong sport"))
    monkeypatch.setattr(probe, "decode_score_frames", decoder)
    probe.Observation("football", event()).record(packet(), elapsed=0)
    decoder.assert_not_called()


@pytest.mark.parametrize("error, expected", [
    (probe.GrpcStatusError(14, "A task was canceled."), "grpc_status_14"),
    (requests.ReadTimeout("idle"), "read_timeout"),
    (requests.ConnectionError("Read timed out."), "read_timeout"),
    (requests.ConnectTimeout("connect"), "connect_timeout"),
    (requests.HTTPError("403"), "http_error"),
    (requests.RequestException("network"), "request_error"),
    (ValueError("bug"), "unexpected_error"),
])
def test_transport_end_is_separate_from_observed_stream_data(error, expected):
    assert probe.transport_error(error) == expected


def test_no_started_candidate_is_inconclusive_and_opens_no_connection():
    client = Mock()
    client.get_matches.return_value = {"matches": [event(minutes=30)]}
    stream = Mock()
    report = probe.run_probe(("tennis",), seconds=10, client=client, streamer=stream)
    assert report["outcome"] == "no_started_live_candidates"
    assert report["stream_requests"] == 0
    stream.assert_not_called()


def test_discovery_error_is_not_misreported_as_empty_live_offer():
    client = Mock()
    client.get_matches.side_effect = requests.ReadTimeout("network")
    report = probe.run_probe(("tennis",), seconds=10, client=client, streamer=Mock())
    assert report["outcome"] == "discovery_failed"
    assert "error" in report["discovery"][0]


def test_sampling_limit_stops_the_one_connection(no_market_parser, monkeypatch):
    monkeypatch.setattr(probe, "MAX_FRAMES", 2)
    sample = probe.Observation("tennis", event())
    for i in range(3):
        sample.record(packet(), elapsed=i)
    assert sample.stop.is_set()
    assert sample.result()["sample_limit_reached"]
    assert sample.result()["frame_count"] == 2

"""Offline regressions for the snapshot transport, including the empty last page."""
import struct
import unittest
from unittest.mock import Mock, patch

import requests
from betclic_api.models import Match, Team
from betclic_api.proto import encode_field_string, encode_field_varint
from app.client import BetclicSnapshotClient, MAX_FRAME_BYTES, SnapshotError
from app.winner_odds import winner_market

def field_bytes(number, value):
    return encode_field_string(number, value)


def _encode_varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def frame(data, flags=0):
    return bytes([flags]) + struct.pack(">I", len(data)) + data


def match_bytes(match_id=101, with_market=False):
    match = encode_field_varint(1, match_id) + encode_field_string(2, "A - B")
    match += encode_field_string(3, "2026-11-05T19:30:00Z")
    competition = encode_field_varint(1, 41288) + encode_field_string(2, "Pologne Tauron Liga H.")
    match += field_bytes(8, competition)
    if with_market:
        market = encode_field_string(2, "Vainqueur du match")
        for name, odds in (("A", 1.5), ("B", 2.5)):
            selection = encode_field_string(10, name)
            selection += _encode_varint((12 << 3) | 1) + struct.pack("<d", odds)
            market += field_bytes(16, selection)
        match += field_bytes(11, field_bytes(3, market))
    return match


def response(chunks):
    result = Mock()
    result.headers = {}
    result.iter_content.return_value = iter(chunks)
    return result


def listed_match(**fields):
    return Match(teams=[Team("A"), Team("B")], **fields)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.client = BetclicSnapshotClient()
        self.addCleanup(self.client._session.close)

    def test_fragmented_listing_stops_before_next_live_update(self):
        body = frame(field_bytes(1, field_bytes(3, match_bytes())))

        def chunks():
            for index in range(0, len(body), 3):
                yield body[index:index + 3]
            raise AssertionError("Client kept waiting for the next live update")

        http = response(chunks())
        with patch.object(self.client._session, "post", return_value=http):
            result = self.client.get_matches("volleyball")
        self.assertEqual([match.id for match in result["matches"]], [101])
        http.close.assert_called_once()
        http.iter_content.assert_called_once_with(chunk_size=1024)

    def test_detail_is_parsed_by_the_real_upstream_parser(self):
        body = frame(field_bytes(1, field_bytes(1, match_bytes(with_market=True))))
        http = response([body])
        with patch.object(self.client._session, "post", return_value=http):
            detail = self.client.get_match(101)
        self.assertEqual(winner_market(detail.markets)["selections"],
                         [{"name": "A", "odds": 1.5}, {"name": "B", "odds": 2.5}])
        http.close.assert_called_once()

    def test_successful_empty_snapshot_requires_success_trailer(self):
        http = response([frame(b"") + frame(b"grpc-status: 0\r\n", 0x80)])
        with patch.object(self.client._session, "post", return_value=http):
            self.assertEqual(self.client.get_matches("volleyball")["matches"], [])

    def test_empty_initial_listing_does_not_wait_for_live_updates(self):
        initial = frame(field_bytes(1, encode_field_varint(4, 0) + encode_field_varint(5, 30)))

        def chunks():
            yield initial
            raise AssertionError("Empty last page kept the notification stream open")

        http = response(chunks())
        with patch.object(self.client._session, "post", return_value=http):
            self.assertEqual(self.client.get_matches("volleyball", offset=22)["matches"], [])
        http.close.assert_called_once()

    def test_failures_are_not_reported_as_an_empty_success_and_close_response(self):
        cases = [
            [],
            [frame(b"")[:3]],
            [frame(b"grpc-status: 7\r\n", 0x80)],
            [frame(b"grpc-status: 0\r\n", 0x80)],
            [frame(b"compressed", 1)],
            [b"\0" + struct.pack(">I", MAX_FRAME_BYTES + 1)],
        ]
        for chunks in cases:
            with self.subTest(chunks=chunks):
                http = response(chunks)
                with patch.object(self.client._session, "post", return_value=http):
                    with self.assertRaises(SnapshotError):
                        self.client.get_matches("volleyball")
                http.close.assert_called_once()

    def test_read_timeout_propagates_and_closes_response(self):
        def timeout():
            yield b"\0\0"
            raise requests.ReadTimeout("No complete frame")

        http = response(timeout())
        with patch.object(self.client._session, "post", return_value=http):
            with self.assertRaises(requests.ReadTimeout):
                self.client.get_matches("volleyball")
        http.close.assert_called_once()

    def test_http_error_closes_response(self):
        http = response([])
        http.raise_for_status.side_effect = requests.HTTPError("403")
        with patch.object(self.client._session, "post", return_value=http):
            with self.assertRaises(requests.HTTPError):
                self.client.get_matches("volleyball")
        http.close.assert_called_once()



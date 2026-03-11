import json
import unittest

from collector.binance import BinanceDepthCollector


class _DummyWS:
    def __init__(self, events):
        self._events = list(events)

    async def recv(self):
        if self._events:
            return json.dumps(self._events.pop(0))
        # keep waiting to emulate live socket
        raise TimeoutError("no more events")


class TestBinanceSequence(unittest.IsolatedAsyncioTestCase):
    async def test_apply_depth_event_allows_first_contiguous_event(self):
        collector = BinanceDepthCollector(
            symbol="BTCUSDT",
            ws_url="wss://example.com/ws",
            rest_base_url="https://example.com",
        )
        collector._book.set_snapshot(bids=[("100", "1")], asks=[("101", "1")], last_update_id=100)

        event = {
            "U": 99,
            "u": 101,
            "pu": 98,
            "b": [["100", "2"]],
            "a": [["101", "3"]],
            "E": 1710000000000,
        }

        ok = await collector._apply_depth_event(event, require_prev_final=False)
        self.assertTrue(ok)
        self.assertEqual(collector._book.last_update_id, 101)

    async def test_gap_after_sync_requires_reconnect(self):
        collector = BinanceDepthCollector(
            symbol="BTCUSDT",
            ws_url="wss://example.com/ws",
            rest_base_url="https://example.com",
        )
        collector._book.set_snapshot(bids=[("100", "1")], asks=[("101", "1")], last_update_id=200)

        event = {
            "U": 205,
            "u": 206,
            "pu": 200,
            "b": [["100", "2"]],
            "a": [["101", "3"]],
        }

        ok = await collector._apply_depth_event(event, require_prev_final=True)
        self.assertFalse(ok)

    async def test_bootstrap_reads_bridging_event_from_socket(self):
        collector = BinanceDepthCollector(
            symbol="BTCUSDT",
            ws_url="wss://example.com/ws",
            rest_base_url="https://example.com",
        )
        snapshot = {
            "lastUpdateId": 100,
            "bids": [["100", "1"]],
            "asks": [["101", "1"]],
        }
        ws = _DummyWS(
            [
                {"U": 100, "u": 101, "b": [["100", "2"]], "a": [["101", "2"]]},
                {"U": 102, "u": 103, "pu": 101, "b": [["99", "3"]], "a": [["102", "1"]]},
            ]
        )

        ok = await collector._bootstrap_from_snapshot(ws, snapshot, [])
        self.assertTrue(ok)
        self.assertEqual(collector._book.last_update_id, 101)


if __name__ == "__main__":
    unittest.main()

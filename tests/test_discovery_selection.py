import unittest

from collector.discovery import _select_events


class TestDiscoverySelection(unittest.TestCase):
    def test_current_live_picks_nearest_end(self):
        now = 1_000
        events = [
            {"slug": "a", "active": True, "closed": False, "endDate": "1970-01-01T00:20:00Z"},  # 1200
            {"slug": "b", "active": True, "closed": False, "endDate": "1970-01-01T00:18:20Z"},  # 1100
            {"slug": "c", "active": True, "closed": False, "endDate": "1970-01-01T00:16:40Z"},  # 1000
        ]
        selected = _select_events(events, "current_live", now)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["slug"], "c")

    def test_current_live_ignores_closed(self):
        now = 1_000
        events = [
            {"slug": "closed", "active": True, "closed": True, "endDate": "1970-01-01T00:16:40Z"},
            {"slug": "open", "active": True, "closed": False, "endDate": "1970-01-01T00:20:00Z"},
        ]
        selected = _select_events(events, "current_live", now)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["slug"], "open")

    def test_all_open_keeps_all(self):
        now = 1_000
        events = [
            {"slug": "a", "active": True, "closed": False, "endDate": "1970-01-01T00:20:00Z"},
            {"slug": "b", "active": True, "closed": False, "endDate": "1970-01-01T00:18:20Z"},
        ]
        selected = _select_events(events, "all_open", now)
        self.assertEqual(len(selected), 2)


if __name__ == "__main__":
    unittest.main()

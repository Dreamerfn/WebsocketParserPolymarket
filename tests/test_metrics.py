import unittest

from collector.metrics import density_features, flatten_levels


class TestMetrics(unittest.TestCase):
    def test_flatten_levels_fills_nulls(self):
        row = flatten_levels(bids=[(100.0, 1.0)], asks=[(101.0, 2.0)], levels=3)
        self.assertEqual(row["bid_px_1"], 100.0)
        self.assertEqual(row["ask_sz_1"], 2.0)
        self.assertIsNone(row["bid_px_3"])
        self.assertIsNone(row["ask_sz_3"])

    def test_density_features(self):
        bids = [(100.0, 1.0), (99.0, 2.0)]
        asks = [(101.0, 1.5), (102.0, 1.5)]
        out = density_features(bids, asks)

        self.assertAlmostEqual(out["qty_sum_bid_5"], 3.0)
        self.assertAlmostEqual(out["qty_sum_ask_5"], 3.0)
        self.assertAlmostEqual(out["spread_bps"], ((101.0 - 100.0) / 100.5) * 10000)


if __name__ == "__main__":
    unittest.main()

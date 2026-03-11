import unittest

from collector.models import MarketMeta
from collector.polymarket import PolymarketBookCollector


class TestPolymarketFilter(unittest.IsolatedAsyncioTestCase):
    async def test_ignores_unknown_asset_updates(self):
        collector = PolymarketBookCollector(ws_url="wss://example.com/ws")
        await collector.update_assets(
            {
                "known_asset": MarketMeta(
                    asset_id="known_asset",
                    market_id="m1",
                    slug="slug",
                    question="q",
                )
            }
        )

        await collector._apply_update(
            {
                "type": "book",
                "asset_id": "unknown_asset",
                "bids": [["0.5", "10"]],
                "asks": [["0.6", "10"]],
            }
        )

        snaps = await collector.snapshots()
        self.assertEqual(len(snaps), 1)
        self.assertEqual(snaps[0].instrument, "known_asset")
        self.assertEqual(snaps[0].bids, [])
        self.assertEqual(snaps[0].asks, [])


if __name__ == "__main__":
    unittest.main()

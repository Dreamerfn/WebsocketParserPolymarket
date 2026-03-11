import unittest

from collector.orderbook import OrderBook


class TestOrderBook(unittest.TestCase):
    def test_snapshot_and_top(self):
        book = OrderBook()
        book.set_snapshot(
            bids=[("100", "2"), ("99", "3")],
            asks=[("101", "1.5"), ("102", "1")],
            last_update_id=10,
        )

        self.assertEqual(book.top_bids(2), [(100.0, 2.0), (99.0, 3.0)])
        self.assertEqual(book.top_asks(2), [(101.0, 1.5), (102.0, 1.0)])
        self.assertEqual(book.last_update_id, 10)

    def test_apply_delta(self):
        book = OrderBook()
        book.set_snapshot(bids=[("100", "2")], asks=[("101", "1")])

        book.apply_delta("bid", 100.0, 0.0)
        book.apply_delta("ask", 101.0, 2.5)

        self.assertEqual(book.top_bids(1), [])
        self.assertEqual(book.top_asks(1), [(101.0, 2.5)])

    def test_snapshot_accepts_dict_levels(self):
        book = OrderBook()
        book.set_snapshot(
            bids=[{"price": "100", "size": "1.2"}],
            asks=[{"price": "101", "quantity": "0.8"}],
        )
        self.assertEqual(book.top_bids(1), [(100.0, 1.2)])
        self.assertEqual(book.top_asks(1), [(101.0, 0.8)])


if __name__ == "__main__":
    unittest.main()

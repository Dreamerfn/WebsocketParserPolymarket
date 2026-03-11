from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class OrderBook:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_update_id: int | None = None
    exchange_ts_ns: int | None = None

    def set_snapshot(
        self,
        bids: list,
        asks: list,
        last_update_id: int | None = None,
        exchange_ts_ns: int | None = None,
    ) -> None:
        self.bids.clear()
        self.asks.clear()
        for level in bids:
            parsed = _parse_level(level)
            if parsed is None:
                continue
            p, s = parsed
            if s > 0:
                self.bids[p] = s
        for level in asks:
            parsed = _parse_level(level)
            if parsed is None:
                continue
            p, s = parsed
            if s > 0:
                self.asks[p] = s
        self.last_update_id = last_update_id
        self.exchange_ts_ns = exchange_ts_ns

    def apply_delta(self, side: str, price: float, size: float) -> None:
        side_book = self.bids if side == "bid" else self.asks
        if size <= 0:
            side_book.pop(price, None)
            return
        side_book[price] = size

    def apply_bulk(self, bids: list | None = None, asks: list | None = None) -> None:
        if bids:
            for level in bids:
                parsed = _parse_level(level)
                if parsed is None:
                    continue
                px, sz = parsed
                self.apply_delta("bid", px, sz)
        if asks:
            for level in asks:
                parsed = _parse_level(level)
                if parsed is None:
                    continue
                px, sz = parsed
                self.apply_delta("ask", px, sz)

    def top_bids(self, n: int) -> list[tuple[float, float]]:
        return sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]

    def top_asks(self, n: int) -> list[tuple[float, float]]:
        return sorted(self.asks.items(), key=lambda x: x[0])[:n]


def _parse_level(level) -> tuple[float, float] | None:
    if isinstance(level, dict):
        px = level.get("price")
        sz = level.get("size")
        if sz is None:
            sz = level.get("quantity")
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        px, sz = level[0], level[1]
    else:
        return None

    try:
        return float(px), float(sz)
    except (TypeError, ValueError):
        return None

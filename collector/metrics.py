from __future__ import annotations

from typing import Any


MAX_LEVELS = 20



def flatten_levels(bids: list[tuple[float, float]], asks: list[tuple[float, float]], levels: int = MAX_LEVELS) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for i in range(levels):
        idx = i + 1
        if i < len(bids):
            out[f"bid_px_{idx}"] = bids[i][0]
            out[f"bid_sz_{idx}"] = bids[i][1]
        else:
            out[f"bid_px_{idx}"] = None
            out[f"bid_sz_{idx}"] = None

        if i < len(asks):
            out[f"ask_px_{idx}"] = asks[i][0]
            out[f"ask_sz_{idx}"] = asks[i][1]
        else:
            out[f"ask_px_{idx}"] = None
            out[f"ask_sz_{idx}"] = None
    return out



def _qty_sum(levels: list[tuple[float, float]], n: int) -> float:
    return sum(size for _, size in levels[:n])



def _notional_sum(levels: list[tuple[float, float]], n: int) -> float:
    return sum(price * size for price, size in levels[:n])



def density_features(bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> dict[str, Any]:
    bid_5 = _qty_sum(bids, 5)
    bid_10 = _qty_sum(bids, 10)
    bid_20 = _qty_sum(bids, 20)
    ask_5 = _qty_sum(asks, 5)
    ask_10 = _qty_sum(asks, 10)
    ask_20 = _qty_sum(asks, 20)

    bid_notional_20 = _notional_sum(bids, 20)
    ask_notional_20 = _notional_sum(asks, 20)

    imbalance = None
    denom = bid_20 + ask_20
    if denom > 0:
        imbalance = (bid_20 - ask_20) / denom

    spread_bps = None
    if bids and asks:
        bid1 = bids[0][0]
        ask1 = asks[0][0]
        mid = (bid1 + ask1) / 2
        if mid > 0:
            spread_bps = ((ask1 - bid1) / mid) * 10_000

    return {
        "qty_sum_bid_5": bid_5,
        "qty_sum_bid_10": bid_10,
        "qty_sum_bid_20": bid_20,
        "qty_sum_ask_5": ask_5,
        "qty_sum_ask_10": ask_10,
        "qty_sum_ask_20": ask_20,
        "notional_sum_bid_20": bid_notional_20,
        "notional_sum_ask_20": ask_notional_20,
        "imbalance_20": imbalance,
        "spread_bps": spread_bps,
    }

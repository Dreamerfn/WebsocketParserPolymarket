from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class MarketMeta:
    asset_id: str
    market_id: str | None
    slug: str | None
    question: str | None


@dataclass
class BookSnapshot:
    source: str
    instrument: str
    market_id: str | None
    exchange_ts_ns: int | None
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    extras: dict[str, Any]

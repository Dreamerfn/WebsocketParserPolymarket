from __future__ import annotations

import asyncio
import json
import logging
from json import JSONDecodeError

import websockets

from collector.models import BookSnapshot, MarketMeta
from collector.orderbook import OrderBook

logger = logging.getLogger(__name__)


class PolymarketBookCollector:
    def __init__(
        self,
        ws_url: str,
        levels: int = 5,
        ws_max_queue: int = 2048,
        ws_max_size_bytes: int = 2_000_000,
    ) -> None:
        self.ws_url = ws_url
        self.levels = levels
        self.ws_max_queue = ws_max_queue
        self.ws_max_size_bytes = ws_max_size_bytes
        self._books: dict[str, OrderBook] = {}
        self._meta: dict[str, MarketMeta] = {}
        self._target_assets: set[str] = set()
        self._lock = asyncio.Lock()

    async def update_assets(self, by_asset: dict[str, MarketMeta]) -> None:
        async with self._lock:
            self._target_assets = set(by_asset.keys())
            self._meta = dict(by_asset)
            # Keep existing books for overlapping assets; drop stale.
            self._books = {a: b for a, b in self._books.items() if a in self._target_assets}
            for asset_id in self._target_assets:
                self._books.setdefault(asset_id, OrderBook())

    async def snapshots(self) -> list[BookSnapshot]:
        async with self._lock:
            snaps: list[BookSnapshot] = []
            for asset_id, book in self._books.items():
                meta = self._meta.get(asset_id)
                snaps.append(
                    BookSnapshot(
                        source="polymarket",
                        instrument=asset_id,
                        market_id=meta.market_id if meta else None,
                        exchange_ts_ns=book.exchange_ts_ns,
                        bids=book.top_bids(self.levels),
                        asks=book.top_asks(self.levels),
                        extras={
                            "slug": meta.slug if meta else None,
                            "question": meta.question if meta else None,
                        },
                    )
                )
            return snaps

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self._run_once(stop_event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Polymarket WS loop failed; reconnecting")
                await asyncio.sleep(2)

    async def _run_once(self, stop_event: asyncio.Event) -> None:
        target_assets = await self._get_target_assets()
        if not target_assets:
            await asyncio.sleep(1)
            return

        logger.info("Polymarket connect with %d assets", len(target_assets))
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=20,
            max_queue=self.ws_max_queue,
            max_size=self.ws_max_size_bytes,
        ) as ws:
            # Keep both key variants for compatibility with minor upstream schema differences.
            await ws.send(
                json.dumps(
                    {
                        "type": "market",
                        "assets_ids": target_assets,
                        "asset_ids": target_assets,
                    }
                )
            )

            while not stop_event.is_set():
                current_assets = await self._get_target_assets()
                if set(current_assets) != set(target_assets):
                    logger.info("Polymarket target assets changed, reconnecting")
                    return

                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                await self._handle_message(raw)

    async def _get_target_assets(self) -> list[str]:
        async with self._lock:
            return sorted(self._target_assets)

    async def _handle_message(self, raw: str | bytes) -> None:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        try:
            payload = json.loads(text)
        except JSONDecodeError:
            logger.debug("Polymarket WS message is not valid JSON, skipped")
            return
        messages = payload if isinstance(payload, list) else [payload]
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            await self._apply_update(msg)

    async def _apply_update(self, msg: dict) -> None:
        event_type = str(msg.get("event_type") or msg.get("type") or "").lower()

        if event_type in {"book", "book_snapshot"}:
            asset_id = _extract_asset_id(msg)
            if not asset_id:
                return
            bids = msg.get("bids") or msg.get("buys") or []
            asks = msg.get("asks") or msg.get("sells") or []
            ts_ns = _extract_ts_ns(msg)
            async with self._lock:
                if asset_id not in self._target_assets:
                    return
                book = self._books.setdefault(asset_id, OrderBook())
                book.set_snapshot(bids=bids, asks=asks, exchange_ts_ns=ts_ns)
            return

        if event_type in {"price_change", "book_delta", "delta", "tick_size_change"}:
            changes = msg.get("price_changes") or msg.get("changes") or []
            if not isinstance(changes, list):
                return
            async with self._lock:
                for change in changes:
                    if not isinstance(change, dict):
                        continue
                    asset_id = _extract_asset_id(change) or _extract_asset_id(msg)
                    if not asset_id:
                        continue
                    if asset_id not in self._target_assets:
                        continue
                    side = _normalize_side(change.get("side"))
                    if side is None:
                        continue
                    price = _to_float(change.get("price"))
                    size = _to_float(change.get("size") or change.get("newSize") or change.get("quantity"))
                    if price is None or size is None:
                        continue
                    book = self._books.setdefault(asset_id, OrderBook())
                    book.apply_delta(side, price, size)
                    ts_ns = _extract_ts_ns(change) or _extract_ts_ns(msg)
                    if ts_ns is not None:
                        book.exchange_ts_ns = ts_ns



def _extract_asset_id(payload: dict) -> str | None:
    for key in ("asset_id", "assetId", "token_id", "tokenId", "market"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    return None



def _normalize_side(value) -> str | None:
    if value is None:
        return None
    side = str(value).lower()
    if side in {"buy", "bid", "b"}:
        return "bid"
    if side in {"sell", "ask", "a"}:
        return "ask"
    return None



def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None



def _extract_ts_ns(payload: dict) -> int | None:
    for key in ("timestamp", "ts", "event_time", "time"):
        value = payload.get(key)
        if value is None:
            continue
        try:
            ts = int(value)
        except (TypeError, ValueError):
            continue
        # Assume ms when value is in 13-digit range.
        if ts < 10_000_000_000:
            return ts * 1_000_000_000
        if ts < 10_000_000_000_000:
            return ts * 1_000_000
        return ts
    return None

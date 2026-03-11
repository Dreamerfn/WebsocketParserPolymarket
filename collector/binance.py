from __future__ import annotations

import asyncio
import json
import logging
from json import JSONDecodeError

import aiohttp
import websockets

from collector.orderbook import OrderBook

logger = logging.getLogger(__name__)


class BinanceDepthCollector:
    def __init__(
        self,
        symbol: str,
        ws_url: str,
        rest_base_url: str,
        speed: str = "100ms",
        snapshot_limit: int = 1000,
        ws_max_queue: int = 2048,
        ws_max_size_bytes: int = 2_000_000,
    ) -> None:
        self.symbol = symbol.upper()
        self.ws_url = ws_url.rstrip("/")
        self.rest_base_url = rest_base_url.rstrip("/")
        self.speed = speed
        self.snapshot_limit = snapshot_limit
        self.ws_max_queue = ws_max_queue
        self.ws_max_size_bytes = ws_max_size_bytes

        self._book = OrderBook()
        self._lock = asyncio.Lock()

    async def snapshot(self, levels: int) -> tuple[list[tuple[float, float]], list[tuple[float, float]], int | None]:
        async with self._lock:
            return (
                self._book.top_bids(levels),
                self._book.top_asks(levels),
                self._book.exchange_ts_ns,
            )

    async def run(self, stop_event: asyncio.Event) -> None:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not stop_event.is_set():
                try:
                    await self._run_once(stop_event, session)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Binance WS loop failed; reconnecting")
                    await asyncio.sleep(2)

    async def _run_once(self, stop_event: asyncio.Event, session: aiohttp.ClientSession) -> None:
        stream = f"{self.symbol.lower()}@depth@{self.speed}"
        uri = f"{self.ws_url}/{stream}"
        logger.info("Binance connect %s", uri)

        async with websockets.connect(
            uri,
            ping_interval=20,
            ping_timeout=20,
            max_queue=self.ws_max_queue,
            max_size=self.ws_max_size_bytes,
        ) as ws:
            snapshot, buffered_events = await self._buffer_events_while_fetching_snapshot(ws, session)
            bootstrap_ok = await self._bootstrap_from_snapshot(ws, snapshot, buffered_events)
            if not bootstrap_ok:
                logger.warning("Binance failed to bootstrap from snapshot+stream; reconnecting")
                return

            while not stop_event.is_set():
                try:
                    event = await self._recv_event(ws, timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                if event is None:
                    continue

                ok = await self._apply_depth_event(event, require_prev_final=True)
                if not ok:
                    logger.warning("Binance sequence gap detected, reconnecting stream")
                    return

    async def _buffer_events_while_fetching_snapshot(
        self,
        ws,
        session: aiohttp.ClientSession,
    ) -> tuple[dict, list[dict]]:
        snapshot_task = asyncio.create_task(self._fetch_snapshot(session))
        events: list[dict] = []

        try:
            while not snapshot_task.done():
                try:
                    event = await self._recv_event(ws, timeout=0.2)
                except asyncio.TimeoutError:
                    continue
                if event is not None:
                    events.append(event)
            snapshot = await snapshot_task
            # Drain a small tail window: events can arrive right as snapshot completes.
            for _ in range(100):
                try:
                    event = await self._recv_event(ws, timeout=0.01)
                except asyncio.TimeoutError:
                    break
                if event is not None:
                    events.append(event)
            return snapshot, events
        except Exception:
            snapshot_task.cancel()
            await asyncio.gather(snapshot_task, return_exceptions=True)
            raise

    async def _bootstrap_from_snapshot(self, ws, snapshot: dict, buffered_events: list[dict]) -> bool:
        last_update_id = int(snapshot["lastUpdateId"])

        async with self._lock:
            self._book.set_snapshot(
                bids=snapshot.get("bids", []),
                asks=snapshot.get("asks", []),
                last_update_id=last_update_id,
                exchange_ts_ns=None,
            )

        pending = [event for event in buffered_events if _extract_update_ids(event) is not None]
        first_applied = False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5.0

        while loop.time() < deadline:
            expected_next = await self._expected_next_id()
            if expected_next is None:
                return False

            while pending:
                ids = _extract_update_ids(pending[0])
                if ids is None:
                    pending.pop(0)
                    continue
                _, final_id = ids
                if final_id < expected_next:
                    pending.pop(0)
                    continue
                break

            if not pending:
                try:
                    event = await self._recv_event(ws, timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                if event is not None:
                    pending.append(event)
                continue

            first_id, final_id = _extract_update_ids(pending[0])  # type: ignore[arg-type]
            if first_id > expected_next:
                return False
            if not (first_id <= expected_next <= final_id):
                pending.pop(0)
                continue

            ok = await self._apply_depth_event(pending.pop(0), require_prev_final=False)
            if not ok:
                return False
            first_applied = True
            break

        if not first_applied:
            return False

        while pending:
            event = pending.pop(0)
            ok = await self._apply_depth_event(event, require_prev_final=True)
            if not ok:
                return False

        return True

    async def _expected_next_id(self) -> int | None:
        async with self._lock:
            if self._book.last_update_id is None:
                return None
            return self._book.last_update_id + 1

    async def _fetch_snapshot(self, session: aiohttp.ClientSession) -> dict:
        url = f"{self.rest_base_url}/fapi/v1/depth"
        params = {
            "symbol": self.symbol,
            "limit": str(self.snapshot_limit),
        }
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            snap = await resp.json()
        return snap

    async def _recv_event(self, ws, timeout: float) -> dict | None:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        return _decode_event(raw)

    async def _apply_depth_event(self, event: dict, require_prev_final: bool) -> bool:
        ids = _extract_update_ids(event)
        if ids is None:
            return True

        first_id, final_id = ids
        prev_final = event.get("pu")

        async with self._lock:
            last_id = self._book.last_update_id
            if last_id is None:
                return False

            expected_next = last_id + 1
            if final_id < expected_next:
                return True

            if first_id > expected_next:
                return False

            if not (first_id <= expected_next <= final_id):
                return False

            if require_prev_final and prev_final is not None:
                try:
                    if int(prev_final) != last_id:
                        return False
                except (TypeError, ValueError):
                    return False

            self._book.apply_bulk(bids=event.get("b", []), asks=event.get("a", []))
            self._book.last_update_id = final_id
            event_ms = event.get("E")
            if event_ms is not None:
                self._book.exchange_ts_ns = int(event_ms) * 1_000_000

        return True



def _decode_event(raw: str | bytes) -> dict | None:
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        payload = json.loads(text)
    except JSONDecodeError:
        return None
    data = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
    if not isinstance(data, dict):
        return None
    return data



def _extract_update_ids(event: dict) -> tuple[int, int] | None:
    if "u" not in event or "U" not in event:
        return None
    try:
        first_id = int(event["U"])
        final_id = int(event["u"])
    except (TypeError, ValueError):
        return None
    return first_id, final_id

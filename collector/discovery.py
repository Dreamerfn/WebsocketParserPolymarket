from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp

from collector.models import MarketMeta

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryResult:
    by_asset: dict[str, MarketMeta]


class PolymarketDiscovery:
    def __init__(
        self,
        gamma_base_url: str,
        slug_keywords: list[str],
        interval_sec: int = 30,
        slug_scan_enabled: bool = True,
        slug_scan_prefix: str = "btc-updown-15m",
        slug_scan_hours_back: int = 2,
        slug_scan_hours_forward: int = 8,
        slug_scan_step_sec: int = 900,
        slug_scan_concurrency: int = 24,
        market_selection_mode: str = "current_live",
    ) -> None:
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self.slug_keywords = [k.lower() for k in slug_keywords]
        self.interval_sec = interval_sec
        self.slug_scan_enabled = slug_scan_enabled
        self.slug_scan_prefix = slug_scan_prefix
        self.slug_scan_hours_back = slug_scan_hours_back
        self.slug_scan_hours_forward = slug_scan_hours_forward
        self.slug_scan_step_sec = slug_scan_step_sec
        self.slug_scan_concurrency = slug_scan_concurrency
        self.market_selection_mode = market_selection_mode

    async def run(self, stop_event: asyncio.Event, on_update) -> None:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not stop_event.is_set():
                try:
                    result = await self.fetch_markets(session)
                    await on_update(result)
                except Exception:
                    logger.exception("Polymarket discovery tick failed")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.interval_sec)
                except asyncio.TimeoutError:
                    pass

    async def fetch_markets(self, session: aiohttp.ClientSession) -> DiscoveryResult:
        by_asset: dict[str, MarketMeta] = {}

        if self.slug_scan_enabled:
            by_asset = await self._fetch_by_slug_scan(session)
            if by_asset:
                return DiscoveryResult(by_asset=by_asset)

        markets = await self._fetch_markets_endpoint(session)

        for market in markets:
            if not self._is_target_market(market):
                continue

            market_id = _string_or_none(market.get("id") or market.get("marketId"))
            slug = _string_or_none(market.get("slug"))
            question = _string_or_none(market.get("question") or market.get("title"))
            asset_ids = _extract_asset_ids(market)

            for asset_id in asset_ids:
                if not asset_id:
                    continue
                by_asset[asset_id] = MarketMeta(
                    asset_id=asset_id,
                    market_id=market_id,
                    slug=slug,
                    question=question,
                )

        return DiscoveryResult(by_asset=by_asset)

    async def _fetch_by_slug_scan(self, session: aiohttp.ClientSession) -> dict[str, MarketMeta]:
        now = int(time.time())
        aligned_now = now - (now % self.slug_scan_step_sec)
        start = aligned_now - (self.slug_scan_hours_back * 3600)
        end = aligned_now + (self.slug_scan_hours_forward * 3600)

        timestamps = list(range(start, end + self.slug_scan_step_sec, self.slug_scan_step_sec))
        sem = asyncio.Semaphore(self.slug_scan_concurrency)
        events: list[dict] = []

        async def fetch_one(ts: int) -> None:
            slug = f"{self.slug_scan_prefix}-{ts}"
            url = f"{self.gamma_base_url}/events/slug/{slug}"
            async with sem:
                event = await _safe_get_optional_json(session, url)
            if isinstance(event, dict):
                events.append(event)

        await asyncio.gather(*(fetch_one(ts) for ts in timestamps))

        selected_events = _select_events(events, self.market_selection_mode, now)
        by_asset: dict[str, MarketMeta] = {}
        for event in selected_events:
            if event.get("closed") is True:
                continue
            if event.get("active") is False:
                continue

            event_slug = _string_or_none(event.get("slug"))
            event_title = _string_or_none(event.get("title"))
            markets = event.get("markets")
            if not isinstance(markets, list):
                continue

            for market in markets:
                if not isinstance(market, dict):
                    continue
                market_id = _string_or_none(market.get("id") or market.get("marketId"))
                question = _string_or_none(market.get("question") or event_title)
                asset_ids = _extract_asset_ids(market)
                for asset_id in asset_ids:
                    if not asset_id:
                        continue
                    by_asset[asset_id] = MarketMeta(
                        asset_id=asset_id,
                        market_id=market_id,
                        slug=event_slug,
                        question=question,
                    )

        return by_asset

    async def _fetch_markets_endpoint(self, session: aiohttp.ClientSession) -> list[dict]:
        # Gamma endpoint shape can vary across deployments, so we keep parsing permissive.
        out: list[dict] = []
        limit = 500
        for offset in range(0, 20 * limit, limit):
            url = f"{self.gamma_base_url}/markets"
            params = {"limit": str(limit), "offset": str(offset), "active": "true", "closed": "false"}
            payload = await _safe_get_json(session, url, params=params)
            if not isinstance(payload, list):
                break
            out.extend([x for x in payload if isinstance(x, dict)])
            if len(payload) < limit:
                break
        return out

    def _is_target_market(self, market: dict) -> bool:
        blob = " ".join(
            [
                _string_or_empty(market.get("slug")),
                _string_or_empty(market.get("question")),
                _string_or_empty(market.get("title")),
                _string_or_empty(market.get("description")),
            ]
        ).lower()
        return any(keyword in blob for keyword in self.slug_keywords)


async def _safe_get_json(session: aiohttp.ClientSession, url: str, params: dict[str, str]):
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        return await resp.json()


async def _safe_get_optional_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as resp:
        if resp.status == 404:
            return None
        resp.raise_for_status()
        return await resp.json()



def _string_or_none(value) -> str | None:
    if value is None:
        return None
    return str(value)



def _string_or_empty(value) -> str:
    return "" if value is None else str(value)



def _extract_asset_ids(market: dict) -> list[str]:
    out: list[str] = []

    def push(candidate) -> None:
        if candidate is None:
            return
        text = str(candidate).strip()
        if text:
            out.append(text)

    for key in ("asset_id", "assetId", "token_id", "tokenId", "clobTokenId"):
        if key in market:
            push(market[key])

    list_like_keys = ("asset_ids", "assetIds", "tokenIds", "clobTokenIds")
    for key in list_like_keys:
        value = market.get(key)
        if isinstance(value, list):
            for item in value:
                push(item)
        elif isinstance(value, str):
            parsed = _try_parse_json_list(value)
            if parsed is not None:
                for item in parsed:
                    push(item)

    for key in ("tokens", "outcomes", "outcomeTokens"):
        value = market.get(key)
        if isinstance(value, list):
            for token in value:
                if isinstance(token, dict):
                    for token_key in ("token_id", "tokenId", "asset_id", "assetId", "id", "clobTokenId"):
                        if token_key in token:
                            push(token[token_key])

    # preserve order but deduplicate
    seen = set()
    unique = []
    for item in out:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique



def _try_parse_json_list(raw: str) -> list[str] | None:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, list):
        return None
    out = []
    for item in parsed:
        out.append(str(item))
    return out


def _parse_end_ts(event: dict) -> int | None:
    end_date = event.get("endDate")
    if not isinstance(end_date, str) or not end_date:
        return None
    try:
        dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(dt.timestamp())


def _select_events(events: list[dict], mode: str, now_ts: int) -> list[dict]:
    open_events = []
    for event in events:
        if not isinstance(event, dict):
            continue
        if event.get("closed") is True:
            continue
        if event.get("active") is False:
            continue
        open_events.append(event)

    if mode == "all_open":
        return open_events

    # mode == current_live
    future_or_live = []
    for event in open_events:
        end_ts = _parse_end_ts(event)
        if end_ts is not None and end_ts >= now_ts:
            future_or_live.append((end_ts, event))

    if future_or_live:
        future_or_live.sort(key=lambda item: item[0])
        return [future_or_live[0][1]]

    # Fallback: when endDate cannot be parsed, keep one deterministic event.
    if open_events:
        open_events.sort(key=lambda e: str(e.get("slug") or ""))
        return [open_events[0]]
    return []

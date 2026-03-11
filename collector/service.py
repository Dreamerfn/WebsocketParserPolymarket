from __future__ import annotations

import asyncio
import logging
import signal
import time

from collector.binance import BinanceDepthCollector
from collector.config import AppConfig
from collector.discovery import DiscoveryResult, PolymarketDiscovery
from collector.metrics import density_features, flatten_levels
from collector.parquet_writer import AsyncParquetWriter
from collector.polymarket import PolymarketBookCollector

logger = logging.getLogger(__name__)


class CollectorService:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.stop_event = asyncio.Event()

        self.polymarket = PolymarketBookCollector(
            ws_url=cfg.polymarket.ws_url,
            levels=5,
            ws_max_queue=cfg.polymarket.ws_max_queue,
            ws_max_size_bytes=cfg.polymarket.ws_max_size_bytes,
        )
        self.discovery = PolymarketDiscovery(
            gamma_base_url=cfg.polymarket.gamma_base_url,
            slug_keywords=cfg.polymarket.slug_keywords,
            interval_sec=cfg.polymarket.discovery_interval_sec,
            slug_scan_enabled=cfg.polymarket.slug_scan_enabled,
            slug_scan_prefix=cfg.polymarket.slug_scan_prefix,
            slug_scan_hours_back=cfg.polymarket.slug_scan_hours_back,
            slug_scan_hours_forward=cfg.polymarket.slug_scan_hours_forward,
            slug_scan_step_sec=cfg.polymarket.slug_scan_step_sec,
            slug_scan_concurrency=cfg.polymarket.slug_scan_concurrency,
            market_selection_mode=cfg.polymarket.market_selection_mode,
        )
        self.binance = BinanceDepthCollector(
            symbol=cfg.binance.symbol,
            ws_url=cfg.binance.ws_url,
            rest_base_url=cfg.binance.rest_base_url,
            speed=cfg.binance.diff_stream_speed,
            snapshot_limit=cfg.binance.snapshot_limit,
            ws_max_queue=cfg.binance.ws_max_queue,
            ws_max_size_bytes=cfg.binance.ws_max_size_bytes,
        )
        self.writer = AsyncParquetWriter(
            base_path=cfg.storage.base_path,
            flush_rows=cfg.collector.flush_rows,
            flush_interval_sec=cfg.collector.flush_interval_sec,
            partition_cols=cfg.storage.dataset_partition_cols,
            queue_max_batches=cfg.collector.writer_queue_max_batches,
        )

    async def run(self) -> None:
        self._install_signal_handlers()
        tasks = [
            asyncio.create_task(self.writer.run(), name="parquet_writer"),
            asyncio.create_task(
                self.discovery.run(self.stop_event, self._on_discovery_update),
                name="polymarket_discovery",
            ),
            asyncio.create_task(self.polymarket.run(self.stop_event), name="polymarket_ws"),
            asyncio.create_task(self.binance.run(self.stop_event), name="binance_ws"),
            asyncio.create_task(self._sampling_loop(), name="sampler"),
            asyncio.create_task(self._writer_shutdown_watcher(), name="writer_shutdown"),
        ]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                exc = task.exception()
                if exc is not None:
                    raise exc
            if pending:
                await asyncio.gather(*pending)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:
                pass

    async def _writer_shutdown_watcher(self) -> None:
        await self.stop_event.wait()
        await self.writer.close()

    async def _on_discovery_update(self, result: DiscoveryResult) -> None:
        await self.polymarket.update_assets(result.by_asset)
        logger.info("Discovery updated %d polymarket assets", len(result.by_asset))

    async def _sampling_loop(self) -> None:
        interval = self.cfg.collector.sample_interval_ms / 1000.0
        levels = self.cfg.binance.levels

        next_tick = time.perf_counter()
        while not self.stop_event.is_set():
            capture_ns = time.time_ns()
            rows: list[dict] = []

            bids, asks, exchange_ts_ns = await self.binance.snapshot(levels=levels)
            if bids and asks:
                rows.append(
                    _build_row(
                        ts_capture_ns=capture_ns,
                        ts_exchange_ns=exchange_ts_ns,
                        source="binance",
                        instrument=self.cfg.binance.symbol,
                        market_id=self.cfg.binance.symbol,
                        bids=bids,
                        asks=asks,
                        schema_version=self.cfg.collector.schema_version,
                        extras=None,
                    )
                )

            poly_snaps = await self.polymarket.snapshots()
            for snap in poly_snaps:
                rows.append(
                    _build_row(
                        ts_capture_ns=capture_ns,
                        ts_exchange_ns=snap.exchange_ts_ns,
                        source=snap.source,
                        instrument=snap.instrument,
                        market_id=snap.market_id,
                        bids=snap.bids,
                        asks=snap.asks,
                        schema_version=self.cfg.collector.schema_version,
                        extras=snap.extras,
                    )
                )

            if rows:
                await self.writer.submit_rows(rows)

            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            else:
                # If we lag, realign to current monotonic time.
                next_tick = time.perf_counter()



def _build_row(
    ts_capture_ns: int,
    ts_exchange_ns: int | None,
    source: str,
    instrument: str,
    market_id: str | None,
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    schema_version: int,
    extras: dict | None,
) -> dict:
    row = {
        "ts_capture_ns": ts_capture_ns,
        "ts_exchange_ns": ts_exchange_ns,
        "source": source,
        "instrument": instrument,
        "market_id": market_id,
        "schema_version": schema_version,
    }
    row.update(flatten_levels(bids=bids, asks=asks))
    row.update(density_features(bids=bids, asks=asks))
    row["slug"] = extras.get("slug") if extras else None
    row["question"] = extras.get("question") if extras else None
    return row

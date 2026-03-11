from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class CollectorConfig:
    sample_interval_ms: int = 12
    schema_version: int = 1
    flush_rows: int = 5000
    flush_interval_sec: float = 1.0
    writer_queue_max_batches: int = 5000


@dataclass
class PolymarketConfig:
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    discovery_interval_sec: int = 30
    slug_keywords: list[str] = field(
        default_factory=lambda: ["btc-updown-15m", "btc up/down 15m", "bitcoin up/down 15m"]
    )
    slug_scan_enabled: bool = True
    slug_scan_prefix: str = "btc-updown-15m"
    slug_scan_hours_back: int = 2
    slug_scan_hours_forward: int = 8
    slug_scan_step_sec: int = 900
    slug_scan_concurrency: int = 24
    market_selection_mode: str = "current_live"
    ws_max_queue: int = 2048
    ws_max_size_bytes: int = 2_000_000


@dataclass
class BinanceConfig:
    symbol: str = "BTCUSDT"
    ws_url: str = "wss://fstream.binance.com/ws"
    rest_base_url: str = "https://fapi.binance.com"
    diff_stream_speed: str = "100ms"
    snapshot_limit: int = 1000
    levels: int = 20
    ws_max_queue: int = 2048
    ws_max_size_bytes: int = 2_000_000


@dataclass
class StorageConfig:
    base_path: str = "data/parquet/orderbook_snapshots"
    dataset_partition_cols: list[str] = field(default_factory=lambda: ["dt", "hour", "source"])


@dataclass
class AppConfig:
    collector: CollectorConfig = field(default_factory=CollectorConfig)
    polymarket: PolymarketConfig = field(default_factory=PolymarketConfig)
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)



def _merge_dict(base: dict, patch: dict) -> dict:
    out = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge_dict(out[key], value)
        else:
            out[key] = value
    return out



def load_config(path: str | Path) -> AppConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}

    defaults = {
        "collector": CollectorConfig().__dict__,
        "polymarket": PolymarketConfig().__dict__,
        "binance": BinanceConfig().__dict__,
        "storage": StorageConfig().__dict__,
    }
    cfg = _merge_dict(defaults, raw)

    app_cfg = AppConfig(
        collector=CollectorConfig(**cfg["collector"]),
        polymarket=PolymarketConfig(**cfg["polymarket"]),
        binance=BinanceConfig(**cfg["binance"]),
        storage=StorageConfig(**cfg["storage"]),
    )
    _validate_config(app_cfg)
    return app_cfg


def _validate_config(cfg: AppConfig) -> None:
    if cfg.collector.sample_interval_ms <= 0:
        raise ValueError("collector.sample_interval_ms must be > 0")
    if cfg.collector.flush_rows <= 0:
        raise ValueError("collector.flush_rows must be > 0")
    if cfg.collector.flush_interval_sec <= 0:
        raise ValueError("collector.flush_interval_sec must be > 0")
    if cfg.collector.writer_queue_max_batches <= 0:
        raise ValueError("collector.writer_queue_max_batches must be > 0")

    if not (1 <= cfg.binance.levels <= 20):
        raise ValueError("binance.levels must be in range [1, 20]")
    if cfg.binance.snapshot_limit <= 0:
        raise ValueError("binance.snapshot_limit must be > 0")
    if cfg.binance.ws_max_queue <= 0:
        raise ValueError("binance.ws_max_queue must be > 0")
    if cfg.binance.ws_max_size_bytes <= 0:
        raise ValueError("binance.ws_max_size_bytes must be > 0")

    if cfg.polymarket.discovery_interval_sec <= 0:
        raise ValueError("polymarket.discovery_interval_sec must be > 0")
    if cfg.polymarket.slug_scan_hours_back < 0:
        raise ValueError("polymarket.slug_scan_hours_back must be >= 0")
    if cfg.polymarket.slug_scan_hours_forward < 0:
        raise ValueError("polymarket.slug_scan_hours_forward must be >= 0")
    if cfg.polymarket.slug_scan_step_sec <= 0:
        raise ValueError("polymarket.slug_scan_step_sec must be > 0")
    if cfg.polymarket.slug_scan_concurrency <= 0:
        raise ValueError("polymarket.slug_scan_concurrency must be > 0")
    if cfg.polymarket.market_selection_mode not in {"current_live", "all_open"}:
        raise ValueError("polymarket.market_selection_mode must be one of: current_live, all_open")
    if cfg.polymarket.ws_max_queue <= 0:
        raise ValueError("polymarket.ws_max_queue must be > 0")
    if cfg.polymarket.ws_max_size_bytes <= 0:
        raise ValueError("polymarket.ws_max_size_bytes must be > 0")

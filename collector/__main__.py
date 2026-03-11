from __future__ import annotations

import argparse
import asyncio
import logging

from collector.config import load_config
from collector.service import CollectorService



def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket + Binance orderbook collector")
    parser.add_argument(
        "--config",
        type=str,
        default="config.example.yaml",
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()



def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = load_config(args.config)
    service = CollectorService(cfg)
    asyncio.run(service.run())


if __name__ == "__main__":
    main()

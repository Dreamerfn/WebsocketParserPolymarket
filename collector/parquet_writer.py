from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from collector.metrics import MAX_LEVELS

logger = logging.getLogger(__name__)


class AsyncParquetWriter:
    def __init__(
        self,
        base_path: str,
        flush_rows: int,
        flush_interval_sec: float,
        partition_cols: list[str],
        queue_max_batches: int,
    ) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

        self.flush_rows = flush_rows
        self.flush_interval_sec = flush_interval_sec
        self.partition_cols = partition_cols

        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_batches)
        self._sentinel = object()
        self._schema = _make_schema()
        self._dropped_batches = 0

    async def submit_rows(self, rows: list[dict]) -> None:
        try:
            self._queue.put_nowait(rows)
        except asyncio.QueueFull:
            self._dropped_batches += 1
            # Keep service alive under burst pressure; emit periodic warning.
            if self._dropped_batches == 1 or self._dropped_batches % 100 == 0:
                logger.warning(
                    "Parquet queue full; dropped %d batches (latest batch_size=%d)",
                    self._dropped_batches,
                    len(rows),
                )

    async def close(self) -> None:
        await self._queue.put(self._sentinel)

    async def run(self) -> None:
        buffer: list[dict] = []
        last_flush = time.monotonic()

        while True:
            timeout = max(0.0, self.flush_interval_sec - (time.monotonic() - last_flush))
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                item = None

            if item is self._sentinel:
                if buffer:
                    await asyncio.to_thread(self._flush, list(buffer))
                return

            if item is not None:
                buffer.extend(item)

            if buffer and (
                len(buffer) >= self.flush_rows or (time.monotonic() - last_flush) >= self.flush_interval_sec
            ):
                batch = list(buffer)
                await asyncio.to_thread(self._flush, batch)
                buffer.clear()
                last_flush = time.monotonic()

    def _flush(self, rows: list[dict]) -> None:
        if not rows:
            return

        augmented = []
        for row in rows:
            ts_ns = int(row["ts_capture_ns"])
            dt = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
            copy = dict(row)
            copy["dt"] = dt.strftime("%Y-%m-%d")
            copy["hour"] = dt.strftime("%H")
            augmented.append(copy)

        table = pa.Table.from_pylist(augmented, schema=self._schema)
        template = f"part-{uuid.uuid4().hex}-{{i}}.parquet"
        pq.write_to_dataset(
            table,
            root_path=str(self.base_path),
            partition_cols=self.partition_cols,
            basename_template=template,
        )



def _make_schema() -> pa.Schema:
    fields: list[pa.Field] = [
        pa.field("ts_capture_ns", pa.int64(), nullable=False),
        pa.field("ts_exchange_ns", pa.int64(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("instrument", pa.string(), nullable=False),
        pa.field("market_id", pa.string(), nullable=True),
        pa.field("schema_version", pa.int16(), nullable=False),
    ]

    for i in range(1, MAX_LEVELS + 1):
        fields.append(pa.field(f"bid_px_{i}", pa.float64(), nullable=True))
        fields.append(pa.field(f"bid_sz_{i}", pa.float64(), nullable=True))
        fields.append(pa.field(f"ask_px_{i}", pa.float64(), nullable=True))
        fields.append(pa.field(f"ask_sz_{i}", pa.float64(), nullable=True))

    fields.extend(
        [
            pa.field("qty_sum_bid_5", pa.float64(), nullable=True),
            pa.field("qty_sum_bid_10", pa.float64(), nullable=True),
            pa.field("qty_sum_bid_20", pa.float64(), nullable=True),
            pa.field("qty_sum_ask_5", pa.float64(), nullable=True),
            pa.field("qty_sum_ask_10", pa.float64(), nullable=True),
            pa.field("qty_sum_ask_20", pa.float64(), nullable=True),
            pa.field("notional_sum_bid_20", pa.float64(), nullable=True),
            pa.field("notional_sum_ask_20", pa.float64(), nullable=True),
            pa.field("imbalance_20", pa.float64(), nullable=True),
            pa.field("spread_bps", pa.float64(), nullable=True),
            pa.field("slug", pa.string(), nullable=True),
            pa.field("question", pa.string(), nullable=True),
            pa.field("dt", pa.string(), nullable=False),
            pa.field("hour", pa.string(), nullable=False),
        ]
    )

    return pa.schema(fields)

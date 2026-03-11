# DuckDB: ручной просмотр и поиск паттернов

## Быстрый старт

```sql
CREATE VIEW ob AS
SELECT *
FROM read_parquet('data/parquet/orderbook_snapshots/**/*.parquet');
```

## Смотреть как CSV

Да, можно выгружать любые выборки в CSV:

```sql
COPY (
  SELECT
    to_timestamp(ts_capture_ns / 1e9) AS ts,
    source,
    instrument,
    market_id,
    bid_px_1,
    bid_sz_1,
    ask_px_1,
    ask_sz_1,
    spread_bps,
    imbalance_20
  FROM ob
  ORDER BY ts_capture_ns DESC
  LIMIT 50000
) TO 'manual_view.csv' (HEADER, DELIMITER ',');
```

## Полезные паттерны

1) Экстремальные спреды:
```sql
SELECT
  to_timestamp(ts_capture_ns / 1e9) AS ts,
  source,
  instrument,
  spread_bps,
  bid_px_1,
  ask_px_1
FROM ob
WHERE spread_bps IS NOT NULL
ORDER BY spread_bps DESC
LIMIT 200;
```

2) Экстремальный дисбаланс:
```sql
SELECT
  to_timestamp(ts_capture_ns / 1e9) AS ts,
  source,
  instrument,
  imbalance_20,
  qty_sum_bid_20,
  qty_sum_ask_20
FROM ob
WHERE imbalance_20 IS NOT NULL
ORDER BY abs(imbalance_20) DESC
LIMIT 200;
```

3) Резкие сдвиги top-of-book (дельта между соседними snapshot):
```sql
WITH x AS (
  SELECT
    ts_capture_ns,
    source,
    instrument,
    bid_px_1,
    ask_px_1,
    lag(bid_px_1) OVER (PARTITION BY source, instrument ORDER BY ts_capture_ns) AS prev_bid,
    lag(ask_px_1) OVER (PARTITION BY source, instrument ORDER BY ts_capture_ns) AS prev_ask
  FROM ob
)
SELECT
  to_timestamp(ts_capture_ns / 1e9) AS ts,
  source,
  instrument,
  bid_px_1,
  ask_px_1,
  abs(bid_px_1 - prev_bid) AS bid_jump,
  abs(ask_px_1 - prev_ask) AS ask_jump
FROM x
WHERE prev_bid IS NOT NULL
ORDER BY greatest(abs(bid_px_1 - prev_bid), abs(ask_px_1 - prev_ask)) DESC
LIMIT 200;
```

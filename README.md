# WebSocket Parser: Polymarket + Binance

Асинхронный 24/7 коллектор, который одновременно:
- находит активные BTC 15m рынки на Polymarket,
- собирает их стаканы через WebSocket,
- собирает Binance Futures BTCUSDT стакан (REST snapshot + WS diff),
- делает snapshot каждые `12ms`,
- пишет всё в **один логический Parquet dataset**.

## 1. Что такое "одна Parquet база"

В этом проекте "одна база" = одна директория-датасет, например:

`data/parquet/orderbook_snapshots/`

Внутри будет много файлов `part-*.parquet` (это нормально для 24/7). Все аналитические движки (DuckDB/Polars/Pandas) читают это как единое хранилище.

## 2. Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Если у вас старый `pip` или ограниченная сеть, используйте:

```bash
pip install --no-build-isolation -e .
```

## 3. Запуск

```bash
ws-collector --config config.example.yaml --log-level INFO
```

Остановка: `Ctrl+C` или `SIGTERM`.

## 4. Конфиг

См. [config.example.yaml](/Users/daniileth/Desktop/Vibe-Projects/WebSocketParser/config.example.yaml).

Критичные поля:
- `collector.sample_interval_ms`: период snapshot (по умолчанию `12`).
- `collector.flush_rows`, `collector.flush_interval_sec`: как часто писать Parquet батчи.
- `collector.writer_queue_max_batches`: буфер на запись (анти-DoS, анти-OOM).
- `polymarket.slug_scan_*`: целевой discovery по серии `btc-updown-15m-<timestamp>` (рекомендуется).
- `polymarket.market_selection_mode`:
  - `current_live` — писать только текущий live 15m market и автоматически переключаться на следующий после завершения;
  - `all_open` — писать все открытые рынки серии.
- `polymarket.slug_keywords`: fallback-фильтр через общий `/markets`, если slug-scan выключен.
- `binance.levels`: глубина Binance в финальных snapshot (`1..20`, по умолчанию `20`).
- `*_ws_max_queue`, `*_ws_max_size_bytes`: лимиты входящих WS сообщений.

## 5. Архитектура

- `collector/discovery.py`:
  периодически читает Gamma API и обновляет список `asset_id` для Polymarket.
- `collector/polymarket.py`:
  поддерживает in-memory стаканы Polymarket и публикует top-5 bid/ask.
- `collector/binance.py`:
  держит локальный Binance book из REST snapshot + diff stream.
- `collector/service.py`:
  оркестрация задач + sampler 12ms.
- `collector/parquet_writer.py`:
  буферизация и запись в Parquet dataset.

## 6. Схема данных

Базовые поля:
- `ts_capture_ns`, `ts_exchange_ns`
- `source`, `instrument`, `market_id`, `schema_version`

Уровни стакана:
- `bid_px_1..20`, `bid_sz_1..20`
- `ask_px_1..20`, `ask_sz_1..20`

Агрегаты:
- `qty_sum_bid_5/10/20`, `qty_sum_ask_5/10/20`
- `notional_sum_bid_20`, `notional_sum_ask_20`
- `imbalance_20`, `spread_bps`

Polymarket использует top-5; уровни `6..20` остаются `NULL`.

## 7. DuckDB и ручной анализ

Смотрите [docs/DUCKDB_ANALYSIS.md](/Users/daniileth/Desktop/Vibe-Projects/WebSocketParser/docs/DUCKDB_ANALYSIS.md) и [docs/duckdb_queries.sql](/Users/daniileth/Desktop/Vibe-Projects/WebSocketParser/docs/duckdb_queries.sql).

## 8. Надежность и защита

Реализовано:
- лимиты WS очередей/размера сообщений,
- фильтрация Polymarket апдейтов только по целевым `asset_id`,
- корректная инициализация первой Binance diff-секвенции после snapshot,
- валидация конфигурации,
- drop-policy для переполнения очереди writer с предупреждениями в лог.

Подробности аудита: [docs/AUDIT.md](/Users/daniileth/Desktop/Vibe-Projects/WebSocketParser/docs/AUDIT.md).

## 9. Тесты

```bash
python3 -m unittest discover -s tests -v
```

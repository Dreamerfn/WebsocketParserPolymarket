# Code Audit Report

Проведен полный аудит по приоритетам: Security -> Performance -> Correctness -> Maintainability.

## Critical / High findings (исправлено)

1. Unbounded WebSocket queues (memory DoS risk)
- Где: `collector/polymarket.py`, `collector/binance.py`
- Проблема: `max_queue=None` делал внутренние очереди неограниченными.
- Фикс: добавлены лимиты `ws_max_queue` и `ws_max_size_bytes` в конфиг + применение при `websockets.connect`.

2. Polymarket data poisoning / memory growth from unknown assets
- Где: `collector/polymarket.py`
- Проблема: входящие апдейты для неизвестных `asset_id` могли создавать новые книги.
- Фикс: добавлена строгая фильтрация входящих апдейтов только по `self._target_assets`.

3. Binance first diff event handling after snapshot (correctness risk)
- Где: `collector/binance.py`
- Проблема: первая diff-секвенция после REST snapshot обрабатывалась слишком строго по `pu`, что могло вызывать ложные resync и пропуски.
- Фикс: реализован корректный режим `awaiting_first_event` с проверкой окна `U <= last+1 <= u`, затем обычная проверка последовательности.

4. Writer queue pressure handling
- Где: `collector/parquet_writer.py`
- Проблема: без явной политики переполнения очередь могла приводить к блокировке sampler.
- Фикс: добавлена ограниченная очередь из конфига и drop-policy с предупреждениями в логах.

5. Missing config validation
- Где: `collector/config.py`
- Проблема: некорректные значения могли приводить к неочевидным runtime ошибкам.
- Фикс: добавлена валидация диапазонов и обязательных положительных значений.

## Medium findings (исправлено)

1. Invalid JSON WS frame handling
- Где: `collector/polymarket.py`
- Фикс: обработка `JSONDecodeError` с безопасным skip.

2. Compatibility of Polymarket subscribe payload key
- Где: `collector/polymarket.py`
- Фикс: отправка обоих ключей `assets_ids` и `asset_ids` для устойчивости к вариациям схемы.

## Test coverage additions

- `tests/test_binance_sequence.py`
- `tests/test_polymarket_filter.py`
- `tests/test_config_validation.py`

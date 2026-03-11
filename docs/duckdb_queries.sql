CREATE VIEW ob AS
SELECT *
FROM read_parquet('data/parquet/orderbook_snapshots/**/*.parquet');

SELECT source, count(*)
FROM ob
GROUP BY 1
ORDER BY 2 DESC;

SELECT
  source,
  instrument,
  to_timestamp(ts_capture_ns / 1e9) AS ts,
  bid_px_1,
  ask_px_1,
  spread_bps,
  imbalance_20
FROM ob
ORDER BY ts_capture_ns DESC
LIMIT 100;

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
  LIMIT 200000
) TO 'last_200k_snapshots.csv' (HEADER, DELIMITER ',');

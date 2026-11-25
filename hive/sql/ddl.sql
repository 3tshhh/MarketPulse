-- MarketPulse Hive DDL (step 3). Idempotent: safe to run on every start.
-- All tables are EXTERNAL over Spark-written Parquet: dropping a table never deletes data.
-- All times are UTC. Partitions: dt = 'yyyy-MM-dd' (string), hr = 0..23 (int).
-- Column COMMENTs are deliberate: the chatbot (step 6) reads them from the metastore as schema docs.

CREATE DATABASE IF NOT EXISTS marketpulse
  COMMENT 'MarketPulse crypto market data from Binance spot, all times UTC'
  LOCATION 'hdfs://namenode:8020/marketpulse/warehouse';

USE marketpulse;

-- Spark writes a _spark_metadata folder in each table root; it is not a partition.
SET hive.msck.path.validation=ignore;

CREATE EXTERNAL TABLE IF NOT EXISTS candles_1m (
  window_start      TIMESTAMP COMMENT 'Start of the 1-minute candle, UTC (inclusive)',
  window_end        TIMESTAMP COMMENT 'End of the 1-minute candle, UTC (exclusive)',
  symbol            STRING    COMMENT 'Binance spot pair, e.g. BTCUSDT. Quote currency is USDT',
  open              DOUBLE    COMMENT 'First trade price in the minute (USDT)',
  high              DOUBLE    COMMENT 'Highest trade price in the minute (USDT)',
  low               DOUBLE    COMMENT 'Lowest trade price in the minute (USDT)',
  close             DOUBLE    COMMENT 'Last trade price in the minute (USDT)',
  volume            DOUBLE    COMMENT 'Traded quantity in base asset units (e.g. BTC for BTCUSDT)',
  quote_volume      DOUBLE    COMMENT 'Traded value in USDT = sum(price * qty). Use this to compare activity across symbols',
  vwap              DOUBLE    COMMENT 'Volume-weighted average price = quote_volume / volume',
  taker_buy_volume  DOUBLE    COMMENT 'Base volume where the buyer was the aggressor (market buys)',
  agg_trade_count   BIGINT    COMMENT 'Number of Binance aggregate trades in the minute',
  trade_count       BIGINT    COMMENT 'Number of individual trades in the minute; NULL for early data',
  first_agg_id      BIGINT    COMMENT 'Technical: first aggregate trade id, used for gap detection',
  last_agg_id       BIGINT    COMMENT 'Technical: last aggregate trade id, used for gap detection',
  processed_at      TIMESTAMP COMMENT 'Technical: when Spark wrote the candle'
)
COMMENT '1-minute OHLCV candles per symbol. A minute with no trades has no row'
PARTITIONED BY (
  dt STRING COMMENT 'UTC date yyyy-MM-dd of window_start. Always filter on dt for speed',
  hr INT    COMMENT 'UTC hour 0-23 of window_start'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/marketpulse/candles'
TBLPROPERTIES ('discover.partitions'='true');

CREATE EXTERNAL TABLE IF NOT EXISTS raw_trades (
  symbol           STRING,
  agg_id           BIGINT,
  price            DOUBLE,
  qty              DOUBLE,
  first_trade_id   BIGINT,
  last_trade_id    BIGINT,
  trade_ts         BIGINT    COMMENT 'Exchange trade time, epoch ms',
  event_ts         BIGINT    COMMENT 'Exchange event time, epoch ms',
  is_buyer_maker   BOOLEAN,
  ingest_ts        BIGINT    COMMENT 'Producer receive time, epoch ms',
  kafka_partition  INT,
  kafka_offset     BIGINT,
  kafka_ts         TIMESTAMP,
  event_time       TIMESTAMP COMMENT 'trade_ts as timestamp, UTC',
  is_valid         BOOLEAN   COMMENT 'False = failed validation, see raw_json',
  raw_json         STRING    COMMENT 'Original payload, only kept for invalid rows'
)
COMMENT 'Every trade as received from Kafka. For debugging and data quality, not for the chatbot'
PARTITIONED BY (dt STRING, hr INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/marketpulse/raw'
TBLPROPERTIES ('discover.partitions'='true');

-- Schema changed in z_v2 (scope, market_breadth). IF NOT EXISTS won't alter an existing table: to upgrade,
-- once: DROP VIEW anomaly_events; DROP TABLE anomalies;  (external: HDFS data is kept), then re-run this file.
CREATE EXTERNAL TABLE IF NOT EXISTS anomalies (
  detected_minute    TIMESTAMP COMMENT 'UTC minute (candle start) at which the anomaly was detected',
  symbol             STRING    COMMENT 'Binance spot pair, e.g. BTCUSDT',
  direction          STRING    COMMENT 'up = price jumped, down = price dropped',
  price_change_pct   DOUBLE    COMMENT 'Price change over the last 5 minutes, in percent',
  z_score            DOUBLE    COMMENT 'Price change measured in units of the normal 5-minute volatility of this coin. |z| >= 4 is unusual',
  volume_ratio       DOUBLE    COMMENT 'Trading volume in the last 5 minutes divided by the normal volume of the previous hour. 5 = five times normal',
  quote_volume_5m    DOUBLE    COMMENT 'USDT traded in the 5-minute window',
  price              DOUBLE    COMMENT 'Close price at detection (USDT)',
  price_before       DOUBLE    COMMENT 'Close price 5 minutes earlier (USDT)',
  volatility_1m_pct  DOUBLE    COMMENT 'Normal 1-minute volatility of the coin over the previous hour, percent',
  reason             STRING    COMMENT 'Plain-English explanation of why this was flagged',
  scope              STRING    COMMENT 'coin = this coin moved on its own; market = part of a market-wide move (many coins at once)',
  market_breadth     INT       COMMENT 'Number of coins that triggered in the same 10 minutes. 8 or more = market-wide',
  rule_version       STRING    COMMENT 'Detector version, e.g. z_v2',
  detected_at        TIMESTAMP COMMENT 'Technical: when the detector wrote the row'
)
COMMENT 'Detected market anomalies (unusual price move together with unusual volume). Not trading advice, not proof of manipulation. Use the view anomaly_events'
PARTITIONED BY (
  dt STRING COMMENT 'UTC date yyyy-MM-dd of detected_minute',
  hr INT    COMMENT 'UTC hour 0-23 of detected_minute'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/marketpulse/anomalies'
TBLPROPERTIES ('discover.partitions'='true');

-- The detector writes at-least-once (a replayed Spark batch can repeat an event).
-- This view keeps one row per (symbol, detected_minute). The chatbot queries the view.
-- dt is in the window PARTITION BY so a WHERE dt = ... filter can still prune partitions through the view.
CREATE OR REPLACE VIEW anomaly_events
COMMENT 'One row per detected anomaly (deduplicated). Columns as in table anomalies'
AS SELECT detected_minute, symbol, direction, price_change_pct, z_score, volume_ratio, quote_volume_5m,
          price, price_before, volatility_1m_pct, reason, scope, market_breadth, rule_version, detected_at, dt, hr
   FROM (
     SELECT a.*, row_number() OVER (PARTITION BY dt, symbol, detected_minute ORDER BY detected_at) AS rn
     FROM anomalies a
   ) t
   WHERE rn = 1;

-- Register partitions that already exist on HDFS. New ones are found by the metastore's
-- partition discovery task (every 2 min); MSCK is also the manual fallback.
MSCK REPAIR TABLE candles_1m;
MSCK REPAIR TABLE raw_trades;
MSCK REPAIR TABLE anomalies;

SHOW TABLES;
SHOW PARTITIONS candles_1m;

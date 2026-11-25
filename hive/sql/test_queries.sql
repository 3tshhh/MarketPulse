-- Step 3 acceptance + latency benchmark. Beeline prints "(N seconds)" after every query.
-- Run:
--   docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/marketpulse -n analyst -f /opt/sql/test_queries.sql
-- Queries Q2-Q7 are shaped like questions the chatbot will get.

USE marketpulse;

-- Q1 partitions registered
SHOW PARTITIONS candles_1m;

-- Q2 freshness + time zone sanity. minutes_behind should be about 3-5.
--     If it is about +-120 or +-60, timestamps are shifted by a time-zone conversion.
SELECT count(*)                 AS candles_today,
       count(DISTINCT symbol)   AS symbols,
       min(window_start)        AS first_minute,
       max(window_start)        AS last_minute,
       round((unix_timestamp(current_timestamp()) - unix_timestamp(max(window_start))) / 60, 1) AS minutes_behind
FROM candles_1m
WHERE dt = cast(current_date() AS STRING);

-- Q3 "What is the latest BTC price?"
SELECT window_start, close
FROM candles_1m
WHERE dt = cast(current_date() AS STRING) AND symbol = 'BTCUSDT'
ORDER BY window_start DESC
LIMIT 1;

-- Q4 "Which coins traded the most today (in USDT)?"
SELECT symbol, round(sum(quote_volume), 0) AS usdt_volume, sum(trade_count) AS trades
FROM candles_1m
WHERE dt = cast(current_date() AS STRING)
GROUP BY symbol
ORDER BY usdt_volume DESC
LIMIT 10;

-- Q5 "Biggest 1-minute price moves today"
SELECT symbol, window_start, open, close,
       round(100 * (close - open) / open, 3) AS pct_change
FROM candles_1m
WHERE dt = cast(current_date() AS STRING)
ORDER BY abs(close - open) / open DESC
LIMIT 10;

-- Q6 "Unusual volume today" (minute volume vs that symbol's daily average; window function)
SELECT symbol, window_start, round(quote_volume, 0) AS usdt_volume, round(ratio, 1) AS x_avg
FROM (
  SELECT symbol, window_start, quote_volume,
         quote_volume / avg(quote_volume) OVER (PARTITION BY symbol) AS ratio
  FROM candles_1m
  WHERE dt = cast(current_date() AS STRING)
) t
ORDER BY ratio DESC
LIMIT 10;

-- Q7 "BTC per hour today"
SELECT hr, min(low) AS low, max(high) AS high, round(sum(quote_volume), 0) AS usdt_volume, count(*) AS minutes
FROM candles_1m
WHERE dt = cast(current_date() AS STRING) AND symbol = 'BTCUSDT'
GROUP BY hr
ORDER BY hr;

-- Q8 worst case: no partition filter, whole table
SELECT count(*) AS all_candles, count(DISTINCT dt) AS days FROM candles_1m;

-- Q9 raw layer readable + data quality
SELECT is_valid, count(*) AS n
FROM raw_trades
WHERE dt = cast(current_date() AS STRING)
GROUP BY is_valid;

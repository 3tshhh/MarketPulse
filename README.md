# MarketPulse

Real-time crypto market anomaly monitor. Kafka → Spark Structured Streaming → HDFS/Hive → Streamlit + AI analyst.

## Before anything: preflight

```powershell
powershell -ExecutionPolicy Bypass -File scripts\preflight.ps1
```
Checks the Windows clock, `.wslconfig`, Docker memory and the container clock against Binance. Run it as Administrator with `-Fix` to resync the Windows clock, and with `-Fix -RestartWsl` if the container clock is off (common after sleep). Run it before every demo or benchmark.

## Machine profiles
| Machine | `.wslconfig` | Compose |
|---|---|---|
| 24 GB (baseline, must always work) | `docs\wslconfig.example` (14 GB) | `docker-compose.yml` only |
| 32 GB | `docs\wslconfig.32gb.example` (22 GB) | `copy compose.large.yml docker-compose.override.yml` |

Moving machines: clone the repo, set `.wslconfig`, `wsl --shutdown`, run preflight, `docker compose up -d --build`. Start with empty volumes; don't copy HDFS without Kafka (Spark checkpoints hold Kafka offsets).

## Step 1 — Producer → Kafka

```bash
docker compose up -d --build
docker compose logs -f producer          # expect "connected: 40 streams" and a stats line every 30s
docker compose run --rm producer python verify_consumer.py 60
```

Peek at raw messages:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trades --property print.key=true --max-messages 5
```

Done when the verifier prints `PASS`, it sees about 40 symbols, there are no agg_id gaps, and p95 latency is under 1 s.

Survival tests:
- `docker compose restart kafka`: the producer must recover by itself. Check the logs for `failed=0` after it reconnects.
- Turn Wi-Fi off for 30 s: the WebSocket must reconnect with backoff. Expect gaps during the outage. That's real data loss, so write it down for later.

## Step 2 — Spark candles → HDFS

```bash
docker compose up -d --build          # first build downloads Spark image + Kafka jars
docker compose ps                     # hdfs-init and kafka-init should be "Exited (0)"
docker compose logs -f spark-candles  # one [raw] and one [candles] line per minute
```

UIs: HDFS http://localhost:9870 · Spark master http://localhost:8080 · candles job http://localhost:4040

Look at the files:
```bash
docker exec namenode hdfs dfs -ls -R /marketpulse/candles | head
docker exec namenode hdfs dfs -du -h /marketpulse
```

Acceptance check (after the job has run 20+ minutes):
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g /opt/jobs/verify_candles.py --minutes 15
```
Done when it prints `PASS`: 0 duplicate candles, 0 stream/batch mismatches, OHLC and volume match Binance klines on at least 99% of minutes.

Reset step 2 completely (deletes all HDFS data and checkpoints):
```bash
docker compose stop spark-candles
docker exec namenode hdfs dfs -rm -r -skipTrash /marketpulse/candles /marketpulse/raw /marketpulse/_checkpoints
docker compose up -d hdfs-init spark-candles
```

## Step 3 — Hive tables

```bash
docker compose up -d --build                  # adds postgres, hive-metastore, hiveserver2, hive-init
docker compose ps                             # hive-init "Exited (0)"; metastore + hiveserver2 healthy
docker compose logs hive-init                 # DDL output, ends with SHOW PARTITIONS
docker stats --no-stream                      # real memory per container
```

Interactive SQL:
```bash
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000/marketpulse -n analyst
```

Acceptance + latency benchmark:
```bash
docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/marketpulse -n analyst -f /opt/sql/test_queries.sql
```
Done when every query runs, Q2 `minutes_behind` is about 3–5 (no time-zone shift), and a new `hr=` partition shows up within about 2 min of a new UTC hour without running MSCK.

Manual partition fallback:
```bash
docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/marketpulse -n admin -e "MSCK REPAIR TABLE candles_1m; MSCK REPAIR TABLE raw_trades;"
```

## Step 4 — Anomaly detection

Live: the streaming app runs a third query that scores every new candle with `spark/jobs/anomaly_rules.py` and writes events to HDFS `anomalies/` (Hive view `anomaly_events`) and Kafka `anomalies`.
```bash
docker compose restart spark-candles               # job code is mounted; restart picks up the anomaly query
docker compose run --rm hive-init                 # creates table anomalies + view anomaly_events
docker compose logs -f spark-candles | Select-String "anomalies|ANOMALY"
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic anomalies --from-beginning --max-messages 5
```

Backtest + evaluation: see `docs/ANOMALY_EVALUATION.md`.

### Smoke test: live anomaly write path (HDFS + Kafka + Hive)
Real anomalies are rare, so force events with very low thresholds into a **separate** path/topic/checkpoint.
The real `anomalies` table, topic and checkpoint aren't touched; when you switch back, the real query catches up on the minutes it missed.

1. Create the smoke topic (auto-create is off):
   `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic anomalies_smoke --partitions 1 --replication-factor 1`
2. Add to `.env` (and remove any existing `ANOMALIES_TOPIC=` line):
   ```
   AD_VOL_RATIO=1.5
   AD_Z_RET=1.5
   AD_MIN_ABS_RET=0.0005
   AD_MIN_QV5_USDT=0
   ANOMALIES_PATH=hdfs://namenode:8020/marketpulse/_smoke/anomalies
   ANOMALIES_CKPT=hdfs://namenode:8020/marketpulse/_smoke/ckpt
   ANOMALIES_TOPIC=anomalies_smoke
   ```
3. `docker compose up -d spark-candles`, wait 3 min, then check that `[ANOMALY]` lines appear in `docker compose logs spark-candles`.
4. Check all three sinks:
   ```
   docker exec namenode hdfs dfs -ls -R /marketpulse/_smoke/anomalies | head
   docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic anomalies_smoke --from-beginning --max-messages 3
   docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/marketpulse -n admin -e "CREATE EXTERNAL TABLE anomalies_smoke LIKE anomalies LOCATION 'hdfs://namenode:8020/marketpulse/_smoke/anomalies'; MSCK REPAIR TABLE anomalies_smoke; SELECT detected_minute, symbol, scope, reason FROM anomalies_smoke LIMIT 5;"
   ```
5. Clean up: delete the 7 lines from `.env` (put `ANOMALIES_TOPIC=anomalies` back if you had it), then:
   ```
   docker compose up -d spark-candles
   docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/marketpulse -n admin -e "DROP TABLE anomalies_smoke;"
   docker exec namenode hdfs dfs -rm -r -skipTrash /marketpulse/_smoke
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic anomalies_smoke
   ```
   Check the logs show `'rule': 'z_v2', 'vol_ratio': 8.0 ... output: hdfs://namenode:8020/marketpulse/anomalies`.

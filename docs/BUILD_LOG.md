# MarketPulse — Build Log

Running record of every step, decision, problem and result. Newest entries go at the bottom of each section.
Use this as the source for the final report's "Implementation" and "Challenges" chapters.

---

## 0. Design review (2026-11-23)

Reviewed the initial architecture before writing code. Issues raised:

| # | Issue | Impact | Resolution / when |
|---|---|---|---|
| 1 | Anomaly rule mixes fixed thresholds (5x vol, 4%) with "z-score" | A fixed 4%/5min almost never fires on BTC/ETH and fires constantly on small caps | Use per-symbol z-score; keep fixed rule as baseline. Step 4 |
| 2 | Row-based rolling windows (`lag`, last-N rows) not supported in Structured Streaming | Rolling volatility/z-score can't be a plain window function | `applyInPandasWithState` or `foreachBatch` + stored stats. Step 4 |
| 3 | No ground truth for precision | Can't report precision honestly | Backfill klines from data.binance.vision, hand-label 20–30 events. Step 4 |
| 4 | Hive-on-MR latency | Chatbot unusable | Benchmark Hive-on-Tez vs Spark Thrift Server on same metastore. Step 3 |
| 5 | Streaming writes create new partitions Hive doesn't see | Chatbot reads stale data | MSCK REPAIR / ADD PARTITION job. Step 3 |
| 6 | ~10–12 containers on one laptop | OOM, instability | Need ≥16 GB RAM; cap WSL2 memory in `.wslconfig` |
| 7 | `@trade` vs `@aggTrade` vs `@kline` | Volume / loses the Spark candle-building part | Use `@aggTrade` |

---

## 1. Step 1 — Producer → Kafka

### 1.1 What was built
```
docker-compose.yml          kafka (KRaft), kafka-init, producer
env.example                 optional overrides (copy to .env)
producer/producer.py        Binance @aggTrade WS -> Kafka `trades`
producer/verify_consumer.py acceptance test
producer/symbols.txt        40 symbols (large + small caps)
```

### 1.2 Design decisions
- **Kafka KRaft mode, no ZooKeeper.** One fewer container. Single node, RF=1.
- **Auto topic creation off**, topics created by `kafka-init`. A typo fails loudly instead of creating a ghost topic.
- **Topics:** `trades` 3 partitions, key=symbol, 48h retention (HDFS `raw/` is the long-term copy). `anomalies` 1 partition, 7d.
- **Key = symbol.** All trades of one symbol land in one partition, so they stay in order for that symbol.
- **Message schema:** `symbol, agg_id, price, qty, trade_ts, event_ts, is_buyer_maker, ingest_ts`.
  - `trade_ts`: exchange time, used as Spark event time.
  - `agg_id`: contiguous per symbol, so a gap means lost data.
  - `ingest_ts`: producer receive time, used for latency checks.
- **Producer:** idempotent (`enable.idempotence`, `acks=all`), lz4, `linger.ms=50`; exponential backoff reconnect; proactive reconnect at 23h (Binance closes at 24h); validates symbols against REST at startup, because Binance silently sends nothing for bad names.
- **Endpoint:** `data-stream.binance.vision` (market-data-only), overridable in `.env`.
- **`kafka` runs as root** so the broker can write to the fresh named volume. Acceptable for dev only.

### 1.3 Teammate repo review (`ziadmohamed354/stream_data`)
Prototype of step 1 only: one symbol, JSONL file, no Kafka. Not merged. Findings:
- 1,435 of 10,970 raw rows (13%) had no `trade_id` because the schema changed mid-collection; `dropna` silently deleted them.
- One undetected 4.4-min outage (7,571 missing trade ids).
- `trade_id` became float; hardcoded record count; relative-path mismatch; data committed to git.
- **Kept:** the cleaning rules (dedupe id, price>0, qty>0) move to Spark (step 2) and Airflow data-quality checks (step 7). The 39 min of BTC data can be replayed as a test input for step 2.

### 1.4 Problem: `kafka-init` exit 1
- **Symptom:** `service "kafka-init" didn't complete successfully: exit 1`; compose warned `TRADES_TOPIC variable is not set`.
- **Root cause:** `.env` couldn't be written by the tooling, and the follow-up compose file with `${VAR:-default}` fallbacks didn't land on disk the first time. Topic name was empty, so `kafka-topics.sh --topic ""` failed.
- **Fix:** rewrote compose with defaults (`${TRADES_TOPIC:-trades}` etc.), verified file size on disk, `copy env.example .env`.
- **Lesson:** don't rely on `.env` existing; give every compose variable a default.

### 1.5 Results (2026-11-23 20:10)
`kafka-init`: `trades` (3 partitions, retention 172800000) and `anomalies` (1 partition, retention 604800000) created.

Producer stats (30s windows):
```
rate=38.5 msg/s  delivered=1163/1164 failed=0 symbols_active=37 lat_p50=5456ms lat_p95=8624ms
rate=31.2 msg/s  delivered=951/951   failed=0 symbols_active=36 lat_p50=2868ms lat_p95=4096ms
```
Startup warning `Failed to acquire idempotence PID ... Coordinator load in progress: retrying` is harmless: the broker was still loading internal topics, and the client retried.

Verifier (60 s):
| Check | Result |
|---|---|
| Messages | 2,614 (43.6/s) |
| Symbols seen | 38 / 40 |
| Schema errors | 0 |
| Key mismatches | 0 |
| Multi-partition symbols | none |
| agg_id gaps | 0 |
| Out of order | 0 |
| Latency | p50 3,094 ms · p95 7,910 ms · max 10,439 ms |
| Partition spread | 10 / 14 / 14 symbols |
| **Verdict** | **PASS** (correctness) |

### 1.6 Open items from step 1
- **Latency 3–8 s, target < 1 s.** The delay already shows in the producer (`ingest_ts - trade_ts`), so it's before Kafka. Suspect clock skew in the Docker Desktop/WSL2 VM. Check against Binance server time. Doesn't break correctness: Spark windows and watermarks use exchange `trade_ts`, not wall clock. It does distort latency numbers in the report.
- **Quiet symbols:** 2–4 of 40 send no trades in a 30–60 s window. Step 2 must decide: a missing 1-min candle means no row (not zero), or forward-fill.
- **Low throughput (~40 msg/s):** fine for the laptop; not "big data". Report it honestly. The raw/ history in HDFS grows over days.
- **Pending:** Kafka restart recovery test.

### 1.7 Latency investigation (2026-11-23 20:16)
- Measured container clock against Binance `/api/v3/time`: **skew = +2,235 ms**. The Docker Desktop/WSL2 VM clock runs ahead, a known drift after Windows sleep/hibernate.
- That explains most of the p50 (3,094 ms → about 860 ms real). The p95 (7.9 s) is still too high after the correction; recheck after fixing the clock.
- Fix: `wsl --shutdown`, restart Docker Desktop, re-measure skew, re-run the verifier.
- Report note: always report latency together with the measured clock skew, or measure on a synced clock.

### 1.7b Kafka restart recovery test (2026-11-23 20:17)
`docker compose restart kafka` while the producer was running:
```
FAIL ... Connect to 172.18.0.2:9092 failed: Connection refused        <- broker down
rate=37.4 msg/s sent=1122 delivered=934  failed=0  lat_p50=2248 p95=2308
METADATAUPDATE ... new leader-1 not found in cache                     <- broker back, metadata refresh
rate=50.2 msg/s sent=1511 delivered=1699 failed=0  lat_p50=2256 p95=2307
```
- During the outage, 188 messages stayed in the producer's local buffer (sent 1122, delivered 934).
- After recovery the buffer drained: delivered 1699 > sent 1511. Over both windows 934 + 1699 = 2633 = 1122 + 1511, so **0 messages lost, failed=0**, with no manual action.
- The WebSocket kept running the whole time; only the Kafka side paused.
- Latency is now flat at p50 2248 / p95 2307 ms, which is the 2.2 s clock skew. **Real end-to-end latency is about 20–70 ms.** The earlier 8 s p95 came from the initial burst right after connecting, not steady state.
- **Result: PASS.**

### 1.7c Re-run after `wsl --shutdown` (2026-11-23 20:24)
- Skew still **+2,274 ms** after restarting WSL, so it isn't WSL drift. WSL2 syncs its clock from Windows, so the **Windows host clock** is most likely ahead. Next: check with `w32tm /stripchart`, fix with `w32tm /resync` or Settings → Time → Sync now.
- Verifier: 3,563 msgs (59.4/s), 39/40 symbols, 0 schema errors, 0 gaps, 0 out of order, latency p50 2,288 / p95 2,381 / max 2,452 ms. Minus skew, that's **about 15–180 ms real**. PASS.
- Kafka restart #2: producer saw `Connection refused`, then `Failed to resolve 'kafka:9092'` (the container's DNS entry is gone while it restarts; librdkafka labels it "Permanent" but keeps retrying). Totals: sent 845 + 1,169 = 2,014, delivered 839 + 1,175 = 2,014, so **0 lost**. PASS.

- **Confirmed (20:26):** `w32tm /stripchart` against time.windows.com shows offsets of −2.10 to −2.22 s, so the Windows clock is about 2.1 s ahead. Same size as the container skew, so the cause is the host clock.
- `w32tm /resync /force` failed with "no time data was available": the Windows Time service had no time source set up. Fix: configure one (`/manualpeerlist`), then resync.

- **Fixed (20:28):** configured the w32time time source, resynced, then restarted WSL. Container skew is now **170 ms**. This method reads local time after the HTTP response arrives, so it includes roughly half to all of the network round trip to Binance (about 150–300 ms from Cairo). The real clock offset is close to 0.
- **Method note for the report:** skew is measured as local time minus Binance `serverTime`, with an error of about ±RTT. Re-check after Windows sleep/hibernate and before every demo or benchmark.

### Step 1 status: DONE
Remaining housekeeping (not blocking): fix the WSL2 clock so reported latency is honest.

### 1.8 Machine budget
RAM: **24 GB**. Measured Windows usage: 7–11 GB on average.
- First plan was `memory=16GB`, which leaves only 8 GB for Windows: too tight at the 11 GB peaks, and the host would swap.
- **Revised (2026-11-24):** `memory=14GB`, `swap=6GB`, `autoMemoryReclaim=gradual`. Windows keeps 10 GB.
- 2026-11-23: `docker info` showed MemTotal 12,387,708,928 (WSL default = 50% of RAM), so `.wslconfig` wasn't applied yet.
- Stack budget below must fit in about 12 GB, leaving 2 GB of headroom inside WSL. Close heavy apps (browser tabs, IDE indexers) during demos.

Planned memory budget (single node):
| Component | Approx RAM |
|---|---|
| Kafka | 1 GB |
| HDFS NameNode + DataNode | 1.5 GB |
| Spark (master + 1 worker capped at 3 GB, streaming job) | 3.5–4 GB |
| Hive metastore + HiveServer2 + Postgres | 2.5 GB |
| Airflow (webserver, scheduler, Postgres) | 2 GB |
| Streamlit + chatbot | 0.5 GB |
| **Total** | **about 11–12 GB**, under the 14 GB cap |

---

## 2. Step 2 — Spark Structured Streaming: candles → HDFS

### 2.1 What was built (2026-11-24)
```
hadoop.env                    HDFS config (replication 1, permissions off for dev)
spark/Dockerfile              apache/spark 3.5.3 + Kafka connector jars baked in
spark/spark-defaults.conf     fs.defaultFS, UTC session time zone
spark/jobs/candles_stream.py  streaming job: raw/ + candles/
spark/jobs/verify_candles.py  acceptance check
docker-compose.yml            + namenode, datanode, hdfs-init, spark-master, spark-worker, spark-candles
producer/producer.py          + first_trade_id, last_trade_id
```

### 2.2 Design decisions
| Decision | Why |
|---|---|
| Spark 3.5.3 (not 4.x) | More docs and examples; safe Hive metastore compatibility for step 3 |
| Standalone master + 1 worker (not local mode) | Airflow (step 7) and Spark Thrift Server (step 3) need a real master; Spark UI for the demo |
| Kafka jars baked into the image | `--packages` downloads them on every start and breaks offline |
| Two queries in one app: `raw` and `candles` | `raw` is the replayable source of truth; `candles` is derived and can be rebuilt from `raw` |
| `raw` keeps **all** rows, with an `is_valid` flag and `raw_json` for bad rows | Lesson from the teammate repo: never drop data silently |
| Event time = exchange `trade_ts`; watermark 2 min | Candles must not depend on our clock (see the 2.1 s skew in step 1) |
| `dropDuplicatesWithinWatermark(symbol, agg_id)` | Removes redeliveries with bounded state (Spark 3.5 feature) |
| open/close = `min_by`/`max_by(price, agg_id)` | `first()`/`last()` depend on arrival order and are wrong in streaming |
| Append mode | File sink only supports append; each candle is written once, when final |
| `shuffle.partitions = 3` | Default 200 would write about 200 tiny files per trigger |
| Trigger 1 min | Fewer, larger files; candle latency 2–4 min after the minute closes |
| RocksDB state store | State off the JVM heap; safer with a 2 GB executor |
| `failOnDataLoss=false` | If the job is off > 48h (Kafka retention), keep running; the gap is visible in verify |
| No row for empty minutes | A filled row would hide inactivity; step 4 handles gaps explicitly |
| Partition by `dt` (UTC date) / `hr` (int) | Matches the Hive partition plan; pruning for the chatbot |
| `dfs.permissions.enabled=false` | DEV ONLY: Spark, Hive and Airflow run as different users |
| Producer adds `first_trade_id`/`last_trade_id` | Lets us compare trade counts with Binance klines exactly |

Candle schema: `window_start, window_end, symbol, open, high, low, close, volume, quote_volume, vwap, taker_buy_volume, agg_trade_count, trade_count, first_agg_id, last_agg_id, processed_at, dt, hr`.

Memory limits: namenode 1g, datanode 1g, spark-master 2g, spark-worker 3.5g, spark-candles 1.5g, kafka 1g, producer 256m, about 10.3 GB max.

### 2.3 Known limits (for the report)
- **Candle latency is 2–4 min**, the cost of watermark + append mode. The live dashboard (step 5) needs another path for the current minute.
- Trades arriving more than 2 min late are dropped from candles but kept in `raw/`; `verify_candles.py` measures how many.
- Messages produced before the producer change have no trade-id range, so `trade_count` is null for those minutes.
- Changing `shuffle.partitions` or the aggregation means deleting the candles checkpoint and output.

### 2.4 Results

**First start (2026-11-24 21:27 local / 19:27 UTC): all containers up on the first try.**
- Build: hadoop image pull 402 s, Spark image + jars build 234 s. All 4 Maven jar URLs were valid.
- `hdfs-init` and `kafka-init` exited cleanly; the NameNode formatted itself on first run.
- The driver connected to the master; one executor was granted on the worker (2 cores, 2.0 GiB).
- Both queries started.

First micro-batch (catch-up from `startingOffsets=earliest`):
```
[raw]     batch=0 rows=124581 proc=13833/s dur=9004ms  state_rows=0
[candles] batch=0 rows=124582 proc=8765/s  dur=14214ms state_rows=126874 watermark=1970-01-01
```
- 124,581 rows is the whole Kafka backlog. That's under `maxOffsetsPerTrigger` (200k), so it was one batch.
- The watermark is 1970 in batch 0 by design: Spark computes the watermark at the end of a batch, so nothing could be evicted yet. All 126,874 dedupe keys + window groups sit in state until batch 1.
- Raw and candles row counts differ by 1 because they are independent queries, each with its own Kafka offset snapshot.
- Harmless warnings: `spark.sql.adaptive.enabled is not supported in streaming` (AQE is batch-only) and `AdminClientConfig ... not used` (the Kafka source passes consumer configs to its admin client).

Expected next: batch 1 moves the watermark to about now − 2 min, state drops to a few thousand rows, and all backlog candles are written.

**Steady state, batches 1–9 (19:27–19:36 UTC): confirmed.**
| batch | rows | candles dur | state_rows | watermark (UTC) |
|---|---|---|---|---|
| 1 | 688 | 4.4 s | 5,087 | 19:25:33 |
| 3 | 1,136 | 4.0 s | 5,720 | 19:26:59 |
| 5 | 2,088 | 4.2 s | 7,322 | 19:28:59 |
| 7 | 1,550 | 4.2 s | 8,445 | 19:30:59 |
| 9 | 1,917 | 3.3 s | 8,577 | 19:32:59 |
- **Watermark** advances exactly 1 min per trigger and trails wall clock by about 3 min (2 min watermark + 1 min trigger). Late-data handling works as designed.
- **State is bounded.** It dropped from 126,874 to 5,087 after batch 0, then levelled at about 8.5k. That fits the theory: dedupe keys live for about 4 min × 20–35 trades/s ≈ 5–8k, plus about 40 symbols × 3–4 open windows. Memory won't grow over time.
- **Load:** candles takes 3–5 s per 60 s trigger (about 7% busy) and raw about 1–2.7 s. Plenty of headroom for 40 symbols.
- Input rate is 19–35 msg/s, lower than step 1's 40–60/s because the market is quieter in the evening. Throughput depends on market activity.

### 2.5 Problem: HDFS files written with replication 3
- **Symptom:** `hdfs dfs -du -h /marketpulse` shows the second column (space consumed) at about 3× the first (`candles 338.0 K / 984.0 K`, `raw 4.8 M / 14.5 M`).
- **Root cause:** `dfs.replication` is a **client-side** setting, applied by whoever writes the file. `hadoop.env` set it to 1 for the HDFS containers, but Spark has its own Hadoop config, so it used the default of 3. With only one DataNode, every block is under-replicated: the NameNode keeps trying to re-replicate and reports under-replicated blocks. No data is lost (one real copy exists), but the cluster health is wrong.
- **Fix:** add `spark.hadoop.dfs.replication 1` to `spark/spark-defaults.conf`, rebuild the Spark image, and restart the Spark containers (the streaming job resumes from its checkpoint). Fix existing files with `hdfs dfs -setrep -R 1 /marketpulse`.
- **Lesson:** any client that writes to HDFS (Hive, Airflow later) needs `dfs.replication=1` in its own config.

### 2.6 Observation: partitions present
`candles/dt=2026-11-24/` has `hr=2` and `hr=19` only. The Kafka backlog (124k rows) covered the period until about 02:xx UTC, when the producer stopped (laptop off/asleep), and live data resumed at 19:27 UTC. The gap of hours 3–18 is real missing data, not a pipeline bug.

**Replication fix verified (21:40 local):** after the rebuild and `setrep -R 1`, both `-du` columns are equal (candles 355.3 K / 355.3 K, raw 4.9 M / 4.9 M), and `fsck`: `Status: HEALTHY`, `Under-replicated blocks: 0`. The streaming job resumed from its checkpoint after the restart.
Partitions present: `dt=2026-11-23/hr=18`, `dt=2026-11-24/hr=2`, `dt=2026-11-24/hr=19`. These are the producer's run windows; the hours between are real gaps.

### 2.7 First verify_candles run (19:21–19:36 UTC, 15 minutes)
```
stream candles: 555   symbols: 39   invalid raw rows: 0
1. stream vs batch:  duplicates 0 | only stream 0 | only batch 0 | mismatches 0 | late trades 0
2. vs Binance (6 symbols, 90 minutes):
   OHLC exact 88/90 (97.8%) | volume 84/90 (93.3%) | trade count 48/54 | missing 0
3. files today: raw 45 files avg 48.6 KB | candles 39 files avg 6.8 KB
-> CHECK ABOVE
```

**Analysis:**
- **Stream vs batch is perfect.** The streaming logic (dedupe, watermark, min_by/max_by) gives exactly the same result as a batch recompute from raw/, with 0 late trades.
- **Every Binance mismatch is in one minute: 19:27 UTC (ts 1795548420000)**, for all 6 symbols. That's the minute `docker compose up` **recreated the producer container** for the new image (first/last trade ids). Trades during the few seconds of the swap were never received: our volume is 46–93% of Binance's, and DOGE's low was missed. **Data loss at the source, not a pipeline bug.**
- **Trade count 48/54:** 36 of 90 minutes (19:21–19:26) came from the old producer without trade-id fields, so `trade_count` is null and was skipped. The 6 mismatches are the same 19:27 minute.
- **Lesson:** a producer redeploy loses data unless something backfills the gap (Binance REST `aggTrades?fromId=` could do it). Out of scope for now; the gap is detected and reported.

### 2.8 Change: gap detection with agg_id continuity
Binance aggregate trade ids are contiguous per symbol, so a candle can be checked for completeness without any external source:
- internal: `last_agg_id − first_agg_id + 1 == agg_trade_count`
- boundary: `first_agg_id == previous candle's last_agg_id + 1`, and same for the next candle
`verify_candles.py` now flags incomplete minutes, reports them, and excludes them from the Binance comparison. PASS also requires all trade counts to match on complete minutes.
This will be reused: step 4 must not flag anomalies on incomplete minutes (a partial minute looks like a volume drop), and step 7's data-quality DAG reports the incomplete-minute rate.

**Small files:** candles average 6.8 KB per file (3 files per minute per hour partition, about 4,300 files/day). Parquet/HDFS want 64–256 MB. Compaction in step 7 is required, not optional.

### 2.9 verify_candles re-run with gap detection (19:23–19:38 UTC): PASS
```
stream candles: 553   symbols: 39   invalid raw rows: 0
incomplete minutes (agg_id gap): 55 (9.95%)   19:25=2  19:26=18  19:27=32  19:28=3
1. stream vs batch:  duplicates 0 | only stream 0 | only batch 0 | mismatches 0 | late 0
2. vs Binance (6 symbols): OHLC 83/83 (100%) | volume 83/83 (100%) | trade count 60/60 | missing 0
   skipped incomplete: 7 (OHLC still matched on 5)
3. files today: raw 54 avg 46.4 KB | candles 45 avg 6.7 KB
PASS
```
- **Every complete minute matches Binance's official klines exactly:** OHLC, volume and trade count, 100%.
- **The incomplete minutes cluster around 19:27**, the producer redeploy. 32/39 symbols lost trades inside 19:27. 19:26 (18) and 19:25/19:28 (2/3, quiet symbols) are flagged by the **boundary rule**: when ids jump between two candles we can't tell which minute the missing trades belonged to, so both neighbours are marked. That over-flags on purpose; for anomaly detection a false "incomplete" is cheaper than a partial candle treated as real.
- The 9.95% rate is from this window only, which contains the redeploy. Steady state should be about 0%; the step 7 data-quality DAG will track it.

### Step 2 status: DONE (2026-11-24 21:43)
Pipeline: Kafka → Spark Structured Streaming → HDFS Parquet (`raw/`, `candles/`), exact against Binance, bounded state, about 7% CPU per trigger, recovers from restart via checkpoints.

Carried forward:
- Step 4: ignore incomplete minutes (agg_id continuity) when scoring anomalies.
- Step 5: candles land 2–4 min after the minute closes; the live view needs another path for the current minute.
- Step 7: compaction (6.7 KB avg candle files), incomplete-minute rate in the DQ DAG.
- Every HDFS writer must set `dfs.replication=1`.

---

## 3. Step 3 — Hive tables over HDFS

### 3.1 What was built (2026-11-24)
```
hive/Dockerfile           apache/hive:4.0.1 + Postgres JDBC driver + hdfs-site.xml (replication 1)
hive/conf/hdfs-site.xml
hive/sql/ddl.sql          database + external tables + MSCK (idempotent)
hive/sql/test_queries.sql 9 acceptance/latency queries shaped like chatbot questions
docker-compose.yml        + postgres, hive-metastore, hiveserver2, hive-init
```

### 3.2 Design decisions
| Decision | Why |
|---|---|
| Hive **4.0.1** with Tez in local mode | Faster than MapReduce for small interactive queries (the chatbot latency risk). Spark 3.5 isn't fully metastore-compatible with Hive 4, which is irrelevant while Spark writes by path |
| Standalone metastore + Postgres | Derby allows one connection only; HS2, partition discovery and later the chatbot all need the metastore. Postgres will also host Airflow's DB (step 7) |
| **EXTERNAL** tables, `LOCATION` = Spark output | Zero copy; `DROP TABLE` never deletes data; Spark stays the only writer |
| Database `LOCATION` on HDFS (`/marketpulse/warehouse`) | Any managed/gold table created later lands on HDFS, not inside a container |
| **Column COMMENTs** on every column | The step 6 chatbot will read these from the metastore as schema documentation for the LLM |
| Partition discovery: `discover.partitions=true` + metastore housekeeping every 120 s | Hive 4 feature; new `dt/hr` folders get registered without an extra container or Airflow. MSCK stays as the manual fallback |
| `hive.msck.path.validation=ignore` | Spark's `_spark_metadata/` folder in the table root isn't a partition |
| Everything UTC (`TZ`, `user.timezone`, `hive.local.time.zone`) | Spark writes timestamps as INT96 (UTC instants); any non-UTC reader zone shifts them. Q2 checks this |
| `raw_trades` exposed but not for the chatbot | Debug and data-quality queries; it will be off the chatbot allow-list |
| No authorization yet | HS2 runs with auth NONE; the read-only user comes in step 6 with the chatbot. The SQL validator is the main guard; Hive grants are defence in depth |

### 3.3 Memory
Added postgres 384m, metastore 1g (heap 512m), hiveserver2 2g (heap 1.5g, Tez runs queries in-process). spark-master cut 2g → 1.6g.
**Sum of limits: 13.07 GB against the 14 GB WSL cap.** Limits are ceilings, not reservations; real use is lower (check `docker stats`). Airflow (step 7) will not fit on top without cuts: plan to lower Kafka/NameNode/DataNode limits and stop `spark-master` local verify runs during the demo.

### 3.4 Known limits
- At the start of each UTC hour, the new `hr=` partition is invisible to Hive for up to about 2 min (discovery interval). Files inside an already registered partition are visible immediately.
- Hive reads the Parquet files directly and ignores Spark's `_spark_metadata` commit log. A half-written file from a crashed Spark batch could be read. Rare; compaction in step 7 rewrites partitions cleanly.
- Hive keywords: `open/close/high/low` are not reserved, but `time`, `date`, `timestamp` are. Column names avoid them.

### 3.5 Second machine option + clock automation (2026-11-24 22:03)
A second machine is available: Windows, 32 GB, Intel i5 13th gen. It's undecided which machine runs the demo.

**Decision: the 24 GB laptop stays the baseline.** `docker-compose.yml` must always run within 14 GB. The bigger machine gets an opt-in profile:
- `compose.large.yml`, copied to `docker-compose.override.yml` (auto-loaded by Compose, git-ignored): spark-worker 5 GB / 4 cores, spark-master 2.5 GB, hiveserver2 heap 2.5 GB. The streaming executor stays at 2 GB; the extra worker memory is for the step 4/7 batch jobs.
- `docs/wslconfig.32gb.example`: memory 22 GB, swap 8 GB, 10 processors.
- Moving machines means starting with empty volumes. Spark checkpoints store Kafka offsets, so HDFS data copied without Kafka would point at offsets that don't exist.
- Bigger criterion than RAM: **the collector machine must stay on 24/7.** Step 4 evaluation needs days of continuous data, and the laptop sleeping already caused a 17 h gap (hours 3–18 on 2026-11-24).

**Clock handling is now automatic** (the step 1 problem: Windows clock +2.1 s and WSL drift):
- `producer/clock_check.py` measures skew against Binance `/api/v3/time` at the **midpoint** of the round trip, keeping the lowest-RTT of 5 samples, for an error of ±RTT/2. The earlier manual method was biased by about +RTT.
- The producer measures skew at startup and every 10 min, logs `skew=±N ms` in every stats line, warns above 500 ms, and **subtracts skew from the latency stats**. `verify_consumer.py` does the same.
- `scripts/preflight.ps1` checks the Windows clock (w32tm vs time.windows.com), `.wslconfig`, Docker memory and profile, and the container clock. With `-Fix` (Administrator) it configures w32time and resyncs; with `-RestartWsl` it restarts WSL to reset the VM clock and apply `.wslconfig`.
- Candle correctness never depended on the clock (event time = exchange `trade_ts`); this only makes reported latency honest.
- Rebuilding the producer causes a few seconds of trade loss (see 2.7). It is detected by the agg_id gap check.

### 3.6 Problem: `pull access denied for marketpulse-hive`
- **Symptom:** `docker compose up -d --build` → `Image marketpulse-hive:4.0.1 Error pull access denied ... repository does not exist`.
- **Root cause:** only `hive-metastore` had `build: ./hive`. `hiveserver2` and `hive-init` only named `image: marketpulse-hive:4.0.1`, so Compose tried to **pull** that local-only tag from Docker Hub, in parallel with (or before) the build.
- **Fix:** add `build: ./hive` to all three services with the same `image:` tag. Compose builds it once (cache) and never pulls.
- **Lesson:** every service using a locally built image needs `build:`, not just one of them. The Spark services avoid this because they all inherit `build:` from the `x-spark` anchor.

### 3.7 Results

**Real memory with the full step 1–3 stack (2026-11-25 00:52, `docker stats`):**
| Container | Used | Limit |
|---|---|---|
| spark-worker | 832 MiB | 3.4 GiB |
| hiveserver2 | 748 MiB | 2 GiB |
| spark-candles | 511 MiB | 1.5 GiB |
| kafka | 460 MiB | 1 GiB |
| hive-metastore | 382 MiB | 1 GiB |
| namenode | 363 MiB | 1 GiB |
| datanode | 357 MiB | 1 GiB |
| spark-master | 144 MiB | 1.6 GiB |
| postgres | 90 MiB | 384 MiB |
| producer | 23 MiB | 256 MiB |
| **Total** | **about 3.9 GiB** | 13.07 GiB |

- Real use is about **30% of the limits**, so the 14 GB cap has about 10 GB of headroom. Airflow (step 7) fits without cuts unless a container grows under load (HS2 during big queries, the worker during batch jobs).
- spark-worker at 66% CPU at measurement time: probably a catch-up batch after the restart. To re-check in steady state.

**hive-init / DDL (2026-11-24 22:48 UTC): success on the second attempt (after the build fix).**
- Metastore schema initialized in Postgres; `hive-metastore` and `hiveserver2` were healthy in about 30 s.
- `CREATE DATABASE` 4.4 s (first HDFS dir creation), tables 0.1–0.3 s, `MSCK REPAIR` 0.66 s / 0.17 s.
- `SHOW TABLES`: `candles_1m`, `raw_trades`.
- `SHOW PARTITIONS candles_1m`: `dt=2026-11-23/hr=18`, `dt=2026-11-24/hr=2`, `hr=19`, `hr=20`, `hr=22`. **`hr=21` is missing**: no candles for 21:00–21:59 UTC (23:00–23:59 Cairo). Most likely the laptop slept; to confirm in raw_trades.
- Harmless noise: duplicate SLF4J bindings (Hive and Hadoop jars both ship one) and `Failed to create directory /home/hive/.beeline` (beeline history file; no home dir in the image).

### 3.8 test_queries.sql results (2026-11-24 23:28 UTC)
| Query | Result | Time |
|---|---|---|
| Q1 SHOW PARTITIONS | 6 partitions, **`hr=23` included** | 0.11 s |
| Q2 freshness / TZ | 3,828 candles today, 39 symbols, last 23:23, **minutes_behind = 5.0** | **14.1 s (cold)** |
| Q3 latest BTC price | 60,032.23 @ 23:23 | 4.1 s |
| Q4 top 10 by USDT volume | BTC 42.4M, ETH 22.3M, SOL 5.9M ... | 3.3 s |
| Q5 biggest 1-min moves | PEPE/BONK ±0.48–0.58% | 2.9 s |
| Q6 volume vs daily avg (window fn) | BNB 37.7x @ 19:16, ICP 35.3x @ 19:17, SHIB 30.5x @ 23:13 ... | 3.1 s |
| Q7 BTC per hour | hr 2: 2 min, 19: 49, 20: 9, 22: 22, 23: 24 | 3.0 s |
| Q8 full scan | 5,360 candles, 2 days | 2.3 s |
| Q9 raw validity | 156,729 valid, 0 invalid | 2.8 s |

**Acceptance: all three criteria met. Step 3 DONE.**
1. All queries run.
2. **No time-zone shift**: minutes_behind 5.0 = 2 min watermark + 1 min trigger + up to 2 min since the last write. Spark INT96 timestamps read correctly.
3. **Automatic partition discovery works**: `hr=23` (folder created at 23:00) was registered without MSCK. hive-init ran its MSCK at 22:48, before that folder existed.

**Latency analysis:**
- **Warm queries take 2.3–4.1 s, almost all fixed Tez overhead** (DAG submit and container start in local mode). The data is tiny (5,360 rows), so the time won't grow much with data for months.
- **Cold first query: 14 s** = 3.5 s compile (first metadata load) + Tez session start (about 7 s). The Tez session is **per JDBC connection**, so a chatbot that opens a new connection per question would pay about 7 s every time. **Step 6 rule: keep one persistent HiveServer2 connection, and warm it at app start.**
- Hive's query results cache skips queries with non-deterministic functions (`current_date()`), so chatbot SQL that uses "today" won't be cached by Hive. Cache in Streamlit instead (short TTL).
- `No Stats for marketpulse@candles_1m`: no column statistics, so the cost-based optimizer is blind. Harmless at this size; add `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` to the daily Airflow job (step 7).
- **Decision: stay on Hive for the chatbot.** 3–4 s per query is acceptable for a chatbot answer (the LLM call will take as long). Spark Thrift Server is dropped: it would be an extra service for a gain we don't need.

**Data findings that matter for later steps:**
- **Gap confirmed (Q7):** BTC has only 9 minutes in hour 20 and nothing in hour 21; data resumes at 22:38. So the pipeline stopped at about 20:09 UTC and restarted at 22:38 UTC (22:09–00:38 Cairo): **the laptop slept.** The collector must run on a machine that doesn't sleep (Windows power settings or the 32 GB machine).
- **Tick-size artifact (Q5):** the "biggest moves" are PEPE/BONK at ±0.5%, which is just **1–2 price ticks** (PEPE trades at 3.45e-6 with a 1e-8 tick, so one tick ≈ 0.29%). A fixed % threshold would flag noise on micro-priced coins. **Step 4 must normalize per symbol** (z-score of returns), not use a raw % rule.
- **Volume spikes exist (Q6):** BNB 37.7x and ICP 35.3x the daily average in a single minute. These are first candidates for the step 4 evaluation set.
- **Display (step 6):** Hive prints doubles in scientific notation (`4.2366514E7`, `3.45E-6`). The chatbot must format numbers before showing or summarizing them.

### 3.9 Clock automation: first real readings, and a fix to the skew estimator (2026-11-25 01:36)
Preflight passed: Windows clock **+110 ms** vs NTP, `.wslconfig` 14 GB, Docker 13.7 GB (base profile), container skew **−330 ms with RTT 1,075 ms (±537)**.
Producer: `skew=-354ms`, corrected latency **p50 591–610 ms, p95 650–676 ms**, failed=0.

**Problem: the correction made the numbers worse.**
- An RTT of about 1.1 s to `/api/v3/time` is far above the real network round trip. Each sample opened a **new HTTPS connection**, so the measured time included DNS + TCP + TLS handshakes (several round trips) **before** the server stamped its time. The midpoint method assumes a symmetric request/response; the handshake breaks that and pushes the estimate negative.
- Windows NTP says the host is about +110 ms. A −354 ms container skew is inside the ±537 ms error bar, so it isn't a real measurement. Subtracting it turned the raw latency of about 240 ms into a reported about 600 ms.

**Fix (`producer/clock_check.py`):**
1. Use **one keep-alive connection** and throw away a warm-up request, so samples measure only the request round trip.
2. **Apply the correction only if it is significant:** |skew| > RTT/2 + 50 ms. Otherwise report "within noise" and correct by 0. A 2.2 s skew like on day 1 is still corrected; noise is not.
3. The producer log now states the measured skew, the RTT and whether it was applied.

Expected after the producer rebuild: RTT of a few hundred ms or less, no correction applied, latency p50 about 200–300 ms. The rebuild causes a few seconds of trade loss, flagged by the agg_id gap check.
**Lesson for the report:** a correction must never be bigger than its own error bar; measure the uncertainty, don't just use the estimate.

**Clock fix verified (2026-11-25 01:38):** `clock_check`: skew +77 ms, RTT 256 ms (was 1,075 ms with fresh connections), "within noise". Producer: `clock skew +84 ms (rtt 269 ms, ±134) -> within noise, no correction`. Keep-alive cut the RTT about 4x, and no false correction is applied.

### Step 3 status: DONE

---

## 4. Step 4 — Anomaly detection + evaluation

Choices (2026-11-25): **1a** detect inside the streaming app (live feed), **2a** evaluate on the Binance public archive + hand labels.

### 4.1 What was built
```
spark/jobs/anomaly_rules.py   rule z_v1: score() + to_anomalies(); the ONLY detector code, shared by live and backtest
spark/jobs/candles_stream.py  + query 3 "anomalies": file stream over committed candles -> foreachBatch -> score()
spark/jobs/backtest.py        archive download, score(), events, label samples, summary, known-event recall
spark/jobs/precision.py       precision + 95% Wilson CI from labelled CSVs
spark/jobs/known_events.csv   documented events for recall (10/10/2025 crash, source: Galaxy Research)
hive/sql/ddl.sql              + table anomalies, + view anomaly_events (dedup)
docs/ANOMALY_EVALUATION.md    method + labeling guide
docker-compose.yml            spark-master mounts ./data/backtest -> /opt/out; ANOMALIES_TOPIC env
```

### 4.2 Rule z_v1
Signal window 5 min `[t-4..t]`, baseline 60 min `[t-64..t-5]` (no overlap, so a spike can't inflate its own baseline).
- `vol_ratio` = USDT volume per minute in the window ÷ per minute in the baseline (missing minutes = 0 volume).
- `ret_5m` = close vs the last close at or before t−5. `sigma_1m` = stddev of 1-min log returns over the baseline (the "rolling volatility").
- `z_ret` = ln(close/close_ref) ÷ (max(sigma, 0.05%) × √5).
- **Anomaly = eligible AND vol_ratio ≥ 5 AND |z| ≥ 4 AND |ret_5m| ≥ 1%.**
- Eligible = ≥30 baseline candles, no agg_id gap inside the window (BUILD_LOG 2.8), ≥20k USDT traded in the window.
- Event = first triggering minute with no trigger in the previous 15 min (one event per episode, not one per minute).
- The baseline project rule (vol ≥ 5x AND |ret| ≥ 4%) is computed alongside for comparison.
- Output: plain-English `reason` ("Price −5.10% in 5 min (6.2 sigma of its normal moves) on 8.3x its normal trading volume"), for the dashboard and the chatbot.

### 4.3 Design decisions
| Decision | Why |
|---|---|
| One `score()` for live and backtest | The precision we report is the precision of the code that runs live |
| Query 3 re-reads committed candles (file source) instead of chaining on the candles aggregation | Spark doesn't allow a custom stateful step after a streaming aggregation in append mode. The file source reads only files committed in `_spark_metadata`, so no half-written data |
| Batch window functions over HDFS history in `foreachBatch` (about 85 min lookback) | No hand-written streaming state; deterministic; identical to the backtest |
| z-score on per-symbol volatility + 1% floor | Fixes the tick-size issue (BUILD_LOG 3.8) and the BTC-never/small-cap-always bias of a fixed % |
| Log-free volume ratio (not a z of log volume) | "5x normal volume" is explainable to non-technical users |
| At-least-once output + dedup view `anomaly_events` | foreachBatch can replay a batch after a crash. The view keeps the first row per (symbol, minute); step 7 compaction dedupes physically |
| Backtest in local mode in spark-master | The worker's memory is taken by the stream; local mode with 1 GB driver + scoring materialized to local Parquet |
| Evaluation months 2025-10 (contains the 10/10 crash) + 2026-10 (recent, normal) | One stress month with a verified event, one ordinary month |

**Detection latency:** candle written about 3 min after its minute + one more trigger, so an anomaly appears **about 4–5 min after the minute it describes**. Acceptable for a monitoring feed; stated honestly in the report.

### 4.4 Results

**Live query started cleanly (2026-11-24 23:46 UTC).**
- `hive-init` re-run: `anomalies` table and `anomaly_events` view created; existing tables untouched (idempotent DDL works). Tables: anomalies, anomaly_events, candles_1m, raw_trades.
- The spark-master/worker/candles recreate went fine; raw and candles queries resumed from their checkpoints with the same query ids.
- `anomaly rule: {'rule': 'z_v1', 'window_min': 5, 'baseline_min': 60, 'vol_ratio': 5.0, 'z_ret': 4.0, 'min_abs_ret': 0.01, ...}`
- `[anomalies] batch=0 candles=5997 new_events=0` (18.3 s): the first batch back-scored **all existing candles** (about 2 days). **0 events.**
- `[anomalies] batch=1 candles=35 new_events=0` (3.0 s): steady state is one batch per minute of about 35–40 candles, about 3 s each.

**Reading the 0:** the historical data is short (about 10 h of non-contiguous data) and a calm market. The BNB 37.7x volume minute from Q6 had no ≥4σ, ≥1% price move with it, so z_v1 correctly doesn't call it an anomaly (volume alone isn't enough by design). The rule's hit rate on real data is measured by the backtest. The live **write path** (HDFS partition + Kafka + Hive view) hasn't fired yet, because no event has happened. It still needs a test before the demo.

### 4.5 First backtest (2025-10 + 2026-10, 40 symbols, 62 days, 3,482,125 candles)
- 79/80 archive files downloaded. `TONUSDT 2026-10` returned HTTP 404 (not in the archive for that month, probably no longer listed on spot). The live producer may get nothing for TON either: check, and remove it from symbols.txt if so.
- Eligible: 73.6% of candles (the rest: short baseline, thin volume, < 20k USDT in 5 min).

| | z_v1 | baseline (5x & 4%) |
|---|---|---|
| events | **2,067** (33.3/day, 0.83/symbol/day) | 114 (1.8/day, 0.05/symbol/day) |
| large caps (5 of 40 symbols = 12.5%) | 152 (7.4%) | 6 (5.3%) |
| symbols with ≥1 event | 40/40 | 38/40 |
| up / down | 821 / 1,246 | 15 / 99 |
| 10/10 crash recall | **39/40** (missed TRX) | 38/40 |

**Analysis (before labeling):**
1. **z_v1 is far too loud: 33 events/day, 18x the baseline.** A retail feed that fires about every 45 minutes will be ignored. The thresholds (5x volume, 4σ, 1%) are too permissive for 1-minute crypto data, where fat tails make 4σ much more common than under a normal distribution. Precision is very likely low as-is. **Needs tuning before labeling**; labeling the current rule would waste the team's time.
2. **The crash is easy**: both rules catch about all symbols. It proves the pipeline end to end on a real event, but it doesn't discriminate between the rules. The precision labels must do that.
3. **Size bias**: large caps are 12.5% of symbols. z_v1 gives them 7.4% of events, the baseline 5.3%. z_v1 is somewhat more balanced (per symbol: 30 vs 55 events for large vs others; the baseline 1.2 vs 3.1), but neither is neutral. Small caps genuinely move more in volume-adjusted terms, so some skew is expected.
4. **The baseline is dominated by down moves (99 vs 15)**, most of them the 10/10 crash. Outside one crash day it fires about once a day: too quiet to be useful, and it misses moves on calm large caps by construction.
5. **Bug in the overlap metric:** it matched events on the exact same minute (27 overlaps). The two rules often catch the same episode at different minutes (z_v1 fires early, when the move is 2–3%; the baseline waits for 4%). Fixed: overlap now uses the same symbol within 15 min.
6. **Market-wide bursts:** on a market-wide move, dozens of symbols fire in the same minutes. Each is "correct" per coin, but for a user it is ONE event ("the whole market dropped"). The sensitivity run measures that share. If it is large, the feed should group them (step 5 UX), and precision should be reported with and without market-wide events.

**Next (methodology):** tune on one month, evaluate on another. Otherwise reported precision is inflated.
- `sensitivity.py` on **2026-10** (normal month) over a grid: volume {5, 8, 10}x × |z| {4, 5, 6} × min move {1%, 2%}. Pick the cell with **about 3–10 events/day**, crash recall still ≥ 35/40, large-cap share near 12.5%.
- Set the chosen values as the defaults in `anomaly_rules.py` (rule z_v2) and re-run the backtest.
- Label the **2025-10** sample only (held out from tuning).

### 4.6 Problem: sensitivity.py OutOfMemoryError
- Run 1 (`--months 2026-10`) was stopped with Ctrl+C after 13 s. The py4j "reentrant call" traceback is just the interrupt, not a bug.
- Run 2 (`--months 2025-10`): `java.lang.OutOfMemoryError: Java heap space` in `WindowExec` → `UnsafeSorterSpillReader`.
- **Root cause:** 18 cooldown window columns (one per grid cell) over about 1.7M rows. Spark's WindowExec buffers each window partition (one symbol = about 44k rows per month); with this many columns the buffer spills, and every spill file opens a reader with its own buffer. That doesn't fit in the 1 GB local driver.
- **Fix:** the episode/cooldown logic only depends on minutes that trigger. Every grid cell's triggers are a subset of the loosest cell's (vol ≥ 5, |z| ≥ 4, |ret| ≥ 1%), so filter to that superset first (thousands of rows), write it to local Parquet, and run the 18 windows on that. Results are identical (non-trigger rows never affect `max(trigger)` over the cooldown frame); memory drops by about 1000x.
- **Lesson:** filter before you window. Window functions over full partitions are the most memory-hungry operator in Spark.

### 4.7 Sensitivity results and rule z_v2 (2026-11-25 04:04)
`candidate minutes (loosest cell)`: 2,807 (2026-10), 3,019 (2025-10). The filter-first fix works (each run takes a few minutes, no OOM).

**Tuning month 2026-10** (normal, 31 days, 39 symbols; large caps = 12.8%):
| vol | \|z\| | \|ret\| | events/day | large% | market-wide% |
|---|---|---|---|---|---|
| 5 | 4 | 1% | 31.1 | 5.0 | 57.3 |
| 5 | 4 | 2% | 10.5 | 3.4 | 60.3 |
| 8 | 6 | 1% | **13.9** | **7.9** | 61.3 |
| 10 | 6 | 1% | 12.4 | 7.8 | 59.4 |
| 10 | 6 | 2% | 6.9 | 3.7 | 59.3 |
**Crash month 2025-10**: every cell with vol ≤ 8 catches 39/40 symbols on 10/10; vol 10 drops to 36–37/40.

**Findings:**
1. **The dominant effect is not the thresholds: 57–69% of all events (normal month) are part of market-wide bursts** (≥ 8 coins triggering in the same 10 minutes). Tightening thresholds barely changes that share. Per-coin alerting turns one market move into 10–30 feed items, and labeling each would inflate precision with easy "yes, everything dropped" cases.
2. **The 2% floor cuts volume the most, but it kills large-cap coverage** (3–4% vs a neutral 12.8%): BTC/ETH rarely move 2% in 5 min, so the rule would almost ignore them. The 1% floor keeps large caps at 5–8%.
3. Raising |z| from 4 to 6 and vol from 5 to 8 cuts events by 55% while keeping crash recall at 39/40. vol 10 starts losing the crash.

**Decision, rule z_v2:** vol ≥ **8x**, |z| ≥ **6**, |ret| ≥ **1%** (best large-cap balance in the grid at 7.9%, crash 39/40), plus a **scope** per event:
- `market_breadth` = distinct coins triggering in the trailing 10 min; `scope = 'market'` if ≥ 8, else `'coin'`.
- Expected feed: about 13.9 × 39% ≈ **5.4 coin events/day + a few market bursts/day**, inside the 3–10/day target.
- Trailing window = same logic live and in the backtest. Limitation: the first coins of a burst fire before breadth reaches 8 and stay `coin`. The dashboard can regroup by time.
- Hive `anomalies` gets `scope` and `market_breadth` (one-time drop + recreate of the empty table; external, no data lost).

**Evaluation design (updated):**
- Label samples are drawn only from **2025-10** (`--label-months`). The thresholds were picked on 2026-10. Honest caveat: 2025-10 crash recall was looked at as a constraint (≥ 35/40), so it isn't 100% untouched; its precision labels are.
- Three label files: `label_z.csv` (40 coin-scope events, labelled individually), `label_market.csv` (20 bursts, one representative row each), `label_fixed.csv` (40 baseline-rule events).
- Future work (report): an idiosyncratic z (coin return minus market/BTC return) would separate "coin moved alone" from "market moved" in the score itself, instead of by breadth.

### 4.8 Backtest with z_v2 (2025-10 + 2026-10)
```
z_v2: 909 events (14.7/day)  large caps 80  up 345 / down 564
      scope: coin 335 (5.4/day) | market-wide 574 rows in ~31 bursts (0.5/day) -> feed items/day ~5.9
      coin-scope large-cap share 9.0%
baseline: 114 events  scope: coin 32 (0.5/day) | market 82 rows in ~13 bursts -> feed ~0.7/day
overlap: z_v2 catches 94/114 baseline events (82%); baseline catches 94/909 z_v2 events (10%)
10/10 crash: z_v2 39/40 (missed TRX), baseline 38/40
```
- **Feed volume target met: about 5.9 items/day** (5.4 coin events + 0.5 market bursts), down from 33/day with z_v1. A market burst averages about 18.5 coins (574 / 31), which confirms the grouping was needed.
- **Size balance:** 9.0% large caps in coin scope vs a neutral 12.5%, the closest so far.
- **Coverage vs baseline:** z_v2 contains 82% of what the original rule finds, plus about 8x more (smaller-but-unusual moves, large caps). The baseline's 20 missed-by-z_v2 events are the precision labels' job to judge: true anomalies z_v2 misses, or noise the baseline catches?
- **The baseline is mostly crash:** 72% of its rows are market-wide; only 0.5 coin events/day.
- **Down-moves dominate (62%):** consistent with crypto's crash-fast/rally-slow asymmetry.
- Cosmetic: the overlap line still said "z_v1"; now uses the rule version.

**Status:** detector tuned. Remaining for step 4: (1) hand labels → precision (team task), (2) **smoke test** of the live write path (HDFS + Kafka + Hive) with forced low thresholds into a separate path/topic/checkpoint. `candles_stream.py` now takes `ANOMALIES_PATH`, `ANOMALIES_CKPT`, `ANOMALIES_TOPIC` from env; `spark-candles` loads optional `.env` (`required: false`). Procedure in README.

### 4.9 Labeling setup (2026-11-25 04:20)
- The user asked Claude to do the labeling. **Pushed back on doing it alone:** the system's designer grading its own detector isn't a defensible precision figure for an examiner. Compromise: Claude builds rule-based **pre-labels** with full-day context plus a **review page with charts**; humans give the final labels; agreement is reported.
- `label_context.py` (runs in spark-master, reads the cached archive CSVs): 96 events, 172,260 minute rows. 16 market bursts were sampled, not 20, because October 2025 has only 16 market-wide bursts under z_v2.
- Claude's workspace and the device VM can't reach Binance (egress allowlist), so the data had to come from the container.
- **Pre-label criterion bug found and fixed before use:** "≥ p99 of the day" failed on the crash day, where later minutes were even bigger, so the first crash minutes ranked only 96–98th. Fixed with an absolute alternative (≥ 15× the day's median 5-min move). Before the fix, 28 baseline events were "unsure"; after, 1.
- Pre-label outcome: label_z 35 yes / 1 no / 4 unsure; market 16/16 yes; fixed 39 yes / 1 unsure. **These high rates are expected and not evidence:** the criteria overlap with the detector's own thresholds. Example of what only a human can judge: RENDER 2025-10-06 00:17, +1.01% on 512 trades, which passes C1 but looks like ordinary noise next to a bigger unflagged volume spike 30 min later.
- **Sampling caveat:** 28 of the 40 baseline-rule samples are from the 10/10 crash day (the baseline barely fires otherwise), so the baseline's precision is mostly "is a crash an anomaly". `precision.py` now also reports precision without 2025-10-10.
- Review page published as a private claude.ai artifact with a per-reviewer database (`reviews/{self}`: everyone reads, each writes only their own; the owner can tidy) plus a downloads capability for the CSV. Blind mode on by default. Tooling saved under `tools/` (prelabel.py, review_charts.py, review_build.py, review_template.html); pre-labels in `prelabels.csv`.

### 4.10 First review pass: reviewer 1 (2026-11-25 04:31): PROVISIONAL, not reportable yet
96/96 labelled in about 8 minutes (about 5 s per card), no notes. Read directly from the review page's database.
| Set | Precision | 95% CI | w/o 10/10 | agreement with pre-label |
|---|---|---|---|---|
| z_v2 coin events | 35/40 = 87.5% | 74–95% | 34/38 = 89.5% | 92% |
| z_v2 market bursts | 11/16 = 68.8% | 44–86% | 10/14 = 71.4% | 69% |
| original rule | 33/40 = 82.5% | 68–91% | 11/12 = 91.7% | 85% |

**Quality problems that block using these numbers:**
1. **Inconsistent on identical cases.** On the 10/10 crash onset, at the same minute with the same −4 to −5% drop and 12–38x volume: DOGE, ICP, ARB, AVAX, FET, PEPE = 1, but OP, DOT, XLM, ENA, WLD, ETH = 0. Five of those six 0s sit at the end of the "Original rule" tab (f-33…f-38): fatigue or misclicks.
2. **BCH 2025-10-17 08:16 (−7.26%, 18.6σ, a clean cliff on a flat day) = 0.** That's the example agreed as an obvious 1 in the labeling walkthrough.
3. **No notes on any of the 17 zeros**, so they can't be audited or used for error analysis.
4. **One reviewer only**, so there's no inter-reviewer agreement.

**Plausible genuine zeros** (consistent with the guide, worth keeping if confirmed): RENDER 10-06 00:17 (+1.0% on 512 trades, volume spike elsewhere), UNI 10-23, OP 10-02, FIL 10-07, BNB 10-10 11:08 (≈1.1–1.5% moves on quiet days), FLOKI 10-10 22:48 (+11.6% but z = 2.0: normal for that post-crash hour).
**Emerging finding (if confirmed):** most coin-scope zeros are the smallest moves (1.0–1.5%), which points to the 1% floor as the weak spot. Candidate z_v3: a floor of 1.5% for non-large-caps.

Next: reviewer 1 re-checks the 17 zeros with notes; a second teammate labels independently (blind).

### 4.11 Reviewer 1 after re-check (2026-11-25 19:12)
Reviewer 1 re-checked the 17 zeros: 8 flipped to 1 (all the 10/10 crash-onset cards and BCH 10-17). The 9 remaining zeros, 7 with notes ("volume low", "slow rise", "volume low price up").
| Set | Precision | 95% CI | w/o 10/10 | agreement w/ pre-label |
|---|---|---|---|---|
| **z_v2 coin events** | **36/40 = 90.0%** | 77–96% | 35/38 = 92.1% | 94% |
| z_v2 market bursts | 12/16 = 75.0% | 51–90% | 10/14 = 71.4% | 75% |
| original rule | 39/40 = 97.5% | 87–100% | 12/12 = 100% | 97% |

**Findings:**
1. **Every z_v2 rejection is a small move.** All 8 rejected z_v2 events are moves of 1.0–2.2%, 7 of them under 1.65%. Coin events by size: |move| 1–1.5% → 21/25 real; ≥ 1.5% → **15/15 real**. This confirms the 1% floor is the weak spot.
2. **"Volume low" vs an 8–15x volume ratio:** the reviewer judges volume against the whole 6-hour chart, while the detector compares with the previous 60 min. After a very quiet hour, a modest burst is "8x normal" to the detector but unremarkable on a 6-hour view. Concrete z_v3 idea: a longer baseline (e.g. 6 h) for the volume ratio, or both 60 min and 6 h must be elevated.
3. **Market bursts are the weakest (75%):** the zeros are small up-moves (1.1–2.2%, "slow rise") on days when several coins drifted up together. Breadth ≥ 8 catches broad drifts as well as shocks.
4. **Precision vs coverage trade-off:** the original rule is more precise (97.5%) but fires about 0.7 times a day, mostly crashes. z_v2 is 90% precise at about 5.9 feed items/day and covers large caps and smaller moves. The original rule is almost all obvious crash events, so its precision is high by construction (28/40 are 10/10).
5. FLOKI 10-10 22:48 (+11.6%, z = 2.0, original rule only) = 0: a big % move that is normal for that post-crash hour, the exact case the per-symbol z was designed to reject.

**Caveats:** still one reviewer (no inter-rater agreement yet). The z_v3 ideas come from looking at the evaluation sample, so any z_v3 needs a **new** held-out labelled sample (e.g. 2026-09) before claiming its precision.

### 4.12 Smoke test of the live write path: PASS (2026-11-25 17:13–17:17 UTC)
Thresholds forced down (vol 1.5x, |z| 1.5, |ret| 0.05%, no volume floor), output redirected to `_smoke/` + topic `anomalies_smoke` + its own checkpoint:
- `[anomalies] batch=1 candles=6223 new_events=81`, `batch=2 candles=1848 new_events=8`; `[ANOMALY]` lines with scope text ("part of a market-wide move: 17 coins in 10 min").
- **HDFS:** partitioned parquet under `_smoke/anomalies/dt=…/hr=…` (2026-11-23 hr 18 → 2026-11-25 hr 4), replication 1, `_SUCCESS` marker.
- **Kafka:** JSON messages with every field (`detected_minute` ISO-UTC, `scope`, `market_breadth`, `reason`, `rule_version` z_v2), key = symbol.
- **Hive:** `CREATE EXTERNAL TABLE … LIKE anomalies` + MSCK, then `SELECT` returned rows with correct UTC times and reasons in **0.98 s**: a plain `SELECT … LIMIT` runs as a Hive fetch task, without Tez (useful for the chatbot: simple lookups are about 1 s, aggregations about 3 s).
- The real `anomalies` table, topic and checkpoint were untouched.
- Side note: after the restart the raw/candles queries caught up on a 9.6k-row backlog in one batch (1–2.7 s), which shows recovery works.

### Step 4 status: DONE (detector live + write path verified + backtest + precision from reviewer 1). Open: second reviewer.

Cleanup after the smoke test confirmed (19:34): `anomaly rule: {'rule': 'z_v2', 'vol_ratio': 8.0, ...} output: hdfs://namenode:8020/marketpulse/anomalies + topic anomalies`.

---

## 5. Step 5 — Streamlit dashboard

### 5.1 What was built (2026-11-25)
```
dashboard/Dockerfile, requirements.txt   python 3.12, streamlit 1.40.1, plotly, confluent-kafka, pyhive[hive_pure_sasl]
dashboard/app.py                         navigation, sidebar (time zone UTC/Cairo, live status)
dashboard/lib/live.py                    Kafka background reader -> in-memory candles + anomalies
dashboard/lib/hive.py                    one persistent, pre-warmed HiveServer2 connection
dashboard/lib/charts.py                  candlestick + volume + anomaly markers
dashboard/lib/fmt.py                     prices without scientific notation, $ volumes, %
dashboard/views/live.py                  market board (all coins, 5 s refresh) + live coin chart
dashboard/views/feed.py                  anomaly feed, market bursts grouped per 10 min
dashboard/views/history.py               Hive: coin/day chart + anomalies, day summary, SQL + timing shown
dashboard/views/analyst.py               placeholder (step 6)
docker-compose.yml                       + streamlit (8501, 768 MB)
```

### 5.2 Design decisions
| Decision | Why |
|---|---|
| **Hot path from Kafka, cold path from Hive** | Spark candles land 2–4 min late (watermark + append) and Hive aggregations take about 3 s; a live view needs the forming minute. Reading `trades` directly gives it with no extra service |
| Live candles rebuilt in the dashboard from the last 180 min of `trades` (offsets_for_times) | No dependency on Hive for the live page; restart-safe; same open/close-by-agg_id logic as Spark, so the live chart matches candles_1m |
| One reader thread per server (`st.cache_resource`), shared by all sessions | N browser tabs don't create N Kafka consumers |
| Anomalies read from the start of the 7-day topic, deduped on (symbol, minute) | The detector is at-least-once (BUILD_LOG 4.3) |
| Market-wide rows grouped per 10-min bucket in the feed | One market move = one card (BUILD_LOG 4.7) |
| One persistent Hive connection, warmed at start, serialized with a lock, results cached 5 min | Tez session start costs about 7 s per connection (BUILD_LOG 3.8); HS2 runs one statement per connection |
| SQL + run time shown under every history chart | Makes the Hive layer visible and honest in the demo |
| Parameterized SQL (pyformat) even though inputs come from select boxes | Habit for step 6, where SQL comes from an LLM |
| Prices formatted per magnitude (0.00000345, 60,032.23); volumes as $42.37M | Hive/Kafka give raw doubles (BUILD_LOG 3.8 display finding) |
| Time zone switch UTC / Africa/Cairo, storage stays UTC | Users think in local time; data stays unambiguous |

### 5.3 Results
_Pending first run._

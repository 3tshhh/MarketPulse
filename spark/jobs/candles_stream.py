"""
MarketPulse streaming app: Kafka `trades` -> HDFS Parquet `raw/`, `candles/`, `anomalies/` + Kafka `anomalies`.

Three streaming queries in one Spark application:
  raw        every trade as received (valid + invalid, flagged with is_valid). Nothing is dropped silently.
  candles    valid trades, deduplicated on (symbol, agg_id), 1-minute event-time windows,
             2-minute watermark, emitted once per window after it closes (append mode).
  anomalies  (step 4) reads the committed candles back as a file stream; for each new batch it loads
             the recent candle history from HDFS and runs anomaly_rules.score(), the same function
             the backtest uses. New events go to HDFS anomalies/ and to Kafka `anomalies`.

Layout (partitioned by UTC date/hour):
  /marketpulse/raw/dt=YYYY-MM-DD/hr=H/
  /marketpulse/candles/dt=YYYY-MM-DD/hr=H/
  /marketpulse/_checkpoints/{raw,candles}/

IMPORTANT: spark.sql.shuffle.partitions and the aggregation logic are frozen into the candles
checkpoint on first start. To change them, stop the job and delete _checkpoints/candles
(and the candles/ output, or you will get duplicate candles).
"""
import os
from datetime import timedelta

import anomaly_rules as AR
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TRADES_TOPIC", "trades")
LAKE = os.getenv("LAKE_BASE", "hdfs://namenode:8020/marketpulse")
WATERMARK = os.getenv("WATERMARK", "2 minutes")
TRIGGER = os.getenv("TRIGGER", "1 minute")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
ANOMALIES_TOPIC = os.getenv("ANOMALIES_TOPIC", "anomalies")
# Smoke test support (README, step 4): point the anomaly output somewhere else without touching
# the real table/topic, e.g. ANOMALIES_PATH=.../_smoke_anomalies ANOMALIES_TOPIC=anomalies_smoke
ANOMALIES_PATH = os.getenv("ANOMALIES_PATH", f"{LAKE}/anomalies")
ANOMALIES_CKPT = os.getenv("ANOMALIES_CKPT", f"{LAKE}/_checkpoints/anomalies")

spark = (
    SparkSession.builder.appName("marketpulse-candles")
    .config("spark.sql.session.timeZone", "UTC")
    # 3 = number of Kafka partitions. Default 200 would write ~200 tiny files per trigger.
    .config("spark.sql.shuffle.partitions", "3")
    # State lives in RocksDB off-heap instead of the JVM heap: safer on a 2 GB executor.
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


class ProgressLogger(StreamingQueryListener):
    """One line per micro-batch so `docker compose logs` shows the pipeline is alive."""

    def onQueryStarted(self, event):
        print(f"[started] {event.name} id={event.id}", flush=True)

    def onQueryProgress(self, event):
        p = event.progress
        state = sum(op.numRowsTotal for op in p.stateOperators) if p.stateOperators else 0
        wm = p.eventTime.get("watermark", "-") if p.eventTime else "-"
        print(
            f"[{p.name}] batch={p.batchId} rows={p.numInputRows} "
            f"in={p.inputRowsPerSecond:.1f}/s proc={p.processedRowsPerSecond:.1f}/s "
            f"dur={p.durationMs.get('triggerExecution', 0)}ms state_rows={state} watermark={wm}",
            flush=True,
        )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[terminated] id={event.id} exception={event.exception}", flush=True)


spark.streams.addListener(ProgressLogger())

trade_schema = T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("agg_id", T.LongType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("qty", T.DoubleType()),
    T.StructField("first_trade_id", T.LongType()),
    T.StructField("last_trade_id", T.LongType()),
    T.StructField("trade_ts", T.LongType()),
    T.StructField("event_ts", T.LongType()),
    T.StructField("is_buyer_maker", T.BooleanType()),
    T.StructField("ingest_ts", T.LongType()),
])

src = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)   # only used on first start; then checkpoint wins
    # If the job is down longer than Kafka retention (48h), continue instead of crashing.
    # Lost range shows up as a gap in raw/ and in verify_candles.py.
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 200000)        # bound catch-up batches after downtime
    .load()
)

parsed = (
    src.select(
        F.col("value").cast("string").alias("json"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )
    .withColumn("t", F.from_json("json", trade_schema))
    .select("t.*", "json", "kafka_partition", "kafka_offset", "kafka_ts")
    .withColumn("event_time", F.expr("timestamp_millis(trade_ts)"))
    .withColumn(
        "is_valid",
        F.coalesce(
            F.col("symbol").isNotNull()
            & F.col("agg_id").isNotNull()
            & F.col("trade_ts").isNotNull()
            & (F.col("price") > 0)
            & (F.col("qty") > 0),
            F.lit(False),
        ),
    )
    # keep the original payload only for rows we couldn't use -> debuggable quarantine
    .withColumn("raw_json", F.when(~F.col("is_valid"), F.col("json")))
    .drop("json")
)

# ---------- query 1: raw ----------
part_time = F.coalesce(F.col("event_time"), F.col("kafka_ts"))
raw_q = (
    parsed.withColumn("dt", F.date_format(part_time, "yyyy-MM-dd"))
    .withColumn("hr", F.hour(part_time))
    .writeStream.format("parquet")
    .queryName("raw")
    .option("path", f"{LAKE}/raw")
    .option("checkpointLocation", f"{LAKE}/_checkpoints/raw")
    .partitionBy("dt", "hr")
    .outputMode("append")
    .trigger(processingTime=TRIGGER)
    .start()
)

# ---------- query 2: 1-minute candles ----------
valid = parsed.filter("is_valid")

candles = (
    valid.withWatermark("event_time", WATERMARK)
    # Kafka/producer retries can redeliver a trade; agg_id is unique per symbol.
    .dropDuplicatesWithinWatermark(["symbol", "agg_id"])
    .groupBy(F.window("event_time", "1 minute").alias("w"), "symbol")
    .agg(
        # agg_id increases with time per symbol -> deterministic open/close
        # (first()/last() depend on arrival order and are NOT safe here).
        F.min_by("price", "agg_id").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", "agg_id").alias("close"),
        F.sum("qty").alias("volume"),
        F.sum(F.col("price") * F.col("qty")).alias("quote_volume"),
        F.sum(F.when(~F.col("is_buyer_maker"), F.col("qty")).otherwise(F.lit(0.0))).alias("taker_buy_volume"),
        F.count(F.lit(1)).alias("agg_trade_count"),
        # individual trades, comparable to Binance kline "number of trades"
        F.sum(F.col("last_trade_id") - F.col("first_trade_id") + 1).alias("trade_count"),
        F.min("agg_id").alias("first_agg_id"),
        F.max("agg_id").alias("last_agg_id"),
    )
    .select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "symbol", "open", "high", "low", "close", "volume", "quote_volume",
        (F.col("quote_volume") / F.col("volume")).alias("vwap"),
        "taker_buy_volume", "agg_trade_count", "trade_count", "first_agg_id", "last_agg_id",
        F.current_timestamp().alias("processed_at"),
        F.date_format(F.col("w.start"), "yyyy-MM-dd").alias("dt"),
        F.hour(F.col("w.start")).alias("hr"),
    )
)

candles_q = (
    candles.writeStream.format("parquet")
    .queryName("candles")
    .option("path", f"{LAKE}/candles")
    .option("checkpointLocation", f"{LAKE}/_checkpoints/candles")
    .partitionBy("dt", "hr")
    .outputMode("append")
    .trigger(processingTime=TRIGGER)
    .start()
)

# ---------- query 3: anomalies ----------
# Why a separate query that re-reads candles from HDFS instead of chaining on the candles stream:
#  - Spark doesn't allow a custom stateful step (rolling 60-min baseline) after a streaming
#    aggregation in append mode.
#  - Re-reading committed candles keeps ONE scoring function for live and backtest (batch window
#    functions), and the file source only sees files the candles sink has committed.
# Cost: detection lags candle output by one trigger (about 1 min) -> about 4-5 min after the minute ends.
candle_schema = T.StructType([
    T.StructField("window_start", T.TimestampType()), T.StructField("window_end", T.TimestampType()),
    T.StructField("symbol", T.StringType()),
    *[T.StructField(c, T.DoubleType()) for c in
      ("open", "high", "low", "close", "volume", "quote_volume", "vwap", "taker_buy_volume")],
    *[T.StructField(c, T.LongType()) for c in ("agg_trade_count", "trade_count", "first_agg_id", "last_agg_id")],
    T.StructField("processed_at", T.TimestampType()),
    T.StructField("dt", T.StringType()), T.StructField("hr", T.IntegerType()),
])


def score_batch(batch_df, batch_id):
    keys = batch_df.select("symbol", "window_start").distinct().cache()
    n_new = keys.count()
    if n_new == 0:
        keys.unpersist()
        return
    lo, hi = keys.agg(F.min("window_start"), F.max("window_start")).first()
    lo = lo - timedelta(minutes=AR.LOOKBACK_MIN)
    days = sorted({(lo + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((hi - lo).days + 2)})
    hist = (spark.read.parquet(f"{LAKE}/candles")
            .filter(F.col("dt").isin(days))
            .filter((F.col("window_start") >= F.lit(lo)) & (F.col("window_start") <= F.lit(hi))))
    scored = AR.score(hist).join(keys, ["symbol", "window_start"], "inner")
    anoms = AR.to_anomalies(scored).cache()
    n = anoms.count()
    if n:
        # At-least-once: a replayed batch can write an event twice. Readers dedupe on
        # (symbol, detected_minute); step 7 compaction removes duplicates for good.
        anoms.write.mode("append").partitionBy("dt", "hr").parquet(ANOMALIES_PATH)
        (anoms.select(F.col("symbol").alias("key"), F.to_json(F.struct(*anoms.columns)).alias("value"))
         .write.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("topic", ANOMALIES_TOPIC).save())
        for r in anoms.select("detected_minute", "symbol", "reason").collect():
            print(f"[ANOMALY] {r['detected_minute']} {r['symbol']}: {r['reason']}", flush=True)
    print(f"[anomalies] batch={batch_id} candles={n_new} new_events={n}", flush=True)
    anoms.unpersist()
    keys.unpersist()


anomalies_q = (
    spark.readStream.schema(candle_schema)
    .option("maxFilesPerTrigger", 500)
    .parquet(f"{LAKE}/candles")
    .writeStream.queryName("anomalies")
    .foreachBatch(score_batch)
    .option("checkpointLocation", ANOMALIES_CKPT)
    .trigger(processingTime=TRIGGER)
    .start()
)
print(f"anomaly rule: {AR.params()}  output: {ANOMALIES_PATH} + topic {ANOMALIES_TOPIC}", flush=True)

print(f"running: raw={raw_q.id} candles={candles_q.id} anomalies={anomalies_q.id} "
      f"watermark={WATERMARK} trigger={TRIGGER}", flush=True)
spark.streams.awaitAnyTermination()

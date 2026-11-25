"""
Step 2 acceptance check (batch job, run in local mode inside spark-master).

For the last N finalized minutes it checks:
  1. Stream vs batch: recompute candles from raw/ with the same logic and compare to candles/.
     Minutes present in raw-batch but missing in stream = trades that arrived after the watermark.
  2. Stream vs Binance: compare against official 1m klines from the Binance REST API
     (OHLC must match exactly, volume within 1e-6 relative, trade count exactly).
     Minutes with an agg_id gap (trades we never received) are reported, not compared.
  3. Small files: number and average size of Parquet files per layer.

Run:
  docker compose exec spark-master /opt/spark/bin/spark-submit --master local[2] \
      --driver-memory 1g /opt/jobs/verify_candles.py --minutes 15
"""
import argparse
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--minutes", type=int, default=15)
ap.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT,DOGEUSDT,PEPEUSDT,SUIUSDT")
ap.add_argument("--rest", default=os.getenv("BINANCE_REST_BASE", "https://data-api.binance.vision"))
args = ap.parse_args()

LAKE = os.getenv("LAKE_BASE", "hdfs://namenode:8020/marketpulse")
# a window is final only after watermark (2m) + one trigger (1m); add margin
SETTLE_MIN = 4

spark = (SparkSession.builder.appName("verify-candles")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
end = now - timedelta(minutes=SETTLE_MIN)
start = end - timedelta(minutes=args.minutes)
days = sorted({(start + timedelta(minutes=i)).strftime("%Y-%m-%d") for i in range(args.minutes + 1)})
print(f"\nwindow: {start:%Y-%m-%d %H:%M} -> {end:%H:%M} UTC  ({args.minutes} finalized minutes)")


def in_range(df, col):
    return df.filter(F.col("dt").isin(days)).filter((F.col(col) >= F.lit(start)) & (F.col(col) < F.lit(end)))


# ---------- load ----------
# Completeness: Binance agg ids are contiguous per symbol, so any jump in agg_id means trades we
# never received (producer restart, network drop). A minute is complete only if
#   - its own ids are contiguous          (last - first + 1 == count)
#   - it continues the previous candle    (first == prev.last + 1)
#   - the next candle continues it        (next.first == last + 1)
# Read a wider range so the first/last minute of the window have neighbours.
all_c = spark.read.parquet(f"{LAKE}/candles")
wide_days = sorted(set(days) | {(start - timedelta(minutes=30)).strftime("%Y-%m-%d")})
wide = all_c.filter(F.col("dt").isin(wide_days)).filter(
    (F.col("window_start") >= F.lit(start - timedelta(minutes=30)))
    & (F.col("window_start") < F.lit(end + timedelta(minutes=5))))
w = Window.partitionBy("symbol").orderBy("window_start")
prev_last = F.lag("last_agg_id").over(w)
next_first = F.lead("first_agg_id").over(w)
wide = wide.withColumn(
    "complete",
    ((F.col("last_agg_id") - F.col("first_agg_id") + 1) == F.col("agg_trade_count"))
    & (prev_last.isNull() | (F.col("first_agg_id") == prev_last + 1))
    & (next_first.isNull() | (next_first == F.col("last_agg_id") + 1)),
)
stream = wide.filter((F.col("window_start") >= F.lit(start)) & (F.col("window_start") < F.lit(end))).cache()
raw = spark.read.parquet(f"{LAKE}/raw").filter(F.col("dt").isin(days))
raw_all = in_range(raw, "event_time")
invalid = raw_all.filter(~F.col("is_valid")).count()

valid = raw_all.filter("is_valid").dropDuplicates(["symbol", "agg_id"])
batch = (
    valid.groupBy(F.window("event_time", "1 minute").alias("w"), "symbol")
    .agg(
        F.min_by("price", "agg_id").alias("open"), F.max("price").alias("high"),
        F.min("price").alias("low"), F.max_by("price", "agg_id").alias("close"),
        F.sum("qty").alias("volume"), F.count(F.lit(1)).alias("agg_trade_count"),
    )
    .select(F.col("w.start").alias("window_start"), "symbol", "open", "high", "low", "close",
            "volume", "agg_trade_count")
)

n_stream = stream.count()
n_incomplete = stream.filter(~F.col("complete")).count()
print(f"stream candles: {n_stream}   symbols: {stream.select('symbol').distinct().count()}   "
      f"invalid raw rows: {invalid}")
print(f"incomplete minutes (agg_id gap): {n_incomplete}  ({100 * n_incomplete / max(n_stream, 1):.2f}%)")
if n_incomplete:
    (stream.filter(~F.col("complete")).groupBy("window_start").count()
     .orderBy("window_start").show(10, truncate=False))

# duplicates in stream output (should be impossible with append mode + checkpoint)
dups = stream.groupBy("window_start", "symbol").count().filter("count > 1").count()

# ---------- 1. stream vs batch ----------
k = ["window_start", "symbol"]
s = stream.select(*k, *[F.col(c).alias("s_" + c) for c in ["open", "high", "low", "close", "volume", "agg_trade_count"]])
b = batch.select(*k, *[F.col(c).alias("b_" + c) for c in ["open", "high", "low", "close", "volume", "agg_trade_count"]])
j = s.join(b, k, "full_outer")
only_stream = j.filter(F.col("b_open").isNull()).count()
only_batch = j.filter(F.col("s_open").isNull()).count()
both = j.filter(F.col("s_open").isNotNull() & F.col("b_open").isNotNull())
mismatch = both.filter(
    (F.col("s_open") != F.col("b_open")) | (F.col("s_high") != F.col("b_high"))
    | (F.col("s_low") != F.col("b_low")) | (F.col("s_close") != F.col("b_close"))
    | (F.abs(F.col("s_volume") - F.col("b_volume")) > 1e-9 * F.col("b_volume"))
    | (F.col("s_agg_trade_count") != F.col("b_agg_trade_count"))
)
n_mismatch = mismatch.count()
# late trades = counted in batch, missing in stream for the same minute
late_rows = both.select(F.sum(F.col("b_agg_trade_count") - F.col("s_agg_trade_count"))).first()[0] or 0

print("\n--- 1. stream vs batch (from raw/) ---")
print(f"duplicate candles:      {dups}")
print(f"only in stream:         {only_stream}")
print(f"only in batch (late):   {only_batch}")
print(f"value mismatches:       {n_mismatch}   (late trades missing from stream: {late_rows})")
if n_mismatch:
    mismatch.orderBy("window_start").show(5, truncate=False)

# ---------- 2. stream vs Binance ----------
print("\n--- 2. stream vs Binance klines ---")
start_ms = int(start.timestamp() * 1000)
end_ms = int(end.timestamp() * 1000) - 1
rows = {}
for sym in [x.strip().upper() for x in args.symbols.split(",") if x.strip()]:
    url = f"{args.rest}/api/v3/klines?symbol={sym}&interval=1m&startTime={start_ms}&endTime={end_ms}&limit=1000"
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            for k_ in json.load(r):
                rows[(sym, k_[0])] = dict(open=float(k_[1]), high=float(k_[2]), low=float(k_[3]),
                                          close=float(k_[4]), volume=float(k_[5]), trades=int(k_[8]))
    except Exception as e:  # noqa: BLE001
        print(f"  {sym}: REST failed ({e})")
    time.sleep(0.2)

ours = {
    (r["symbol"], int(r["window_start"].replace(tzinfo=timezone.utc).timestamp() * 1000)): r
    for r in stream.filter(F.col("symbol").isin(list({s_ for s_, _ in rows})))
    .select("symbol", "window_start", "open", "high", "low", "close", "volume", "trade_count", "complete").collect()
}

stats = dict(compared=0, ohlc_ok=0, vol_ok=0, trades_ok=0, trades_checked=0,
             binance_empty=0, missing_ours=0, extra_ours=0, incomplete=0, incomplete_ohlc_ok=0)
bad = []
for key, kb in rows.items():
    o = ours.get(key)
    if kb["trades"] == 0:
        stats["binance_empty"] += 1
        if o is not None:
            stats["extra_ours"] += 1
        continue
    if o is None:
        stats["missing_ours"] += 1
        bad.append((key, "missing in stream", kb))
        continue
    ohlc = all(o[c] == kb[c] for c in ("open", "high", "low", "close"))
    if not o["complete"]:
        # known data gap: expected to differ, reported separately, not counted against the pipeline
        stats["incomplete"] += 1
        stats["incomplete_ohlc_ok"] += ohlc
        continue
    stats["compared"] += 1
    vol = abs(o["volume"] - kb["volume"]) <= 1e-6 * max(kb["volume"], 1e-12)
    stats["ohlc_ok"] += ohlc
    stats["vol_ok"] += vol
    if o["trade_count"] is not None:
        stats["trades_checked"] += 1
        stats["trades_ok"] += int(o["trade_count"]) == kb["trades"]
    if not (ohlc and vol):
        bad.append((key, {c: o[c] for c in ("open", "high", "low", "close", "volume")}, kb))

c = max(stats["compared"], 1)
print(f"minutes compared:       {stats['compared']}  (symbols: {len({s_ for s_, _ in rows})})")
print(f"OHLC exact match:       {stats['ohlc_ok']}/{stats['compared']}  ({100 * stats['ohlc_ok'] / c:.1f}%)")
print(f"volume match (1e-6):    {stats['vol_ok']}/{stats['compared']}  ({100 * stats['vol_ok'] / c:.1f}%)")
print(f"trade count match:      {stats['trades_ok']}/{stats['trades_checked']}")
print(f"missing in stream:      {stats['missing_ours']}   (producer outage or late data)")
print(f"skipped incomplete:     {stats['incomplete']}   (agg_id gap; OHLC still matched on {stats['incomplete_ohlc_ok']})")
print(f"Binance empty minutes:  {stats['binance_empty']}  (we correctly emit no row: {stats['binance_empty'] - stats['extra_ours']})")
for item in bad[:5]:
    print("  diff:", item)

# ---------- 3. small files ----------
print("\n--- 3. files on HDFS (today) ---")
jvm = spark._jvm
conf = spark.sparkContext._jsc.hadoopConfiguration()
today = now.strftime("%Y-%m-%d")
for layer in ("raw", "candles"):
    p = jvm.org.apache.hadoop.fs.Path(f"{LAKE}/{layer}/dt={today}")
    fs = p.getFileSystem(conf)
    if not fs.exists(p):
        print(f"{layer:8s} no data for {today}")
        continue
    it = fs.listFiles(p, True)
    n = size = 0
    while it.hasNext():
        f = it.next()
        if f.getPath().getName().endswith(".parquet"):
            n += 1
            size += f.getLen()
    print(f"{layer:8s} files={n}  total={size / 1e6:.1f} MB  avg={size / max(n, 1) / 1e3:.1f} KB")

ok = (
    n_stream > 0 and dups == 0 and only_stream == 0 and n_mismatch == 0
    and stats["compared"] > 0
    and stats["ohlc_ok"] / c >= 0.99 and stats["vol_ok"] / c >= 0.99
    and stats["trades_ok"] == stats["trades_checked"]
)
print("\nPASS" if ok else "\nCHECK ABOVE")
spark.stop()

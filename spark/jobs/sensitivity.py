"""
Threshold sensitivity for rule z_v1, reusing the scored backtest output (no re-download, no re-scoring).

For a grid of (volume_ratio, |z|, min |ret|) it reports events/day, events per symbol per day,
the large-cap share, how many events are part of a market-wide burst (>= MARKET_N symbols in the
same 10-minute bucket), and recall on the 10/10 crash. Use it to pick thresholds, then label
events of the CHOSEN rule only.

Run after backtest.py (it reads /tmp/bt_scored inside spark-master):
  docker compose exec spark-master /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g \
      /opt/jobs/sensitivity.py --months 2026-10
--months filters which months to tune on. Tune on one month, report precision on another.
"""
import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession, Window, functions as F

import anomaly_rules as AR

ap = argparse.ArgumentParser()
ap.add_argument("--months", default="", help="comma list YYYY-MM to include (default: all scored)")
ap.add_argument("--vol", default="5,8,10")
ap.add_argument("--z", default="4,5,6")
ap.add_argument("--minret", default="0.01,0.02")
ap.add_argument("--market-n", type=int, default=8)
ap.add_argument("--out", default="/opt/out/sensitivity.txt")
args = ap.parse_args()

LARGE = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
CRASH = (datetime(2025, 10, 10, 20, 50), datetime(2025, 10, 10, 21, 45))

spark = (SparkSession.builder.appName("sensitivity").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

s = spark.read.parquet("file:///tmp/bt_scored").withColumn("month", F.date_format("window_start", "yyyy-MM"))
if args.months:
    s = s.filter(F.col("month").isin([m.strip() for m in args.months.split(",")]))
s = s.withColumn("m", (F.col("window_start").cast("long") / 60).cast("long"))

grid = [(float(v), float(z), float(r)) for v in args.vol.split(",") for z in args.z.split(",")
        for r in args.minret.split(",")]

# Day/symbol counts and crash coverage come from the full scored set (cheap aggregations).
n_days = s.select(F.to_date("window_start")).distinct().count()
n_sym = s.select("symbol").distinct().count()
crash_syms = (s.filter((F.col("window_start") >= F.lit(CRASH[0])) & (F.col("window_start") <= F.lit(CRASH[1])))
              .select("symbol").distinct().count())

# Episode logic only needs the minutes that trigger. Every grid cell triggers on a SUBSET of the rows
# that pass the loosest cell, so filter to that superset first (thousands of rows, not millions) and
# run the cooldown windows on it. Running 18 window columns over 1.7M rows OOMs a 1 GB driver
# (BUILD_LOG 4.6).
vmin = min(g[0] for g in grid); zmin = min(g[1] for g in grid); rmin = min(g[2] for g in grid)
cand = (s.filter(F.col("eligible") & (F.col("vol_ratio") >= vmin) & (F.abs(F.col("z_ret")) >= zmin)
                 & (F.abs(F.col("ret_5m")) >= rmin))
        .select("symbol", "window_start", "m", "vol_ratio", "z_ret", "ret_5m"))
cand.write.mode("overwrite").parquet("file:///tmp/bt_cand")
cand = spark.read.parquet("file:///tmp/bt_cand")
print(f"candidate minutes (loosest cell): {cand.count():,}", flush=True)

w = Window.partitionBy("symbol").orderBy("m")
cool = w.rangeBetween(-AR.COOLDOWN_MIN, -1)
for i, (v, z, r) in enumerate(grid):
    cand = cand.withColumn(f"t{i}", (F.col("vol_ratio") >= v) & (F.abs(F.col("z_ret")) >= z) & (F.abs(F.col("ret_5m")) >= r))
for i in range(len(grid)):
    cand = cand.withColumn(f"e{i}", F.col(f"t{i}") & (F.coalesce(F.max(F.col(f"t{i}").cast("int")).over(cool), F.lit(0)) == 0))
ev = cand.select("symbol", "window_start", (F.col("m") / 10).cast("long").alias("bucket"),
                 *[F.col(f"e{i}") for i in range(len(grid))]).cache()

lines = [f"sensitivity  months={args.months or 'all'}  days={n_days}  symbols={n_sym}  market-wide = >= {args.market_n} symbols in one 10-min bucket",
         f"{'vol>=':>6} {'|z|>=':>6} {'|ret|>=':>8} {'events':>7} {'per_day':>8} {'sym/day':>8} {'large%':>7} {'mkt_wide%':>9} {'crash':>7}"]
for i, (v, z, r) in enumerate(grid):
    e = ev.filter(F.col(f"e{i}"))
    n = e.count()
    large = e.filter(F.col("symbol").isin(LARGE)).count()
    busy = e.groupBy("bucket").count().filter(F.col("count") >= args.market_n).select("bucket")
    mkt = e.join(busy, "bucket").count()
    crash = (e.filter((F.col("window_start") >= F.lit(CRASH[0])) & (F.col("window_start") <= F.lit(CRASH[1])))
             .select("symbol").distinct().count())
    crash_s = f"{crash}/{crash_syms}" if crash_syms else "-"
    lines.append(f"{v:>6g} {z:>6g} {r * 100:>7g}% {n:>7} {n / max(n_days, 1):>8.1f} {n / max(n_days * n_sym, 1):>8.3f} "
                 f"{100 * large / max(n, 1):>6.1f}% {100 * mkt / max(n, 1):>8.1f}% {crash_s:>7}")
lines.append(f"(large caps are {100 * len(LARGE) / n_sym:.1f}% of symbols: a large% near that = no size bias)")
text = "\n".join(lines)
os.makedirs(os.path.dirname(args.out), exist_ok=True)
with open(args.out, "w") as f:
    f.write(text + "\n")
print("\n" + text)
spark.stop()

"""
Step 4 backtest: run the live anomaly rule (anomaly_rules.score) over historical 1-minute klines
from the official Binance public archive (data.binance.vision) and produce material for evaluation.

Outputs (in --out, mounted to ./data/backtest on the host):
  summary.txt            counts, events per day, large vs small caps, rule overlap, known-event recall
  events_z.csv           every z_v1 event with context
  events_fixed.csv       every baseline-rule event (vol >= 5x AND |ret| >= 4%)
  label_z.csv            random sample of z_v1 events to hand-label (fill the `label` column: 1 or 0)
  label_fixed.csv        same for the baseline rule
Then:  python3 /opt/jobs/precision.py /opt/out/label_z.csv /opt/out/label_fixed.csv

Run (local mode inside spark-master; the cluster worker is busy with the stream):
  docker compose exec spark-master /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g \
      /opt/jobs/backtest.py --months 2025-10,2026-10
"""
import argparse
import csv
import io
import os
import random
import urllib.error
import urllib.request
import zipfile
from datetime import datetime

from pyspark.sql import SparkSession, Window, functions as F, types as T

import anomaly_rules as AR

DEFAULT_SYMBOLS = (
    "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,TRXUSDT,AVAXUSDT,LINKUSDT,DOTUSDT,LTCUSDT,"
    "BCHUSDT,UNIUSDT,ATOMUSDT,XLMUSDT,ETCUSDT,NEARUSDT,APTUSDT,ARBUSDT,OPUSDT,FILUSDT,ICPUSDT,INJUSDT,"
    "SUIUSDT,SEIUSDT,TIAUSDT,AAVEUSDT,PEPEUSDT,SHIBUSDT,WIFUSDT,FETUSDT,RENDERUSDT,HBARUSDT,TONUSDT,"
    "WLDUSDT,BONKUSDT,FLOKIUSDT,ENAUSDT,POLUSDT"
)
LARGE_CAPS = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}
ARCHIVE = "https://data.binance.vision/data/spot/monthly/klines/{s}/1m/{s}-1m-{m}.zip"

ap = argparse.ArgumentParser()
ap.add_argument("--months", default="2025-10,2026-10", help="comma list YYYY-MM")
ap.add_argument("--symbols", default=DEFAULT_SYMBOLS)
ap.add_argument("--out", default="/opt/out")
ap.add_argument("--cache", default="/tmp/klines")
ap.add_argument("--known", default="/opt/jobs/known_events.csv")
ap.add_argument("--sample", type=int, default=40)
ap.add_argument("--seed", type=int, default=42)
ap.add_argument("--label-months", default="2025-10",
                help="label samples are drawn only from these months (held out from threshold tuning)")
args = ap.parse_args()

months = [m.strip() for m in args.months.split(",") if m.strip()]
symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
os.makedirs(args.cache, exist_ok=True)
os.makedirs(args.out, exist_ok=True)

# ---------- 1. download (cached) ----------
missing = []
for s in symbols:
    for m in months:
        path = os.path.join(args.cache, f"{s}-1m-{m}.csv")
        if os.path.exists(path):
            continue
        try:
            with urllib.request.urlopen(ARCHIVE.format(s=s, m=m), timeout=60) as r:
                z = zipfile.ZipFile(io.BytesIO(r.read()))
            with z.open(z.namelist()[0]) as src, open(path, "wb") as dst:
                dst.write(src.read())
            print(f"downloaded {s} {m}", flush=True)
        except urllib.error.HTTPError as e:
            missing.append(f"{s} {m} (HTTP {e.code})")
        except Exception as e:  # noqa: BLE001
            missing.append(f"{s} {m} ({e})")
if missing:
    print("not available:", ", ".join(missing))

spark = (SparkSession.builder.appName("backtest")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------- 2. load klines into the live candle shape ----------
cols = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume",
        "trades", "taker_buy_base", "taker_buy_quote", "ignore"]
raw = (spark.read.schema(T.StructType([T.StructField(c, T.StringType()) for c in cols]))
       .csv(f"file://{args.cache}/*.csv")
       .withColumn("file", F.input_file_name()))
ot = F.col("open_time").cast("long")
klines = (
    raw.filter(F.col("open_time").rlike("^[0-9]+$"))            # skip a header line if present
    .withColumn("symbol", F.regexp_extract("file", r"/([A-Z0-9]+)-1m-", 1))
    .filter(F.col("symbol").isin(symbols))
    # Binance spot archive switched to MICROsecond timestamps from 2025-01-01
    .withColumn("ms", F.when(ot > F.lit(10**14), (ot / 1000).cast("long")).otherwise(ot))
    .withColumn("window_start", F.expr("timestamp_millis(ms)"))
    .select("symbol", "window_start",
            *[F.col(c).cast("double").alias(c) for c in ("open", "high", "low", "close", "volume", "quote_volume")],
            F.col("trades").cast("long").alias("trade_count"))
    .filter(F.col("trade_count") > 0)                           # live candles have no empty minutes
    .dropDuplicates(["symbol", "window_start"])
)

scored = AR.score(klines)
w = Window.partitionBy("symbol").orderBy("m")
scored = (
    scored.withColumn("close_60m_after", F.first("close", ignorenulls=True).over(w.rangeBetween(55, 65)))
    .withColumn("ret_60m_after_pct", F.round((F.col("close_60m_after") / F.col("close") - 1) * 100, 2))
    .withColumn("ret_60m_before_pct",
                F.round((F.col("close") / F.first("close", ignorenulls=True).over(w.rangeBetween(-65, -55)) - 1) * 100, 2))
    .withColumn("day", F.to_date("window_start"))
)
# Materialize once to local disk instead of cache(): millions of rows x many columns don't fit in
# a 1 GB driver, and every later step (counts, events, recall) would otherwise recompute all windows.
KEEP = ["window_start", "symbol", "day", "ret_5m", "z_ret", "vol_ratio", "qv5", "close", "close_ref", "sigma_1m",
        "ret_60m_before_pct", "ret_60m_after_pct", "eligible", "new_event", "new_event_fixed", "scope", "market_breadth"]
scored.select(*KEEP).write.mode("overwrite").parquet("file:///tmp/bt_scored")
scored = spark.read.parquet("file:///tmp/bt_scored")

n_candles = scored.count()
n_eligible = scored.filter("eligible").count()
n_days = scored.select("day").distinct().count()
n_syms = scored.select("symbol").distinct().count()

EV_COLS = ["window_start", "symbol", "ret_5m", "z_ret", "vol_ratio", "qv5", "close", "close_ref",
           "sigma_1m", "ret_60m_before_pct", "ret_60m_after_pct", "scope", "market_breadth"]


def collect(flag):
    rows = (scored.filter(F.col(flag)).select(*EV_COLS).orderBy("window_start", "symbol").collect())
    return [r.asDict() for r in rows]


ev_z = collect("new_event")
ev_f = collect("new_event_fixed")


def fmt(r):
    return {
        "detected_minute_utc": r["window_start"].strftime("%Y-%m-%d %H:%M"),
        "symbol": r["symbol"],
        "price_change_5m_pct": round(r["ret_5m"] * 100, 3),
        "z_score": round(r["z_ret"], 2),
        "volume_ratio": round(r["vol_ratio"], 2),
        "usdt_volume_5m": round(r["qv5"]),
        "price": r["close"],
        "price_5m_before": r["close_ref"],
        "volatility_1m_pct": round((r["sigma_1m"] or 0) * 100, 4),
        "ret_60m_before_pct": r["ret_60m_before_pct"],
        "ret_60m_after_pct": r["ret_60m_after_pct"],
        "scope": r["scope"],
        "market_breadth": r["market_breadth"],
    }


def write_csv(path, rows, extra=None):
    rows = [dict(fmt(r), **(extra or {})) for r in rows]
    with open(path, "w", newline="") as f:
        if not rows:
            f.write("no events\n")
            return
        wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        wr.writeheader()
        wr.writerows(rows)


write_csv(os.path.join(args.out, "events_z.csv"), ev_z)
write_csv(os.path.join(args.out, "events_fixed.csv"), ev_f)
rnd = random.Random(args.seed)
label_months = {m.strip() for m in args.label_months.split(",") if m.strip()}


def held_out(ev):
    return [r for r in ev if r["window_start"].strftime("%Y-%m") in label_months]


# Coin-scope events are labelled one by one. Market-wide bursts are labelled per burst (one row per
# 10-min bucket, the event with the largest |z| as representative), because that is how the feed shows them.
coin_z = [r for r in held_out(ev_z) if r["scope"] == "coin"]
bursts = {}
for r in held_out(ev_z):
    if r["scope"] == "market":
        b = int(r["window_start"].timestamp() // 600)
        if b not in bursts or abs(r["z_ret"]) > abs(bursts[b]["z_ret"]):
            bursts[b] = r
write_csv(os.path.join(args.out, "label_z.csv"), rnd.sample(coin_z, min(args.sample, len(coin_z))),
          {"label": "", "notes": ""})
write_csv(os.path.join(args.out, "label_market.csv"),
          rnd.sample(list(bursts.values()), min(args.sample // 2, len(bursts))), {"label": "", "notes": ""})
write_csv(os.path.join(args.out, "label_fixed.csv"),
          rnd.sample(held_out(ev_f), min(args.sample, len(held_out(ev_f)))), {"label": "", "notes": ""})

# ---------- 3. summary ----------
lines = []
P = lines.append
P(f"MarketPulse backtest  {datetime.utcnow():%Y-%m-%d %H:%M} UTC")
P(f"rule params: {AR.params()}")
P(f"months: {months}   symbols: {n_syms}/{len(symbols)}   days: {n_days}   candles: {n_candles:,}   "
  f"eligible: {n_eligible:,} ({100 * n_eligible / max(n_candles, 1):.1f}%)")
if missing:
    P(f"not in archive: {', '.join(missing)}")
P("")


def block(name, ev):
    by_sym = {}
    for r in ev:
        by_sym[r["symbol"]] = by_sym.get(r["symbol"], 0) + 1
    large = sum(v for k, v in by_sym.items() if k in LARGE_CAPS)
    P(f"== {name}: {len(ev)} events  ({len(ev) / max(n_days, 1):.1f}/day, "
      f"{len(ev) / max(n_days * n_syms, 1):.3f}/symbol/day)")
    P(f"   large caps (BTC/ETH/BNB/SOL/XRP): {large}   others: {len(ev) - large}   "
      f"symbols with >=1 event: {len(by_sym)}/{n_syms}")
    P("   top symbols: " + ", ".join(f"{k} {v}" for k, v in sorted(by_sym.items(), key=lambda x: -x[1])[:10]))
    up = sum(1 for r in ev if r["ret_5m"] > 0)
    P(f"   up {up} / down {len(ev) - up}")
    coin = [r for r in ev if r["scope"] == "coin"]
    nb = len({int(r["window_start"].timestamp() // 600) for r in ev if r["scope"] == "market"})
    P(f"   scope: coin {len(coin)} ({len(coin) / max(n_days, 1):.1f}/day)   market-wide rows {len(ev) - len(coin)} "
      f"in ~{nb} bursts ({nb / max(n_days, 1):.1f}/day)   -> feed items/day ~{(len(coin) + nb) / max(n_days, 1):.1f}")
    lc = sum(1 for r in coin if r["symbol"] in LARGE_CAPS)
    P(f"   coin-scope large-cap share: {100 * lc / max(len(coin), 1):.1f}%")
    P("")


block(f"{AR.RULE_VERSION} (per-symbol z-score)", ev_z)
block("baseline (vol>=5x AND |ret|>=4%)", ev_f)

# Overlap with a tolerance: both rules can catch the same episode at different minutes
# (z_v1 usually fires earlier, before the move reaches 4%). Exact-minute matching undercounts.
TOL = AR.COOLDOWN_MIN * 60


def near(r, others):
    return any(o["symbol"] == r["symbol"] and abs((o["window_start"] - r["window_start"]).total_seconds()) <= TOL
               for o in others)


fz_both = sum(1 for r in ev_f if near(r, ev_z))
z_both = sum(1 for r in ev_z if near(r, ev_f))
P(f"== overlap (same symbol within {AR.COOLDOWN_MIN} min): baseline events also caught by {AR.RULE_VERSION}: "
  f"{fz_both}/{len(ev_f)}   {AR.RULE_VERSION} events also caught by baseline: {z_both}/{len(ev_z)}")
P("")

# known events recall
if os.path.exists(args.known):
    P("== known events (recall): detected = any new event for the symbol inside [start, end]")
    with open(args.known) as f:
        known = [r for r in csv.DictReader(f) if r.get("start_utc") and not r["start_utc"].startswith("#")]
    for k in known:
        st = datetime.strptime(k["start_utc"], "%Y-%m-%d %H:%M")
        en = datetime.strptime(k["end_utc"], "%Y-%m-%d %H:%M")
        syms = symbols if k["symbol"] in ("*", "ALL") else [k["symbol"]]
        present = {r["symbol"] for r in scored.filter((F.col("window_start") >= F.lit(st)) & (F.col("window_start") <= F.lit(en)))
                   .select("symbol").distinct().collect()}
        syms = [s for s in syms if s in present]
        if not syms:
            P(f"   [no data] {k['description']} ({k['start_utc']}..{k['end_utc']})")
            continue

        def hit(ev):
            return {r["symbol"] for r in ev if st <= r["window_start"] <= en and r["symbol"] in syms}
        hz, hf = hit(ev_z), hit(ev_f)
        P(f"   {k['description']} ({k['start_utc']}..{k['end_utc']} UTC, {len(syms)} symbols)")
        P(f"      {AR.RULE_VERSION}: {len(hz)}/{len(syms)} symbols flagged   baseline: {len(hf)}/{len(syms)}")
        missed = sorted(set(syms) - hz)
        if missed and len(syms) > 1:
            P(f"      {AR.RULE_VERSION} missed: {', '.join(missed[:15])}{' ...' if len(missed) > 15 else ''}")
    P("")

P(f"Label samples drawn from held-out months {sorted(label_months)}: label_z.csv (coin-scope events),")
P("label_market.csv (market-wide bursts), label_fixed.csv (baseline rule). Fill `label` (1/0) per")
P("docs/ANOMALY_EVALUATION.md, then run precision.py on all three.")
text = "\n".join(lines)
with open(os.path.join(args.out, "summary.txt"), "w") as f:
    f.write(text + "\n")
print("\n" + text)
spark.stop()

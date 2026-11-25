"""
MarketPulse anomaly detector, rule z_v2 (z_v1 + tuned thresholds + market-wide scope).

ONE function, `score()`, used by BOTH the live stream (candles_stream.py, query 3)
and the offline backtest (backtest.py). Whatever the backtest measures is exactly what runs live.

Input: a DataFrame of 1-minute candles with at least
    symbol, window_start, close, quote_volume
  and optionally (live data only; archive klines don't have them)
    first_agg_id, last_agg_id, agg_trade_count

For every candle t of every symbol, with W = 5-minute signal window and B = 60-minute baseline
that ends where the signal window starts (no overlap, so a spike can't inflate its own baseline):

    signal window    [t-4 .. t]        5 minutes
    baseline window  [t-64 .. t-5]     60 minutes

    vol_ratio = (USDT volume per minute in signal window) / (USDT volume per minute in baseline)
                Missing minutes count as zero volume, which is correct for volume.
    ret_5m    = close_t / close_ref - 1, close_ref = last close at or before t-5 (within baseline)
    sigma_1m  = stddev of 1-minute log returns in the baseline   ("rolling volatility")
    z_ret     = ln(close_t / close_ref) / (max(sigma_1m, SIGMA_FLOOR) * sqrt(W))

    anomaly (z_v1)  = eligible AND vol_ratio >= VOL_RATIO AND |z_ret| >= Z_RET AND |ret_5m| >= MIN_ABS_RET
    baseline rule   = eligible AND vol_ratio >= 5         AND |ret_5m| >= 4%     (original project rule)

    eligible = baseline has >= MIN_BASE_MINUTES candles
               AND all candles in the signal window are complete (no agg_id gap)
               AND signal-window volume >= MIN_QV5_USDT

Why z instead of a fixed %: a fixed 4% almost never fires on BTC and fires on noise for
micro-priced coins (PEPE: 1 price tick = 0.29%). Dividing by each symbol's own recent
volatility puts every coin on the same scale. MIN_ABS_RET keeps tiny-but-unusual moves on
very calm coins out of a retail-facing feed.

An event can stay "on" for several consecutive minutes; `new_event` marks only the first
minute of a run (no trigger in the previous COOLDOWN_MIN minutes). Live output and
precision are counted on new events.

Scope (z_v2): on 2026-10, 57-69% of triggers were part of a market-wide burst (>= 8 symbols in
the same 10 minutes), e.g. the whole market dropping together. For a user that is ONE event, not
twenty. Every trigger gets market_breadth = number of distinct symbols triggering in the last
MARKET_WINDOW_MIN minutes (trailing, so live and backtest agree), and
scope = 'market' if breadth >= MARKET_N else 'coin'. The dashboard groups 'market' rows.
Limitation: the first few coins of a burst fire before breadth reaches MARKET_N and stay 'coin'.

Thresholds z_v2 were chosen on 2026-10 (BUILD_LOG 4.7): vol >= 8x, |z| >= 6, |ret| >= 1%.
"""
import os

from pyspark.sql import Window, functions as F

RULE_VERSION = "z_v2"

W = int(os.getenv("AD_WINDOW_MIN", "5"))
B = int(os.getenv("AD_BASELINE_MIN", "60"))
VOL_RATIO = float(os.getenv("AD_VOL_RATIO", "8"))
Z_RET = float(os.getenv("AD_Z_RET", "6"))
MIN_ABS_RET = float(os.getenv("AD_MIN_ABS_RET", "0.01"))       # 1%
SIGMA_FLOOR = float(os.getenv("AD_SIGMA_FLOOR", "0.0005"))      # 0.05% per minute
MIN_BASE_MINUTES = int(os.getenv("AD_MIN_BASE_MINUTES", "30"))
MIN_QV5_USDT = float(os.getenv("AD_MIN_QV5_USDT", "20000"))
COOLDOWN_MIN = int(os.getenv("AD_COOLDOWN_MIN", "15"))
MARKET_N = int(os.getenv("AD_MARKET_N", "8"))
MARKET_WINDOW_MIN = int(os.getenv("AD_MARKET_WINDOW_MIN", "10"))

# how much history score() needs before the first minute it should judge
LOOKBACK_MIN = B + W + COOLDOWN_MIN + 5


def params() -> dict:
    return dict(rule=RULE_VERSION, window_min=W, baseline_min=B, vol_ratio=VOL_RATIO, z_ret=Z_RET,
                min_abs_ret=MIN_ABS_RET, sigma_floor=SIGMA_FLOOR, min_base_minutes=MIN_BASE_MINUTES,
                min_qv5_usdt=MIN_QV5_USDT, cooldown_min=COOLDOWN_MIN, market_n=MARKET_N,
                market_window_min=MARKET_WINDOW_MIN)


def score(candles):
    c = candles
    for col in ("first_agg_id", "last_agg_id", "agg_trade_count"):
        if col not in c.columns:
            c = c.withColumn(col, F.lit(None).cast("long"))

    c = c.withColumn("m", (F.col("window_start").cast("long") / 60).cast("long"))
    w = Window.partitionBy("symbol").orderBy("m")
    sig = w.rangeBetween(-(W - 1), 0)
    base = w.rangeBetween(-(B + W - 1), -W)

    c = (
        c.withColumn("prev_close", F.lag("close").over(w))
        .withColumn("prev_last_agg", F.lag("last_agg_id").over(w))
        .withColumn("r1", F.log(F.col("close") / F.col("prev_close")))
        # completeness from agg_id continuity (BUILD_LOG 2.8); archive data has no ids -> complete
        .withColumn(
            "complete",
            F.coalesce(F.col("last_agg_id") - F.col("first_agg_id") + 1 == F.col("agg_trade_count"), F.lit(True))
            & F.coalesce(F.col("first_agg_id") == F.col("prev_last_agg") + 1, F.lit(True)),
        )
    )

    c = (
        c.withColumn("qv5", F.sum("quote_volume").over(sig))
        .withColumn("incomplete_in_window", F.sum(F.when(~F.col("complete"), 1).otherwise(0)).over(sig))
        .withColumn("base_qv", F.sum("quote_volume").over(base))
        .withColumn("base_minutes", F.count(F.lit(1)).over(base))
        .withColumn("sigma_1m", F.stddev_samp("r1").over(base))
        .withColumn("close_ref", F.last("close", ignorenulls=True).over(base))
    )

    sigma_eff = F.greatest(F.coalesce(F.col("sigma_1m"), F.lit(SIGMA_FLOOR)), F.lit(SIGMA_FLOOR))
    c = (
        c.withColumn("vol_ratio", (F.col("qv5") / W) / (F.col("base_qv") / B))
        .withColumn("ret_5m", F.col("close") / F.col("close_ref") - 1)
        .withColumn("z_ret", F.log(F.col("close") / F.col("close_ref")) / (sigma_eff * F.sqrt(F.lit(float(W)))))
        .withColumn(
            "eligible",
            F.coalesce(
                (F.col("base_minutes") >= MIN_BASE_MINUTES)
                & (F.col("incomplete_in_window") == 0)
                & (F.col("qv5") >= MIN_QV5_USDT)
                & F.col("close_ref").isNotNull()
                & (F.col("base_qv") > 0),
                F.lit(False),
            ),
        )
    )

    trig = (
        F.col("eligible")
        & (F.col("vol_ratio") >= VOL_RATIO)
        & (F.abs(F.col("z_ret")) >= Z_RET)
        & (F.abs(F.col("ret_5m")) >= MIN_ABS_RET)
    )
    trig_fixed = F.col("eligible") & (F.col("vol_ratio") >= 5) & (F.abs(F.col("ret_5m")) >= 0.04)
    c = c.withColumn("trig", F.coalesce(trig, F.lit(False))).withColumn("trig_fixed", F.coalesce(trig_fixed, F.lit(False)))

    cool = w.rangeBetween(-COOLDOWN_MIN, -1)
    c = (
        c.withColumn("prev_trig", F.max(F.col("trig").cast("int")).over(cool))
        .withColumn("prev_trig_fixed", F.max(F.col("trig_fixed").cast("int")).over(cool))
        .withColumn("new_event", F.col("trig") & (F.coalesce(F.col("prev_trig"), F.lit(0)) == 0))
        .withColumn("new_event_fixed", F.col("trig_fixed") & (F.coalesce(F.col("prev_trig_fixed"), F.lit(0)) == 0))
    )

    # market breadth: distinct symbols with a trigger in [m-(MW-1), m]. Computed on trigger rows
    # only (few), expanded to the minutes they cover, so there is no window over all symbols.
    span = F.sequence(F.col("m"), F.col("m") + (MARKET_WINDOW_MIN - 1))
    breadth = (c.filter("trig").select("symbol", F.explode(span).alias("m"))
               .groupBy("m").agg(F.countDistinct("symbol").alias("market_breadth")))
    c = (c.join(breadth, "m", "left")
         .withColumn("market_breadth", F.coalesce(F.col("market_breadth"), F.lit(0)))
         .withColumn("scope", F.when(F.col("market_breadth") >= MARKET_N, F.lit("market")).otherwise(F.lit("coin"))))
    return c


def to_anomalies(scored, flag="new_event"):
    """Shape scored rows into the published anomaly record (Hive table `anomalies`, Kafka topic)."""
    direction = F.when(F.col("ret_5m") > 0, F.lit("up")).otherwise(F.lit("down"))
    reason = F.format_string(
        "Price %s%.2f%% in %d min (%.1f sigma of its normal moves) on %.1fx its normal trading volume",
        F.when(F.col("ret_5m") > 0, F.lit("+")).otherwise(F.lit("")),
        F.col("ret_5m") * 100, F.lit(W), F.abs(F.col("z_ret")), F.col("vol_ratio"),
    )
    return (
        scored.filter(F.col(flag))
        .select(
            F.col("window_start").alias("detected_minute"),
            "symbol",
            direction.alias("direction"),
            F.round(F.col("ret_5m") * 100, 3).alias("price_change_pct"),
            F.round(F.col("z_ret"), 2).alias("z_score"),
            F.round(F.col("vol_ratio"), 2).alias("volume_ratio"),
            F.round(F.col("qv5"), 0).alias("quote_volume_5m"),
            F.col("close").alias("price"),
            F.col("close_ref").alias("price_before"),
            F.round(F.col("sigma_1m") * 100, 4).alias("volatility_1m_pct"),
            F.when(F.col("scope") == "market",
                   F.concat(reason, F.format_string(" (part of a market-wide move: %d coins in %d min)",
                                                    F.col("market_breadth"), F.lit(MARKET_WINDOW_MIN))))
            .otherwise(reason).alias("reason"),
            F.col("scope"),
            F.col("market_breadth").cast("int").alias("market_breadth"),
            F.lit(RULE_VERSION).alias("rule_version"),
            F.current_timestamp().alias("detected_at"),
            F.date_format("window_start", "yyyy-MM-dd").alias("dt"),
            F.hour("window_start").alias("hr"),
        )
    )

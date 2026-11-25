"""
Live state for the dashboard, fed straight from Kafka (no Hive in the hot path).

One background thread per Streamlit server (st.cache_resource), shared by all browser sessions:
  - `trades`: starts at the offsets for (now - LIVE_WINDOW_MIN) and rebuilds 1-minute candles in
    memory, then keeps the current minute updating live. Same candle logic as Spark
    (open/close by agg_id), so the live chart matches candles_1m once Spark writes them.
  - `anomalies`: read from the beginning of the topic (7-day retention), deduplicated on
    (symbol, detected_minute) because the detector is at-least-once.
"""
import json
import os
import threading
import time
from collections import defaultdict

import pandas as pd
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TRADES = os.getenv("TRADES_TOPIC", "trades")
ANOMALIES = os.getenv("ANOMALIES_TOPIC", "anomalies")
WINDOW_MIN = int(os.getenv("LIVE_WINDOW_MIN", "180"))


class LiveState:
    def __init__(self):
        self.lock = threading.Lock()
        # candles[symbol][minute_ms] = [open, high, low, close, volume, quote_volume, n, first_agg, last_agg]
        self.candles = defaultdict(dict)
        self.anoms = {}                      # (symbol, detected_minute) -> record
        self.last_trade_ms = 0
        self.msgs = 0
        self.rate = 0.0
        self.started = time.time()
        self.caught_up = False
        self.error = None
        threading.Thread(target=self._run, daemon=True, name="kafka-live").start()

    # ---------------- consumer ----------------
    def _run(self):
        while True:
            try:
                self._consume()
            except Exception as e:  # noqa: BLE001  keep the dashboard alive, retry
                self.error = f"{type(e).__name__}: {e}"
                time.sleep(5)

    def _consume(self):
        c = Consumer({
            "bootstrap.servers": KAFKA,
            "group.id": f"dashboard-{os.getpid()}-{int(time.time())}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        })
        md = c.list_topics(timeout=10)
        start_ms = int((time.time() - WINDOW_MIN * 60) * 1000)
        tps = [TopicPartition(TRADES, p, start_ms) for p in md.topics[TRADES].partitions]
        tps = c.offsets_for_times(tps, timeout=10)
        if ANOMALIES in md.topics:
            tps += [TopicPartition(ANOMALIES, p, OFFSET_BEGINNING) for p in md.topics[ANOMALIES].partitions]
        c.assign(tps)
        self.error = None
        t_rate, n_rate = time.time(), 0
        while True:
            batch = c.consume(num_messages=2000, timeout=0.5)
            with self.lock:
                for m in batch:
                    if m.error():
                        continue
                    try:
                        v = json.loads(m.value())
                    except ValueError:
                        continue
                    if m.topic() == TRADES:
                        self._add_trade(v)
                    else:
                        self.anoms[(v.get("symbol"), v.get("detected_minute"))] = v
                n_rate += len(batch)
                self.msgs += len(batch)
                if time.time() - t_rate >= 5:
                    self.rate = n_rate / (time.time() - t_rate)
                    t_rate, n_rate = time.time(), 0
                    self._prune()
                self.caught_up = self.last_trade_ms > 0 and time.time() * 1000 - self.last_trade_ms < 15000

    def _add_trade(self, v):
        try:
            sym, aid, px, q, ts = v["symbol"], v["agg_id"], float(v["price"]), float(v["qty"]), int(v["trade_ts"])
        except (KeyError, TypeError, ValueError):
            return
        if px <= 0 or q <= 0:
            return
        minute = ts - ts % 60000
        c = self.candles[sym].get(minute)
        if c is None:
            self.candles[sym][minute] = [px, px, px, px, q, px * q, 1, aid, aid]
        else:
            if aid < c[7]:
                c[0], c[7] = px, aid
            if aid > c[8]:
                c[3], c[8] = px, aid
            c[1] = max(c[1], px)
            c[2] = min(c[2], px)
            c[4] += q
            c[5] += px * q
            c[6] += 1
        self.last_trade_ms = max(self.last_trade_ms, ts)

    def _prune(self):
        cutoff = int((time.time() - (WINDOW_MIN + 5) * 60) * 1000)
        for sym in self.candles:
            for k in [k for k in self.candles[sym] if k < cutoff]:
                del self.candles[sym][k]

    # ---------------- readers (copy under lock, build frames outside) ----------------
    def symbols(self):
        with self.lock:
            return sorted(self.candles)

    def candles_df(self, sym):
        with self.lock:
            rows = [(k, *v[:7]) for k, v in self.candles.get(sym, {}).items()]
        df = pd.DataFrame(rows, columns=["ms", "open", "high", "low", "close", "volume", "quote_volume", "trades"])
        if df.empty:
            return df
        df["window_start"] = pd.to_datetime(df["ms"], unit="ms")
        return df.sort_values("window_start").reset_index(drop=True)

    def overview(self):
        """One row per symbol: last price and 5 / 60-minute changes, volume vs the last hour."""
        now_min = int(time.time() // 60 * 60000)
        with self.lock:
            snap = {s: dict(c) for s, c in self.candles.items()}
        out = []
        for s, c in snap.items():
            if not c:
                continue
            ks = sorted(c)
            last = c[ks[-1]][3]

            def close_at_or_before(ms):
                prev = [k for k in ks if k <= ms]
                return c[prev[-1]][3] if prev else None
            p5, p60 = close_at_or_before(now_min - 5 * 60000), close_at_or_before(now_min - 60 * 60000)
            qv5 = sum(c[k][5] for k in ks if k > now_min - 5 * 60000)
            qv60 = sum(c[k][5] for k in ks if now_min - 65 * 60000 < k <= now_min - 5 * 60000)
            out.append(dict(
                symbol=s, price=last,
                chg_5m=(last / p5 - 1) * 100 if p5 else None,
                chg_60m=(last / p60 - 1) * 100 if p60 else None,
                usdt_5m=qv5,
                vol_ratio=(qv5 / 5) / (qv60 / 60) if qv60 > 0 else None,
                last_minute=pd.to_datetime(ks[-1], unit="ms"),
            ))
        return pd.DataFrame(out)

    def anomalies_df(self):
        with self.lock:
            rows = list(self.anoms.values())
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["detected_minute"] = pd.to_datetime(df["detected_minute"], utc=True).dt.tz_localize(None)
        return df.sort_values("detected_minute", ascending=False).reset_index(drop=True)

    def status(self):
        lag = (time.time() * 1000 - self.last_trade_ms) / 1000 if self.last_trade_ms else None
        return dict(caught_up=self.caught_up, rate=self.rate, msgs=self.msgs, lag_s=lag, error=self.error,
                    n_anoms=len(self.anoms), n_symbols=len(self.candles))

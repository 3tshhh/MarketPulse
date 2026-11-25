"""
MarketPulse producer: Binance aggTrade WebSocket -> Kafka topic `trades` (key = symbol).

Message value (JSON, one per aggregated trade):
{
  "symbol": "BTCUSDT",
  "agg_id": 123456789,        # aggregate trade id, contiguous per symbol -> lets us detect gaps
  "price": 97123.45,
  "qty": 0.0123,
  "first_trade_id": 4000001,  # first/last individual trade ids in this aggregate
  "last_trade_id": 4000003,   #   -> trade count comparable to Binance klines
  "trade_ts": 1732380000123,  # exchange trade time (ms)  -> Spark event time
  "event_ts": 1732380000125,  # exchange event time (ms)
  "is_buyer_maker": true,
  "ingest_ts": 1732380000170  # producer receive time (ms) -> latency checks
}
"""
import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
import urllib.request
from collections import Counter

from clock_check import WARN_MS, correction_ms, measure_skew, significant
from confluent_kafka import KafkaException, Producer
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

LOG = logging.getLogger("producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TRADES_TOPIC", "trades")
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://data-stream.binance.vision")
REST_BASE = os.getenv("BINANCE_REST_BASE", "https://data-api.binance.vision")
SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", "symbols.txt")
STATS_EVERY_S = 30
# Binance drops every connection at 24h. Reconnect ourselves slightly before that.
MAX_CONN_AGE_S = 23 * 3600
SKEW_EVERY_S = 600  # re-measure clock skew every 10 min (WSL2 clocks drift after sleep)


def load_symbols(path: str) -> list[str]:
    with open(path) as f:
        syms = [l.strip().upper() for l in f if l.strip() and not l.startswith("#")]
    return list(dict.fromkeys(syms))  # dedupe, keep order


def validate_symbols(symbols: list[str]) -> list[str]:
    """Drop symbols that don't exist. Binance silently sends nothing for a bad stream name,
    so without this a typo looks like a 'quiet' symbol. Non-fatal if REST is unreachable."""
    try:
        with urllib.request.urlopen(f"{REST_BASE}/api/v3/ticker/price", timeout=10) as r:
            listed = {row["symbol"] for row in json.load(r)}
    except Exception as e:  # noqa: BLE001
        LOG.warning("symbol validation skipped (%s)", e)
        return symbols
    bad = [s for s in symbols if s not in listed]
    if bad:
        LOG.warning("dropping unknown/delisted symbols: %s", bad)
    good = [s for s in symbols if s in listed]
    if not good:
        sys.exit("no valid symbols")
    return good


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "marketpulse-producer",
        "acks": "all",
        "enable.idempotence": True,     # no duplicates on producer retries
        "compression.type": "lz4",
        "linger.ms": 50,                # small batching; latency stays well under 1s
        "batch.size": 131072,
        "queue.buffering.max.messages": 500000,
    })


class Stats:
    def __init__(self):
        self.sent = 0
        self.delivered = 0
        self.failed = 0
        self.per_symbol = Counter()
        self.lat_ms = []  # ingest_ts - trade_ts, sampled
        self.t0 = time.time()
        self.skew_ms = 0  # local clock minus Binance clock; subtracted from latency

    def on_delivery(self, err, msg):
        if err is not None:
            self.failed += 1
            LOG.error("delivery failed: %s", err)
        else:
            self.delivered += 1

    def report(self):
        dt = max(time.time() - self.t0, 1e-6)
        lat = sorted(x - self.skew_ms for x in self.lat_ms)  # skew-corrected
        p50 = lat[len(lat) // 2] if lat else None
        p95 = lat[int(len(lat) * 0.95)] if lat else None
        LOG.info(
            "rate=%.1f msg/s sent=%d delivered=%d failed=%d symbols_active=%d lat_p50=%sms lat_p95=%sms skew=%+dms top=%s",
            self.sent / dt, self.sent, self.delivered, self.failed,
            len(self.per_symbol), p50, p95, self.skew_ms, self.per_symbol.most_common(3),
        )
        self.sent = self.delivered = self.failed = 0
        self.per_symbol.clear()
        self.lat_ms.clear()
        self.t0 = time.time()


def to_record(d: dict, now_ms: int) -> dict:
    return {
        "symbol": d["s"],
        "agg_id": d["a"],
        "price": float(d["p"]),
        "qty": float(d["q"]),
        "first_trade_id": d["f"],
        "last_trade_id": d["l"],
        "trade_ts": d["T"],
        "event_ts": d["E"],
        "is_buyer_maker": d["m"],
        "ingest_ts": now_ms,
    }


def produce(p: Producer, stats: Stats, rec: dict):
    payload = json.dumps(rec, separators=(",", ":")).encode()
    while True:
        try:
            p.produce(TOPIC, key=rec["symbol"].encode(), value=payload, on_delivery=stats.on_delivery)
            break
        except BufferError:
            # local queue full (Kafka slow/down): block briefly, let callbacks drain
            p.poll(0.5)
    p.poll(0)


def update_skew(stats: Stats):
    """Measure clock skew vs Binance (about 1 s, every 10 min; the WS buffer absorbs it).
    Latency stats are corrected only when the skew is larger than its measurement error."""
    skew, rtt = measure_skew(samples=3)
    if skew is None:
        LOG.warning("clock skew: Binance REST unreachable, keeping correction %+d ms", stats.skew_ms)
        return
    stats.skew_ms = correction_ms(skew, rtt)
    sig = significant(skew, rtt)
    if sig and abs(skew) > WARN_MS:
        LOG.warning("clock skew %+d ms (rtt %d ms) exceeds %d ms: fix the host clock "
                    "(scripts/preflight.ps1); latency stats are corrected", skew, rtt, WARN_MS)
    else:
        LOG.info("clock skew %+d ms (rtt %d ms, +-%d) -> %s", skew, rtt, rtt // 2,
                 f"correcting {stats.skew_ms:+d} ms" if sig else "within noise, no correction")


async def stream(symbols: list[str], p: Producer, stats: Stats, stop: asyncio.Event):
    streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
    url = f"{WS_BASE}/stream?streams={streams}"
    backoff = 1
    last_report = time.time()
    last_skew = time.time()

    while not stop.is_set():
        try:
            # websockets answers Binance's server pings automatically.
            async with connect(url, ping_interval=20, ping_timeout=20, max_queue=10000) as ws:
                LOG.info("connected: %d streams", len(symbols))
                backoff = 1
                opened = time.time()
                async for raw in ws:
                    if stop.is_set():
                        break
                    msg = json.loads(raw)
                    d = msg.get("data")
                    if not d or d.get("e") != "aggTrade":
                        continue
                    now_ms = int(time.time() * 1000)
                    rec = to_record(d, now_ms)
                    produce(p, stats, rec)
                    stats.sent += 1
                    stats.per_symbol[rec["symbol"]] += 1
                    if stats.sent % 20 == 0:
                        stats.lat_ms.append(now_ms - rec["trade_ts"])

                    if time.time() - last_skew >= SKEW_EVERY_S:
                        update_skew(stats)
                        last_skew = time.time()
                    if time.time() - last_report >= STATS_EVERY_S:
                        stats.report()
                        last_report = time.time()
                    if time.time() - opened > MAX_CONN_AGE_S:
                        LOG.info("connection near 24h limit, reconnecting")
                        break
        except (ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            LOG.warning("websocket error: %r; reconnect in %ss", e, backoff)
        except KafkaException as e:
            LOG.error("kafka error: %r; reconnect in %ss", e, backoff)
        if stop.is_set():
            break
        await asyncio.sleep(backoff + random.random())
        backoff = min(backoff * 2, 60)


async def main():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    symbols = validate_symbols(load_symbols(SYMBOLS_FILE))
    LOG.info("symbols (%d): %s", len(symbols), ",".join(symbols))

    p = make_producer()
    stats = Stats()
    update_skew(stats)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await stream(symbols, p, stats, stop)
    finally:
        LOG.info("flushing producer...")
        left = p.flush(10)
        LOG.info("shutdown complete, %d messages not delivered", left)


if __name__ == "__main__":
    asyncio.run(main())

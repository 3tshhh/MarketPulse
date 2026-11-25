"""
Step 1 acceptance check. Reads `trades` for N seconds and reports:
  - messages/sec, number of symbols seen
  - key == value.symbol (partitioning correctness)
  - each symbol sticks to ONE partition (ordering guarantee per symbol)
  - agg_id gaps per symbol (missed trades) and out-of-order ids
  - end-to-end latency: consume time - trade_ts
  - schema errors

Run:  docker compose run --rm producer python verify_consumer.py 60
"""
import json
import os
import sys
import time
import uuid
from collections import defaultdict

from clock_check import correction_ms, measure_skew
from confluent_kafka import Consumer

secs = int(sys.argv[1]) if len(sys.argv) > 1 else 60
REQUIRED = {"symbol", "agg_id", "price", "qty", "trade_ts", "event_ts", "is_buyer_maker", "ingest_ts"}

c = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
    "group.id": f"verify-{uuid.uuid4().hex[:8]}",  # fresh group, no committed offsets
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
})
c.subscribe([os.getenv("TRADES_TOPIC", "trades")])

count = 0
bad_schema = key_mismatch = 0
last_id = {}
gaps = defaultdict(int)       # number of missing agg ids
out_of_order = defaultdict(int)
partitions = defaultdict(set)
lat = []

measured, rtt = measure_skew()
skew = correction_ms(measured, rtt) if measured is not None else 0
print(f"clock skew measured {measured} ms (rtt {rtt} ms) -> correction applied: {skew:+d} ms")
t_end = time.time() + secs
print(f"consuming for {secs}s ...")
while time.time() < t_end:
    m = c.poll(1.0)
    if m is None:
        continue
    if m.error():
        print("error:", m.error())
        continue
    count += 1
    try:
        v = json.loads(m.value())
        if not REQUIRED.issubset(v):
            raise ValueError("missing fields")
    except Exception:
        bad_schema += 1
        continue
    s = v["symbol"]
    if m.key().decode() != s:
        key_mismatch += 1
    partitions[s].add(m.partition())
    prev = last_id.get(s)
    if prev is not None:
        if v["agg_id"] > prev + 1:
            gaps[s] += v["agg_id"] - prev - 1
        elif v["agg_id"] <= prev:
            out_of_order[s] += 1
    last_id[s] = max(v["agg_id"], prev or 0)
    if count % 10 == 0:
        lat.append(int(time.time() * 1000) - v["trade_ts"] - skew)
c.close()

lat.sort()
print("\n===== RESULT =====")
print(f"messages:        {count}  ({count / secs:.1f}/s)")
print(f"symbols seen:    {len(last_id)}")
print(f"schema errors:   {bad_schema}")
print(f"key mismatches:  {key_mismatch}")
multi = {s: p for s, p in partitions.items() if len(p) > 1}
print(f"multi-partition symbols: {multi or 'none'}")
print(f"agg_id gaps:     {sum(gaps.values())} missing ids  {dict(gaps) if gaps else ''}")
print(f"out of order:    {sum(out_of_order.values())}")
if lat:
    print(f"latency ms:      p50={lat[len(lat)//2]}  p95={lat[int(len(lat)*0.95)]}  max={lat[-1]}")
print("per-partition symbol count:",
      {p: sum(1 for ps in partitions.values() if p in ps) for p in sorted({x for ps in partitions.values() for x in ps})})
ok = count > 0 and bad_schema == 0 and key_mismatch == 0 and not multi and not out_of_order
print("\nPASS" if ok else "\nCHECK ABOVE")

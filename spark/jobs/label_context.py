"""
Extract the 1-minute klines around every event in the label files, so events can be reviewed
(charts + day context) without a browser. Pure Python, reads the archive CSVs that backtest.py
cached in /tmp/klines inside spark-master.

  docker compose exec spark-master python3 /opt/jobs/label_context.py

Output: /opt/out/label_context.csv (= ./data/backtest/label_context.csv on the host).
Window per event: its whole UTC day plus 3 h on each side.
"""
import csv
import os
from datetime import datetime, timedelta, timezone

CACHE = "/tmp/klines"
OUT = "/opt/out"
FILES = ["label_z.csv", "label_market.csv", "label_fixed.csv"]
PAD = timedelta(hours=3)

_cache = {}


def month_rows(symbol, month):
    key = (symbol, month)
    if key not in _cache:
        path = os.path.join(CACHE, f"{symbol}-1m-{month}.csv")
        rows = {}
        if os.path.exists(path):
            with open(path) as f:
                for r in csv.reader(f):
                    if not r or not r[0].isdigit():
                        continue
                    ot = int(r[0])
                    ms = ot // 1000 if ot > 10**14 else ot      # 2025+ archive uses microseconds
                    rows[ms] = r
        _cache[key] = rows
    return _cache[key]


out_path = os.path.join(OUT, "label_context.csv")
n_events = n_rows = 0
with open(out_path, "w", newline="") as fo:
    w = csv.writer(fo)
    w.writerow(["file", "row", "symbol", "event_minute_utc", "ts_utc",
                "open", "high", "low", "close", "volume", "quote_volume", "trades"])
    for fname in FILES:
        path = os.path.join(OUT, fname)
        if not os.path.exists(path):
            continue
        with open(path, newline="") as f:
            events = [r for r in csv.DictReader(f) if r.get("symbol")]
        for i, ev in enumerate(events):
            t = datetime.strptime(ev["detected_minute_utc"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            day0 = t.replace(hour=0, minute=0)
            lo, hi = day0 - PAD, day0 + timedelta(days=1) + PAD
            months = sorted({lo.strftime("%Y-%m"), hi.strftime("%Y-%m"), t.strftime("%Y-%m")})
            data = {}
            for m in months:
                data.update(month_rows(ev["symbol"], m))
            lo_ms, hi_ms = int(lo.timestamp() * 1000), int(hi.timestamp() * 1000)
            for ms in sorted(k for k in data if lo_ms <= k < hi_ms):
                r = data[ms]
                w.writerow([fname, i, ev["symbol"], ev["detected_minute_utc"],
                            datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M"),
                            r[1], r[2], r[3], r[4], r[5], r[7], r[8]])
                n_rows += 1
            n_events += 1
print(f"wrote {out_path}: {n_events} events, {n_rows:,} minute rows")

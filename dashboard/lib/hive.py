"""
HiveServer2 access for the history pages (and later the chatbot).

One persistent connection per Streamlit server (BUILD_LOG 3.8: a new connection pays about 7 s
of Tez session start-up; a warm one answers aggregations in about 3 s and simple lookups in about 1 s).
HiveServer2 runs one statement per connection at a time, so access is serialized with a lock.
Results are cached per query text for a few minutes (st.cache_data in the pages).
"""
import os
import threading
import time

import pandas as pd
from pyhive import hive

HOST = os.getenv("HIVE_HOST", "hiveserver2")
PORT = int(os.getenv("HIVE_PORT", "10000"))
USER = os.getenv("HIVE_USER", "analyst")
DB = os.getenv("HIVE_DB", "marketpulse")


class Hive:
    def __init__(self):
        self.lock = threading.Lock()
        self.conn = None

    def _connect(self):
        self.conn = hive.Connection(host=HOST, port=PORT, username=USER, database=DB, auth="NONE")
        cur = self.conn.cursor()
        # Tez is slow to start per session; pay it once, here.
        cur.execute("SELECT count(*) FROM candles_1m WHERE dt = '1970-01-01'")
        cur.fetchall()

    def query(self, sql, params=None):
        """Run one SELECT. Returns (DataFrame, seconds). Reconnects once on a broken connection."""
        with self.lock:
            for attempt in (1, 2):
                try:
                    if self.conn is None:
                        self._connect()
                    t0 = time.time()
                    cur = self.conn.cursor()
                    cur.execute(sql, params)
                    rows = cur.fetchall()
                    cols = [d[0].split(".")[-1] for d in cur.description]
                    return pd.DataFrame(rows, columns=cols), time.time() - t0
                except Exception:  # noqa: BLE001
                    self.conn = None
                    if attempt == 2:
                        raise


def partitions(h, table="candles_1m"):
    df, _ = h.query(f"SHOW PARTITIONS {table}")
    if df.empty:
        return []
    days = sorted({p.split("/")[0].split("=")[1] for p in df.iloc[:, 0]}, reverse=True)
    return days

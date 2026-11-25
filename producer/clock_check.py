"""
Clock skew against Binance server time.

skew_ms = local_clock - binance_clock (positive = our clock is ahead), measured at the midpoint
of the request round trip.

Two details matter, learned the hard way (BUILD_LOG 3.9):
  1. Reuse ONE keep-alive HTTPS connection and discard a warm-up request. Otherwise every sample
     includes the TCP + TLS handshake (several round trips) *before* the server stamps its time,
     the round trip looks ~1 s long, and the midpoint estimate is biased.
  2. Only trust the estimate when it is bigger than its own uncertainty (rtt/2). Below that,
     the honest answer is "no measurable skew" and no correction is applied.

    docker compose run --rm producer python clock_check.py
"""
import http.client
import json
import os
import sys
import time
from urllib.parse import urlparse

REST_BASE = os.getenv("BINANCE_REST_BASE", "https://data-api.binance.vision")
WARN_MS = 500
MARGIN_MS = 50  # extra margin before a skew counts as significant


def measure_skew(samples: int = 5, timeout: float = 5.0):
    """Return (skew_ms, rtt_ms) from the lowest-RTT sample on a warm connection, or (None, None)."""
    u = urlparse(REST_BASE)
    conn_cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
    best = None
    try:
        conn = conn_cls(u.netloc, timeout=timeout)
        conn.request("GET", "/api/v3/time")          # warm-up: pays DNS + TCP + TLS once
        conn.getresponse().read()
        for _ in range(samples):
            t0 = time.time() * 1000
            conn.request("GET", "/api/v3/time")
            server = json.loads(conn.getresponse().read())["serverTime"]
            t1 = time.time() * 1000
            rtt = t1 - t0
            skew = (t0 + t1) / 2 - server
            if best is None or rtt < best[1]:
                best = (skew, rtt)
            time.sleep(0.2)
        conn.close()
    except Exception:  # noqa: BLE001
        pass
    if best is None:
        return None, None
    return round(best[0]), round(best[1])


def significant(skew_ms, rtt_ms) -> bool:
    """True if the measured skew is larger than its measurement uncertainty."""
    return skew_ms is not None and abs(skew_ms) > rtt_ms / 2 + MARGIN_MS


def correction_ms(skew_ms, rtt_ms) -> int:
    """Skew to subtract from latencies: the measured value if significant, else 0."""
    return skew_ms if significant(skew_ms, rtt_ms) else 0


if __name__ == "__main__":
    skew, rtt = measure_skew()
    if skew is None:
        print("clock check FAILED: Binance REST unreachable")
        sys.exit(2)
    sig = "significant" if significant(skew, rtt) else "within noise"
    status = "OK" if abs(skew) <= WARN_MS or not significant(skew, rtt) else "BAD"
    print(f"clock skew {skew:+d} ms (rtt {rtt} ms, accuracy +-{rtt // 2} ms, {sig}) -> {status}")
    sys.exit(0 if status == "OK" else 1)

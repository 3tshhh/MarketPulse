"""Number and time formatting shared by all pages (Hive/Kafka give raw doubles: 3.45e-06, 4.2E7)."""
import pandas as pd

LARGE = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}


def price(v):
    """Enough significant digits for any coin, never scientific notation."""
    if v is None or pd.isna(v):
        return "–"
    v = float(v)
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}".rstrip("0").rstrip(".")
    s = f"{v:.12f}".rstrip("0")
    # keep 4 significant digits after the leading zeros
    i = next((k for k, ch in enumerate(s.split(".")[1]) if ch != "0"), 0)
    return f"{v:.{i + 4}f}".rstrip("0").rstrip(".")


def usd(v):
    if v is None or pd.isna(v):
        return "–"
    v = float(v)
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return f"${v / div:,.2f}{suf}"
    return f"${v:,.0f}"


def pct(v, signed=True):
    if v is None or pd.isna(v):
        return "–"
    return f"{float(v):+.2f}%" if signed else f"{float(v):.2f}%"


def coin(sym):
    return sym[:-4] if sym.endswith("USDT") else sym


def to_tz(ts, tz):
    """ts: tz-naive UTC or tz-aware pandas Timestamp/Series -> tz-aware in `tz`."""
    t = pd.to_datetime(ts)
    if getattr(t, "dt", None) is not None:
        return (t.dt.tz_localize("UTC") if t.dt.tz is None else t.dt.tz_convert("UTC")).dt.tz_convert(tz)
    return (t.tz_localize("UTC") if t.tzinfo is None else t).tz_convert(tz)

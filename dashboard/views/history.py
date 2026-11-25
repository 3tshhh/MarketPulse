"""History from Hive: candles + anomalies for one coin and day, and a daily market summary.
Every query shows its SQL and run time, which makes the Hive layer visible in the demo."""
import pandas as pd
import streamlit as st

from lib import charts, fmt, state
from lib.hive import partitions

st.title("History")
st.caption("Served by Hive over the Parquet files on HDFS (table candles_1m, view anomaly_events).")
H = state.hive()


@st.cache_data(ttl=300, show_spinner=False)
def run(sql, params=None):
    df, secs = H.query(sql, params)
    return df, secs


@st.cache_data(ttl=120, show_spinner=False)
def days():
    return partitions(H)


def show_sql(sql, params, secs):
    with st.expander(f"Hive SQL · {secs:.1f} s"):
        st.code(sql if not params else sql % {k: repr(v) for k, v in params.items()}, language="sql")


try:
    with st.spinner("Connecting to Hive (first query starts a Tez session, about 10 s)…"):
        ds = days()
except Exception as e:  # noqa: BLE001
    st.error(f"Hive is not reachable: {e}")
    st.stop()
if not ds:
    st.info("No candle partitions yet.")
    st.stop()

L = state.live()
syms = L.symbols() or ["BTCUSDT"]
c1, c2 = st.columns(2)
day = c1.selectbox("Day (UTC)", ds)
sym = c2.selectbox("Coin", syms, index=syms.index("BTCUSDT") if "BTCUSDT" in syms else 0, format_func=fmt.coin)

tab1, tab2 = st.tabs(["Coin chart", "Day summary"])

with tab1:
    sql = ("SELECT window_start, open, high, low, close, quote_volume, trade_count FROM candles_1m "
           "WHERE dt = %(dt)s AND symbol = %(sym)s ORDER BY window_start")
    params = {"dt": day, "sym": sym}
    with st.spinner("Querying Hive…"):
        df, secs = run(sql, params)
    show_sql(sql, params, secs)
    if df.empty:
        st.info("No candles for this coin on this day.")
    else:
        df["window_start"] = pd.to_datetime(df["window_start"])
        for col in ("open", "high", "low", "close", "quote_volume"):
            df[col] = pd.to_numeric(df[col])
        asql = ("SELECT detected_minute, direction, price_change_pct, z_score, volume_ratio, scope, reason "
                "FROM anomaly_events WHERE dt = %(dt)s AND symbol = %(sym)s ORDER BY detected_minute")
        an, asecs = run(asql, params)
        if not an.empty:
            an["detected_minute"] = pd.to_datetime(an["detected_minute"])
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Open → close", f"{fmt.price(df['open'].iloc[0])} → {fmt.price(df['close'].iloc[-1])}",
                  fmt.pct((df["close"].iloc[-1] / df["open"].iloc[0] - 1) * 100))
        m2.metric("Range", f"{fmt.price(df['low'].min())} – {fmt.price(df['high'].max())}")
        m3.metric("Traded", fmt.usd(df["quote_volume"].sum()))
        m4.metric("Anomalies", len(an), help=f"from anomaly_events in {asecs:.1f} s")
        st.plotly_chart(charts.candles(df, state.tz(), an if not an.empty else None), use_container_width=True)
        st.caption(f"{len(df)} one-minute candles. Gaps = minutes with no trades or times the collector was off.")
        if not an.empty:
            t = an.copy()
            t["time"] = fmt.to_tz(t["detected_minute"], state.tz()).dt.strftime("%H:%M")
            st.dataframe(t[["time", "reason", "scope"]], hide_index=True, use_container_width=True)

with tab2:
    sql = ("SELECT symbol, count(*) AS minutes, min(low) AS low, max(high) AS high, "
           "sum(quote_volume) AS usdt_volume, sum(trade_count) AS trades "
           "FROM candles_1m WHERE dt = %(dt)s GROUP BY symbol ORDER BY usdt_volume DESC")
    with st.spinner("Querying Hive…"):
        s, secs = run(sql, {"dt": day})
    show_sql(sql, {"dt": day}, secs)
    if s.empty:
        st.info("No data for this day.")
    else:
        asql = "SELECT symbol, count(*) AS anomalies FROM anomaly_events WHERE dt = %(dt)s GROUP BY symbol"
        a, _ = run(asql, {"dt": day})
        s = s.merge(a, on="symbol", how="left").fillna({"anomalies": 0})
        out = pd.DataFrame({
            "Coin": s["symbol"].map(fmt.coin),
            "Minutes with trades": s["minutes"].astype(int),
            "Low": s["low"].map(fmt.price),
            "High": s["high"].map(fmt.price),
            "Traded (USDT)": pd.to_numeric(s["usdt_volume"]).map(fmt.usd),
            "Anomalies": s["anomalies"].astype(int),
        })
        st.dataframe(out, hide_index=True, use_container_width=True)
        st.caption("Minutes with trades below 1,440 mean the collector was off or the coin was quiet.")

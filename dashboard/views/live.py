"""Live market: every coin's last 5 and 60 minutes, and one coin's chart with the forming minute."""
import time

import pandas as pd
import streamlit as st

from lib import charts, fmt, state

st.title("Live market")
L = state.live()


@st.fragment(run_every=5)
def board():
    ov = L.overview()
    if ov.empty:
        st.info("Waiting for trades from Kafka…")
        return
    ov = ov.sort_values("chg_5m", key=lambda s: s.abs(), ascending=False)
    show = pd.DataFrame({
        "Coin": ov["symbol"].map(fmt.coin),
        "Price (USDT)": ov["price"].map(fmt.price),
        "5 min": ov["chg_5m"].map(fmt.pct),
        "60 min": ov["chg_60m"].map(fmt.pct),
        "Traded, last 5 min": ov["usdt_5m"].map(fmt.usd),
        "Volume vs last hour": ov["vol_ratio"].map(lambda v: "–" if pd.isna(v) else f"{v:.1f}×"),
    })
    st.caption("Sorted by the biggest 5-minute move. Updates every 5 seconds.")
    st.dataframe(show, hide_index=True, use_container_width=True, height=320)


@st.fragment(run_every=5)
def chart(sym):
    df = L.candles_df(sym)
    if df.empty:
        st.info("No trades for this coin yet.")
        return
    last = df.iloc[-1]
    ov = L.overview()
    row = ov[ov["symbol"] == sym].iloc[0] if not ov.empty and (ov["symbol"] == sym).any() else None
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"{fmt.coin(sym)} price", fmt.price(last["close"]))
    c2.metric("5 min", fmt.pct(row["chg_5m"]) if row is not None else "–")
    c3.metric("60 min", fmt.pct(row["chg_60m"]) if row is not None else "–")
    c4.metric("Volume vs last hour", "–" if row is None or pd.isna(row["vol_ratio"]) else f"{row['vol_ratio']:.1f}×")
    an = L.anomalies_df()
    an = an[an["symbol"] == sym] if not an.empty else an
    st.plotly_chart(charts.candles(df, state.tz(), an, live_minute=last["window_start"]), use_container_width=True)
    st.caption("Built live from Kafka trades. The last candle is still forming (dotted line). "
               "Orange = flagged anomaly window.")


board()
syms = L.symbols() or ["BTCUSDT"]
sym = st.selectbox("Coin", syms, index=syms.index("BTCUSDT") if "BTCUSDT" in syms else 0, format_func=fmt.coin)
chart(sym)

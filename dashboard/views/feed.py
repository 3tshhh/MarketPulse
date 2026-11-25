"""Anomaly feed: single-coin events one by one, market-wide bursts grouped into one card."""
import pandas as pd
import streamlit as st

from lib import charts, fmt, state

st.title("Anomaly feed")
st.caption("Unusual price moves together with unusual trading volume, per coin and relative to that coin's own "
           "normal behaviour (rule z_v2). These are anomalies, not trading signals or proof of manipulation.")
L = state.live()


def items(an):
    """Coin events stay single; market rows are grouped per 10-minute bucket."""
    out = []
    coin = an[an["scope"] != "market"]
    for _, r in coin.iterrows():
        out.append(dict(kind="coin", t=r["detected_minute"], rows=pd.DataFrame([r])))
    mk = an[an["scope"] == "market"].copy()
    if not mk.empty:
        mk["bucket"] = mk["detected_minute"].dt.floor("10min")
        for _, g in mk.groupby("bucket"):
            out.append(dict(kind="market", t=g["detected_minute"].min(), rows=g.sort_values("z_score", key=abs, ascending=False)))
    return sorted(out, key=lambda x: x["t"], reverse=True)


@st.fragment(run_every=15)
def feed(kind_filter, limit):
    an = L.anomalies_df()
    if an.empty:
        st.info("No anomalies in the last 7 days yet. The detector fires about 6 times a day on average; "
                "quiet markets can go hours without one.")
        return
    its = [i for i in items(an) if kind_filter == "All" or (i["kind"] == "coin") == (kind_filter == "Single coin")]
    tz = state.tz()
    for it in its[:limit]:
        when = fmt.to_tz(it["t"], tz).strftime("%Y-%m-%d %H:%M")
        r0 = it["rows"].iloc[0]
        with st.container(border=True):
            if it["kind"] == "market":
                n, down = len(it["rows"]), (it["rows"]["direction"] == "down").sum()
                st.markdown(f"**Market-wide move** · {when} {tz} · {n} coins · "
                            f"{'mostly down' if down > n / 2 else 'mostly up'}")
                st.caption(", ".join(f"{fmt.coin(r['symbol'])} {fmt.pct(r['price_change_pct'])}"
                                     for _, r in it["rows"].head(12).iterrows()))
            else:
                arrow = "▲" if r0["direction"] == "up" else "▼"
                st.markdown(f"**{arrow} {fmt.coin(r0['symbol'])}** · {when} {tz} · "
                            f"{fmt.pct(r0['price_change_pct'])} in 5 min")
                st.write(r0["reason"])
            sym = r0["symbol"]
            if st.toggle("Show chart", key=f"chart-{sym}-{it['t']}"):
                df = L.candles_df(sym)
                df = df[(df["window_start"] >= it["t"] - pd.Timedelta(minutes=60)) &
                        (df["window_start"] <= it["t"] + pd.Timedelta(minutes=60))] if not df.empty else df
                if df.empty:
                    st.caption("Older than the live window: open it on the History page.")
                else:
                    st.plotly_chart(charts.candles(df, tz, it["rows"], height=360), use_container_width=True)


c1, c2 = st.columns([2, 1])
kind = c1.radio("Show", ["All", "Single coin", "Market-wide"], horizontal=True)
limit = c2.number_input("Items", 5, 200, 30, step=5)
feed(kind, int(limit))

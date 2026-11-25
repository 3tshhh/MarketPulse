"""MarketPulse dashboard. Live pages read Kafka directly; history pages read Hive."""
import streamlit as st

from lib import state

st.set_page_config(page_title="MarketPulse", page_icon="📈", layout="wide")

pages = [
    st.Page("views/live.py", title="Live market", default=True),
    st.Page("views/feed.py", title="Anomaly feed"),
    st.Page("views/history.py", title="History"),
    st.Page("views/analyst.py", title="AI analyst"),
]
nav = st.navigation(pages)

with st.sidebar:
    st.markdown("### MarketPulse")
    st.caption("Real-time crypto anomaly monitor · Binance spot · not trading advice")
    st.radio("Time zone", ["UTC", "Africa/Cairo"], key="tz", horizontal=True)
    s = state.live().status()
    if s["error"]:
        st.error(f"Kafka: {s['error']}")
    elif not s["caught_up"]:
        st.warning(f"Loading the last hours from Kafka… {s['msgs']:,} messages read")
    else:
        st.success(f"Live · {s['rate']:.0f} msg/s · last trade {s['lag_s']:.1f}s ago")
    st.caption(f"{s['n_symbols']} coins · {s['n_anoms']} anomalies in the last 7 days")

nav.run()

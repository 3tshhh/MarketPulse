"""Candlestick + volume figure used by every page. Times are converted to the chosen zone."""
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from lib import fmt

UP, DOWN, FLAG = "#2f9e8f", "#e8452c", "rgba(245,166,35,0.30)"


def candles(df, tz="UTC", anomalies=None, height=460, live_minute=None):
    """df: window_start (UTC, naive), open, high, low, close, quote_volume.
    anomalies: DataFrame with detected_minute (UTC naive), direction, reason, scope."""
    x = fmt.to_tz(df["window_start"], tz)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25], vertical_spacing=0.03)
    fig.add_trace(go.Candlestick(x=x, open=df["open"], high=df["high"], low=df["low"], close=df["close"],
                                 increasing_line_color=UP, decreasing_line_color=DOWN, name="price",
                                 showlegend=False), row=1, col=1)
    colors = [UP if c >= o else DOWN for o, c in zip(df["open"], df["close"])]
    fig.add_trace(go.Bar(x=x, y=df["quote_volume"], marker_color=colors, opacity=0.6, name="USDT volume",
                         showlegend=False, hovertemplate="%{x}<br>$%{y:,.0f}<extra></extra>"), row=2, col=1)
    if anomalies is not None and not anomalies.empty:
        lo, hi = df["window_start"].min(), df["window_start"].max()
        a = anomalies[(anomalies["detected_minute"] >= lo) & (anomalies["detected_minute"] <= hi)]
        for _, r in a.iterrows():
            t = r["detected_minute"]
            x0, x1 = fmt.to_tz(t - pd.Timedelta(minutes=4), tz), fmt.to_tz(t + pd.Timedelta(minutes=1), tz)
            fig.add_vrect(x0=x0, x1=x1, fillcolor=FLAG, line_width=0, row="all", col=1)
        if not a.empty:
            pts = df.set_index("window_start").reindex(a["detected_minute"])
            fig.add_trace(go.Scatter(
                x=fmt.to_tz(a["detected_minute"], tz), y=pts["high"].values * 1.002, mode="markers",
                marker=dict(symbol="triangle-down", size=12, color="#f5a623", line=dict(width=1, color="#7a5200")),
                text=a["reason"], hovertemplate="%{text}<extra>anomaly</extra>", showlegend=False), row=1, col=1)
    if live_minute is not None:
        fig.add_vline(x=fmt.to_tz(live_minute, tz), line_dash="dot", line_color="#9aa5cc", row="all", col=1)
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=10, b=10), xaxis_rangeslider_visible=False,
                      hovermode="x unified", bargap=0)
    fig.update_yaxes(tickformat=",.6~g", row=1, col=1)
    fig.update_yaxes(tickprefix="$", tickformat="~s", row=2, col=1)
    return fig

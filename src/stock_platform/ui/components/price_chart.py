"""Clean Plotly price chart component."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def build_price_chart(
    frame: pd.DataFrame,
    technical_frame: pd.DataFrame,
    *,
    symbol: str,
    source_label: str,
    interval: str = "1d",
    show_20_ema: bool = False,
    show_200_ema: bool = False,
    show_bollinger: bool = False,
    show_52w: bool = False,
    show_volume: bool = True,
    active_signals: list[object] | None = None,
) -> go.Figure:
    """Return a readable candlestick chart with minimal default clutter."""
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=frame.index,
            open=frame["open"],
            high=frame["high"],
            low=frame["low"],
            close=frame["close"],
            name=symbol,
        )
    )

    if "ema_50" in technical_frame:
        fig.add_trace(
            go.Scatter(
                x=technical_frame.index,
                y=technical_frame["ema_50"],
                mode="lines",
                name="50 EMA",
                line=dict(width=1.5),
            )
        )
    if show_20_ema and "ema_20" in technical_frame:
        fig.add_trace(
            go.Scatter(
                x=technical_frame.index, y=technical_frame["ema_20"], mode="lines", name="20 EMA"
            )
        )
    if show_200_ema and "ema_200" in technical_frame:
        fig.add_trace(
            go.Scatter(
                x=technical_frame.index, y=technical_frame["ema_200"], mode="lines", name="200 EMA"
            )
        )
    if show_bollinger and {"bb_upper", "bb_lower"}.issubset(technical_frame.columns):
        fig.add_trace(
            go.Scatter(
                x=technical_frame.index,
                y=technical_frame["bb_upper"],
                mode="lines",
                name="BB upper",
                line=dict(width=1, dash="dot"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=technical_frame.index,
                y=technical_frame["bb_lower"],
                mode="lines",
                name="BB lower",
                line=dict(width=1, dash="dot"),
                fill="tonexty",
                fillcolor="rgba(100,116,139,0.08)",
            )
        )
    if show_52w and {"high_52w", "low_52w"}.issubset(technical_frame.columns):
        fig.add_trace(
            go.Scatter(
                x=technical_frame.index,
                y=technical_frame["high_52w"],
                mode="lines",
                name="52W high",
                line=dict(width=1, dash="dash"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=technical_frame.index,
                y=technical_frame["low_52w"],
                mode="lines",
                name="52W low",
                line=dict(width=1, dash="dash"),
            )
        )
    if show_volume and "volume" in technical_frame:
        fig.add_trace(
            go.Bar(
                x=technical_frame.index,
                y=technical_frame["volume"],
                name="Volume",
                yaxis="y2",
                marker_color="rgba(100,116,139,0.25)",
            )
        )
        fig.update_layout(yaxis2=dict(title="Volume", overlaying="y", side="right", showgrid=False))

    for signal in active_signals or []:
        _add_signal_context(fig, frame, signal)

    last_bar = frame.index[-1].strftime("%Y-%m-%d") if not frame.empty else "N/A"
    fig.update_layout(
        title=f"{symbol} daily chart<br><sup>{source_label} · last bar {last_bar} · {interval}</sup>",
        xaxis_title="Date",
        yaxis_title="Price",
        xaxis_rangeslider_visible=True,
        hovermode="x unified",
        height=620,
        margin=dict(l=20, r=20, t=70, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(
        rangeselector=dict(
            buttons=[
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(step="all", label="All"),
            ]
        )
    )
    return fig


def _add_signal_context(fig: go.Figure, frame: pd.DataFrame, signal: object) -> None:
    low = getattr(signal, "entry_zone_low", None)
    high = getattr(signal, "entry_zone_high", None)
    stop = getattr(signal, "stop_loss", None)
    target = getattr(signal, "target_price", None)
    if low is not None and high is not None:
        fig.add_hrect(
            y0=low,
            y1=high,
            fillcolor="rgba(59,130,246,0.12)",
            line_width=0,
            annotation_text="Entry zone",
            annotation_position="top left",
        )
    if stop is not None:
        fig.add_hline(y=stop, line_color="#DC2626", line_dash="dot", annotation_text="Stop")
    if target is not None:
        fig.add_hline(y=target, line_color="#16A34A", line_dash="dash", annotation_text="Target")

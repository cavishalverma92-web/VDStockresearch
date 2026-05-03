"""Reusable Plotly price chart component for the Streamlit UI."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def _series(frame: pd.DataFrame, column: str) -> pd.Series | None:
    if column not in frame.columns:
        return None
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.dropna().empty:
        return None
    return values


def _volume_colors(frame: pd.DataFrame) -> list[str]:
    closes = pd.to_numeric(frame.get("close"), errors="coerce")
    previous = closes.shift(1).fillna(closes)
    return [
        "rgba(20, 130, 90, 0.38)" if close >= prev else "rgba(210, 72, 72, 0.38)"
        for close, prev in zip(closes, previous, strict=False)
    ]


def _safe_attr(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _add_signal_context(fig: go.Figure, signal: Any) -> None:
    entry_low = _safe_attr(signal, "entry_zone_low")
    entry_high = _safe_attr(signal, "entry_zone_high")
    stop_loss = _safe_attr(signal, "stop_loss")
    target = _safe_attr(signal, "target_price")
    name = _safe_attr(signal, "name") or "Active signal"

    if entry_low is not None and entry_high is not None:
        fig.add_hrect(
            y0=entry_low,
            y1=entry_high,
            fillcolor="rgba(37, 99, 235, 0.12)",
            line_width=0,
            annotation_text=f"{name} entry zone",
            annotation_position="top left",
            row=1,
            col=1,
        )
    if stop_loss is not None:
        fig.add_hline(
            y=stop_loss,
            line_color="#dc2626",
            line_dash="dot",
            annotation_text="Stop-loss",
            annotation_position="bottom right",
            row=1,
            col=1,
        )
    if target is not None:
        fig.add_hline(
            y=target,
            line_color="#16a34a",
            line_dash="dash",
            annotation_text="Target",
            annotation_position="top right",
            row=1,
            col=1,
        )


def _add_event_marker(fig: go.Figure, event: dict[str, object]) -> None:
    date = event.get("date")
    if date is None:
        return
    label = str(event.get("label") or "Event")
    marker_date = pd.Timestamp(date)
    fig.add_shape(
        type="line",
        x0=marker_date,
        x1=marker_date,
        y0=0,
        y1=1,
        xref="x",
        yref="y domain",
        line_color="#7c3aed",
        line_dash="dot",
        line_width=1,
    )
    fig.add_annotation(
        x=marker_date,
        y=1,
        xref="x",
        yref="y domain",
        text=label,
        showarrow=False,
        yshift=10,
        font=dict(size=10, color="#6d28d9"),
    )


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
    active_signals: list[Any] | None = None,
    event_markers: list[dict[str, object]] | None = None,
    freshness_note: str | None = None,
) -> go.Figure:
    """Build a readable stock chart with optional overlays off by default."""

    if frame.empty:
        return go.Figure()

    price_frame = frame.copy()
    price_frame.index = pd.to_datetime(price_frame.index)
    technicals = technical_frame.copy()
    technicals.index = pd.to_datetime(technicals.index)

    rows = 2 if show_volume else 1
    row_heights = [0.74, 0.26] if show_volume else [1.0]
    fig = make_subplots(
        rows=rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=row_heights,
    )

    fig.add_trace(
        go.Candlestick(
            x=price_frame.index,
            open=price_frame["open"],
            high=price_frame["high"],
            low=price_frame["low"],
            close=price_frame["close"],
            name=symbol,
            increasing_line_color="#14825a",
            decreasing_line_color="#d24848",
        ),
        row=1,
        col=1,
    )

    overlay_specs = [
        ("ema_50", "50 EMA", "#2563eb", True),
        ("ema_20", "20 EMA", "#0f766e", show_20_ema),
        ("ema_200", "200 EMA", "#9333ea", show_200_ema),
    ]
    for column, name, color, enabled in overlay_specs:
        values = _series(technicals, column)
        if enabled and values is not None:
            fig.add_trace(
                go.Scatter(
                    x=technicals.index,
                    y=values,
                    mode="lines",
                    line=dict(color=color, width=1.6),
                    name=name,
                ),
                row=1,
                col=1,
            )

    if show_bollinger:
        upper = _series(technicals, "bb_upper")
        lower = _series(technicals, "bb_lower")
        if upper is not None and lower is not None:
            fig.add_trace(
                go.Scatter(
                    x=technicals.index,
                    y=upper,
                    mode="lines",
                    line=dict(color="rgba(100,116,139,0.55)", width=1),
                    name="Bollinger upper",
                ),
                row=1,
                col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=technicals.index,
                    y=lower,
                    mode="lines",
                    fill="tonexty",
                    fillcolor="rgba(100,116,139,0.10)",
                    line=dict(color="rgba(100,116,139,0.55)", width=1),
                    name="Bollinger lower",
                ),
                row=1,
                col=1,
            )

    if show_52w:
        high_52w = _series(technicals, "high_52w")
        low_52w = _series(technicals, "low_52w")
        if high_52w is not None:
            fig.add_trace(
                go.Scatter(
                    x=technicals.index,
                    y=high_52w,
                    mode="lines",
                    line=dict(color="#ca8a04", width=1, dash="dash"),
                    name="52W high",
                ),
                row=1,
                col=1,
            )
        if low_52w is not None:
            fig.add_trace(
                go.Scatter(
                    x=technicals.index,
                    y=low_52w,
                    mode="lines",
                    line=dict(color="#64748b", width=1, dash="dash"),
                    name="52W low",
                ),
                row=1,
                col=1,
            )

    if show_volume and "volume" in price_frame.columns:
        fig.add_trace(
            go.Bar(
                x=price_frame.index,
                y=pd.to_numeric(price_frame["volume"], errors="coerce"),
                marker_color=_volume_colors(price_frame),
                name="Volume",
            ),
            row=2,
            col=1,
        )
        fig.update_yaxes(title_text="Volume", row=2, col=1, fixedrange=True)

    for signal in active_signals or []:
        _add_signal_context(fig, signal)
    for event in event_markers or []:
        _add_event_marker(fig, event)

    last_bar = price_frame.index[-1].strftime("%Y-%m-%d")
    title_parts = [source_label, f"last bar {last_bar}", interval]
    if freshness_note:
        title_parts.append(freshness_note)
    subtitle = " | ".join(title_parts)

    fig.update_layout(
        title=dict(text=f"{symbol}<br><sup>{subtitle}</sup>", x=0.01),
        height=620 if show_volume else 520,
        margin=dict(l=8, r=8, t=72, b=28),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template="plotly_white",
    )
    fig.update_xaxes(
        rangeslider_visible=False,
        rangeselector=dict(
            buttons=[
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(count=3, label="3Y", step="year", stepmode="backward"),
                dict(step="all", label="All"),
            ]
        ),
        row=1,
        col=1,
    )
    fig.update_yaxes(title_text="Price", row=1, col=1, fixedrange=False)
    return fig

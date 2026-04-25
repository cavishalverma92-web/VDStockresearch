"""Technical indicator calculations for Phase 2."""

from __future__ import annotations

import pandas as pd


def add_technical_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    """Return OHLCV data with common technical indicators appended."""
    if frame.empty:
        return frame.copy()

    df = frame.sort_index().copy()
    df["sma_20"] = df["close"].rolling(window=20, min_periods=20).mean()
    df["sma_50"] = df["close"].rolling(window=50, min_periods=50).mean()
    df["sma_100"] = df["close"].rolling(window=100, min_periods=100).mean()
    df["sma_200"] = df["close"].rolling(window=200, min_periods=200).mean()
    df["ema_20"] = df["close"].ewm(span=20, adjust=False, min_periods=20).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False, min_periods=50).mean()
    df["ema_100"] = df["close"].ewm(span=100, adjust=False, min_periods=100).mean()
    df["ema_200"] = df["close"].ewm(span=200, adjust=False, min_periods=200).mean()
    df["rsi_14"] = calculate_rsi(df["close"], period=14)

    ema_12 = df["close"].ewm(span=12, adjust=False, min_periods=12).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False, min_periods=26).mean()
    df["macd"] = ema_12 - ema_26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False, min_periods=9).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    df["bb_mid"] = df["close"].rolling(window=20, min_periods=20).mean()
    bb_std = df["close"].rolling(window=20, min_periods=20).std()
    df["bb_upper"] = df["bb_mid"] + (2 * bb_std)
    df["bb_lower"] = df["bb_mid"] - (2 * bb_std)

    df["atr_14"] = calculate_atr(df, period=14)
    df["atr_pct"] = df["atr_14"] / df["close"] * 100
    df["historical_volatility_20"] = (
        df["close"].pct_change().rolling(window=20, min_periods=20).std() * (252**0.5) * 100
    )
    df["avg_volume_20"] = df["volume"].rolling(window=20, min_periods=20).mean()
    df["relative_volume"] = df["volume"] / df["avg_volume_20"]
    df["high_52w"] = df["high"].rolling(window=252, min_periods=20).max()
    df["low_52w"] = df["low"].rolling(window=252, min_periods=20).min()
    df["all_time_high"] = df["high"].cummax()
    df["distance_from_52w_high_pct"] = (df["close"] - df["high_52w"]) / df["high_52w"] * 100
    df["distance_from_52w_low_pct"] = (df["close"] - df["low_52w"]) / df["low_52w"] * 100
    df["distance_from_all_time_high_pct"] = (
        (df["close"] - df["all_time_high"]) / df["all_time_high"] * 100
    )
    bullish_stack = df["sma_20"] > df["sma_50"]
    bullish_stack &= df["sma_50"] > df["sma_100"]
    bullish_stack &= df["sma_100"] > df["sma_200"]
    bearish_stack = df["sma_20"] < df["sma_50"]
    bearish_stack &= df["sma_50"] < df["sma_100"]
    bearish_stack &= df["sma_100"] < df["sma_200"]
    df["ma_stack_status"] = "mixed"
    df.loc[bullish_stack, "ma_stack_status"] = "bullish"
    df.loc[bearish_stack, "ma_stack_status"] = "bearish"
    return df


def calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Calculate Wilder-style RSI."""
    delta = close.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = losses.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.where(avg_loss != 0, 100.0)


def calculate_atr(frame: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range."""
    previous_close = frame["close"].shift(1)
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

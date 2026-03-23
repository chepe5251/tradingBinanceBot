"""Shared indicator and market-context helpers used across runtime modules."""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException


def ema(series: pd.Series, period: int) -> pd.Series:
    """Return EMA with Binance-compatible smoothing."""
    return series.ewm(span=period, adjust=False).mean()


def atr_series(df: pd.DataFrame, period: int) -> pd.Series:
    """Return ATR series based on true range EWMA."""
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def atr_last(df: pd.DataFrame, period: int) -> float:
    """Return latest ATR value or 0.0 when insufficient data."""
    if df.empty or len(df) < period + 2:
        return 0.0
    atr = atr_series(df, period)
    last = atr.iloc[-1]
    return float(last) if not pd.isna(last) else 0.0


def rsi(series: pd.Series, period: int) -> pd.Series:
    """Return RSI series using EWMA gain/loss smoothing."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def context_direction(df: pd.DataFrame, ema_period: int) -> Optional[str]:
    """Infer trend direction from price relative to EMA."""
    if df.empty or len(df) < ema_period:
        return None
    ema_values = ema(df["close"], ema_period)
    last_ema = ema_values.iloc[-1]
    if pd.isna(last_ema):
        return None
    return "LONG" if float(df["close"].iloc[-1]) > float(last_ema) else "SHORT"


def context_slope(df: pd.DataFrame, ema_period: int) -> float:
    """Return normalized 1-bar EMA slope for trend-strength checks."""
    if df.empty or len(df) < ema_period + 2:
        return 0.0
    ema_values = ema(df["close"], ema_period)
    last = float(ema_values.iloc[-1])
    prev = float(ema_values.iloc[-2])
    if prev == 0.0 or pd.isna(prev) or pd.isna(last):
        return 0.0
    return (last - prev) / prev


def safe_mark_price(
    client: Client,
    symbol: str,
    logger: logging.Logger | None = None,
) -> float | None:
    """Fetch mark price and return None on recoverable API/network errors."""
    try:
        payload = client.futures_mark_price(symbol=symbol)
        mark = payload.get("markPrice")
        return float(mark) if mark is not None else None
    except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
        if logger is not None:
            logger.debug("mark_price_failed symbol=%s err=%s", symbol, exc)
        return None

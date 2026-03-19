"""Liquidity Sweep Reversal signal engine for M15 futures trading.

Detects candles that sweep a key liquidity level (breaking a 20-bar high/low)
but close back inside the prior range, signalling absorption and a likely
reversal.  Confirmation is required from the next closed candle before any
signal is emitted.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

EMA_TREND_FAST = 50
EMA_TREND_SLOW = 200
SWEEP_LOOKBACK = 20
VOL_LOOKBACK = 20
MIN_WICK_RATIO = 0.6
VOL_MULT = 1.3
MAX_RANGE_ATR = 2.0
MIN_RISK_ATR = 0.5
RR_TARGET = 2.0


def _ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=period, adjust=False).mean()


def _atr(df: pd.DataFrame, period: int) -> pd.Series:
    """ATR using EWMA of true range."""
    high = df["high"]
    low = df["low"]
    close = df["close"]
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


def _htf_bias(context_df: pd.DataFrame, ema_period: int) -> Optional[str]:
    """Return 'LONG', 'SHORT', or None if insufficient 1H data."""
    if context_df is None or context_df.empty or len(context_df) < ema_period:
        return None
    ema = _ema(context_df["close"], ema_period)
    if ema.isna().iloc[-1]:
        return None
    return "LONG" if float(context_df["close"].iloc[-1]) > float(ema.iloc[-1]) else "SHORT"


def evaluate_signal(
    main_df: pd.DataFrame,
    context_df: pd.DataFrame,
    ema_trend: int,
    ema_fast: int,
    ema_mid: int,
    atr_period: int,
    atr_avg_window: int,
    volume_avg_window: int,
    rsi_period: int,
    rsi_long_min: float,
    rsi_long_max: float,
    rsi_short_min: float,
    rsi_short_max: float,
    volume_min_ratio: float,
) -> Optional[dict]:
    """Evaluate one symbol for a Liquidity Sweep Reversal entry.

    Requires at least 230 M15 candles so EMA200 is meaningful.
    Uses context_df (1H) to gate signals against the higher-timeframe trend.
    """
    del (
        ema_fast, ema_mid, atr_avg_window, volume_avg_window,
        rsi_period, rsi_long_min, rsi_long_max, rsi_short_min, rsi_short_max,
        volume_min_ratio,
    )

    if main_df.empty or len(main_df) < 230:
        return None

    # 1H bias — used both as a gate and surfaced in the signal dict
    bias = _htf_bias(context_df, ema_trend)

    m15 = main_df.copy()

    m15["ema50"] = _ema(m15["close"], EMA_TREND_FAST)
    m15["ema200"] = _ema(m15["close"], EMA_TREND_SLOW)
    m15["atr"] = _atr(m15, atr_period)
    m15["avg_vol20"] = m15["volume"].rolling(VOL_LOOKBACK).mean()
    # Reference levels are shifted so the sweep candle itself is excluded.
    m15["highest_high_20"] = m15["high"].rolling(SWEEP_LOOKBACK).max().shift(1)
    m15["lowest_low_20"] = m15["low"].rolling(SWEEP_LOOKBACK).min().shift(1)

    sweep = m15.iloc[-2]    # candle that swept the level
    confirm = m15.iloc[-1]  # candle that confirms the reversal (just closed)

    required = [
        sweep["ema50"], sweep["ema200"], sweep["atr"],
        sweep["avg_vol20"], sweep["highest_high_20"], sweep["lowest_low_20"],
        sweep["high"], sweep["low"], sweep["open"], sweep["close"], sweep["volume"],
        confirm["high"], confirm["low"], confirm["close"],
    ]
    if any(pd.isna(v) for v in required):
        return None

    ema50 = float(sweep["ema50"])
    ema200 = float(sweep["ema200"])
    atr_val = float(sweep["atr"])
    avg_vol = float(sweep["avg_vol20"])
    highest_20 = float(sweep["highest_high_20"])
    lowest_20 = float(sweep["lowest_low_20"])

    s_high = float(sweep["high"])
    s_low = float(sweep["low"])
    s_open = float(sweep["open"])
    s_close = float(sweep["close"])
    s_vol = float(sweep["volume"])

    c_high = float(confirm["high"])
    c_low = float(confirm["low"])
    c_close = float(confirm["close"])

    if atr_val <= 0 or avg_vol <= 0:
        return None

    sweep_range = s_high - s_low
    if sweep_range <= 0:
        return None

    entry_price = c_close

    ts = confirm.get("close_time")
    timestamp = (
        ts.strftime("%Y-%m-%d %H:%M:%S UTC")
        if isinstance(ts, pd.Timestamp)
        else str(ts)
    )

    # ── LONG ──────────────────────────────────────────────────────────────────
    # Gate: 1H bias must be LONG or unknown (permissive when data insufficient)
    if bias is None or bias == "LONG":
        if s_close > ema200 and ema50 > ema200:               # 1. trend bullish
            if s_low < lowest_20:                              # 2. bearish sweep
                if s_close > lowest_20:                        # 3. false breakout
                    lower_wick = min(s_open, s_close) - s_low
                    wick_ratio = lower_wick / sweep_range
                    if wick_ratio >= MIN_WICK_RATIO:           # 4. absorption
                        if s_vol >= VOL_MULT * avg_vol:        # 5. volume
                            if sweep_range < MAX_RANGE_ATR * atr_val:   # 6. size
                                if c_high > s_high and c_close > s_close:  # 7. confirm
                                    stop_price = s_low
                                    risk = entry_price - stop_price
                                    if risk >= MIN_RISK_ATR * atr_val and risk > 0:
                                        tp_price = entry_price + risk * RR_TARGET
                                        score = round(
                                            min(3.0, (wick_ratio - MIN_WICK_RATIO) / (1 - MIN_WICK_RATIO) * 3)
                                            + min(2.0, (s_vol / avg_vol - VOL_MULT)),
                                            2,
                                        )
                                        return {
                                            "side": "BUY",
                                            "price": entry_price,
                                            "stop_price": stop_price,
                                            "tp_price": tp_price,
                                            "risk_per_unit": risk,
                                            "rr_target": RR_TARGET,
                                            "atr": atr_val,
                                            "score": score,
                                            "strategy": "liquidity_sweep_reversal",
                                            "htf_bias": bias or "LONG",
                                            "estructura_valida": True,
                                            "retroceso_valido": True,
                                            "volumen_confirmado": True,
                                            "volume_ok": True,
                                            "confirm_m15": (
                                                f"Sweep below {lowest_20:.4f} | "
                                                f"wick={wick_ratio:.2f} | "
                                                f"vol={s_vol/avg_vol:.1f}x | "
                                                f"confirm close={c_close:.4f}"
                                            ),
                                            "breakout_time": timestamp,
                                        }

    # ── SHORT ─────────────────────────────────────────────────────────────────
    # Gate: 1H bias must be SHORT or unknown
    if bias is None or bias == "SHORT":
        if s_close < ema200 and ema50 < ema200:               # 1. trend bearish
            if s_high > highest_20:                            # 2. bullish sweep
                if s_close < highest_20:                       # 3. false breakout
                    upper_wick = s_high - max(s_open, s_close)
                    wick_ratio = upper_wick / sweep_range
                    if wick_ratio >= MIN_WICK_RATIO:           # 4. absorption
                        if s_vol >= VOL_MULT * avg_vol:        # 5. volume
                            if sweep_range < MAX_RANGE_ATR * atr_val:   # 6. size
                                if c_low < s_low and c_close < s_close:  # 7. confirm
                                    stop_price = s_high
                                    risk = stop_price - entry_price
                                    if risk >= MIN_RISK_ATR * atr_val and risk > 0:
                                        tp_price = entry_price - risk * RR_TARGET
                                        score = round(
                                            min(3.0, (wick_ratio - MIN_WICK_RATIO) / (1 - MIN_WICK_RATIO) * 3)
                                            + min(2.0, (s_vol / avg_vol - VOL_MULT)),
                                            2,
                                        )
                                        return {
                                            "side": "SELL",
                                            "price": entry_price,
                                            "stop_price": stop_price,
                                            "tp_price": tp_price,
                                            "risk_per_unit": risk,
                                            "rr_target": RR_TARGET,
                                            "atr": atr_val,
                                            "score": score,
                                            "strategy": "liquidity_sweep_reversal",
                                            "htf_bias": bias or "SHORT",
                                            "estructura_valida": True,
                                            "retroceso_valido": True,
                                            "volumen_confirmado": True,
                                            "volume_ok": True,
                                            "confirm_m15": (
                                                f"Sweep above {highest_20:.4f} | "
                                                f"wick={wick_ratio:.2f} | "
                                                f"vol={s_vol/avg_vol:.1f}x | "
                                                f"confirm close={c_close:.4f}"
                                            ),
                                            "breakout_time": timestamp,
                                        }

    return None

"""EMA pullback long-only signal engine."""
from __future__ import annotations

from typing import Optional

import pandas as pd

from indicators import atr_series, ema, rsi


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
    volume_min_ratio: float,
    volume_max_ratio: float = 1.5,
    pullback_tolerance_atr: float = 0.8,
    min_ema_spread_atr: float = 0.15,
    max_ema_spread_atr: float = 1.0,
    min_body_ratio: float = 0.35,
    rr_target: float = 2.0,
    min_risk_atr: float = 0.5,
    max_risk_atr: float = 3.0,
    min_score: float = 1.5,
    context_missing_penalty: float = 0.5,
) -> Optional[dict]:
    """Evaluate one symbol for a long setup.

    All thresholds and windows are consumed from arguments so live and backtest
    can share the same strategy behavior from a single configuration source.
    """
    if main_df.empty:
        return None
    min_len = max(ema_trend + 3, volume_avg_window + 3, atr_avg_window + 3, rsi_period + 3)
    if len(main_df) < min_len:
        return None

    df = main_df.copy()
    df["ema_fast"] = ema(df["close"], ema_fast)
    df["ema_mid"] = ema(df["close"], ema_mid)
    df["ema_trend"] = ema(df["close"], ema_trend)
    df["atr"] = atr_series(df, atr_period)
    df["atr_avg"] = df["atr"].rolling(max(1, atr_avg_window)).mean()
    df["avg_vol"] = df["volume"].rolling(max(1, volume_avg_window)).mean()
    df["rsi"] = rsi(df["close"], rsi_period)

    if len(df) < 3:
        return None

    sig = df.iloc[-2]  # signal candle
    conf = df.iloc[-1]  # confirmation candle
    prev = df.iloc[-3]

    s_open = float(sig["open"])
    s_close = float(sig["close"])
    s_high = float(sig["high"])
    s_low = float(sig["low"])
    s_vol = float(sig["volume"])
    s_ema_fast = float(sig["ema_fast"])
    s_ema_mid = float(sig["ema_mid"])
    s_ema_trend = float(sig["ema_trend"])
    s_atr = float(sig["atr"])
    s_avg_atr = float(sig["atr_avg"])
    s_avg_vol = float(sig["avg_vol"])
    s_rsi = float(sig["rsi"])

    c_close = float(conf["close"])
    p_ema_fast = float(prev["ema_fast"])

    required_values = [
        s_open,
        s_close,
        s_high,
        s_low,
        s_vol,
        s_ema_fast,
        s_ema_mid,
        s_ema_trend,
        s_atr,
        s_avg_vol,
        s_rsi,
        c_close,
        p_ema_fast,
    ]
    if any(pd.isna(v) for v in required_values):
        return None
    if s_atr <= 0 or s_avg_vol <= 0:
        return None

    candle_range = s_high - s_low
    if candle_range <= 0:
        return None

    body = abs(s_close - s_open)
    body_ratio = body / candle_range
    entry_price = c_close

    # 1) Structural uptrend.
    if not (s_ema_fast > s_ema_mid and s_ema_mid > s_ema_trend):
        return None

    spread_atr = (s_ema_fast - s_ema_mid) / s_atr
    if spread_atr < min_ema_spread_atr:
        return None
    if spread_atr > max_ema_spread_atr:
        return None

    # 2) Pullback near fast EMA.
    tolerance = pullback_tolerance_atr * s_atr
    if not (s_low <= s_ema_fast + tolerance and s_low >= s_ema_fast - tolerance):
        return None

    # 3) Structure intact after pullback.
    if s_close <= s_ema_mid:
        return None
    if p_ema_fast <= 0:
        return None

    # 4) Momentum and participation filters.
    if not (rsi_long_min <= s_rsi <= rsi_long_max):
        return None
    upper_third = s_low + (2 / 3) * candle_range
    if not (body_ratio >= min_body_ratio and s_close > s_open and s_close > upper_third):
        return None
    volume_ratio = s_vol / s_avg_vol
    if not (volume_min_ratio <= volume_ratio <= volume_max_ratio):
        return None

    # 5) Confirmation candle breaks signal high.
    if c_close <= s_high:
        return None

    htf_bias = "NEUTRAL"
    htf_penalty = 0.0
    min_ctx_len = max(ema_mid, ema_trend)
    if not context_df.empty and len(context_df) >= min_ctx_len:
        ctx_ema_mid = ema(context_df["close"], ema_mid).iloc[-1]
        ctx_ema_trend = ema(context_df["close"], ema_trend).iloc[-1]
        ctx_price = float(context_df["close"].iloc[-1])
        if pd.isna(ctx_ema_mid) or pd.isna(ctx_ema_trend):
            htf_penalty = context_missing_penalty
        elif float(ctx_ema_mid) > float(ctx_ema_trend) and ctx_price > float(ctx_ema_mid):
            htf_bias = "LONG"
        else:
            return None
    else:
        htf_penalty = context_missing_penalty

    stop_price = s_low - (0.1 * s_atr)
    risk_per_unit = entry_price - stop_price
    if risk_per_unit < (min_risk_atr * s_atr) or risk_per_unit > (max_risk_atr * s_atr):
        return None
    tp_price = entry_price + (risk_per_unit * rr_target)

    score = round(
        min(2.0, ((body_ratio - min_body_ratio) / max(1e-9, 1 - min_body_ratio)) * 2)
        + (1.0 if (rsi_long_min + 5) < s_rsi < (rsi_long_max - 5) else 0.0)
        + min(1.0, (spread_atr - min_ema_spread_atr) * 2)
        - htf_penalty,
        2,
    )
    if score < min_score:
        return None

    ts = conf.get("close_time")
    breakout_time = (
        ts.strftime("%Y-%m-%d %H:%M:%S UTC") if isinstance(ts, pd.Timestamp) else str(ts)
    )

    atr_avg_ratio = s_atr / s_avg_atr if s_avg_atr > 0 else 1.0
    return {
        "side": "BUY",
        "price": entry_price,
        "stop_price": stop_price,
        "tp_price": tp_price,
        "risk_per_unit": risk_per_unit,
        "rr_target": rr_target,
        "atr": float(s_atr),
        "score": score,
        "htf_bias": htf_bias,
        "strategy": "ema_pullback_long",
        "confirm_m15": (
            f"ema_pullback body={body_ratio:.2f} vol={volume_ratio:.2f}x "
            f"rsi={s_rsi:.1f} spread={spread_atr:.2f}atr atr_vs_avg={atr_avg_ratio:.2f}x"
        ),
        "breakout_time": breakout_time,
    }

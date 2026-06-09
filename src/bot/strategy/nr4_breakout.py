"""NR4 compression breakout signal engine for 1d."""
from __future__ import annotations

from typing import Optional

import pandas as pd

from bot.indicators import ema
from bot.strategy.config import StrategyConfig
from bot.strategy._helpers import _bump_reject, _format_breakout_time


def _evaluate_nr4_1d(
    df: pd.DataFrame,
    context_df: pd.DataFrame,
    cfg: StrategyConfig,
    rejects: dict | None,
) -> Optional[dict]:
    if len(df) < 14:
        return None

    comp_a = df.iloc[-4]
    comp_b = df.iloc[-3]
    signal = df.iloc[-2]
    current = df.iloc[-1]

    required_values = [
        float(comp_a["high"]),
        float(comp_a["low"]),
        float(comp_b["high"]),
        float(comp_b["low"]),
        float(signal["open"]),
        float(signal["high"]),
        float(signal["low"]),
        float(signal["close"]),
        float(signal["volume"]),
        float(signal["avg_vol"]),
        float(signal["ema_fast"]),
        float(signal["ema_mid"]),
        float(signal["ema_trend"]),
        float(signal["rsi"]),
        float(current["close"]),
        float(current["ema_fast"]),
        float(current["atr"]),
        float(current["avg_vol"]),
        float(current["atr_avg"]),
    ]
    if any(pd.isna(v) for v in required_values):
        return None

    comp_a_high = float(comp_a["high"])
    comp_a_low = float(comp_a["low"])
    comp_b_high = float(comp_b["high"])
    comp_b_low = float(comp_b["low"])

    s_open = float(signal["open"])
    s_high = float(signal["high"])
    s_low = float(signal["low"])
    s_close = float(signal["close"])
    s_vol = float(signal["volume"])
    s_avg_vol = float(signal["avg_vol"])
    s_ema_fast = float(signal["ema_fast"])
    s_ema_mid = float(signal["ema_mid"])
    s_ema_trend = float(signal["ema_trend"])
    s_rsi = float(signal["rsi"])

    c_close = float(current["close"])
    c_ema_fast = float(current["ema_fast"])
    c_atr = float(current["atr"])
    c_atr_avg = float(current["atr_avg"])
    c_avg_vol = float(current["avg_vol"])
    if c_atr <= 0 or s_avg_vol <= 0 or c_avg_vol <= 0:
        return None

    atr10 = float(df["atr"].iloc[-10:].mean())
    if pd.isna(atr10) or atr10 <= 0:
        return None

    comp_range_a = comp_a_high - comp_a_low
    comp_range_b = comp_b_high - comp_b_low
    if comp_range_a <= 0 or comp_range_b <= 0:
        return None
    if not (comp_range_a < 0.90 * atr10 and comp_range_b < 0.90 * atr10):
        _bump_reject(rejects, "reject_nr4_compression")
        return None

    compression_high = max(comp_a_high, comp_b_high)
    breakout_level = compression_high + (0.015 * c_atr)
    if s_close <= breakout_level:
        _bump_reject(rejects, "reject_nr4_breakout")
        return None
    if not (s_ema_fast > s_ema_mid > s_ema_trend):
        _bump_reject(rejects, "reject_nr4_trend")
        return None

    signal_range = s_high - s_low
    if signal_range <= 0:
        return None
    signal_body_ratio = abs(s_close - s_open) / signal_range
    if not (s_close > s_open and signal_body_ratio >= 0.48):
        _bump_reject(rejects, "reject_nr4_signal_body")
        return None

    signal_vol_ratio = s_vol / s_avg_vol
    if signal_vol_ratio < 1.20:
        _bump_reject(rejects, "reject_nr4_signal_volume")
        return None

    confirm_level = s_high + (0.03 * c_atr)
    if c_close <= confirm_level:
        _bump_reject(rejects, "reject_nr4_confirmation")
        return None
    if c_close > s_high + (0.40 * c_atr):
        _bump_reject(rejects, "reject_nr4_confirmation")
        return None
    if c_close > c_ema_fast + (1.10 * c_atr):
        _bump_reject(rejects, "reject_nr4_confirmation")
        return None
    if not (52.0 <= s_rsi <= 62.0):
        _bump_reject(rejects, "reject_nr4_signal_rsi")
        return None

    atr_avg_ratio = c_atr / c_atr_avg if c_atr_avg > 0 else 1.0
    if atr_avg_ratio > cfg.max_atr_avg_ratio:
        _bump_reject(rejects, "reject_nr4_atr_spike")
        return None

    htf_bias = "LONG"
    min_ctx_len = max(cfg.ema_mid, cfg.ema_trend)
    if not context_df.empty and len(context_df) >= min_ctx_len:
        if "ema_mid" in context_df.columns and "ema_trend" in context_df.columns:
            ctx_ema_mid = context_df["ema_mid"].iloc[-1]
            ctx_ema_trend = context_df["ema_trend"].iloc[-1]
        else:
            ctx_ema_mid = ema(context_df["close"], cfg.ema_mid).iloc[-1]
            ctx_ema_trend = ema(context_df["close"], cfg.ema_trend).iloc[-1]
        ctx_price = float(context_df["close"].iloc[-1])
        if not (pd.isna(ctx_ema_mid) or pd.isna(ctx_ema_trend)):
            if not (float(ctx_ema_mid) > float(ctx_ema_trend) and ctx_price > float(ctx_ema_mid)):
                _bump_reject(rejects, "reject_nr4_htf")
                return None

    compression_avg = (comp_range_a + comp_range_b) / 2.0
    score = 0.0
    if compression_avg < (0.60 * atr10):
        score += 0.8
    elif compression_avg < (0.75 * atr10):
        score += 0.5
    if signal_body_ratio >= 0.56:
        score += 0.6
    elif signal_body_ratio >= 0.48:
        score += 0.4
    if signal_vol_ratio >= 1.35:
        score += 0.35
    elif signal_vol_ratio >= 1.20:
        score += 0.2
    if 54.0 <= s_rsi <= 59.5:
        score += 0.5
    elif 53.0 <= s_rsi <= 61.0:
        score += 0.25
    score = round(score, 2)
    if score < 1.90:
        _bump_reject(rejects, "reject_nr4_score")
        return None

    entry_price = c_close
    stop_price = min(comp_a_low, comp_b_low) - (0.3 * c_atr)
    risk_per_unit = entry_price - stop_price
    if risk_per_unit < (cfg.min_risk_atr * c_atr) or risk_per_unit > (cfg.max_risk_atr * c_atr):
        _bump_reject(rejects, "reject_nr4_risk")
        return None
    rr = 2.5
    tp_price = entry_price + (risk_per_unit * rr)

    breakout_time = _format_breakout_time(signal.get("close_time"))
    return {
        "side": "BUY",
        "price": entry_price,
        "stop_price": stop_price,
        "tp_price": tp_price,
        "risk_per_unit": risk_per_unit,
        "rr_target": rr,
        "atr": c_atr,
        "score": score,
        "htf_bias": htf_bias,
        "strategy": "nr4_breakout_1d",
        "confirm_m15": (
            f"nr4_breakout sig_body={signal_body_ratio:.2f} sig_vol={signal_vol_ratio:.2f}x "
            f"sig_rsi={s_rsi:.1f} atr_vs_avg={atr_avg_ratio:.2f}x"
        ),
        "breakout_time": breakout_time,
    }

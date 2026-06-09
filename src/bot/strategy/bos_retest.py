"""BOS (Break of Structure) retest signal engine for 4h."""
from __future__ import annotations

from typing import Optional

import pandas as pd
from bot.indicators import ema
from bot.strategy._helpers import _bump_reject, _format_breakout_time
from bot.strategy.config import StrategyConfig


def _evaluate_bos_4h(
    df: pd.DataFrame,
    context_df: pd.DataFrame,
    cfg: StrategyConfig,
    rejects: dict | None,
) -> Optional[dict]:
    if len(df) < 36:
        return None

    current = df.iloc[-1]
    required_current = [
        float(current["open"]),
        float(current["high"]),
        float(current["low"]),
        float(current["close"]),
        float(current["volume"]),
        float(current["ema_fast"]),
        float(current["ema_mid"]),
        float(current["ema_trend"]),
        float(current["atr"]),
        float(current["atr_avg"]),
        float(current["avg_vol"]),
        float(current["rsi"]),
    ]
    if any(pd.isna(v) for v in required_current):
        return None

    c_open = float(current["open"])
    c_high = float(current["high"])
    c_low = float(current["low"])
    c_close = float(current["close"])
    c_vol = float(current["volume"])
    c_ema_fast = float(current["ema_fast"])
    c_ema_mid = float(current["ema_mid"])
    c_ema_trend = float(current["ema_trend"])
    c_atr = float(current["atr"])
    c_atr_avg = float(current["atr_avg"])
    c_avg_vol = float(current["avg_vol"])
    c_rsi = float(current["rsi"])
    if c_atr <= 0 or c_avg_vol <= 0:
        return None

    current_idx = len(df) - 1
    bos_start = max(2, current_idx - 10)
    bos_end = current_idx - 1
    if bos_end < bos_start:
        _bump_reject(rejects, "reject_bos_missing")
        return None

    bos_idx: int | None = None
    swing_high: float | None = None
    bos_body_ratio = 0.0
    bos_vol_ratio = 0.0
    saw_bos_trend_fail = False
    saw_bos_volume_fail = False

    for idx in range(bos_end, bos_start - 1, -1):
        bos = df.iloc[idx]
        required_bos = [
            float(bos["open"]),
            float(bos["high"]),
            float(bos["low"]),
            float(bos["close"]),
            float(bos["volume"]),
            float(bos["ema_fast"]),
            float(bos["ema_mid"]),
            float(bos["ema_trend"]),
            float(bos["atr"]),
            float(bos["avg_vol"]),
        ]
        if any(pd.isna(v) for v in required_bos):
            continue

        b_open = float(bos["open"])
        b_high = float(bos["high"])
        b_low = float(bos["low"])
        b_close = float(bos["close"])
        b_vol = float(bos["volume"])
        b_avg_vol = float(bos["avg_vol"])
        b_ema_fast = float(bos["ema_fast"])
        b_ema_mid = float(bos["ema_mid"])
        b_ema_trend = float(bos["ema_trend"])
        b_atr = float(bos["atr"])
        b_range = b_high - b_low
        if b_range <= 0 or b_avg_vol <= 0 or b_atr <= 0:
            continue

        # Swing high is the highest high in a simple 10-30 candle lookback,
        # excluding the two candles immediately before BOS.
        hist_end = idx - 3
        hist_start = max(0, idx - 30)
        if hist_end < hist_start:
            continue
        high_window = df["high"].iloc[hist_start : hist_end + 1]
        if len(high_window) < 10:
            continue
        swing_candidate = float(high_window.max())
        if b_close <= swing_candidate:
            continue

        if not (b_ema_fast > b_ema_mid > b_ema_trend):
            saw_bos_trend_fail = True
            continue

        b_vol_ratio = b_vol / b_avg_vol
        if b_vol_ratio < 1.20:
            saw_bos_volume_fail = True
            continue

        b_body_ratio = abs(b_close - b_open) / b_range
        if b_body_ratio < 0.35:
            continue

        bos_idx = idx
        swing_high = swing_candidate
        bos_body_ratio = b_body_ratio
        bos_vol_ratio = b_vol_ratio
        break

    if bos_idx is None or swing_high is None:
        if saw_bos_volume_fail:
            _bump_reject(rejects, "reject_bos_volume")
        elif saw_bos_trend_fail:
            _bump_reject(rejects, "reject_bos_trend")
        else:
            _bump_reject(rejects, "reject_bos_missing")
        return None

    retest_min = swing_high - (0.45 * c_atr)
    retest_max = swing_high + (0.55 * c_atr)
    if not (retest_min <= c_low <= retest_max):
        _bump_reject(rejects, "reject_bos_retest_zone")
        return None
    if c_close <= c_open or c_close < (swing_high - 0.05 * c_atr):
        _bump_reject(rejects, "reject_bos_retest_body")
        return None
    if not (c_ema_fast > c_ema_mid and c_ema_mid >= c_ema_trend):
        _bump_reject(rejects, "reject_bos_structure")
        return None
    if not (45.0 <= c_rsi <= 63.0):
        _bump_reject(rejects, "reject_bos_retest_rsi")
        return None

    atr_avg_ratio = c_atr / c_atr_avg if c_atr_avg > 0 else 1.0
    if atr_avg_ratio > cfg.max_atr_avg_ratio:
        _bump_reject(rejects, "reject_bos_atr_spike")
        return None

    # Block spread >= 1.0 when RSI is not confirmed
    bos_spread_atr = (c_ema_fast - c_ema_mid) / c_atr if c_atr > 0 else 0.0
    if bos_spread_atr >= 1.00 and c_rsi < 57.0:
        _bump_reject(rejects, "reject_bos_spread_cold")
        return None

    htf_bias = "LONG"  # BOS is structural - assume bullish unless context contradicts
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
                strong_bos = (
                    bos_body_ratio >= 0.60
                    and bos_vol_ratio >= 1.40
                    and 50.0 <= c_rsi <= 60.0
                )
                if strong_bos:
                    htf_bias = "NEUTRAL"
                else:
                    _bump_reject(rejects, "reject_bos_htf")
                    return None
    # No penalty when context is absent - BOS is self-sufficient as a structural signal

    score = 0.0
    if bos_body_ratio >= 0.50:
        score += 0.6
    elif bos_body_ratio >= 0.40:
        score += 0.35
    if bos_vol_ratio >= 1.28:
        score += 0.5
    elif bos_vol_ratio >= 1.20:
        score += 0.3
    if abs(c_low - swing_high) <= (0.18 * c_atr):
        score += 0.5
    elif abs(c_low - swing_high) <= (0.30 * c_atr):
        score += 0.3
    if 50.0 <= c_rsi <= 60.0:
        score += 0.4
    elif 48.0 <= c_rsi <= 62.0:
        score += 0.2
    score = round(score, 2)
    if score < 1.70:
        _bump_reject(rejects, "reject_bos_score")
        return None

    entry_price = c_close
    stop_price = swing_high - (0.5 * c_atr)
    risk_per_unit = entry_price - stop_price
    if risk_per_unit < (cfg.min_risk_atr * c_atr) or risk_per_unit > (cfg.max_risk_atr * c_atr):
        _bump_reject(rejects, "reject_bos_risk")
        return None
    rr = 2.5
    tp_price = entry_price + (risk_per_unit * rr)

    breakout_time = _format_breakout_time(df.iloc[bos_idx].get("close_time"))
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
        "strategy": "bos_retest_4h",
        "confirm_m15": (
            f"bos_retest bos_body={bos_body_ratio:.2f} bos_vol={bos_vol_ratio:.2f}x "
            f"entry_rsi={c_rsi:.1f} atr_vs_avg={atr_avg_ratio:.2f}x"
        ),
        "breakout_time": breakout_time,
    }

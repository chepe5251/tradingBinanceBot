"""Pure monitor decision helpers for early-exit reviews."""
from __future__ import annotations

from typing import Any

import pandas as pd

from indicators import context_direction, context_slope, ema


def evaluate_early_exit(
    df: pd.DataFrame,
    side: str,
    ema_fast_period: int,
    ema_mid_period: int,
    ema_trend_period: int,
    volume_avg_window: int,
    trend_slope_min: float,
    break_even: bool,
    context_df: pd.DataFrame | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    """Evaluate structure/volume/context deterioration for early exits."""
    if context_df is None:
        context_df = df
    required_len = max(ema_mid_period, ema_fast_period, volume_avg_window + 2, 4)
    if df.empty or len(df) < required_len:
        return False, "no_data", {
            "ctx_dir": None,
            "ctx_slope": 0.0,
            "struct_break": False,
            "vol_strong": False,
        }

    ema_fast = ema(df["close"], ema_fast_period)
    ema_mid = ema(df["close"], ema_mid_period)
    last = df.iloc[-1]
    ema_fast_last = float(ema_fast.iloc[-1])
    ema_fast_prev = float(ema_fast.iloc[-2])
    ema_mid_last = float(ema_mid.iloc[-1])
    ema_mid_prev = float(ema_mid.iloc[-2])

    candle_range = float(last["high"] - last["low"])
    body = abs(float(last["close"] - last["open"]))
    strong_body = candle_range > 0 and (body / candle_range) >= 0.6

    if side == "BUY":
        cross_against = ema_fast_prev >= ema_mid_prev and ema_fast_last < ema_mid_last
        close_against = float(last["close"]) < ema_mid_last
        struct_break = cross_against and close_against and strong_body
        vol_against = float(last["close"]) < float(last["open"])
    else:
        cross_against = ema_fast_prev <= ema_mid_prev and ema_fast_last > ema_mid_last
        close_against = float(last["close"]) > ema_mid_last
        struct_break = cross_against and close_against and strong_body
        vol_against = float(last["close"]) > float(last["open"])

    volume_slice = df["volume"].iloc[-(volume_avg_window + 1) : -1]
    vol_avg = float(volume_slice.mean()) if not volume_slice.empty else 0.0
    vol_strong = vol_avg > 0 and float(last["volume"]) >= 2 * vol_avg
    volume_break = vol_strong and vol_against and struct_break

    ctx_dir = context_direction(context_df, ema_trend_period)
    ctx_trend_slope = context_slope(context_df, ema_trend_period)
    ctx_changed = (
        (ctx_dir == "SHORT" if side == "BUY" else ctx_dir == "LONG")
        and abs(ctx_trend_slope) >= trend_slope_min
    )

    if break_even:
        return False, "break_even_active", {
            "ctx_dir": ctx_dir,
            "ctx_slope": ctx_trend_slope,
            "struct_break": struct_break,
            "vol_strong": vol_strong,
        }
    if volume_break:
        return True, "volume_break", {
            "ctx_dir": ctx_dir,
            "ctx_slope": ctx_trend_slope,
            "struct_break": struct_break,
            "vol_strong": vol_strong,
        }
    if struct_break:
        return True, "structure_break", {
            "ctx_dir": ctx_dir,
            "ctx_slope": ctx_trend_slope,
            "struct_break": struct_break,
            "vol_strong": vol_strong,
        }
    if ctx_changed:
        return True, "ctx_flip", {
            "ctx_dir": ctx_dir,
            "ctx_slope": ctx_trend_slope,
            "struct_break": struct_break,
            "vol_strong": vol_strong,
        }
    return False, "", {
        "ctx_dir": ctx_dir,
        "ctx_slope": ctx_trend_slope,
        "struct_break": struct_break,
        "vol_strong": vol_strong,
    }

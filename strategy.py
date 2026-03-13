"""Signal engine for conservative M15 continuation entries.

Design goals:
- Reduce low-quality breakouts across a broad symbol universe.
- Enforce strict 1H directional/trend-strength alignment.
- Keep output schema stable for `main.py` and downstream execution code.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

EMA_FAST = 7
EMA_MID = 25
EMA_BIAS_1H = 50
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

VOL_CONFIRM_WINDOW = 5
RANGE_MAX_CROSSES = 4
RANGE_CROSS_LOOKBACK = 15
SWING_LOOKBACK = 8
MIN_RR_TARGET = 1.8


def _ema(series: pd.Series, period: int) -> pd.Series:
    """Compute exponential moving average for a price series."""
    return series.ewm(span=period, adjust=False).mean()


def _macd(close: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return MACD tuple: DIF, DEA (signal), and histogram."""
    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=MACD_SIGNAL, adjust=False).mean()
    hist = dif - dea
    return dif, dea, hist


def _atr(df: pd.DataFrame, period: int) -> pd.Series:
    """Compute ATR using an EWMA of true range."""
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


def _is_flat_range(ema_fast: pd.Series, ema_mid: pd.Series) -> bool:
    """Detect frequent EMA crossover behavior often associated with ranges."""
    if len(ema_fast) < RANGE_CROSS_LOOKBACK or len(ema_mid) < RANGE_CROSS_LOOKBACK:
        return True
    diff = (ema_fast - ema_mid).iloc[-RANGE_CROSS_LOOKBACK:]
    sign = diff > 0
    crosses = int((sign != sign.shift(1)).dropna().sum())
    return crosses >= RANGE_MAX_CROSSES



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
    """Evaluate one symbol and return a normalized signal payload.

    Current ruleset:
    - Strict 1H directional filter (price vs EMA50 + EMA50 slope).
    - M15 trend alignment with early pullback-entry trigger.
    - Anti-range, anti-late-entry, and volume confirmation filters.

    Parameters are kept to preserve the shared strategy interface used by
    `main.py`, even when some knobs are not used by this implementation.
    """
    del ema_trend, ema_fast, ema_mid, atr_avg_window, rsi_period
    del rsi_long_min, rsi_long_max, rsi_short_min, rsi_short_max, volume_min_ratio
    del volume_avg_window

    if main_df.empty or context_df.empty:
        return None
    if len(main_df) < 70 or len(context_df) < 70:
        return None

    m15 = main_df.copy()
    h1 = context_df.copy()

    m15["ema7"] = _ema(m15["close"], EMA_FAST)
    m15["ema25"] = _ema(m15["close"], EMA_MID)
    m15["atr"] = _atr(m15, atr_period)
    m15["vol_avg5_prev"] = m15["volume"].rolling(window=VOL_CONFIRM_WINDOW).mean().shift(1)

    dif, dea, hist = _macd(m15["close"])
    m15["dif"] = dif
    m15["dea"] = dea
    m15["hist"] = hist

    h1["ema50"] = _ema(h1["close"], EMA_BIAS_1H)
    h1["atr"] = _atr(h1, atr_period)

    last = m15.iloc[-1]
    prev1 = m15.iloc[-2]
    prev2 = m15.iloc[-3]
    prev3 = m15.iloc[-4]
    h1_last = h1.iloc[-1]
    h1_prev = h1.iloc[-2]

    required = [
        last["ema7"],
        last["ema25"],
        last["dif"],
        last["vol_avg5_prev"],
        last["atr"],
        last["open"],
        last["high"],
        last["low"],
        prev1["high"],
        prev1["low"],
        prev1["open"],
        prev1["close"],
        prev1["volume"],
        prev1["ema25"],
        prev2["volume"],
        prev2["open"],
        prev2["close"],
        prev2["high"],
        prev2["low"],
        last["hist"],
        prev1["hist"],
        prev3["open"],
        prev3["close"],
        last["close"],
        last["volume"],
        h1_last["close"],
        h1_last["ema50"],
        h1_last["atr"],
        h1_prev["ema50"],
        h1_prev["close"],
    ]
    if any(pd.isna(v) for v in required):
        return None

    price = float(last["close"])
    if price <= 0:
        return None

    # Strong range filter to suppress choppy regimes.
    if _is_flat_range(m15["ema7"], m15["ema25"]):
        return None

    # Strict 1H filter.
    h1_close = float(h1_last["close"])
    h1_ema_last = float(h1_last["ema50"])
    h1_ema_prev = float(h1_prev["ema50"])
    h1_prev_close = float(h1_prev["close"])
    bias_long = (
        h1_close > h1_ema_last
        and h1_ema_last > h1_ema_prev
        and h1_prev_close > h1_ema_prev
    )
    bias_short = (
        h1_close < h1_ema_last
        and h1_ema_last < h1_ema_prev
        and h1_prev_close < h1_ema_prev
    )
    if not (bias_long or bias_short):
        return None

    atr_val = float(last["atr"]) if not pd.isna(last["atr"]) else 0.0
    atr_avg = float(m15["atr"].rolling(window=20).mean().iloc[-1]) if len(m15) >= 25 else 0.0
    atr_1h = float(h1_last["atr"]) if not pd.isna(h1_last["atr"]) else 0.0
    if atr_val <= 0 or atr_avg <= 0 or atr_1h <= 0:
        return None

    # Block late entries.
    candle_range = float(last["high"] - last["low"])
    if candle_range <= 0:
        return None
    if candle_range > (1.6 * atr_val):
        return None

    ema7_last = float(last["ema7"])
    ema25_last = float(last["ema25"])
    if abs(price - ema7_last) > atr_val * 0.45:
        return None

    body = abs(float(last["close"] - last["open"]))
    body_ratio = body / candle_range if candle_range > 0 else 0.0
    if body_ratio > 0.85:
        return None

    # Volume filter.
    vol_avg5_raw = last["vol_avg5_prev"]
    vol_avg5 = float(vol_avg5_raw) if not pd.isna(vol_avg5_raw) else 0.0
    vol_last = float(last["volume"])
    vol_confirm_ok = vol_avg5 > 0 and vol_last >= (vol_avg5 * 1.2)
    if not vol_confirm_ok:
        return None

    prev1_open = float(prev1["open"])
    prev1_close = float(prev1["close"])
    prev1_high = float(prev1["high"])
    prev1_low = float(prev1["low"])
    prev1_ema25 = float(prev1["ema25"])

    prev2_open = float(prev2["open"])
    prev2_close = float(prev2["close"])
    prev2_high = float(prev2["high"])
    prev2_low = float(prev2["low"])
    prev3_open = float(prev3["open"])
    prev3_close = float(prev3["close"])

    swing_w = m15.iloc[-(SWING_LOOKBACK + 1):-1]
    swing_low = float(swing_w["low"].min()) if not swing_w.empty else float(prev1["low"])
    swing_high = float(swing_w["high"].max()) if not swing_w.empty else float(prev1["high"])
    impulse_leg = swing_high - swing_low
    if impulse_leg < atr_val * 0.8:
        return None

    dif_last = float(last["dif"])

    # Pullback structure: allow 1-2 opposite candles, no more.
    red1 = prev1_close < prev1_open
    red2 = prev2_close < prev2_open
    red3 = prev3_close < prev3_open
    green1 = prev1_close > prev1_open
    green2 = prev2_close > prev2_open
    green3 = prev3_close > prev3_open

    pb_range = float(prev1_high - prev1_low)
    pb_body = abs(float(prev1_close - prev1_open))
    pb_body_ratio = (pb_body / pb_range) if pb_range > 0 else 1.0

    # 2-candle pullback: validate second candle body ratio as well.
    if red1 and red2:
        pb2_range = abs(prev2_high - prev2_low)
        pb2_body = abs(prev2_close - prev2_open)
        pb2_body_ratio = pb2_body / pb2_range if pb2_range > 0 else 1.0
        if pb2_body_ratio > 0.6:
            return None
    if green1 and green2:
        pb2_range = abs(prev2_high - prev2_low)
        pb2_body = abs(prev2_close - prev2_open)
        pb2_body_ratio = pb2_body / pb2_range if pb2_range > 0 else 1.0
        if pb2_body_ratio > 0.6:
            return None

    # MACD histogram expanding in the direction of the trade.
    hist_expanding_long = float(last["hist"]) > float(prev1["hist"])
    hist_expanding_short = float(last["hist"]) < float(prev1["hist"])

    # Early trigger: break pullback candle high/low, no giant breakout required.
    long_impulse = (
        bias_long
        and
        ema7_last > ema25_last
        and dif_last > 0
        and red1
        and not (red1 and red2 and red3)
        and prev1_low >= prev1_ema25
        and pb_body_ratio <= 0.6
        and float(last["high"]) > prev1_high
        and price > prev1_close
        and hist_expanding_long
    )
    short_impulse = (
        bias_short
        and
        ema7_last < ema25_last
        and dif_last < 0
        and green1
        and not (green1 and green2 and green3)
        and prev1_high <= prev1_ema25
        and pb_body_ratio <= 0.6
        and float(last["low"]) < prev1_low
        and price < prev1_close
        and hist_expanding_short
    )

    breakout_ts = last.get("close_time")
    if isinstance(breakout_ts, pd.Timestamp):
        breakout_time = breakout_ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        breakout_time = str(breakout_ts)

    if long_impulse:
        if abs(prev1_close - prev1_open) > impulse_leg * 0.60:
            return None
        stop_price = min(swing_low, price - atr_val * 2.0)
        risk_per_unit = price - stop_price
        if risk_per_unit <= 0:
            return None
        if risk_per_unit < atr_val * 0.5:
            return None
        # Keep score for ranking while favoring early, clean pullbacks.
        h1_slope_strength = abs(h1_ema_last - h1_ema_prev) / max(atr_1h, 1e-9)
        pullback_quality = max(0.0, 0.65 - pb_body_ratio)
        impulse_strength = min(2.0, impulse_leg / max(atr_val * 2.0, 1e-9))
        volume_strength = vol_last / max(vol_avg5, 1e-9)
        late_penalty = max(0.0, (candle_range / atr_val) - 1.0)
        score = (
            min(3.0, h1_slope_strength * 20.0)
            + min(2.0, pullback_quality * 6.0)
            + impulse_strength
            + min(1.5, max(0.0, volume_strength - 1.0))
            - min(2.0, late_penalty)
            - min(1.5, max(0.0, (atr_val / atr_avg - 1.3) * 3.0))
        )
        score = round(max(0.0, score), 2)
        if score < 3.0:
            return None
        return {
            "side": "BUY",
            "price": price,
            "atr": atr_val,
            "atr_avg": atr_avg,
            "risk_per_unit": risk_per_unit,
            "rr_target": MIN_RR_TARGET,
            "stop_price": stop_price,
            "tp_price": price + (risk_per_unit * MIN_RR_TARGET),
            "estructura_valida": True,
            "retroceso_valido": True,
            "volumen_confirmado": True,
            "structure_ok": True,
            "volume_ok": True,
            "confirm_m15": "EMA7>EMA25 + close>EMA7 + DIF>0 + cierre sobre maximo previo",
            "breakout_time": breakout_time,
            "score": score,
        }

    if short_impulse:
        if abs(prev1_close - prev1_open) > impulse_leg * 0.60:
            return None
        stop_price = max(swing_high, price + atr_val * 2.0)
        risk_per_unit = stop_price - price
        if risk_per_unit <= 0:
            return None
        if risk_per_unit < atr_val * 0.5:
            return None
        h1_slope_strength = abs(h1_ema_last - h1_ema_prev) / max(atr_1h, 1e-9)
        pullback_quality = max(0.0, 0.65 - pb_body_ratio)
        impulse_strength = min(2.0, impulse_leg / max(atr_val * 2.0, 1e-9))
        volume_strength = vol_last / max(vol_avg5, 1e-9)
        late_penalty = max(0.0, (candle_range / atr_val) - 1.0)
        score = (
            min(3.0, h1_slope_strength * 20.0)
            + min(2.0, pullback_quality * 6.0)
            + impulse_strength
            + min(1.5, max(0.0, volume_strength - 1.0))
            - min(2.0, late_penalty)
            - min(1.5, max(0.0, (atr_val / atr_avg - 1.3) * 3.0))
        )
        score = round(max(0.0, score), 2)
        if score < 3.0:
            return None
        return {
            "side": "SELL",
            "price": price,
            "atr": atr_val,
            "atr_avg": atr_avg,
            "risk_per_unit": risk_per_unit,
            "rr_target": MIN_RR_TARGET,
            "stop_price": stop_price,
            "tp_price": price - (risk_per_unit * MIN_RR_TARGET),
            "estructura_valida": True,
            "retroceso_valido": True,
            "volumen_confirmado": True,
            "structure_ok": True,
            "volume_ok": True,
            "confirm_m15": "EMA7<EMA25 + close<EMA7 + DIF<0 + cierre bajo minimo previo",
            "breakout_time": breakout_time,
            "score": score,
        }

    return None

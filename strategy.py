"""Order Block + Break of Structure (OB+BOS) signal engine for M15 futures.

Detects institutional order blocks by identifying a Break of Structure (BOS)
followed by a price return to the origin zone. Entry is anticipatory —
the bot enters when price returns to the institutional zone, not after
the move has already happened.

Score breakdown (max ~6.0):
  3.0  — wick quality      (wick_ratio vs OB_WICK_RATIO baseline)
  2.0  — volume strength   (current_vol / avg_vol vs OB_VOL_MULT)
  1.0  — OB freshness      (newer BOS = higher score; decays linearly)
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

EMA_FAST = 50
EMA_SLOW = 200
BOS_LOOKBACK = 20          # bars to look back for structure high/low
BOS_BODY_RATIO = 0.60      # minimum body ratio for BOS candle
BOS_VOL_MULT = 1.5         # minimum volume multiplier for BOS candle
OB_WICK_RATIO = 0.40       # minimum wick ratio for OB rejection candle
OB_VOL_MULT = 1.2          # minimum volume for OB rejection candle
VOL_LOOKBACK = 20          # bars for average volume calculation
RR_TARGET = 2.5            # risk:reward target
MIN_RISK_ATR = 0.3         # minimum risk in ATR units
MAX_OB_AGE = 10            # maximum candles since BOS before OB expires


def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _atr(df: pd.DataFrame, period: int) -> pd.Series:
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
    """Evaluate one symbol for an Order Block + BOS entry.

    Requires at least 230 M15 candles so EMA200 is meaningful.
    context_df is accepted for API compatibility but not used by this strategy.
    """
    del (
        ema_trend, ema_fast, ema_mid, atr_avg_window, volume_avg_window,
        rsi_period, rsi_long_min, rsi_long_max, rsi_short_min, rsi_short_max,
        volume_min_ratio, context_df,
    )

    if main_df.empty or len(main_df) < 230:
        return None

    df = main_df.copy()
    df["ema50"] = _ema(df["close"], EMA_FAST)
    df["ema200"] = _ema(df["close"], EMA_SLOW)
    df["atr"] = _atr(df, atr_period)
    df["avg_vol"] = df["volume"].rolling(VOL_LOOKBACK).mean()
    df["body"] = (df["close"] - df["open"]).abs()
    df["range"] = df["high"] - df["low"]
    df["body_ratio"] = df["body"] / df["range"].replace(0, float("nan"))
    # Shifted so the BOS candle itself is not included in its own lookback
    df["highest_high_bos"] = df["high"].rolling(BOS_LOOKBACK).max().shift(1)
    df["lowest_low_bos"] = df["low"].rolling(BOS_LOOKBACK).min().shift(1)

    current = df.iloc[-1]
    required = [
        current["ema50"], current["ema200"], current["atr"], current["avg_vol"],
        current["high"], current["low"], current["open"], current["close"],
        current["volume"],
    ]
    if any(pd.isna(v) for v in required):
        return None

    ema50_cur = float(current["ema50"])
    ema200_cur = float(current["ema200"])
    atr_val = float(current["atr"])
    avg_vol_cur = float(current["avg_vol"])

    if atr_val <= 0 or avg_vol_cur <= 0:
        return None

    cur_close = float(current["close"])
    cur_low = float(current["low"])
    cur_high = float(current["high"])
    cur_open = float(current["open"])
    cur_vol = float(current["volume"])
    cur_range = cur_high - cur_low

    if cur_range <= 0:
        return None

    ts = current.get("close_time")
    timestamp = (
        ts.strftime("%Y-%m-%d %H:%M:%S UTC")
        if isinstance(ts, pd.Timestamp)
        else str(ts)
    )

    n = len(df)
    # Scan bars before the current one; allow enough history for BOS lookback
    scan_end = n - 1      # exclusive: current bar is the rejection candle
    scan_start = max(0, scan_end - (MAX_OB_AGE + 5))

    # ── helpers ──────────────────────────────────────────────────────────────

    def _find_bos_long():
        """Return (bos_idx, ob_candle) for the most recent valid LONG BOS."""
        for i in range(scan_end - 1, scan_start - 1, -1):
            bar = df.iloc[i]
            needed = ["highest_high_bos", "avg_vol", "body_ratio",
                      "high", "low", "open", "close", "volume"]
            if any(pd.isna(bar[c]) for c in needed):
                continue
            if float(bar["range"]) <= 0:
                continue
            # BOS: strong bullish candle breaks above prior structure high
            if not (
                float(bar["high"]) > float(bar["highest_high_bos"])
                and float(bar["body_ratio"]) >= BOS_BODY_RATIO
                and float(bar["volume"]) >= BOS_VOL_MULT * float(bar["avg_vol"])
                and float(bar["close"]) > float(bar["open"])
            ):
                continue
            # Order Block: last bearish candle immediately before the BOS candle
            for j in range(i - 1, max(0, i - 10), -1):
                ob = df.iloc[j]
                if float(ob["close"]) < float(ob["open"]):
                    return i, ob
        return None

    def _find_bos_short():
        """Return (bos_idx, ob_candle) for the most recent valid SHORT BOS."""
        for i in range(scan_end - 1, scan_start - 1, -1):
            bar = df.iloc[i]
            needed = ["lowest_low_bos", "avg_vol", "body_ratio",
                      "high", "low", "open", "close", "volume"]
            if any(pd.isna(bar[c]) for c in needed):
                continue
            if float(bar["range"]) <= 0:
                continue
            # BOS: strong bearish candle breaks below prior structure low
            if not (
                float(bar["low"]) < float(bar["lowest_low_bos"])
                and float(bar["body_ratio"]) >= BOS_BODY_RATIO
                and float(bar["volume"]) >= BOS_VOL_MULT * float(bar["avg_vol"])
                and float(bar["close"]) < float(bar["open"])
            ):
                continue
            # Order Block: last bullish candle immediately before the BOS candle
            for j in range(i - 1, max(0, i - 10), -1):
                ob = df.iloc[j]
                if float(ob["close"]) > float(ob["open"]):
                    return i, ob
        return None

    def _score(wick_ratio: float, vol_ratio: float, ob_age: int) -> float:
        wick_score = min(3.0, (wick_ratio - OB_WICK_RATIO) / (1 - OB_WICK_RATIO) * 3)
        vol_score = min(2.0, vol_ratio - OB_VOL_MULT)
        freshness_score = min(1.0, (MAX_OB_AGE - ob_age) / MAX_OB_AGE)
        return round(wick_score + vol_score + freshness_score, 2)

    # ── LONG ─────────────────────────────────────────────────────────────────
    if cur_close > ema200_cur and ema50_cur > ema200_cur:
        result = _find_bos_long()
        if result is not None:
            bos_idx, ob_candle = result
            ob_age = (n - 1) - bos_idx
            if ob_age <= MAX_OB_AGE:
                ob_high = float(ob_candle["high"])
                ob_low = float(ob_candle["low"])
                # OB integrity: no close below ob_low between BOS and current bar
                ob_intact = all(
                    float(df.iloc[k]["close"]) >= ob_low
                    for k in range(bos_idx + 1, n - 1)
                )
                if ob_intact and ob_low <= cur_low <= ob_high:
                    if cur_close > ob_low:                      # not violated on close
                        lower_wick = min(cur_open, cur_close) - cur_low
                        wick_ratio = lower_wick / cur_range
                        if wick_ratio >= OB_WICK_RATIO:
                            if cur_vol >= OB_VOL_MULT * avg_vol_cur:
                                stop_price = ob_low - (0.1 * atr_val)
                                risk = cur_close - stop_price
                                if risk >= MIN_RISK_ATR * atr_val and risk > 0:
                                    tp_price = cur_close + risk * RR_TARGET
                                    return {
                                        "side": "BUY",
                                        "price": cur_close,
                                        "stop_price": stop_price,
                                        "tp_price": tp_price,
                                        "risk_per_unit": risk,
                                        "rr_target": RR_TARGET,
                                        "atr": atr_val,
                                        "score": _score(wick_ratio, cur_vol / avg_vol_cur, ob_age),
                                        "strategy": "ob_bos",
                                        "htf_bias": "LONG",
                                        "htf_score_bonus": 0.0,
                                        "estructura_valida": True,
                                        "retroceso_valido": True,
                                        "volumen_confirmado": True,
                                        "volume_ok": True,
                                        "confirm_m15": (
                                            f"OB zone {ob_low:.4f}-{ob_high:.4f} | "
                                            f"BOS age={ob_age} candles | "
                                            f"wick={wick_ratio:.2f} | "
                                            f"vol={cur_vol/avg_vol_cur:.1f}x | "
                                            f"close={cur_close:.4f}"
                                        ),
                                        "breakout_time": timestamp,
                                    }

    # ── SHORT ─────────────────────────────────────────────────────────────────
    if cur_close < ema200_cur and ema50_cur < ema200_cur:
        result = _find_bos_short()
        if result is not None:
            bos_idx, ob_candle = result
            ob_age = (n - 1) - bos_idx
            if ob_age <= MAX_OB_AGE:
                ob_high = float(ob_candle["high"])
                ob_low = float(ob_candle["low"])
                # OB integrity: no close above ob_high between BOS and current bar
                ob_intact = all(
                    float(df.iloc[k]["close"]) <= ob_high
                    for k in range(bos_idx + 1, n - 1)
                )
                if ob_intact and ob_low <= cur_high <= ob_high:
                    if cur_close < ob_high:                     # not violated on close
                        upper_wick = cur_high - max(cur_open, cur_close)
                        wick_ratio = upper_wick / cur_range
                        if wick_ratio >= OB_WICK_RATIO:
                            if cur_vol >= OB_VOL_MULT * avg_vol_cur:
                                stop_price = ob_high + (0.1 * atr_val)
                                risk = stop_price - cur_close
                                if risk >= MIN_RISK_ATR * atr_val and risk > 0:
                                    tp_price = cur_close - risk * RR_TARGET
                                    return {
                                        "side": "SELL",
                                        "price": cur_close,
                                        "stop_price": stop_price,
                                        "tp_price": tp_price,
                                        "risk_per_unit": risk,
                                        "rr_target": RR_TARGET,
                                        "atr": atr_val,
                                        "score": _score(wick_ratio, cur_vol / avg_vol_cur, ob_age),
                                        "strategy": "ob_bos",
                                        "htf_bias": "SHORT",
                                        "htf_score_bonus": 0.0,
                                        "estructura_valida": True,
                                        "retroceso_valido": True,
                                        "volumen_confirmado": True,
                                        "volume_ok": True,
                                        "confirm_m15": (
                                            f"OB zone {ob_low:.4f}-{ob_high:.4f} | "
                                            f"BOS age={ob_age} candles | "
                                            f"wick={wick_ratio:.2f} | "
                                            f"vol={cur_vol/avg_vol_cur:.1f}x | "
                                            f"close={cur_close:.4f}"
                                        ),
                                        "breakout_time": timestamp,
                                    }

    return None

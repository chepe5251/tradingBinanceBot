"""Mean-reversion short signal engine for 15m_short."""
from __future__ import annotations

from typing import Optional

import pandas as pd
from bot.indicators import ema
from bot.strategy._helpers import _bump_reject
from bot.strategy.config import StrategyConfig


def _evaluate_short_15m(
    df: pd.DataFrame,
    context_df: pd.DataFrame,
    cfg: StrategyConfig,
    rejects: dict | None,
) -> Optional[dict]:
    """Mean-reversion short for 15m: fade overextended bullish moves."""
    if len(df) < 3:
        return None

    def _r(key: str) -> None:
        _bump_reject(rejects, key)

    sig  = df.iloc[-2]   # signal candle
    conf = df.iloc[-1]   # confirmation candle

    required = [
        "open","high","low","close","volume",
        "ema_fast","ema_mid","ema_trend","atr","atr_avg","avg_vol","rsi",
    ]
    if any(pd.isna(sig.get(c)) for c in required):
        return None
    if any(pd.isna(conf.get(c)) for c in required[:6]):
        return None

    s_open     = float(sig["open"])
    s_high     = float(sig["high"])
    s_low      = float(sig["low"])
    s_close    = float(sig["close"])
    s_vol      = float(sig["volume"])
    s_ema_fast = float(sig["ema_fast"])
    s_ema_mid  = float(sig["ema_mid"])
    s_ema_trend= float(sig["ema_trend"])
    s_atr      = float(sig["atr"])
    s_avg_atr  = float(sig["atr_avg"])
    s_avg_vol  = float(sig["avg_vol"])
    s_rsi      = float(sig["rsi"])

    c_close    = float(conf["close"])
    c_open     = float(conf["open"])
    c_high     = float(conf["high"])
    c_low      = float(conf["low"])

    if s_atr <= 0 or s_avg_vol <= 0:
        return None

    candle_range = s_high - s_low
    if candle_range <= 0:
        return None

    # 1) Tendencia: NO requiere downtrend estructural.
    #    Solo requiere que EMA20 > EMA50 (mercado no en colapso total)
    #    para confirmar que hay algo que revertir.
    if s_ema_fast <= s_ema_mid:
        _r("reject_short_no_extension_base")
        return None

    # 2) Extensión: precio muy por encima de EMA20.
    extension_atr = (s_high - s_ema_fast) / s_atr
    if extension_atr < cfg.short_min_extension_atr:
        _r("reject_short_extension")
        return None

    # Block when EMA20-EMA50 spread is too wide — strong trend,
    # mean reversion has no edge.
    short_spread_atr = (s_ema_fast - s_ema_mid) / s_atr
    if short_spread_atr >= cfg.short_max_spread_atr:
        _r("reject_short_spread_wide")
        return None

    # 3) RSI sobrecomprado. Ventana amplia: rechazar solo si NO esta
    #    sobrecomprado o si esta tan extremo que suele preceder squeeze.
    if not (68.0 <= s_rsi <= 82.0):
        _r("reject_short_rsi")
        return None

    # 4) Vela de señal bajista con cuerpo fuerte en tercio inferior.
    body = abs(s_close - s_open)
    body_ratio = body / candle_range
    lower_third = s_high - (2 / 3) * candle_range
    if not (s_close < s_open and body_ratio >= cfg.short_min_body_ratio and s_close < lower_third):
        _r("reject_short_body")
        return None

    # 5) Volumen presente.
    volume_ratio = s_vol / s_avg_vol
    if volume_ratio < 1.10:
        _r("reject_short_volume")
        return None

    # 6) ATR no en spike extremo.
    atr_avg_ratio = s_atr / s_avg_atr if s_avg_atr > 0 else 1.0
    if atr_avg_ratio > cfg.max_atr_avg_ratio:
        _r("reject_short_atr_spike")
        return None

    # 7) Confirmación: vela bajista que cierra bajo el low de la señal.
    conf_range = c_high - c_low
    conf_body  = abs(c_close - c_open)
    conf_body_ratio = conf_body / conf_range if conf_range > 0 else 0.0
    if not (
        c_close < c_open          # bajista
        and c_close < s_low       # cierra bajo el low de señal
        and conf_body_ratio >= 0.35  # cuerpo decente
    ):
        _r("reject_short_confirmation")
        return None

    # 8) Contexto HTF: solo operar short si HTF no es fuertemente alcista,
    #    o si la extensión es tan extrema que justifica contra-tendencia.
    htf_penalty = 0.0
    min_ctx_len = max(cfg.ema_mid, cfg.ema_trend)
    if not context_df.empty and len(context_df) >= min_ctx_len:
        if "ema_mid" in context_df.columns and "ema_trend" in context_df.columns:
            ctx_ema_mid   = context_df["ema_mid"].iloc[-1]
            ctx_ema_trend = context_df["ema_trend"].iloc[-1]
        else:
            ctx_ema_mid   = ema(context_df["close"], cfg.ema_mid).iloc[-1]
            ctx_ema_trend = ema(context_df["close"], cfg.ema_trend).iloc[-1]
        ctx_price = float(context_df["close"].iloc[-1])
        if not (pd.isna(ctx_ema_mid) or pd.isna(ctx_ema_trend)):
            if float(ctx_ema_mid) > float(ctx_ema_trend) and ctx_price > float(ctx_ema_mid):
                # HTF alcista: solo permitir si extensión es extrema (>= 2.5 ATR)
                if extension_atr >= 2.50:
                    htf_penalty = 0.25  # penalidad por ir contra tendencia
                else:
                    _r("reject_short_htf")
                    return None
    else:
        htf_penalty = 0.15  # sin contexto: penalidad leve

    # 9) Score. ext_score y rsi_score escalan de forma continua.
    ext_score = min(1.0, max(0.0, (extension_atr - 1.80) / 0.80))   # 1.8..2.6 ATR
    rsi_score = min(1.0, max(0.0, (s_rsi - 68.0) / 12.0))           # RSI 68..80
    score = round(
        0.8 * body_ratio
        + 0.7 * ext_score
        + 0.6 * rsi_score
        + (0.3 if volume_ratio >= 1.30 else 0.0)
        - htf_penalty,
        2,
    )
    if score < 1.30:
        _r("reject_short_score")
        return None

    # 10) SL/TP.
    entry_price   = c_close
    stop_price    = s_high + (0.30 * s_atr)   # sobre el high de señal
    risk_per_unit = stop_price - entry_price
    if risk_per_unit < (cfg.min_risk_atr * s_atr) or risk_per_unit > (cfg.max_risk_atr * s_atr):
        _r("reject_short_risk")
        return None
    # RR de contra-tendencia: 1.8 para cubrir fees y dar margen positivo.
    rr = 1.8
    tp_price = entry_price - (risk_per_unit * rr)

    ts = conf.get("close_time")
    breakout_time = (
        ts.strftime("%Y-%m-%d %H:%M:%S UTC")
        if isinstance(ts, pd.Timestamp) else str(ts)
    )

    return {
        "side": "SELL",
        "price": entry_price,
        "stop_price": stop_price,
        "tp_price": tp_price,
        "risk_per_unit": risk_per_unit,
        "rr_target": rr,
        "atr": float(s_atr),
        "score": score,
        "htf_bias": "SHORT_MR",
        "strategy": "mean_reversion_short_15m",
        "confirm_m15": (
            f"mr_short body={body_ratio:.2f} vol={volume_ratio:.2f}x "
            f"rsi={s_rsi:.1f} ext={extension_atr:.2f}atr atr_vs_avg={atr_avg_ratio:.2f}x"
        ),
        "breakout_time": breakout_time,
    }

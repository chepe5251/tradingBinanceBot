"""Signal evaluation and ranking helpers."""
from __future__ import annotations

import threading
from dataclasses import dataclass

import pandas as pd

from config import Settings
from indicators import atr_series, ema, rsi
from strategy import StrategyConfig, evaluate_signal

_INDICATOR_CACHE_LOCK = threading.Lock()
_INDICATOR_CACHE_MAX = 8000
_INDICATOR_DF_CACHE: dict[
    tuple[int, str, str, str, tuple[int, int, int, int, int, int, int]],
    tuple[tuple, pd.DataFrame],
] = {}


@dataclass(frozen=True)
class SignalCandidate:
    """A validated candidate ready for entry gating/execution."""

    symbol: str
    interval: str
    payload: dict

    @property
    def score(self) -> float:
        return float((self.payload or {}).get("score") or 0.0)


def strategy_config_from_settings(settings: Settings) -> StrategyConfig:
    """Build a StrategyConfig from the runtime Settings object."""
    return StrategyConfig(
        ema_fast=settings.ema_fast,
        ema_mid=settings.ema_mid,
        ema_trend=settings.ema_trend,
        atr_period=settings.atr_period,
        atr_avg_window=settings.atr_avg_window,
        volume_avg_window=settings.volume_avg_window,
        rsi_period=settings.rsi_period,
        rsi_long_min=settings.rsi_long_min,
        rsi_long_max=settings.rsi_long_max,
        volume_min_ratio=settings.volume_min_ratio,
        volume_max_ratio=settings.volume_max_ratio,
        pullback_tolerance_atr=settings.pullback_tolerance_atr,
        min_ema_spread_atr=settings.min_ema_spread_atr,
        max_ema_spread_atr=settings.max_ema_spread_atr,
        min_body_ratio=settings.min_body_ratio,
        rr_target=settings.rr_target,
        min_risk_atr=settings.min_risk_atr,
        max_risk_atr=settings.max_risk_atr,
        min_score=settings.min_score,
        max_atr_avg_ratio=settings.max_atr_avg_ratio,
    )


def _indicator_cfg_signature(cfg: StrategyConfig) -> tuple[int, int, int, int, int, int, int]:
    return (
        int(cfg.ema_fast),
        int(cfg.ema_mid),
        int(cfg.ema_trend),
        int(cfg.atr_period),
        int(cfg.atr_avg_window),
        int(cfg.volume_avg_window),
        int(cfg.rsi_period),
    )


def _token_value(value: object) -> int | float | str:
    if isinstance(value, pd.Timestamp):
        return int(value.value)
    if isinstance(value, (int, float, str)):
        return value
    return str(value)


def _df_snapshot_token(df: pd.DataFrame) -> tuple:
    if df.empty:
        return (0,)

    last = df.iloc[-1]
    token: list[object] = [len(df)]
    for col in ("open_time", "close_time", "open", "high", "low", "close", "volume"):
        if col in df.columns:
            token.append(col)
            token.append(_token_value(last[col]))
    return tuple(token)


def _trim_indicator_cache_locked() -> None:
    overflow = len(_INDICATOR_DF_CACHE) - _INDICATOR_CACHE_MAX
    if overflow <= 0:
        return
    for key in list(_INDICATOR_DF_CACHE.keys())[:overflow]:
        _INDICATOR_DF_CACHE.pop(key, None)


def _ensure_main_indicators(df: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    required_cols = ("ema_fast", "ema_mid", "ema_trend", "atr", "atr_avg", "avg_vol", "rsi")
    if all(col in df.columns for col in required_cols):
        return df

    out = df.copy()
    if "ema_fast" not in out.columns:
        out["ema_fast"] = ema(out["close"], cfg.ema_fast)
    if "ema_mid" not in out.columns:
        out["ema_mid"] = ema(out["close"], cfg.ema_mid)
    if "ema_trend" not in out.columns:
        out["ema_trend"] = ema(out["close"], cfg.ema_trend)
    if "atr" not in out.columns:
        out["atr"] = atr_series(out, cfg.atr_period)
    if "atr_avg" not in out.columns:
        out["atr_avg"] = out["atr"].rolling(max(1, cfg.atr_avg_window)).mean()
    if "avg_vol" not in out.columns:
        out["avg_vol"] = out["volume"].rolling(max(1, cfg.volume_avg_window)).mean()
    if "rsi" not in out.columns:
        out["rsi"] = rsi(out["close"], cfg.rsi_period)
    return out


def _ensure_context_indicators(df: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    if df.empty:
        return df
    if "ema_mid" in df.columns and "ema_trend" in df.columns:
        return df

    out = df.copy()
    if "ema_mid" not in out.columns:
        out["ema_mid"] = ema(out["close"], cfg.ema_mid)
    if "ema_trend" not in out.columns:
        out["ema_trend"] = ema(out["close"], cfg.ema_trend)
    return out


def _cached_with_indicators(
    *,
    stream,
    symbol: str,
    interval: str,
    frame: pd.DataFrame,
    cfg: StrategyConfig,
    cfg_sig: tuple[int, int, int, int, int, int, int],
    kind: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame

    cache_key = (id(stream), symbol, interval, kind, cfg_sig)
    token = _df_snapshot_token(frame)
    with _INDICATOR_CACHE_LOCK:
        cached = _INDICATOR_DF_CACHE.get(cache_key)
        if cached is not None and cached[0] == token:
            return cached[1]

    if kind == "main":
        enriched = _ensure_main_indicators(frame, cfg)
    else:
        enriched = _ensure_context_indicators(frame, cfg)

    with _INDICATOR_CACHE_LOCK:
        _INDICATOR_DF_CACHE[cache_key] = (token, enriched)
        _trim_indicator_cache_locked()
    return enriched


def evaluate_interval_signals(
    stream,
    symbols: list[str],
    interval: str,
    context_interval: str | None,
    settings: Settings,
    trades_logger,
    operations=None,
) -> list[SignalCandidate]:
    """Evaluate all symbols for one timeframe and return sorted candidates."""
    valid_signals: list[SignalCandidate] = []
    cfg = strategy_config_from_settings(settings)
    cfg_sig = _indicator_cfg_signature(cfg)
    scanned = 0

    for symbol in symbols:
        symbol_df_raw = stream.get_dataframe(symbol, interval)
        if symbol_df_raw.empty:
            continue
        scanned += 1

        symbol_df = _cached_with_indicators(
            stream=stream,
            symbol=symbol,
            interval=interval,
            frame=symbol_df_raw,
            cfg=cfg,
            cfg_sig=cfg_sig,
            kind="main",
        )

        if context_interval:
            context_df_raw = stream.get_dataframe(symbol, context_interval)
            context_df = _cached_with_indicators(
                stream=stream,
                symbol=symbol,
                interval=context_interval,
                frame=context_df_raw,
                cfg=cfg,
                cfg_sig=cfg_sig,
                kind="context",
            )
        else:
            context_df = pd.DataFrame()

        signal = evaluate_signal(symbol_df, context_df, cfg, interval=interval)
        # Second pass: mean-reversion short for 15m when long fails.
        if signal is None and interval == "15m":
            signal = evaluate_signal(
                symbol_df,
                context_df,
                cfg,
                interval="15m_short",
            )
        if not signal:
            continue

        if interval in settings.block_sell_on_intervals and signal.get("side") == "SELL":
            trades_logger.info(
                "skip %s reason=block_sell_on_interval tf=%s",
                symbol,
                interval,
            )
            if operations is not None:
                try:
                    operations.record_signal_discarded(
                        reason="block_sell_on_interval",
                        symbol=symbol,
                        interval=interval,
                    )
                except (AttributeError, TypeError, ValueError, RuntimeError):
                    pass
            continue

        signal["timeframe"] = interval.upper()
        trades_logger.info(
            "signal %s tf=%s side=%s confirm=%s breakout=%s",
            symbol,
            interval,
            signal.get("side"),
            signal.get("confirm_m15", ""),
            signal.get("breakout_time", ""),
        )
        valid_signals.append(SignalCandidate(symbol=symbol, interval=interval, payload=signal))

    valid_signals.sort(key=lambda item: item.score, reverse=True)
    trades_logger.debug(
        "scan tf=%s scanned=%d signals=%d",
        interval,
        scanned,
        len(valid_signals),
    )
    return valid_signals

"""Signal evaluation and ranking helpers."""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from config import Settings
from strategy import StrategyConfig, evaluate_signal


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


def evaluate_interval_signals(
    stream,
    symbols: list[str],
    interval: str,
    context_interval: str | None,
    settings: Settings,
    trades_logger,
) -> list[SignalCandidate]:
    """Evaluate all symbols for one timeframe and return sorted candidates."""
    valid_signals: list[SignalCandidate] = []
    cfg = strategy_config_from_settings(settings)

    for symbol in symbols:
        symbol_df = stream.get_dataframe(symbol, interval)
        if symbol_df.empty:
            continue

        context_df = (
            stream.get_dataframe(symbol, context_interval) if context_interval else pd.DataFrame()
        )
        signal = evaluate_signal(symbol_df, context_df, cfg)
        if not signal:
            continue

        if interval in settings.block_sell_on_intervals and signal.get("side") == "SELL":
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
    return valid_signals

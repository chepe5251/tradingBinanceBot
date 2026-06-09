"""Strategy configuration dataclass."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyConfig:
    """All tunable parameters for evaluate_signal.

    Build one instance per run and share it between live and backtest so both
    always operate from the same configuration source.
    """

    ema_fast: int = 20
    ema_mid: int = 50
    ema_trend: int = 200
    atr_period: int = 14
    atr_avg_window: int = 30
    volume_avg_window: int = 20
    rsi_period: int = 14
    rsi_long_min: float = 48.0
    rsi_long_max: float = 68.0
    volume_min_ratio: float = 1.05
    volume_max_ratio: float = 1.5
    pullback_tolerance_atr: float = 0.8
    min_ema_spread_atr: float = 0.15
    max_ema_spread_atr: float = 1.0
    min_body_ratio: float = 0.35
    rr_target: float = 2.0
    min_risk_atr: float = 0.5
    max_risk_atr: float = 3.0
    min_score: float = 1.5
    context_missing_penalty: float = 0.5
    max_atr_avg_ratio: float = 2.5
    # Parametros del short de reversion 15m (antes hardcodeados).
    short_min_extension_atr: float = 1.80
    short_max_spread_atr: float = 1.20
    short_min_body_ratio: float = 0.65

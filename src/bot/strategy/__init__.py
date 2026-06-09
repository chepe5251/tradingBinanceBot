"""Strategy package: signal evaluation engines.

Public API:
    StrategyConfig — tunable parameters for evaluate_signal.
    evaluate_signal — dispatcher that selects the right engine per interval.

Internal modules:
    config, engine, bos_retest, nr4_breakout, mean_reversion, _helpers.
"""
from bot.strategy.config import StrategyConfig
from bot.strategy.engine import evaluate_signal

__all__ = ["StrategyConfig", "evaluate_signal"]

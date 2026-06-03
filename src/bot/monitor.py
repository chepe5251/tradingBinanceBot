"""Position monitor facade with modular runtime/protection/orphan helpers.

`PositionMonitor` keeps the public API stable while delegating heavy logic to:
- `monitor_runtime.py` (main supervision loop)
- `monitor_protection.py` (TP/SL protection coordination)
- `monitor_scaling.py` (optional loss-scaling)
- `monitor_orphan.py` (startup orphan recovery)
- `monitor_decisions.py` (early-exit review)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Optional

from binance import Client
from bot.execution import FuturesExecutor
from bot.monitor_decisions import evaluate_monitor_review
from bot.monitor_orphan import resume_orphan_position
from bot.monitor_runtime import run_position_monitor
from bot.monitor_scaling import evaluate_loss_scaling
from bot.risk import RiskManager

if TYPE_CHECKING:
    from config import Settings
    from data_stream import MarketDataStream
    from services.operational_service import OperationalService


class PositionMonitor:
    """Supervises an open position from TP/SL placement through exit."""

    def __init__(
        self,
        executor: FuturesExecutor,
        stream: "MarketDataStream",
        settings: "Settings",
        risk: RiskManager,
        trade_state: dict,
        level_state: dict,
        side: str,
        symbol: str,
        interval: str,
        client_id_prefix: str,
        logger: logging.Logger,
        trades_logger: logging.Logger,
        price_fn: Callable[[], Optional[float]],
        atr_fn: Callable[[], Optional[float]],
        on_event: Callable[[str, float], None],
        pos_cache_invalidate: Callable[[], None],
        risk_updater: Callable[[float, datetime], None],
        min_qty: float = 0.0,
        min_notional: float = 0.0,
        atr_val: float = 0.0,
        signal: Optional[dict] = None,
        sl_swing: float = 0.0,
        sl_atr: float = 0.0,
        exec_type: str = "MARKET",
        margin_to_use: float = 0.0,
        max_hold_candles: int = 50,
        operations: "OperationalService | None" = None,
        trace_id: str = "",
    ) -> None:
        self.executor = executor
        self.stream = stream
        self.settings = settings
        self.risk = risk
        self.trade_state = trade_state
        self.level_state = level_state
        self.side = side
        self.symbol = symbol
        self.interval = interval
        self.client_id_prefix = client_id_prefix
        self.logger = logger
        self.trades_logger = trades_logger
        self.price_fn = price_fn
        self.atr_fn = atr_fn
        self.on_event = on_event
        self.pos_cache_invalidate = pos_cache_invalidate
        self.risk_updater = risk_updater
        self.min_qty = min_qty
        self.min_notional = min_notional
        self.atr_val = atr_val
        self.signal = signal or {}
        self.sl_swing = sl_swing
        self.sl_atr = sl_atr
        self.exec_type = exec_type
        self.margin_to_use = margin_to_use
        self.max_hold_candles = max_hold_candles
        self.operations = operations
        self.trace_id = trace_id

    def _ops_call(self, method: str, **kwargs) -> None:
        if self.operations is None:
            return
        try:
            getattr(self.operations, method)(**kwargs)
        except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
            # Operational telemetry must not interfere with live monitoring.
            self.logger.debug("ops_hook_failed method=%s err=%s", method, exc)

    def _review_fn(self, break_even: bool) -> tuple[bool, str]:
        """Run structure/volume/context checks for potential early exits."""
        return evaluate_monitor_review(self, break_even)

    def _scale_fn(self, state: dict) -> dict | None:
        """Evaluate and execute loss-based DCA scaling stages for a losing position.

        What it does:
            Adds margin in 3 progressive levels when floating loss exceeds
            50%, 100%, and 200% of the initial margin.

        Why it is disabled:
            This martingale-style logic is not validated for live trading and
            can amplify drawdowns on adverse trends.

        How to activate:
            Set ENABLE_LOSS_SCALING=true in .env only after exhaustive
            backtesting with realistic slippage/fees.

        Maximum risk:
            Level 3 can add 4x the initial margin to a position already in loss.
        """
        if not getattr(self.settings, "enable_loss_scaling", False):
            return None
        return evaluate_loss_scaling(self, state)

    def run(self) -> None:
        """Place protections and supervise the trade to completion."""
        run_position_monitor(self)

    @staticmethod
    def resume_orphan(
        orphan: dict,
        symbols: list[str],
        stream: "MarketDataStream",
        settings: "Settings",
        get_executor: Callable[[str], FuturesExecutor],
        risk: RiskManager,
        trade_client: Client,
        pos_cache_invalidate: Callable[[], None],
        risk_updater: Callable[[float, datetime], None],
        logger: logging.Logger,
        trades_logger: logging.Logger,
        operations: "OperationalService | None" = None,
    ) -> None:
        """Recover a single orphaned position and launch daemon monitor thread."""
        resume_orphan_position(
            orphan=orphan,
            symbols=symbols,
            stream=stream,
            settings=settings,
            get_executor=get_executor,
            risk=risk,
            trade_client=trade_client,
            pos_cache_invalidate=pos_cache_invalidate,
            risk_updater=risk_updater,
            logger=logger,
            trades_logger=trades_logger,
            operations=operations,
        )


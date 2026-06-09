"""Risk manager state (pure data)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class RiskState:
    """Mutable runtime state used by `RiskManager` decision rules."""

    consecutive_losses: int = 0
    last_trade_time: datetime | None = None
    day_start_equity: float = 0.0
    current_day: datetime | None = None
    equity: float = 0.0
    paused: bool = False
    loss_pause_until: datetime | None = None

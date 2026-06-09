"""Risk manager: pure trading risk logic.

No I/O. Persistence is delegated to an injected JsonRiskRepository.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from bot.risk.repository import JsonRiskRepository
from bot.risk.state import RiskState

if TYPE_CHECKING:
    import pandas as pd


@dataclass
class RiskManager:
    """Encapsulates cooldown and daily loss guards.

    The caller controls when `can_trade` and `update_trade` are invoked.
    This class does not place orders or read market data from exchanges.
    """

    cooldown_sec: int
    max_consecutive_losses: int
    daily_drawdown_limit: float
    daily_drawdown_limit_usdt: float
    loss_pause_sec: int
    volatility_pause: bool
    volatility_threshold: float
    repository: JsonRiskRepository | None = None
    state: RiskState = field(default_factory=RiskState, init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.repository is not None:
            self.state = self.repository.load()

    def init_equity(self, equity: float) -> None:
        """Initialize day/equity baselines at process startup."""
        now = datetime.now(timezone.utc)
        # Locking protects shared mutable state accessed from scheduler + monitor threads.
        with self._lock:
            self.state.equity = equity
            self.state.day_start_equity = equity
            self.state.current_day = now.date()

    def _roll_day(self, now: datetime) -> None:
        """Reset day-scoped counters when UTC date changes."""
        with self._lock:
            if self.state.current_day != now.date():
                self.state.current_day = now.date()
                self.state.day_start_equity = self.state.equity
                self.state.consecutive_losses = 0
                self.state.paused = False

    def can_trade(self, now: datetime) -> bool:
        """Return whether a new trade may be opened at `now`."""
        with self._lock:
            if self.state.current_day != now.date():
                self.state.current_day = now.date()
                self.state.day_start_equity = self.state.equity
                self.state.consecutive_losses = 0
                self.state.paused = False

            if self.state.paused:
                return False

            if self.state.loss_pause_until and now < self.state.loss_pause_until:
                return False
            if self.state.loss_pause_until and now >= self.state.loss_pause_until:
                self.state.loss_pause_until = None

            if self.state.last_trade_time:
                elapsed = (now - self.state.last_trade_time).total_seconds()
                if elapsed < self.cooldown_sec:
                    return False

            if self.max_consecutive_losses > 0 and self.state.consecutive_losses >= self.max_consecutive_losses:
                self.state.loss_pause_until = now + timedelta(seconds=max(0, self.loss_pause_sec))
                self.state.consecutive_losses = 0
                return False

            if self.daily_drawdown_limit > 0:
                drawdown = 0.0
                if self.state.day_start_equity > 0:
                    drawdown = (
                        self.state.day_start_equity - self.state.equity
                    ) / self.state.day_start_equity
                if drawdown >= self.daily_drawdown_limit:
                    self.state.paused = True
                    return False

            if self.daily_drawdown_limit_usdt > 0:
                dd_usdt = self.state.day_start_equity - self.state.equity
                if dd_usdt >= self.daily_drawdown_limit_usdt:
                    self.state.paused = True
                    return False
            return True

    def update_trade(self, pnl: float, now: datetime) -> None:
        """Apply realized PnL and update post-trade throttling counters."""
        with self._lock:
            if self.state.current_day != now.date():
                self.state.current_day = now.date()
                self.state.day_start_equity = self.state.equity
                self.state.consecutive_losses = 0
                self.state.paused = False
            self.state.equity += pnl
            self.state.last_trade_time = now
            if pnl < 0:
                self.state.consecutive_losses += 1
            else:
                self.state.consecutive_losses = 0

    def reconcile_equity(self, real_equity: float, now: datetime) -> None:
        """Sincroniza el equity con el balance real del exchange.

        Llamar tras cada cierre de trade en modo live para corregir la
        deriva entre el PnL estimado y el balance real (funding, fees,
        redondeos, cierres fuera del monitor).
        """
        with self._lock:
            if self.state.current_day != now.date():
                self.state.current_day = now.date()
                self.state.day_start_equity = real_equity
                self.state.consecutive_losses = 0
                self.state.paused = False
            if real_equity > 0:
                self.state.equity = real_equity

    def snapshot(self) -> RiskState:
        """Return a copy of the mutable state for read-only consumers."""
        with self._lock:
            return RiskState(
                consecutive_losses=self.state.consecutive_losses,
                last_trade_time=self.state.last_trade_time,
                day_start_equity=self.state.day_start_equity,
                current_day=self.state.current_day,
                equity=self.state.equity,
                paused=self.state.paused,
                loss_pause_until=self.state.loss_pause_until,
            )

    def volatility_ok(self, df: pd.DataFrame) -> bool:
        """Check optional single-candle volatility guard."""
        if not self.volatility_pause:
            return True
        if df.empty:
            return False
        last = df.iloc[-1]
        if last["close"] <= 0:
            return False
        candle_range = (last["high"] - last["low"]) / last["close"]
        return candle_range <= self.volatility_threshold

"""Persistence adapter for RiskState.

Reads and writes the risk state to a JSON file. Decoupled from
RiskManager so the manager can be tested without touching disk.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

from bot.persistence import atomic_write_json, load_json_safe
from bot.risk.state import RiskState

logger = logging.getLogger(__name__)


class JsonRiskRepository:
    """JSON-file backed repository for RiskState."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def load(self) -> RiskState:
        """Return the persisted state, or a fresh default RiskState."""
        data = load_json_safe(
            str(self.path),
            on_corrupt=lambda err: logger.warning(
                "risk_state_corrupt path=%s err=%s (moved to .bad)", self.path, err
            ),
        )
        if not data:
            return RiskState()

        try:
            lt = data.get("last_trade_time")
            cd = data.get("current_day")
            lpu = data.get("loss_pause_until")
            return RiskState(
                consecutive_losses=int(data.get("consecutive_losses", 0)),
                last_trade_time=datetime.fromisoformat(lt) if lt else None,
                day_start_equity=float(data.get("day_start_equity", 0.0)),
                current_day=date.fromisoformat(cd) if cd else None,
                equity=float(data.get("equity", 0.0)),
                paused=bool(data.get("paused", False)),
                loss_pause_until=datetime.fromisoformat(lpu) if lpu else None,
            )
        except (TypeError, ValueError) as exc:
            logger.warning("risk_state_invalid_payload path=%s err=%s", self.path, exc)
            return RiskState()

    def save(self, state: RiskState) -> None:
        """Persist the current state atomically."""
        data = {
            "consecutive_losses": state.consecutive_losses,
            "last_trade_time": state.last_trade_time.isoformat() if state.last_trade_time else None,
            "day_start_equity": state.day_start_equity,
            "current_day": state.current_day.isoformat() if state.current_day else None,
            "equity": state.equity,
            "paused": state.paused,
            "loss_pause_until": state.loss_pause_until.isoformat() if state.loss_pause_until else None,
        }
        atomic_write_json(str(self.path), data)

"""Typed domain models for entry/monitor orchestration."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from execution import FuturesExecutor


class SignalPayload(TypedDict, total=False):
    """Canonical signal payload emitted by strategy/evaluation layer."""

    side: str
    price: float
    atr: float
    risk_per_unit: float
    rr_target: float
    score: float
    strategy: str
    htf_bias: str
    timeframe: str
    confirm_m15: str
    breakout_time: str
    stop_price: float
    tp_price: float
    trace_id: str


@dataclass(frozen=True)
class EntryAttempt:
    """Single entry intent used for traceability/logging."""

    symbol: str
    interval: str
    side: str
    qty: float
    entry_price: float
    margin_to_use: float
    trace_id: str


@dataclass
class TradePlan:
    """Pre-entry plan built from signal + market/account context."""

    symbol: str
    interval: str
    side: str
    trace_id: str
    signal: dict[str, Any]
    executor: "FuturesExecutor"
    entry_price: float
    atr_value: float
    signal_risk: float
    signal_rr: float
    risk_distance: float
    reward_distance: float
    sl: float
    tp: float
    entry_df_len: int
    sl_swing: float
    sl_atr: float
    sl_common: float
    available_balance: float
    min_qty: float
    min_notional: float
    margin_to_use: float
    qty_by_margin: float
    qty_l1: float


@dataclass(frozen=True)
class EntryValidationResult:
    """Structured outcome for pre-order validation."""

    ok: bool
    stage: str
    reason: str


@dataclass(frozen=True)
class EntryFillResult:
    """Order submission/fill result."""

    success: bool
    filled_qty: float
    avg_price: float
    exec_type: str
    stage: str = ""
    reason: str = ""
    error_message: str = ""


@dataclass(frozen=True)
class OperationalEvent:
    """Structured operational telemetry event payload."""

    kind: str
    detail: dict[str, Any]
    trace_id: str = ""


@dataclass
class TradeState:
    """Typed trade state passed to monitor runtime."""

    entry_price: float
    qty: float
    sl: float
    tp: float
    risk_distance: float
    breakeven_trigger_pct: float
    anchor_entry_price: float
    anchor_risk_distance: float
    tp_risk_cap: float
    trace_id: str = ""

    def to_dict(self) -> dict[str, float | str]:
        return {
            "entry_price": self.entry_price,
            "qty": self.qty,
            "sl": self.sl,
            "tp": self.tp,
            "risk_distance": self.risk_distance,
            "breakeven_trigger_pct": self.breakeven_trigger_pct,
            "anchor_entry_price": self.anchor_entry_price,
            "anchor_risk_distance": self.anchor_risk_distance,
            "tp_risk_cap": self.tp_risk_cap,
            "trace_id": self.trace_id,
        }


@dataclass
class MonitorState:
    """Combined monitor runtime state for trade and scaling levels."""

    trade: TradeState
    levels: "LevelState" = field(default_factory=lambda: LevelState())

    def to_dict(self) -> dict[str, dict[str, bool | int | float | str]]:
        return {"trade": self.trade.to_dict(), "levels": self.levels.to_dict()}


@dataclass
class LevelState:
    """Typed loss-scaling stage state for monitor runtime."""

    loss_l1_done: bool = False
    loss_l2_done: bool = False
    loss_l3_done: bool = False
    loss_l1_attempts: int = 0
    loss_l2_attempts: int = 0
    loss_l3_attempts: int = 0
    loss_l1_next_try_ts: float = 0.0
    loss_l2_next_try_ts: float = 0.0
    loss_l3_next_try_ts: float = 0.0

    def to_dict(self) -> dict[str, bool | int | float]:
        return {
            "loss_l1_done": self.loss_l1_done,
            "loss_l2_done": self.loss_l2_done,
            "loss_l3_done": self.loss_l3_done,
            "loss_l1_attempts": self.loss_l1_attempts,
            "loss_l2_attempts": self.loss_l2_attempts,
            "loss_l3_attempts": self.loss_l3_attempts,
            "loss_l1_next_try_ts": self.loss_l1_next_try_ts,
            "loss_l2_next_try_ts": self.loss_l2_next_try_ts,
            "loss_l3_next_try_ts": self.loss_l3_next_try_ts,
        }


@dataclass
class MonitorLaunchContext:
    """Finalized context required to instantiate and start PositionMonitor."""

    symbol: str
    side: str
    interval: str
    trace_id: str
    plan: TradePlan
    trade_state: TradeState
    level_state: LevelState = field(default_factory=LevelState)
    filled_qty: float = 0.0
    entry_price: float = 0.0
    exec_type: str = "UNKNOWN"


@dataclass(frozen=True)
class OrphanRecoveryContext:
    """Normalized orphan payload used by startup recovery flow."""

    symbol: str
    side: str
    qty: float
    entry_price: float
    trace_id: str

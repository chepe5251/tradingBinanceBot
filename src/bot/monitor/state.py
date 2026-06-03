"""Typed monitor state adapters used by runtime/orphan flows."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MonitorState:
    """Mutable state for an active monitored trade."""

    entry_price: float
    qty: float
    sl: float
    tp: float
    risk_distance: float
    breakeven_trigger_pct: float
    anchor_entry_price: float = 0.0
    anchor_risk_distance: float = 0.0
    tp_risk_cap: float = 0.0
    db_trade_id: int | None = None
    trace_id: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "MonitorState":
        return cls(
            entry_price=float(raw.get("entry_price", 0.0) or 0.0),
            qty=float(raw.get("qty", 0.0) or 0.0),
            sl=float(raw.get("sl", 0.0) or 0.0),
            tp=float(raw.get("tp", 0.0) or 0.0),
            risk_distance=float(raw.get("risk_distance", 0.0) or 0.0),
            breakeven_trigger_pct=float(raw.get("breakeven_trigger_pct", 0.0) or 0.0),
            anchor_entry_price=float(raw.get("anchor_entry_price", 0.0) or 0.0),
            anchor_risk_distance=float(raw.get("anchor_risk_distance", 0.0) or 0.0),
            tp_risk_cap=float(raw.get("tp_risk_cap", 0.0) or 0.0),
            db_trade_id=raw.get("db_trade_id"),
            trace_id=str(raw.get("trace_id") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
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
            "db_trade_id": self.db_trade_id,
            "trace_id": self.trace_id,
        }


@dataclass
class LevelState:
    """Mutable scaling-level progress state for monitor runtime."""

    loss_l1_done: bool = False
    loss_l2_done: bool = False
    loss_l3_done: bool = False
    loss_l1_attempts: int = 0
    loss_l2_attempts: int = 0
    loss_l3_attempts: int = 0
    loss_l1_next_try_ts: float = 0.0
    loss_l2_next_try_ts: float = 0.0
    loss_l3_next_try_ts: float = 0.0

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "LevelState":
        return cls(
            loss_l1_done=bool(raw.get("loss_l1_done", False)),
            loss_l2_done=bool(raw.get("loss_l2_done", False)),
            loss_l3_done=bool(raw.get("loss_l3_done", False)),
            loss_l1_attempts=int(raw.get("loss_l1_attempts", 0) or 0),
            loss_l2_attempts=int(raw.get("loss_l2_attempts", 0) or 0),
            loss_l3_attempts=int(raw.get("loss_l3_attempts", 0) or 0),
            loss_l1_next_try_ts=float(raw.get("loss_l1_next_try_ts", 0.0) or 0.0),
            loss_l2_next_try_ts=float(raw.get("loss_l2_next_try_ts", 0.0) or 0.0),
            loss_l3_next_try_ts=float(raw.get("loss_l3_next_try_ts", 0.0) or 0.0),
        )

    def to_dict(self) -> dict[str, Any]:
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
class OrphanRecoveryContext:
    """Runtime context used to supervise an orphaned position."""

    symbol: str
    side: str
    entry_price: float
    qty: float
    tp: float
    sl: float
    atr_value: float
    breakeven_trigger_pct: float
    client_id_prefix: str
    trace_id: str
    metadata: dict[str, Any] = field(default_factory=dict)


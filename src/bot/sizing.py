"""Position sizing policies for entry margin selection."""
from __future__ import annotations

from dataclasses import dataclass

SIZING_MODE_FIXED_MARGIN = "fixed_margin"
SIZING_MODE_RISK_BASED = "risk_based"
SIZING_MODE_PCT_BALANCE = "pct_balance"
VALID_SIZING_MODES = {SIZING_MODE_FIXED_MARGIN, SIZING_MODE_RISK_BASED, SIZING_MODE_PCT_BALANCE}


def normalize_sizing_mode(mode: str) -> str:
    """Normalize sizing mode, falling back to pct_balance on unknown values."""
    normalized = (mode or "").strip().lower()
    if normalized in VALID_SIZING_MODES:
        return normalized
    return SIZING_MODE_PCT_BALANCE


@dataclass(frozen=True)
class SizingInputs:
    """Inputs required to compute margin allocation for one entry candidate."""

    available_balance: float
    entry_price: float
    stop_price: float
    leverage: int
    fixed_margin_per_trade_usdt: float
    margin_utilization: float
    risk_per_trade_pct: float


class PositionSizer:
    """Policy selector for trade sizing."""

    def __init__(self, sizing_mode: str) -> None:
        self.sizing_mode = normalize_sizing_mode(sizing_mode)

    def margin_to_use(self, inputs: SizingInputs) -> float:
        """Return the margin budget to use for the entry."""
        if self.sizing_mode == SIZING_MODE_RISK_BASED:
            return self._risk_based_margin(inputs)
        if self.sizing_mode == SIZING_MODE_PCT_BALANCE:
            return self._pct_balance_margin(inputs)
        return self._fixed_margin(inputs)

    @staticmethod
    def _fixed_margin(inputs: SizingInputs) -> float:
        if inputs.available_balance <= 0:
            return 0.0
        if inputs.fixed_margin_per_trade_usdt <= 0:
            return 0.0
        return max(0.0, min(inputs.fixed_margin_per_trade_usdt, inputs.available_balance))

    @staticmethod
    def _pct_balance_margin(inputs: SizingInputs) -> float:
        """Use a fixed percentage of available balance as margin.

        margin = available_balance * risk_per_trade_pct  (e.g. 100 USDT * 0.05 = 5 USDT)
        Leverage is applied later by calc_qty in FuturesExecutor: notional = margin * leverage.
        """
        if inputs.available_balance <= 0:
            return 0.0
        margin = inputs.available_balance * inputs.risk_per_trade_pct
        return max(0.0, min(margin, inputs.available_balance))

    @staticmethod
    def _risk_based_margin(inputs: SizingInputs) -> float:
        if inputs.available_balance <= 0 or inputs.entry_price <= 0:
            return 0.0
        risk_distance = abs(inputs.entry_price - inputs.stop_price)
        if risk_distance <= 0:
            return 0.0

        usable_balance = max(0.0, inputs.available_balance * inputs.margin_utilization)
        if usable_balance <= 0:
            return 0.0

        risk_budget = max(0.0, usable_balance * inputs.risk_per_trade_pct)
        if risk_budget <= 0:
            return 0.0

        qty = risk_budget / risk_distance
        if qty <= 0:
            return 0.0

        notional = qty * inputs.entry_price
        leverage = max(1, int(inputs.leverage))
        margin = notional / leverage
        return max(0.0, min(margin, usable_balance, inputs.available_balance))


def is_entry_size_valid(
    qty: float,
    price: float,
    min_qty: float,
    min_notional: float,
) -> bool:
    """Return whether qty/price satisfy exchange minimums."""
    if qty <= 0 or price <= 0:
        return False
    if min_qty > 0 and qty < min_qty:
        return False
    if min_notional > 0 and (qty * price) < min_notional:
        return False
    return True

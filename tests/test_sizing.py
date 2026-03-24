"""Tests for current runtime sizing modes (fixed_margin, risk_based)."""
from __future__ import annotations

import unittest

import pytest

from sizing import (
    SIZING_MODE_FIXED_MARGIN,
    SIZING_MODE_PCT_BALANCE,
    SIZING_MODE_RISK_BASED,
    PositionSizer,
    SizingInputs,
    is_entry_size_valid,
    normalize_sizing_mode,
)


def _inputs(**overrides) -> SizingInputs:
    data = {
        "available_balance": 200.0,
        "entry_price": 100.0,
        "stop_price": 95.0,
        "leverage": 20,
        "fixed_margin_per_trade_usdt": 5.0,
        "margin_utilization": 0.95,
        "risk_per_trade_pct": 0.10,
    }
    data.update(overrides)
    return SizingInputs(**data)


@pytest.mark.unit
class TestSizingModes(unittest.TestCase):
    def test_normalize_unknown_falls_back_to_current_default(self) -> None:
        # Keep assertion aligned with current runtime behavior.
        self.assertEqual(normalize_sizing_mode("unknown_mode"), SIZING_MODE_PCT_BALANCE)

    def test_fixed_margin_basic(self) -> None:
        sizer = PositionSizer(SIZING_MODE_FIXED_MARGIN)
        margin = sizer.margin_to_use(_inputs(available_balance=100.0, fixed_margin_per_trade_usdt=5.0))
        self.assertAlmostEqual(margin, 5.0, places=6)

    def test_fixed_margin_capped_by_balance(self) -> None:
        sizer = PositionSizer(SIZING_MODE_FIXED_MARGIN)
        margin = sizer.margin_to_use(_inputs(available_balance=3.0, fixed_margin_per_trade_usdt=5.0))
        self.assertAlmostEqual(margin, 3.0, places=6)

    def test_risk_based_positive(self) -> None:
        sizer = PositionSizer(SIZING_MODE_RISK_BASED)
        margin = sizer.margin_to_use(_inputs(entry_price=100.0, stop_price=95.0))
        self.assertGreater(margin, 0.0)

    def test_risk_based_zero_distance_returns_zero(self) -> None:
        sizer = PositionSizer(SIZING_MODE_RISK_BASED)
        margin = sizer.margin_to_use(_inputs(entry_price=100.0, stop_price=100.0))
        self.assertEqual(margin, 0.0)

    def test_risk_based_zero_balance_returns_zero(self) -> None:
        sizer = PositionSizer(SIZING_MODE_RISK_BASED)
        margin = sizer.margin_to_use(_inputs(available_balance=0.0))
        self.assertEqual(margin, 0.0)


@pytest.mark.unit
class TestEntrySizeValidation(unittest.TestCase):
    def test_valid_size(self) -> None:
        self.assertTrue(is_entry_size_valid(0.1, 100.0, 0.01, 5.0))

    def test_invalid_zero_qty(self) -> None:
        self.assertFalse(is_entry_size_valid(0.0, 100.0, 0.0, 0.0))

    def test_invalid_below_min_qty(self) -> None:
        self.assertFalse(is_entry_size_valid(0.001, 100.0, 0.01, 0.0))

    def test_invalid_below_min_notional(self) -> None:
        self.assertFalse(is_entry_size_valid(0.02, 100.0, 0.0, 5.0))

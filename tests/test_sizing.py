"""Tests for PositionSizer sizing modes."""
from __future__ import annotations

import unittest

from sizing import PositionSizer, SizingInputs, SIZING_MODE_PCT_BALANCE, normalize_sizing_mode


def _make_inputs(**overrides) -> SizingInputs:
    defaults = dict(
        available_balance=200.0,
        entry_price=100.0,
        stop_price=95.0,
        leverage=20,
        fixed_margin_per_trade_usdt=5.0,
        margin_utilization=0.95,
        risk_per_trade_pct=0.05,
    )
    defaults.update(overrides)
    return SizingInputs(**defaults)


class PctBalanceSizingTests(unittest.TestCase):
    def test_5pct_of_200_gives_10(self) -> None:
        sizer = PositionSizer(SIZING_MODE_PCT_BALANCE)
        margin = sizer.margin_to_use(_make_inputs(available_balance=200.0, risk_per_trade_pct=0.05))
        self.assertAlmostEqual(margin, 10.0, places=6)

    def test_5pct_of_100_gives_5(self) -> None:
        sizer = PositionSizer(SIZING_MODE_PCT_BALANCE)
        margin = sizer.margin_to_use(_make_inputs(available_balance=100.0, risk_per_trade_pct=0.05))
        self.assertAlmostEqual(margin, 5.0, places=6)

    def test_zero_balance_returns_zero(self) -> None:
        sizer = PositionSizer(SIZING_MODE_PCT_BALANCE)
        margin = sizer.margin_to_use(_make_inputs(available_balance=0.0))
        self.assertEqual(margin, 0.0)

    def test_margin_capped_at_balance(self) -> None:
        sizer = PositionSizer(SIZING_MODE_PCT_BALANCE)
        margin = sizer.margin_to_use(_make_inputs(available_balance=10.0, risk_per_trade_pct=2.0))
        self.assertLessEqual(margin, 10.0)

    def test_normalize_unknown_mode_falls_back_to_pct_balance(self) -> None:
        self.assertEqual(normalize_sizing_mode("unknown_xyz"), SIZING_MODE_PCT_BALANCE)

    def test_normalize_pct_balance(self) -> None:
        self.assertEqual(normalize_sizing_mode("pct_balance"), SIZING_MODE_PCT_BALANCE)


if __name__ == "__main__":
    unittest.main()

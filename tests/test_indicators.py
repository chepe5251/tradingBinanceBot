"""Tests for pure indicator helpers (no Binance dependency)."""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd
import pytest

from bot.indicators import atr_last, context_direction, context_slope, ema, rsi


def _make_df(n: int = 30, start: float = 100.0, step: float = 1.0) -> pd.DataFrame:
    close = np.linspace(start, start + step * (n - 1), n)
    return pd.DataFrame(
        {
            "open": close - 0.1,
            "high": close + 0.5,
            "low": close - 0.5,
            "close": close,
            "volume": np.ones(n) * 100.0,
        }
    )


@pytest.mark.unit
class AtrLastTests(unittest.TestCase):
    def test_empty_df_returns_zero(self) -> None:
        self.assertEqual(atr_last(pd.DataFrame(), 14), 0.0)

    def test_too_short_df_returns_zero(self) -> None:
        df = _make_df(n=5)
        self.assertEqual(atr_last(df, 14), 0.0)

    def test_sufficient_df_returns_positive(self) -> None:
        df = _make_df(n=30)
        result = atr_last(df, 14)
        self.assertGreater(result, 0.0)

    def test_flat_series_atr_near_zero(self) -> None:
        df = _make_df(n=30, step=0.0)  # all same price
        result = atr_last(df, 14)
        # high-low spread = 1.0 always (0.5 + 0.5), so ATR should be ~1.0
        self.assertAlmostEqual(result, 1.0, delta=0.1)


@pytest.mark.unit
class ContextDirectionTests(unittest.TestCase):
    def test_empty_df_returns_none(self) -> None:
        self.assertIsNone(context_direction(pd.DataFrame(), 20))

    def test_too_short_df_returns_none(self) -> None:
        df = _make_df(n=5)
        self.assertIsNone(context_direction(df, 20))

    def test_uptrend_returns_long(self) -> None:
        df = _make_df(n=50, start=100.0, step=1.0)  # rising prices
        result = context_direction(df, 20)
        self.assertEqual(result, "LONG")

    def test_downtrend_returns_short(self) -> None:
        df = _make_df(n=50, start=150.0, step=-1.0)  # falling prices
        result = context_direction(df, 20)
        self.assertEqual(result, "SHORT")


@pytest.mark.unit
class ContextSlopeTests(unittest.TestCase):
    def test_empty_df_returns_zero(self) -> None:
        self.assertEqual(context_slope(pd.DataFrame(), 10), 0.0)

    def test_too_short_df_returns_zero(self) -> None:
        df = _make_df(n=5)
        self.assertEqual(context_slope(df, 10), 0.0)

    def test_rising_prices_positive_slope(self) -> None:
        df = _make_df(n=30, step=1.0)
        slope = context_slope(df, 5)
        self.assertGreater(slope, 0.0)

    def test_falling_prices_negative_slope(self) -> None:
        df = _make_df(n=30, start=100.0, step=-0.5)
        slope = context_slope(df, 5)
        self.assertLess(slope, 0.0)

    def test_flat_prices_near_zero_slope(self) -> None:
        df = _make_df(n=30, step=0.0)
        slope = context_slope(df, 5)
        self.assertAlmostEqual(slope, 0.0, places=8)


@pytest.mark.unit
class EmaTests(unittest.TestCase):
    def test_ema_same_length_as_input(self) -> None:
        series = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0])
        result = ema(series, 3)
        self.assertEqual(len(result), len(series))

    def test_ema_last_value_reasonable(self) -> None:
        series = pd.Series([100.0] * 20 + [110.0])
        result = ema(series, 10)
        # EMA should be between 100 and 110 after one spike
        self.assertGreater(float(result.iloc[-1]), 100.0)
        self.assertLess(float(result.iloc[-1]), 110.0)


@pytest.mark.unit
class RsiTests(unittest.TestCase):
    def test_rsi_bounds(self) -> None:
        series = pd.Series(np.linspace(100, 120, 30))
        result = rsi(series, 14)
        # In a steadily rising series, RSI should be high but ≤ 100
        self.assertLessEqual(float(result.iloc[-1]), 100.0)
        self.assertGreater(float(result.iloc[-1]), 50.0)

    def test_rsi_length_matches_input(self) -> None:
        series = pd.Series(np.linspace(100, 110, 20))
        result = rsi(series, 14)
        self.assertEqual(len(result), len(series))


if __name__ == "__main__":
    unittest.main()

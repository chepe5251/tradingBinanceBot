from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from strategy import evaluate_signal


def _build_candidate_dataframe() -> pd.DataFrame:
    n = 260
    idx = pd.date_range("2025-01-01", periods=n, freq="15min", tz="UTC")
    close = np.linspace(100, 130, n)
    open_ = close - 0.1
    high = close + 1.0
    low = close - 1.0
    volume = np.full(n, 100.0)

    sig = n - 2
    open_[sig] = close[sig] - 0.2
    close[sig] = close[sig] + 0.25
    high[sig] = close[sig] + 0.3
    low[sig] = close[sig] - 1.0
    volume[sig] = 120.0

    open_[n - 1] = close[sig] + 0.1
    close[n - 1] = high[sig] + 0.3
    high[n - 1] = close[n - 1] + 0.2
    low[n - 1] = open_[n - 1] - 0.2
    volume[n - 1] = 110.0

    return pd.DataFrame(
        {
            "open_time": idx,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "close_time": idx,
        }
    )


def _base_kwargs() -> dict:
    return {
        "ema_trend": 200,
        "ema_fast": 20,
        "ema_mid": 50,
        "atr_period": 14,
        "atr_avg_window": 30,
        "volume_avg_window": 20,
        "rsi_period": 14,
        "rsi_long_min": 0.0,
        "rsi_long_max": 100.0,
        "volume_min_ratio": 0.5,
        "volume_max_ratio": 2.0,
        "pullback_tolerance_atr": 2.0,
        "min_ema_spread_atr": 0.0,
        "max_ema_spread_atr": 10.0,
        "min_body_ratio": 0.1,
        "rr_target": 2.0,
        "min_risk_atr": 0.1,
        "max_risk_atr": 10.0,
        "min_score": 0.0,
    }


class StrategyTests(unittest.TestCase):
    def test_returns_signal_on_controlled_dataset(self) -> None:
        df = _build_candidate_dataframe()
        signal = evaluate_signal(df, df.copy(), **_base_kwargs())
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "BUY")

    def test_blocks_when_volume_filter_fails(self) -> None:
        df = _build_candidate_dataframe()
        kwargs = _base_kwargs()
        kwargs["volume_min_ratio"] = 1.3
        signal = evaluate_signal(df, df.copy(), **kwargs)
        self.assertIsNone(signal)

    def test_blocks_when_rsi_filter_fails(self) -> None:
        df = _build_candidate_dataframe()
        kwargs = _base_kwargs()
        kwargs["rsi_long_max"] = 60.0
        signal = evaluate_signal(df, df.copy(), **kwargs)
        self.assertIsNone(signal)

    def test_blocks_when_context_not_aligned(self) -> None:
        df = _build_candidate_dataframe()
        ctx = df.copy()
        ctx["close"] = np.linspace(130, 100, len(ctx))
        kwargs = _base_kwargs()
        signal = evaluate_signal(df, ctx, **kwargs)
        self.assertIsNone(signal)

    def test_parameterization_changes_result(self) -> None:
        df = _build_candidate_dataframe()
        kwargs = _base_kwargs()
        baseline = evaluate_signal(df, df.copy(), **kwargs)
        self.assertIsNotNone(baseline)

        kwargs["min_score"] = 2.5
        stricter = evaluate_signal(df, df.copy(), **kwargs)
        self.assertIsNone(stricter)


if __name__ == "__main__":
    unittest.main()

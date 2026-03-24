from __future__ import annotations

import unittest

import pandas as pd
import pytest

from monitor_logic import evaluate_early_exit


def _build_monitor_df() -> pd.DataFrame:
    closes = [100 + 0.5 * i for i in range(30)]
    closes[-3] = 114.0
    closes[-2] = 113.0
    closes[-1] = 108.0

    opens = [c - 0.2 for c in closes]
    highs = [max(o, c) + 0.3 for o, c in zip(opens, closes)]
    lows = [min(o, c) - 0.3 for o, c in zip(opens, closes)]
    volumes = [100.0 for _ in closes]

    opens[-1] = 112.0
    highs[-1] = 112.2
    lows[-1] = 107.8
    volumes[-1] = 260.0

    idx = pd.date_range("2025-01-01", periods=len(closes), freq="15min", tz="UTC")
    return pd.DataFrame(
        {"open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes},
        index=idx,
    )


@pytest.mark.unit
class MonitorLogicTests(unittest.TestCase):
    def test_detects_structure_or_volume_break(self) -> None:
        df = _build_monitor_df()
        should_exit, reason, metrics = evaluate_early_exit(
            df=df,
            side="BUY",
            ema_fast_period=5,
            ema_mid_period=8,
            ema_trend_period=10,
            volume_avg_window=5,
            trend_slope_min=0.0,
            break_even=False,
        )
        self.assertTrue(should_exit)
        self.assertIn(reason, {"volume_break", "structure_break", "ctx_flip"})
        self.assertTrue(metrics["struct_break"])

    def test_break_even_blocks_early_exit(self) -> None:
        df = _build_monitor_df()
        should_exit, reason, _ = evaluate_early_exit(
            df=df,
            side="BUY",
            ema_fast_period=5,
            ema_mid_period=8,
            ema_trend_period=10,
            volume_avg_window=5,
            trend_slope_min=0.0,
            break_even=True,
        )
        self.assertFalse(should_exit)
        self.assertEqual(reason, "break_even_active")


if __name__ == "__main__":
    unittest.main()

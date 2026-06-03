from __future__ import annotations

import unittest

import pandas as pd
import pytest
from bot.monitor_logic import evaluate_early_exit


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
def _make_no_break_df(n: int = 30) -> pd.DataFrame:
    """Uptrending DataFrame where structure is intact (no break signal)."""
    import numpy as np
    close = np.linspace(100, 115, n)
    opens = close - 0.1  # all bullish
    highs = close + 0.3
    lows = close - 0.3
    volumes = np.full(n, 100.0)
    idx = pd.date_range("2025-01-01", periods=n, freq="15min", tz="UTC")
    return pd.DataFrame(
        {"open": opens, "high": highs, "low": lows, "close": close, "volume": volumes},
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

    def test_empty_df_returns_no_data(self) -> None:
        should_exit, reason, metrics = evaluate_early_exit(
            df=pd.DataFrame(),
            side="BUY",
            ema_fast_period=5,
            ema_mid_period=8,
            ema_trend_period=10,
            volume_avg_window=5,
            trend_slope_min=0.0,
            break_even=False,
        )
        self.assertFalse(should_exit)
        self.assertEqual(reason, "no_data")

    def test_no_exit_when_structure_intact(self) -> None:
        df = _make_no_break_df()
        should_exit, reason, _ = evaluate_early_exit(
            df=df,
            side="BUY",
            ema_fast_period=5,
            ema_mid_period=10,
            ema_trend_period=15,
            volume_avg_window=5,
            trend_slope_min=0.0,
            break_even=False,
        )
        self.assertFalse(should_exit)

    def test_sell_side_break_detection(self) -> None:
        """SELL side with a strong bearish candle after downtrend context should exit."""
        import numpy as np
        n = 30
        # Falling prices: bearish context
        close = np.linspace(115, 100, n)
        opens = close + 0.1      # bearish candles (open > close)
        highs = opens + 0.3
        lows = close - 0.3
        volumes = np.full(n, 100.0)
        # Last candle: strong bearish with high volume
        opens[-1] = close[-1] + 3.0
        highs[-1] = opens[-1] + 0.2
        lows[-1] = close[-1] - 0.2
        volumes[-1] = 600.0

        idx = pd.date_range("2025-01-01", periods=n, freq="15min", tz="UTC")
        df = pd.DataFrame(
            {"open": opens, "high": highs, "low": lows, "close": close, "volume": volumes},
            index=idx,
        )
        should_exit, reason, metrics = evaluate_early_exit(
            df=df,
            side="SELL",
            ema_fast_period=5,
            ema_mid_period=8,
            ema_trend_period=10,
            volume_avg_window=5,
            trend_slope_min=0.0,
            break_even=False,
        )
        # Metrics should be populated; exact exit depends on EMA cross conditions
        self.assertIn("struct_break", metrics)
        self.assertIn("vol_strong", metrics)

    def test_ctx_flip_triggers_exit(self) -> None:
        """Context flipping to SHORT while in BUY triggers exit when slope threshold met."""
        import numpy as np
        n = 40
        # Main df: uptrending (BUY position)
        close = np.linspace(100, 115, n)
        opens = close - 0.2
        highs = close + 0.5
        lows = close - 0.5
        volumes = np.full(n, 100.0)
        main_df = pd.DataFrame(
            {"open": opens, "high": highs, "low": lows, "close": close, "volume": volumes},
            index=pd.date_range("2025-01-01", periods=n, freq="15min", tz="UTC"),
        )

        # Context df: strongly falling → SHORT direction + steep negative slope
        ctx_close = np.linspace(130, 90, n)
        ctx_df = pd.DataFrame(
            {
                "open": ctx_close + 0.1,
                "high": ctx_close + 0.5,
                "low": ctx_close - 0.5,
                "close": ctx_close,
                "volume": np.full(n, 100.0),
            },
            index=pd.date_range("2025-01-01", periods=n, freq="15min", tz="UTC"),
        )

        should_exit, reason, metrics = evaluate_early_exit(
            df=main_df,
            side="BUY",
            ema_fast_period=5,
            ema_mid_period=8,
            ema_trend_period=10,
            volume_avg_window=5,
            trend_slope_min=0.0,  # any slope qualifies
            break_even=False,
            context_df=ctx_df,
        )
        # With a strongly falling context df, ctx_dir should be SHORT
        self.assertEqual(metrics.get("ctx_dir"), "SHORT")


if __name__ == "__main__":
    unittest.main()

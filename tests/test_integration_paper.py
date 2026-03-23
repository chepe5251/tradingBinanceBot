"""End-to-end integration test for the paper trading entry flow.

Verifies that a valid signal generates an ENTRY log with margin ≈ 5% of balance.
No real exchange calls are made; Client is fully mocked.
"""
from __future__ import annotations

import logging
import re
import threading
import unittest
from unittest.mock import MagicMock, patch

from config import Settings
from execution import FuturesExecutor
from risk import RiskManager
from services.entry_service import EntryService
from services.position_service import PositionCache
from tests.test_strategy import _build_candidate_dataframe


class _CapturingHandler(logging.Handler):
    """In-memory log handler that stores formatted messages."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(self.format(record))


class PaperTradingIntegrationTests(unittest.TestCase):
    """Integration tests for paper trading entry flow."""

    def _make_settings(self) -> Settings:
        settings = Settings()
        settings.use_paper_trading = True
        settings.paper_start_balance = 100.0
        settings.sizing_mode = "pct_balance"
        settings.risk_per_trade_pct = 0.05
        settings.max_positions = 1
        settings.use_limit_only = True
        settings.cooldown_sec = 0
        # Loosen strategy filters so the controlled dataframe always passes
        settings.min_score = 0.0
        settings.rsi_long_min = 0.0
        settings.rsi_long_max = 100.0
        settings.volume_min_ratio = 0.5
        settings.volume_max_ratio = 2.0
        settings.pullback_tolerance_atr = 2.0
        settings.min_ema_spread_atr = 0.0
        settings.max_ema_spread_atr = 10.0
        settings.min_body_ratio = 0.3   # test df body_ratio ≈ 0.346; 0.35 default would block
        settings.min_risk_atr = 0.1
        settings.max_risk_atr = 10.0
        # Disable the liquidation-zone check (uses fixed_margin as reference)
        settings.fixed_margin_per_trade_usdt = 0.0
        return settings

    def _make_exchange_info(self, symbol: str) -> dict:
        return {
            "symbols": [
                {
                    "symbol": symbol,
                    "filters": [
                        {
                            "filterType": "LOT_SIZE",
                            "stepSize": "0.001",
                            "minQty": "0.001",
                        },
                        {
                            "filterType": "PRICE_FILTER",
                            "tickSize": "0.01",
                            "minPrice": "0.01",
                            "maxPrice": "1000000.0",
                        },
                        {"filterType": "NOTIONAL", "notional": "5.0"},
                    ],
                }
            ]
        }

    def test_paper_entry_logs_signal_and_entry(self) -> None:
        """Signal fires → EntryService logs 'signal' then 'ENTRY' with margin ≈ 5 USDT."""
        symbol = "BTCUSDT"
        interval = "15m"
        settings = self._make_settings()

        df = _build_candidate_dataframe()

        mock_client = MagicMock()
        mock_client.futures_position_information.return_value = []
        mock_client.futures_exchange_info.return_value = self._make_exchange_info(symbol)
        mock_client.futures_cancel_all_open_orders.return_value = {}
        mock_client.futures_get_open_orders.return_value = []

        mock_stream = MagicMock()
        mock_stream.get_dataframe.return_value = df

        risk = RiskManager(
            cooldown_sec=0,
            max_consecutive_losses=10,
            daily_drawdown_limit=1.0,
            daily_drawdown_limit_usdt=100.0,
            loss_pause_sec=0,
            volatility_pause=False,
            volatility_threshold=0.0,
        )
        risk.init_equity(100.0)

        executor = FuturesExecutor(
            client=mock_client,
            symbol=symbol,
            leverage=settings.leverage,
            margin_type=settings.margin_type,
            paper=True,
        )

        position_cache = PositionCache(mock_client)

        # Capture all trades_logger output
        trades_logger = logging.getLogger("test.trades_integration")
        trades_logger.setLevel(logging.DEBUG)
        handler = _CapturingHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        trades_logger.addHandler(handler)
        trades_logger.propagate = False

        service = EntryService(
            settings=settings,
            stream=mock_stream,
            symbols=[symbol],
            context_map={interval: interval},
            trade_client=mock_client,
            risk=risk,
            position_cache=position_cache,
            get_executor=lambda sym: executor,
            logger=logging.getLogger("test.logger_integration"),
            trades_logger=trades_logger,
            telegram=MagicMock(),
        )

        # Run the monitor thread synchronously so ENTRY is logged before assertions
        def _sync_thread(*args, **kwargs):
            target = kwargs.get("target")

            class _FakeThread:
                def start(self_) -> None:
                    if target:
                        target()

            return _FakeThread()

        # Initialize interval state (normally done by make_on_close during bot startup)
        service.make_on_close(interval)

        with patch("services.entry_service.threading.Thread", side_effect=_sync_thread):
            service._on_close(interval)

        records = handler.records

        # 1. "signal" must have been logged by evaluate_interval_signals
        signal_records = [r for r in records if r.startswith("signal")]
        self.assertTrue(
            len(signal_records) >= 1,
            f"Expected at least one 'signal' log. Got: {records}",
        )

        # 2. "ENTRY" must have been logged by PositionMonitor.run
        entry_records = [r for r in records if r.startswith("ENTRY")]
        self.assertTrue(
            len(entry_records) >= 1,
            f"Expected at least one 'ENTRY' log. Got: {records}",
        )

        # 3. margin ≈ 5% of 100 USDT = 5.0 (±0.5)
        entry_msg = entry_records[0]
        match = re.search(r"margin=([\d.]+)", entry_msg)
        self.assertIsNotNone(match, f"'margin=...' not found in: {entry_msg}")
        margin_val = float(match.group(1))
        self.assertAlmostEqual(
            margin_val,
            5.0,
            delta=0.5,
            msg=f"Expected margin ≈ 5.0, got {margin_val}",
        )


if __name__ == "__main__":
    unittest.main()

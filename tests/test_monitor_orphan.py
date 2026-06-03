from __future__ import annotations

import logging
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bot.config import Settings
from bot.monitor.orphan import resume_orphan_position
from bot.risk import RiskManager


@pytest.mark.unit
class MonitorOrphanTests(unittest.TestCase):
    def _risk(self) -> RiskManager:
        risk = RiskManager(
            cooldown_sec=0,
            max_consecutive_losses=3,
            daily_drawdown_limit=1.0,
            daily_drawdown_limit_usdt=100.0,
            loss_pause_sec=0,
            volatility_pause=False,
            volatility_threshold=0.0,
        )
        risk.init_equity(100.0)
        return risk

    def test_invalid_orphan_payload_does_not_start_thread(self) -> None:
        settings = Settings()
        stream = MagicMock()
        stream.get_dataframe.return_value = pd.DataFrame()

        with patch("bot.monitor.orphan.threading.Thread") as thread_mock:
            resume_orphan_position(
                orphan={"symbol": "BTCUSDT", "positionAmt": "NaN", "entryPrice": "x"},
                symbols=["BTCUSDT"],
                stream=stream,
                settings=settings,
                get_executor=lambda _symbol: MagicMock(),
                risk=self._risk(),
                trade_client=MagicMock(),
                pos_cache_invalidate=lambda: None,
                risk_updater=lambda _pnl, _now: None,
                logger=logging.getLogger("test.orphan"),
                trades_logger=logging.getLogger("test.orphan.trades"),
                operations=MagicMock(),
            )
            thread_mock.assert_not_called()

    def test_symbol_outside_universe_does_not_start_thread(self) -> None:
        settings = Settings()
        stream = MagicMock()
        stream.get_dataframe.return_value = pd.DataFrame()

        with patch("bot.monitor.orphan.threading.Thread") as thread_mock:
            resume_orphan_position(
                orphan={"symbol": "ETHUSDT", "positionAmt": "1", "entryPrice": "100"},
                symbols=["BTCUSDT"],
                stream=stream,
                settings=settings,
                get_executor=lambda _symbol: MagicMock(),
                risk=self._risk(),
                trade_client=MagicMock(),
                pos_cache_invalidate=lambda: None,
                risk_updater=lambda _pnl, _now: None,
                logger=logging.getLogger("test.orphan"),
                trades_logger=logging.getLogger("test.orphan.trades"),
                operations=MagicMock(),
            )
            thread_mock.assert_not_called()


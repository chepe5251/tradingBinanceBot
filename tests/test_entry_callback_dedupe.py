from __future__ import annotations

import logging
import unittest
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bot.config import Settings
from bot.risk import RiskManager
from bot.services.entry_service import EntryService
from bot.services.position_service import PositionCache
from bot.services.signal_service import SignalCandidate


def _close_df() -> pd.DataFrame:
    ts = pd.Timestamp("2026-03-24T12:00:00Z")
    return pd.DataFrame({"close_time": [ts], "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [1.0]})


@pytest.mark.unit
class EntryCallbackDedupeTests(unittest.TestCase):
    def test_on_close_same_candle_is_processed_once(self) -> None:
        settings = Settings()
        settings.max_positions = 1
        settings.use_paper_trading = True

        client = MagicMock()
        client.futures_position_information.return_value = []
        stream = MagicMock()
        stream.get_dataframe.return_value = _close_df()

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

        service = EntryService(
            settings=settings,
            stream=stream,
            symbols=["BTCUSDT"],
            context_map={"15m": "1h"},
            trade_client=client,
            risk=risk,
            position_cache=PositionCache(client),
            get_executor=lambda _symbol: MagicMock(),
            logger=logging.getLogger("test.entry.dedupe"),
            trades_logger=logging.getLogger("test.entry.dedupe.trades"),
            telegram=MagicMock(),
        )
        service.make_on_close("15m")

        candidate = SignalCandidate(
            symbol="BTCUSDT",
            interval="15m",
            payload={"side": "BUY", "price": 1.0, "score": 2.0},
        )
        service._evaluate_signals = MagicMock(return_value=[candidate])  # type: ignore[method-assign]
        service._broadcast_signal_alerts = MagicMock()  # type: ignore[method-assign]
        service._execute_candidate = MagicMock(return_value=False)  # type: ignore[method-assign]

        service._on_close("15m")
        service._on_close("15m")

        service._evaluate_signals.assert_called_once()
        service._execute_candidate.assert_called_once()


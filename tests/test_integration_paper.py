"""Integration test for paper trading entry flow."""
from __future__ import annotations

import logging
import re
import unittest
from unittest.mock import MagicMock, patch

import pytest

from config import Settings
from execution import FuturesExecutor
from risk import RiskManager
from services.entry_service import EntryService
from services.position_service import PositionCache
from tests.test_strategy import _build_candidate_dataframe


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(self.format(record))


@pytest.mark.integration
class TestIntegrationPaper(unittest.TestCase):
    def _settings(self) -> Settings:
        settings = Settings()
        settings.use_paper_trading = True
        settings.paper_start_balance = 100.0
        settings.max_positions = 1
        settings.use_limit_only = True
        settings.cooldown_sec = 0

        # Keep runtime logic unchanged; use risk_based only inside this test setup.
        settings.sizing_mode = "risk_based"
        settings.risk_per_trade_pct = 0.10
        settings.margin_utilization = 0.95
        settings.fixed_margin_per_trade_usdt = 0.0  # disables liquidation-zone guard branch

        # Make controlled dataframe pass strategy gates.
        settings.min_score = 0.0
        settings.rsi_long_min = 0.0
        settings.rsi_long_max = 100.0
        settings.volume_min_ratio = 0.5
        settings.volume_max_ratio = 2.0
        settings.pullback_tolerance_atr = 2.0
        settings.min_ema_spread_atr = 0.0
        settings.max_ema_spread_atr = 10.0
        settings.min_body_ratio = 0.3
        settings.min_risk_atr = 0.1
        settings.max_risk_atr = 10.0
        return settings

    def _exchange_info(self, symbol: str) -> dict:
        return {
            "symbols": [
                {
                    "symbol": symbol,
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        {
                            "filterType": "PRICE_FILTER",
                            "tickSize": "0.01",
                            "minPrice": "0.01",
                            "maxPrice": "1000000.0",
                        },
                        {"filterType": "NOTIONAL", "notional": "0.01"},
                    ],
                }
            ]
        }

    def test_on_close_generates_signal_and_entry_logs(self) -> None:
        symbol = "BTCUSDT"
        interval = "15m"
        settings = self._settings()
        data = _build_candidate_dataframe()

        client = MagicMock()
        client.futures_position_information.return_value = []
        client.futures_exchange_info.return_value = self._exchange_info(symbol)
        client.futures_cancel_all_open_orders.return_value = {}
        client.futures_get_open_orders.return_value = []

        stream = MagicMock()
        stream.get_dataframe.return_value = data

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
            client=client,
            symbol=symbol,
            leverage=settings.leverage,
            margin_type=settings.margin_type,
            paper=True,
        )
        pos_cache = PositionCache(client)

        trades_logger = logging.getLogger("test.integration.paper.trades")
        trades_logger.setLevel(logging.DEBUG)
        trades_logger.propagate = False
        handler = _CaptureHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        trades_logger.addHandler(handler)

        service = EntryService(
            settings=settings,
            stream=stream,
            symbols=[symbol],
            context_map={interval: interval},
            trade_client=client,
            risk=risk,
            position_cache=pos_cache,
            get_executor=lambda _: executor,
            logger=logging.getLogger("test.integration.paper"),
            trades_logger=trades_logger,
            telegram=MagicMock(),
        )

        service.make_on_close(interval)

        def _sync_thread(*args, **kwargs):
            target = kwargs.get("target")

            class _FakeThread:
                def start(self) -> None:
                    if target:
                        target()

            return _FakeThread()

        with patch("services.entry_service.threading.Thread", side_effect=_sync_thread):
            service._on_close(interval)

        assert any(message.startswith("signal ") for message in handler.messages)
        entry_messages = [message for message in handler.messages if message.startswith("ENTRY ")]
        assert entry_messages, handler.messages

        margin_match = re.search(r"margin=([0-9.]+)", entry_messages[0])
        assert margin_match is not None
        assert float(margin_match.group(1)) > 0.0

from __future__ import annotations

import logging
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bot.config import Settings
from bot.risk import RiskManager
from bot.services.domain_models import EntryFillResult
from bot.services.entry_service import EntryService
from bot.services.position_service import PositionCache
from bot.services.signal_service import SignalCandidate


def _df(rows: int = 20) -> pd.DataFrame:
    idx = pd.date_range("2026-01-01", periods=rows, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "open_time": idx,
            "open": [100.0 + i * 0.1 for i in range(rows)],
            "high": [100.5 + i * 0.1 for i in range(rows)],
            "low": [99.5 + i * 0.1 for i in range(rows)],
            "close": [100.1 + i * 0.1 for i in range(rows)],
            "volume": [100.0 for _ in range(rows)],
            "close_time": idx,
        }
    )


@pytest.mark.unit
class EntryRecoverableErrorsTests(unittest.TestCase):
    def _service(self, rows: int = 20) -> EntryService:
        settings = Settings()
        settings.use_paper_trading = False
        settings.sizing_mode = "fixed_margin"
        settings.fixed_margin_per_trade_usdt = 5.0
        settings.max_positions = 1

        stream = MagicMock()
        stream.get_dataframe.return_value = _df(rows)

        client = MagicMock()
        client.futures_position_information.return_value = []
        client.futures_account_balance.return_value = [{"asset": "USDT", "availableBalance": "100"}]

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

        executor = MagicMock()
        executor.get_min_qty.return_value = 0.001
        executor.get_min_notional.return_value = 5.0
        executor.calc_qty.return_value = 0.2
        executor.round_qty.return_value = 0.2

        return EntryService(
            settings=settings,
            stream=stream,
            symbols=["BTCUSDT"],
            context_map={"15m": "1h"},
            trade_client=client,
            risk=risk,
            position_cache=PositionCache(client),
            get_executor=lambda _symbol: executor,
            logger=logging.getLogger("test.entry.recoverable"),
            trades_logger=logging.getLogger("test.entry.recoverable.trades"),
            telegram=MagicMock(),
        )

    def test_build_trade_plan_handles_balance_fetch_error(self) -> None:
        service = self._service()
        candidate = SignalCandidate(
            symbol="BTCUSDT",
            interval="15m",
            payload={"side": "BUY", "price": 100.0, "atr": 1.0, "risk_per_unit": 0.5, "rr_target": 2.0},
        )
        with patch.object(service, "_available_balance_for_entry", side_effect=OSError("timeout")):
            plan = service._build_trade_plan(candidate, "15m", trace_id="t1")  # noqa: SLF001
        self.assertIsNone(plan)

    def test_build_trade_plan_handles_insufficient_dataframe(self) -> None:
        service = self._service(rows=8)
        candidate = SignalCandidate(
            symbol="BTCUSDT",
            interval="15m",
            payload={"side": "BUY", "price": 100.0, "atr": 1.0, "risk_per_unit": 0.5, "rr_target": 2.0},
        )
        plan = service._build_trade_plan(candidate, "15m", trace_id="t2")  # noqa: SLF001
        self.assertIsNone(plan)

    def test_finalize_entry_uses_signal_rr_for_tp(self) -> None:
        service = self._service()
        candidate = SignalCandidate(
            symbol="BTCUSDT",
            interval="15m",
            payload={"side": "BUY", "price": 100.0, "atr": 1.0, "risk_per_unit": 0.5, "rr_target": 2.0},
        )
        plan = service._build_trade_plan(candidate, "15m", trace_id="t3")  # noqa: SLF001
        self.assertIsNotNone(plan)
        assert plan is not None

        fill = EntryFillResult(
            success=True,
            filled_qty=plan.qty_l1,
            avg_price=plan.entry_price,
            exec_type="MAKER",
        )
        context = service._finalize_entry(plan, fill)  # noqa: SLF001
        self.assertIsNotNone(context)
        assert context is not None

        expected_basis = min(context.trade_state.risk_distance, plan.signal_risk)
        expected_tp = context.trade_state.entry_price + (expected_basis * plan.signal_rr)
        self.assertAlmostEqual(context.trade_state.tp, expected_tp, places=8)

from __future__ import annotations

import logging

import pytest
from bot.config import Settings
from bot.risk import RiskManager
from bot.services.entry_service import EntryService
from bot.services.position_service import PositionCache


class _BrokenOps:
    def record_event(self, **_kwargs) -> None:
        raise RuntimeError("ops unavailable")


@pytest.mark.unit
def test_ops_hook_failures_do_not_break_entry_service() -> None:
    settings = Settings()
    settings.use_paper_trading = True

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

    fake_client = type("C", (), {"futures_position_information": lambda self: []})()
    service = EntryService(
        settings=settings,
        stream=type("S", (), {"get_dataframe": lambda self, *_a, **_k: None})(),
        symbols=["BTCUSDT"],
        context_map={"15m": "1h"},
        trade_client=fake_client,
        risk=risk,
        position_cache=PositionCache(fake_client),  # type: ignore[arg-type]
        get_executor=lambda _symbol: None,
        logger=logging.getLogger("test.entry.ops"),
        trades_logger=logging.getLogger("test.entry.ops.trades"),
        telegram=type("T", (), {"send": lambda self, *_a, **_k: None})(),
        operations=_BrokenOps(),  # type: ignore[arg-type]
    )

    # Must not raise even if operational hook crashes.
    service._ops_call("record_event", kind="x", detail={})  # noqa: SLF001


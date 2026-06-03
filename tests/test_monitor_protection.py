from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from bot.monitor_protection import ensure_monitor_protections, ensure_orphan_protections


@pytest.mark.unit
def test_monitor_protection_returns_none_when_position_not_visible() -> None:
    monitor = SimpleNamespace(
        executor=SimpleNamespace(
            paper=False,
            has_open_position=MagicMock(return_value=False),
        ),
        trades_logger=MagicMock(),
        symbol="BTCUSDT",
        side="BUY",
        client_id_prefix="cid",
        trace_id="trace-1",
        _ops_call=MagicMock(),  # noqa: SLF001
    )
    trade_state = {"tp": 110.0, "sl": 90.0, "qty": 1.0}

    with patch("bot.monitor_protection.time.time", side_effect=[0.0, 9.0]):
        refs = ensure_monitor_protections(monitor, trade_state)

    assert refs is None
    monitor._ops_call.assert_any_call(  # noqa: SLF001
        "record_protection_result",
        symbol="BTCUSDT",
        ok=False,
        stage="position_closed_no_protection",
        trace_id="trace-1",
    )


@pytest.mark.unit
def test_orphan_protection_submits_emergency_close_after_repeated_failures() -> None:
    executor = SimpleNamespace(
        has_open_position=MagicMock(return_value=True),
        get_protection_refs=MagicMock(return_value=(None, None)),
        place_tp_sl=MagicMock(side_effect=OSError("tp/sl failed")),
        close_position_market=MagicMock(),
    )
    logger = MagicMock()
    trades_logger = MagicMock()
    ops_call = MagicMock()

    with patch("bot.monitor_protection.time.sleep", return_value=None):
        refs = ensure_orphan_protections(
            executor=executor,
            side="BUY",
            symbol="BTCUSDT",
            orphan_trade_state={"tp": 110.0, "sl": 90.0, "qty": 1.25},
            client_id_prefix="cid",
            logger=logger,
            trades_logger=trades_logger,
            ops_call=ops_call,
            trace_id="trace-2",
        )

    assert refs is None
    executor.close_position_market.assert_called_once_with("BUY", 1.25)
    ops_call.assert_any_call(
        "record_orphan_status",
        symbol="BTCUSDT",
        status="forced_close_submitted",
        detail="orphan_tp_sl_fail",
        trace_id="trace-2",
    )

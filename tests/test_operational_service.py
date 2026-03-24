from __future__ import annotations

import logging
from pathlib import Path

import pytest

from config import Settings
from risk import RiskState
from services.operational_service import OperationalService, render_operational_markdown


def _logger() -> logging.Logger:
    logger = logging.getLogger("tests.operational")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.NullHandler())
    return logger


@pytest.mark.unit
def test_operational_metrics_and_snapshot(tmp_path: Path) -> None:
    settings = Settings()
    settings.ops_status_json_path = str(tmp_path / "ops_status.json")
    settings.ops_summary_md_path = str(tmp_path / "ops_summary.md")
    settings.ops_report_interval_sec = 10
    service = OperationalService(settings=settings, logger=_logger())
    service.set_runtime_mode("PAPER")
    service.record_startup(symbols=80, intervals=["15m", "1h", "4h"])
    service.record_signal_detected(symbol="BTCUSDT", interval="15m", side="BUY", score=2.1)
    service.record_entry_attempt(symbol="BTCUSDT", side="BUY", interval="15m")
    service.record_entry_executed(
        symbol="BTCUSDT",
        side="BUY",
        interval="15m",
        qty=0.123,
        entry=100.0,
        margin=5.0,
        exec_type="LIMIT_ONLY",
    )
    service.record_protection_result(symbol="BTCUSDT", ok=True, stage="placed")
    service.record_trade_closed(
        symbol="BTCUSDT",
        result="TP",
        pnl=1.75,
        paper=True,
        equity_after=101.75,
    )
    service.heartbeat(
        stream_status={
            "event_count": 3,
            "scheduler_alive": True,
            "next_close_in_sec": 500.0,
            "last_poll_duration_sec": 1.25,
        },
        risk_state=RiskState(equity=101.75, day_start_equity=100.0),
        open_positions=0,
    )
    snapshot = service.snapshot()

    assert snapshot["runtime"]["mode"] == "PAPER"
    assert snapshot["counters"]["polls"] == 3
    assert snapshot["metrics"]["polling_cycles"] == 3
    assert snapshot["metrics"]["heartbeat_count"] == 1
    assert snapshot["counters"]["signals_detected"] == 1
    assert snapshot["metrics"]["signals_detected"] == 1
    assert snapshot["counters"]["entries_executed"] == 1
    assert snapshot["counters"]["protections_ok"] == 1
    assert snapshot["counters"]["paper_realized_pnl"] == pytest.approx(1.75)
    assert snapshot["last_exit"]["result"] == "TP"

    service.force_report()
    assert (tmp_path / "ops_status.json").exists()
    assert (tmp_path / "ops_summary.md").exists()


@pytest.mark.unit
def test_kill_switch_pauses_entries_when_enabled() -> None:
    settings = Settings()
    settings.enable_operational_kill_switches = True
    settings.kill_switch_max_consecutive_errors = 2
    settings.operational_suspend_sec = 300
    service = OperationalService(settings=settings, logger=_logger())

    assert service.can_open_new_entries() is True
    service.record_error(stage="entry", err=RuntimeError("x"), symbol="BTCUSDT")
    assert service.can_open_new_entries() is True
    service.record_error(stage="entry", err=RuntimeError("y"), symbol="BTCUSDT")
    assert service.can_open_new_entries() is False
    assert "consecutive_errors" in service.active_suspensions()


@pytest.mark.unit
def test_suspension_expires_and_entries_resume() -> None:
    settings = Settings()
    settings.enable_operational_kill_switches = True
    settings.kill_switch_max_consecutive_errors = 1
    settings.operational_suspend_sec = 60
    service = OperationalService(settings=settings, logger=_logger())
    service.record_error(stage="entry", err=RuntimeError("x"), symbol="BTCUSDT")
    assert service.can_open_new_entries() is False

    # Force-expire for deterministic test (no sleep needed).
    service._suspensions["consecutive_errors"] = 0.0  # noqa: SLF001
    assert service.can_open_new_entries() is True


@pytest.mark.unit
def test_scheduler_stale_rule_blocks_entries() -> None:
    settings = Settings()
    settings.enable_operational_kill_switches = True
    settings.kill_switch_max_scheduler_idle_sec = 30
    settings.operational_suspend_sec = 120
    service = OperationalService(settings=settings, logger=_logger())

    service.heartbeat(
        stream_status={"event_count": 1, "scheduler_alive": True, "next_close_in_sec": 10},
        risk_state=RiskState(equity=100.0, day_start_equity=100.0),
        open_positions=0,
    )
    service._last_poll_ts = service._last_poll_ts - 40.0  # noqa: SLF001
    assert service.can_open_new_entries() is False
    assert "scheduler_stale" in service.active_suspensions()


@pytest.mark.unit
def test_api_degraded_kill_switch() -> None:
    settings = Settings()
    settings.enable_operational_kill_switches = True
    settings.kill_switch_max_api_errors = 2
    settings.operational_suspend_sec = 120
    service = OperationalService(settings=settings, logger=_logger())
    service.record_error(stage="exchange_api", err=RuntimeError("e1"), api_related=True)
    assert service.can_open_new_entries() is True
    service.record_error(stage="exchange_api", err=RuntimeError("e2"), api_related=True)
    assert service.can_open_new_entries() is False
    assert "api_degraded" in service.active_suspensions()


@pytest.mark.unit
def test_traceability_recent_signal_entry_exit() -> None:
    settings = Settings()
    service = OperationalService(settings=settings, logger=_logger())
    trace = "BTCUSDT-15m-123"
    service.record_signal_detected(
        symbol="BTCUSDT",
        interval="15m",
        side="BUY",
        score=1.9,
        trace_id=trace,
    )
    service.record_entry_executed(
        symbol="BTCUSDT",
        side="BUY",
        interval="15m",
        qty=1.0,
        entry=100.0,
        margin=5.0,
        exec_type="MARKET",
        trace_id=trace,
    )
    service.record_trade_closed(
        symbol="BTCUSDT",
        result="TP",
        pnl=2.0,
        paper=False,
        equity_after=102.0,
        trace_id=trace,
    )
    snapshot = service.snapshot()
    assert snapshot["recent_signals"][-1]["trace_id"] == trace
    assert snapshot["recent_entries"][-1]["trace_id"] == trace
    assert snapshot["recent_exits"][-1]["trace_id"] == trace


@pytest.mark.unit
def test_render_operational_markdown_contains_sections() -> None:
    snapshot = {
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "runtime": {"mode": "LIVE", "uptime_sec": 10, "health": "healthy"},
        "scheduler": {
            "alive": True,
            "last_poll_age_sec": 5.0,
            "last_poll_duration_sec": 1.1,
            "next_close_in_sec": 30.0,
        },
        "counters": {
            "polls": 3,
            "signals_detected": 2,
            "signals_discarded": 1,
            "entry_attempts": 1,
            "entries_executed": 1,
            "entries_failed": 0,
            "protections_ok": 1,
            "protections_failed": 0,
            "open_positions": 1,
            "paper_realized_pnl": 0.0,
            "live_realized_pnl": 2.5,
        },
        "metrics": {
            "signals_detected": 2,
            "signals_rejected": 1,
            "entries_attempted": 1,
            "entries_executed": 1,
            "entries_failed": 0,
            "protection_failures": 0,
            "order_failures": 0,
            "orphan_recoveries": 0,
            "heartbeat_count": 2,
            "polling_cycles": 3,
            "last_signal_time": "2026-01-01T00:00:00+00:00",
            "last_entry_time": "2026-01-01T00:00:00+00:00",
            "last_exit_time": "2026-01-01T00:00:00+00:00",
            "uptime_sec": 10.0,
        },
        "risk": {"equity": 102.5, "day_start_equity": 100.0, "consecutive_losses": 0, "paused": False},
        "suspensions": {"active": False, "reasons": []},
        "errors": {"by_type": {}, "by_stage": {}},
        "last_signal": {"symbol": "BTCUSDT"},
        "last_entry": {"symbol": "BTCUSDT"},
        "last_exit": {"symbol": "BTCUSDT"},
    }
    text = render_operational_markdown(snapshot)
    assert "# Operational Status" in text
    assert "## Scheduler" in text
    assert "## Core Metrics" in text
    assert "## Suspensions" in text

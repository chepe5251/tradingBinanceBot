"""Runtime loop orchestration for active position monitoring."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from monitor_protection import ensure_monitor_protections

INTERVAL_SECONDS: dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
}


def run_position_monitor(monitor: Any) -> None:
    """Place protections, supervise TP/SL lifecycle, and finalize trade accounting."""
    trade_state = monitor.trade_state

    refs = ensure_monitor_protections(monitor, trade_state)
    if not refs:
        return
    tp_ref, sl_ref = refs

    risk_dist = float(trade_state["risk_distance"])
    monitor.trades_logger.info(
        "ENTRY %s tf=%s side=%s entry=%.4f sl_strategy=%.4f sl_swing=%.4f "
        "sl_atr=%.4f sl_final=%.4f tp=%.4f risk=%.4f rr_real=%.3f "
        "atr=%.4f score=%.2f exec=%s qty=%.6f margin=%.2f trace=%s",
        monitor.symbol,
        monitor.interval,
        monitor.side,
        float(trade_state["entry_price"]),
        float(monitor.signal.get("stop_price", 0)),
        monitor.sl_swing,
        monitor.sl_atr,
        float(trade_state["sl"]),
        float(trade_state["tp"]),
        risk_dist,
        abs(float(trade_state["tp"]) - float(trade_state["entry_price"])) / risk_dist if risk_dist > 0 else 0.0,
        monitor.atr_val,
        float(monitor.signal.get("score", 0)),
        monitor.exec_type,
        float(trade_state["qty"]),
        monitor.margin_to_use,
        monitor.trace_id or "",
    )

    interval_sec = INTERVAL_SECONDS.get(monitor.interval, 900)
    max_hold_sec = monitor.max_hold_candles * interval_sec if monitor.max_hold_candles > 0 else 0.0
    result, exit_price = monitor.executor.monitor_oco(
        tp_ref,
        sl_ref,
        side=monitor.side,
        entry_price=float(trade_state["entry_price"]),
        tp_price=float(trade_state["tp"]),
        sl_price=float(trade_state["sl"]),
        qty=float(trade_state["qty"]),
        atr=monitor.atr_val,
        breakeven_trigger_pct=float(trade_state["breakeven_trigger_pct"]),
        price_fn=monitor.price_fn,
        atr_fn=monitor.atr_fn,
        on_event=monitor.on_event,
        scale_fn=None,  # _scale_fn desactivado — ver enable_loss_scaling en Settings
        safety_check_sec=2,
        review_fn=monitor._review_fn,  # noqa: SLF001
        review_sec=7,
        client_id_prefix=monitor.client_id_prefix,
        max_hold_sec=max_hold_sec,
    )

    if result in {"TP", "SL"} or result.startswith("EARLY"):
        final_entry = float(trade_state["entry_price"])
        final_qty = float(trade_state["qty"])
        pnl = (exit_price - final_entry) * final_qty
        if monitor.side == "SELL":
            pnl = -pnl
        monitor.risk_updater(pnl, datetime.now(timezone.utc))
        monitor.pos_cache_invalidate()
        equity_after = float(monitor.risk.snapshot().equity)
        monitor._ops_call(  # noqa: SLF001
            "record_trade_closed",
            symbol=monitor.symbol,
            result=result,
            pnl=pnl,
            paper=bool(monitor.executor.paper),
            equity_after=equity_after,
            trace_id=monitor.trace_id,
        )
        monitor.trades_logger.info(
            "exit %s result=%s pnl=%.4f trace=%s",
            monitor.symbol,
            result,
            pnl,
            monitor.trace_id or "",
        )
        return

    equity_after = float(monitor.risk.snapshot().equity)
    monitor._ops_call(  # noqa: SLF001
        "record_trade_closed",
        symbol=monitor.symbol,
        result=result,
        pnl=0.0,
        paper=bool(monitor.executor.paper),
        equity_after=equity_after,
        trace_id=monitor.trace_id,
    )
    monitor.trades_logger.info(
        "exit %s result=%s pnl=0.0 trace=%s",
        monitor.symbol,
        result,
        monitor.trace_id or "",
    )


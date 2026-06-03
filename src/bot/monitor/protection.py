"""Protection-order helpers for monitor and orphan runtime flows."""
from __future__ import annotations

import time
from typing import Any, Callable, Iterable

from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException

MONITOR_ERRORS = (
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
    OSError,
    ValueError,
    TypeError,
)


def extract_orphan_protection_prices(
    *,
    trade_client: Any,
    symbol: str,
    stop_order_types: Iterable[str],
    tp_order_types: Iterable[str],
    logger: Any,
) -> tuple[float | None, float | None]:
    """Read currently open TP/SL prices for orphan recovery."""
    sl_price: float | None = None
    tp_price: float | None = None
    open_orders = trade_client.futures_get_open_orders(symbol=symbol)
    for order in open_orders:
        order_type = order.get("type", "")
        stop_price = float(order.get("stopPrice", 0) or 0)
        if stop_price <= 0:
            continue
        # Binance may return either MARKET or non-MARKET variants depending on endpoint/version.
        if order_type in stop_order_types and sl_price is None:
            sl_price = stop_price
        elif order_type in tp_order_types and tp_price is None:
            tp_price = stop_price
    return sl_price, tp_price


def ensure_monitor_protections(monitor: Any, trade_state: dict) -> tuple[Any, Any] | None:
    """Wait for position visibility and ensure active TP/SL protections exist."""
    attempts = 0
    emergency = False
    tp_ref = None
    sl_ref = None
    position_wait_deadline = time.time() + 8.0

    while True:
        if not monitor.executor.paper:
            try:
                has_pos = monitor.executor.has_open_position()
            except MONITOR_ERRORS:
                has_pos = False
            if not has_pos:
                if time.time() < position_wait_deadline:
                    time.sleep(0.25)
                    continue
                monitor.trades_logger.info(
                    "critical %s reason=position_closed_no_protection",
                    monitor.symbol,
                )
                monitor._ops_call(  # noqa: SLF001
                    "record_protection_result",
                    symbol=monitor.symbol,
                    ok=False,
                    stage="position_closed_no_protection",
                    trace_id=monitor.trace_id,
                )
                return None
        try:
            existing_tp, existing_sl = monitor.executor.get_protection_refs(
                monitor.side,
                client_id_prefix=monitor.client_id_prefix,
            )
            if existing_tp and existing_sl:
                tp_ref, sl_ref = existing_tp, existing_sl
                monitor._ops_call(  # noqa: SLF001
                    "record_protection_result",
                    symbol=monitor.symbol,
                    ok=True,
                    stage="reused_existing",
                    trace_id=monitor.trace_id,
                )
                break
            tp_ref, sl_ref = monitor.executor.place_tp_sl(
                monitor.side,
                float(trade_state["tp"]),
                float(trade_state["sl"]),
                float(trade_state["qty"]),
                client_id_prefix=monitor.client_id_prefix,
            )
            if tp_ref and sl_ref:
                monitor._ops_call(  # noqa: SLF001
                    "record_protection_result",
                    symbol=monitor.symbol,
                    ok=True,
                    stage="placed",
                    trace_id=monitor.trace_id,
                )
                break
        except MONITOR_ERRORS as exc:
            monitor.logger.error("TP/SL placement failed %s: %s", monitor.symbol, exc)
            monitor.trades_logger.info("error %s stage=tp_sl msg=%s", monitor.symbol, exc)
            monitor._ops_call(  # noqa: SLF001
                "record_error",
                stage="tp_sl_placement",
                err=exc,
                symbol=monitor.symbol,
                recoverable=True,
                api_related=True,
                trace_id=monitor.trace_id,
            )
            monitor._ops_call(  # noqa: SLF001
                "record_protection_result",
                symbol=monitor.symbol,
                ok=False,
                stage="placement_exception",
                trace_id=monitor.trace_id,
            )

        attempts += 1
        if attempts >= 10 and not emergency:
            emergency = True
            monitor.trades_logger.info("critical %s reason=tp_sl_emergency", monitor.symbol)
            monitor._ops_call(  # noqa: SLF001
                "record_error",
                stage="tp_sl_emergency",
                err="tp_sl_emergency",
                symbol=monitor.symbol,
                recoverable=False,
                api_related=True,
                trace_id=monitor.trace_id,
            )
        time.sleep(2 if emergency else 1)
    return tp_ref, sl_ref


def ensure_orphan_protections(
    *,
    executor: Any,
    side: str,
    symbol: str,
    orphan_trade_state: dict,
    client_id_prefix: str,
    logger: Any,
    trades_logger: Any,
    ops_call: Callable[..., None],
    trace_id: str,
) -> tuple[Any, Any] | None:
    """Ensure protections for an orphaned position before monitor loop."""
    attempts = 0
    tp_ref = None
    sl_ref = None
    position_wait_deadline = time.time() + 10.0

    while True:
        try:
            has_pos = executor.has_open_position()
        except MONITOR_ERRORS:
            has_pos = False
        if not has_pos:
            if time.time() < position_wait_deadline:
                time.sleep(0.5)
                continue
            trades_logger.info("orphan %s reason=position_gone", symbol)
            ops_call(
                "record_orphan_status",
                symbol=symbol,
                status="unrecoverable",
                detail="position_gone_before_protection",
                trace_id=trace_id,
            )
            return None

        try:
            existing_tp, existing_sl = executor.get_protection_refs(
                side,
                client_id_prefix=client_id_prefix,
            )
            if existing_tp and existing_sl:
                tp_ref, sl_ref = existing_tp, existing_sl
                ops_call(
                    "record_protection_result",
                    symbol=symbol,
                    ok=True,
                    stage="orphan_reused_existing",
                    trace_id=trace_id,
                )
                break
            tp_ref, sl_ref = executor.place_tp_sl(
                side,
                float(orphan_trade_state["tp"]),
                float(orphan_trade_state["sl"]),
                float(orphan_trade_state["qty"]),
                client_id_prefix=client_id_prefix,
            )
            if tp_ref and sl_ref:
                ops_call(
                    "record_protection_result",
                    symbol=symbol,
                    ok=True,
                    stage="orphan_placed",
                    trace_id=trace_id,
                )
                break
        except MONITOR_ERRORS as exc:
            logger.error("Orphan TP/SL placement failed %s: %s", symbol, exc)
            ops_call(
                "record_error",
                stage="orphan_tp_sl_placement",
                err=exc,
                symbol=symbol,
                recoverable=True,
                api_related=True,
                trace_id=trace_id,
            )
            ops_call(
                "record_protection_result",
                symbol=symbol,
                ok=False,
                stage="orphan_placement_exception",
                trace_id=trace_id,
            )

        attempts += 1
        time.sleep(2)
        if attempts >= 15:
            trades_logger.info("critical %s reason=orphan_tp_sl_fail", symbol)
            emergency_qty = float(orphan_trade_state.get("qty") or 0.0)
            try:
                if emergency_qty > 0:
                    executor.close_position_market(side, emergency_qty)
                    trades_logger.info(
                        "orphan %s emergency_close_submitted qty=%.6f",
                        symbol,
                        emergency_qty,
                    )
                ops_call(
                    "record_orphan_status",
                    symbol=symbol,
                    status="forced_close_submitted",
                    detail="orphan_tp_sl_fail",
                    trace_id=trace_id,
                )
            except MONITOR_ERRORS as close_exc:
                logger.error("Orphan emergency close failed %s: %s", symbol, close_exc)
                ops_call(
                    "record_error",
                    stage="orphan_emergency_close",
                    err=close_exc,
                    symbol=symbol,
                    recoverable=False,
                    api_related=True,
                    trace_id=trace_id,
                )
            ops_call(
                "record_orphan_status",
                symbol=symbol,
                status="unrecoverable",
                detail="orphan_tp_sl_fail",
                trace_id=trace_id,
            )
            return None

    return tp_ref, sl_ref

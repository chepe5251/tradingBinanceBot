"""Orphan recovery flow extracted from monitor facade."""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException

from exchange_utils import safe_mark_price
from execution import STOP_ORDER_TYPES, TP_ORDER_TYPES, FuturesExecutor
from indicators import atr_last
from monitor_protection import ensure_orphan_protections, extract_orphan_protection_prices
from risk import RiskManager

MONITOR_ERRORS = (
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
    OSError,
    ValueError,
    TypeError,
)


def resume_orphan_position(
    *,
    orphan: dict,
    symbols: list[str],
    stream: Any,
    settings: Any,
    get_executor: Callable[[str], FuturesExecutor],
    risk: RiskManager,
    trade_client: Client,
    pos_cache_invalidate: Callable[[], None],
    risk_updater: Callable[[float, datetime], None],
    logger: logging.Logger,
    trades_logger: logging.Logger,
    operations: Any = None,
) -> None:
    """Recover a single orphaned position and launch daemon monitoring."""

    def _ops_call(method: str, **kwargs) -> None:
        if operations is None:
            return
        try:
            getattr(operations, method)(**kwargs)
        except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
            logger.debug("ops_orphan_hook_failed method=%s err=%s", method, exc)

    symbol = orphan.get("symbol", "")
    orphan_trace_id = f"orphan-{symbol or 'UNKNOWN'}-{int(time.time() * 1000)}"
    if not symbol or symbol not in set(symbols):
        logger.warning("Orphan recovery: symbol %s not in universe, skipping.", symbol)
        _ops_call(
            "record_orphan_status",
            symbol=symbol or "UNKNOWN",
            status="unrecoverable",
            detail="symbol_not_in_universe",
            trace_id=orphan_trace_id,
        )
        return

    try:
        qty = abs(float(orphan.get("positionAmt", 0)))
        entry_price = float(orphan.get("entryPrice", 0))
        side = "BUY" if float(orphan.get("positionAmt", 0)) > 0 else "SELL"
    except (TypeError, ValueError) as exc:
        logger.warning("Orphan recovery: could not parse position data: %s", exc)
        _ops_call(
            "record_orphan_status",
            symbol=symbol,
            status="unrecoverable",
            detail=f"parse_failed:{exc}",
            trace_id=orphan_trace_id,
        )
        return

    if qty <= 0 or entry_price <= 0:
        logger.warning("Orphan recovery: invalid qty=%.4f entry=%.4f, skipping.", qty, entry_price)
        _ops_call(
            "record_orphan_status",
            symbol=symbol,
            status="unrecoverable",
            detail="invalid_qty_or_entry",
            trace_id=orphan_trace_id,
        )
        return

    logger.info(
        "Orphan recovery: resuming %s side=%s qty=%.4f entry=%.4f",
        symbol,
        side,
        qty,
        entry_price,
    )
    _ops_call(
        "record_orphan_status",
        symbol=symbol,
        status="detected",
        detail="resuming",
        trace_id=orphan_trace_id,
    )

    sl_price: float | None = None
    tp_price: float | None = None
    try:
        sl_price, tp_price = extract_orphan_protection_prices(
            trade_client=trade_client,
            symbol=symbol,
            stop_order_types=STOP_ORDER_TYPES,
            tp_order_types=TP_ORDER_TYPES,
            logger=logger,
        )
    except MONITOR_ERRORS as exc:
        logger.warning("Orphan recovery: could not fetch open orders: %s", exc)
        _ops_call(
            "record_error",
            stage="orphan_open_orders",
            err=exc,
            symbol=symbol,
            recoverable=True,
            api_related=True,
            trace_id=orphan_trace_id,
        )

    df_orphan = stream.get_dataframe(symbol, settings.main_interval)
    atr_orphan = atr_last(df_orphan, settings.atr_period) if not df_orphan.empty else 0.0

    if sl_price is None:
        sl_price = (
            entry_price - settings.stop_atr_mult * atr_orphan
            if side == "BUY"
            else entry_price + settings.stop_atr_mult * atr_orphan
        )
    if tp_price is None:
        risk_est = abs(entry_price - sl_price)
        tp_price = (
            entry_price + risk_est * max(float(settings.tp_rr), 1.8)
            if side == "BUY"
            else entry_price - risk_est * max(float(settings.tp_rr), 1.8)
        )

    executor = get_executor(symbol)
    qty_rounded = executor.round_qty(qty)
    risk_distance = abs(entry_price - sl_price)
    breakeven_pct = max(risk_distance / entry_price, 0.005) if entry_price > 0 else 0.005

    orphan_trade_state = {
        "entry_price": entry_price,
        "qty": qty_rounded,
        "sl": sl_price,
        "tp": tp_price,
        "risk_distance": risk_distance,
        "breakeven_trigger_pct": breakeven_pct,
        "anchor_entry_price": entry_price,
        "anchor_risk_distance": risk_distance,
        "tp_risk_cap": risk_distance,
        "db_trade_id": None,
    }

    client_id_prefix = f"{symbol}-orphan-{int(time.time() * 1000)}"

    def _price_fn() -> Optional[float]:
        return safe_mark_price(trade_client, symbol, logger=logger)

    def _atr_fn() -> Optional[float]:
        orphan_df = stream.get_dataframe(symbol, settings.main_interval)
        return atr_last(orphan_df, settings.atr_period)

    def _on_event(kind: str, new_sl: float) -> None:
        trades_logger.info("%s %s new_sl=%.4f (orphan)", kind, symbol, new_sl)

    def _orphan_monitor() -> None:
        refs = ensure_orphan_protections(
            executor=executor,
            side=side,
            symbol=symbol,
            orphan_trade_state=orphan_trade_state,
            client_id_prefix=client_id_prefix,
            logger=logger,
            trades_logger=trades_logger,
            ops_call=_ops_call,
            trace_id=orphan_trace_id,
        )
        if not refs:
            return
        tp_ref, sl_ref = refs

        trades_logger.info(
            "orphan_resumed %s side=%s price=%.4f qty=%.6f tp=%.4f sl=%.4f trace=%s",
            symbol,
            side,
            entry_price,
            qty_rounded,
            float(orphan_trade_state["tp"]),
            float(orphan_trade_state["sl"]),
            orphan_trace_id,
        )
        _ops_call(
            "record_orphan_status",
            symbol=symbol,
            status="resumed",
            detail="monitor_started",
            trace_id=orphan_trace_id,
        )

        result, exit_price_val = executor.monitor_oco(
            tp_ref,
            sl_ref,
            side=side,
            entry_price=float(orphan_trade_state["entry_price"]),
            tp_price=float(orphan_trade_state["tp"]),
            sl_price=float(orphan_trade_state["sl"]),
            qty=float(orphan_trade_state["qty"]),
            atr=atr_orphan,
            breakeven_trigger_pct=float(orphan_trade_state["breakeven_trigger_pct"]),
            price_fn=_price_fn,
            atr_fn=_atr_fn,
            on_event=_on_event,
            scale_fn=None,  # _scale_fn desactivado — ver enable_loss_scaling en Settings
            safety_check_sec=2,
            review_fn=None,
            review_sec=7,
            client_id_prefix=client_id_prefix,
        )
        final_entry = float(orphan_trade_state["entry_price"])
        final_qty = float(orphan_trade_state["qty"])
        pnl = (exit_price_val - final_entry) * final_qty
        if side == "SELL":
            pnl = -pnl
        risk_updater(pnl, datetime.now(timezone.utc))
        pos_cache_invalidate()
        trades_logger.info(
            "orphan_exit %s result=%s pnl=%.4f trace=%s",
            symbol,
            result,
            pnl,
            orphan_trace_id,
        )
        _ops_call(
            "record_trade_closed",
            symbol=symbol,
            result=f"orphan:{result}",
            pnl=pnl,
            paper=bool(executor.paper),
            equity_after=float(risk.snapshot().equity),
            trace_id=orphan_trace_id,
        )

    threading.Thread(target=_orphan_monitor, daemon=True, name=f"orphan-{symbol}").start()


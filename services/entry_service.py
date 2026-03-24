"""Signal gating, entry execution, and monitor spawn flow."""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

from config import Settings
from exchange_utils import safe_mark_price
from execution import FuturesExecutor
from indicators import atr_last
from monitor import PositionMonitor
from risk import RiskManager
from services.position_service import PositionCache, count_active_positions, get_available_balance
from services.signal_service import SignalCandidate, evaluate_interval_signals
from services.telegram_service import TelegramService, format_signal_message
from sizing import (
    SIZING_MODE_FIXED_MARGIN,
    PositionSizer,
    SizingInputs,
    is_entry_size_valid,
)

RECOVERABLE_ERRORS = (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError)

if TYPE_CHECKING:
    from services.operational_service import OperationalService


class EntryService:
    """Orchestrates signal-to-entry execution for scheduler callbacks."""

    def __init__(
        self,
        settings: Settings,
        stream,
        symbols: list[str],
        context_map: dict[str, str],
        trade_client: Client,
        risk: RiskManager,
        position_cache: PositionCache,
        get_executor,
        logger: logging.Logger,
        trades_logger: logging.Logger,
        telegram: TelegramService,
        operations: "OperationalService | None" = None,
    ) -> None:
        self.settings = settings
        self.stream = stream
        self.symbols = symbols
        self.context_map = context_map
        self.trade_client = trade_client
        self.risk = risk
        self.position_cache = position_cache
        self.get_executor = get_executor
        self.logger = logger
        self.trades_logger = trades_logger
        self.telegram = telegram
        self.operations = operations
        self.sizer = PositionSizer(settings.sizing_mode)

        self._entry_lock = threading.Lock()
        self._interval_states: dict[str, dict] = {}

    def make_on_close(self, interval: str):
        """Return interval callback expected by MarketDataStream scheduler."""
        if interval not in self._interval_states:
            self._interval_states[interval] = {"last_close_ms": None, "lock": threading.Lock()}

        def _callback() -> None:
            self._on_close(interval)

        return _callback

    def _ops_call(self, method: str, **kwargs) -> None:
        """Invoke optional operational hooks without affecting trading flow."""
        if self.operations is None:
            return
        try:
            getattr(self.operations, method)(**kwargs)
        except Exception as exc:  # noqa: BLE001
            # Operational telemetry must never block the trading flow.
            self.logger.debug("ops_hook_failed method=%s err=%s", method, exc)

    def _mark_entry_failed(
        self,
        symbol: str,
        stage: str,
        reason: str,
        trace_id: str = "",
    ) -> None:
        self._ops_call(
            "record_entry_failed",
            symbol=symbol,
            stage=stage,
            reason=reason,
            trace_id=trace_id,
        )

    def _on_close(self, interval: str) -> None:
        interval_state = self._interval_states[interval]
        anchor_symbol = self.symbols[0]
        anchor_df = self.stream.get_dataframe(anchor_symbol, interval)
        if anchor_df.empty:
            return

        close_time = anchor_df.iloc[-1]["close_time"]
        close_ms = int(close_time.timestamp() * 1000)
        with interval_state["lock"]:
            if interval_state["last_close_ms"] == close_ms:
                return
            interval_state["last_close_ms"] = close_ms

        now = datetime.now(timezone.utc)
        can_trade_now = self.risk.can_trade(now)

        try:
            positions_snapshot = self.position_cache.get()
        except RECOVERABLE_ERRORS as exc:
            self.logger.warning("entry_gate_positions_failed interval=%s err=%s", interval, exc)
            self._ops_call(
                "record_error",
                stage="entry_gate_positions",
                err=exc,
                recoverable=True,
                api_related=True,
            )
            return

        active_positions, symbols_with_positions = count_active_positions(positions_snapshot)
        has_open_position = active_positions >= self.settings.max_positions

        context_interval = self.context_map.get(interval)
        valid_signals = evaluate_interval_signals(
            stream=self.stream,
            symbols=self.symbols,
            interval=interval,
            context_interval=context_interval,
            settings=self.settings,
            trades_logger=self.trades_logger,
            operations=self.operations,
        )
        if not valid_signals:
            return

        for candidate in valid_signals:
            trace_id = str(candidate.payload.get("trace_id") or f"{candidate.symbol}-{interval}-{close_ms}")
            candidate.payload["trace_id"] = trace_id
            self._ops_call(
                "record_signal_detected",
                symbol=candidate.symbol,
                interval=interval,
                side=str(candidate.payload.get("side") or ""),
                score=float(candidate.score),
                trace_id=trace_id,
            )

        pre_filter_candidates = list(valid_signals)
        valid_signals = [
            candidate for candidate in valid_signals if candidate.symbol not in symbols_with_positions
        ]
        if not valid_signals:
            self.trades_logger.info(
                "all_signals_skipped tf=%s reason=per_symbol_limit active=%d",
                interval,
                active_positions,
            )
            for candidate in pre_filter_candidates:
                self._ops_call(
                    "record_signal_discarded",
                    reason="per_symbol_limit",
                    symbol=candidate.symbol,
                    interval=interval,
                    trace_id=str(candidate.payload.get("trace_id") or ""),
                )
            return

        execution_allowed = can_trade_now and not has_open_position
        block_reason = ""
        if has_open_position:
            block_reason = (
                "SEÑAL BLOQUEADA: MAXIMO DE POSICIONES "
                f"({active_positions}/{self.settings.max_positions})"
            )
        elif not can_trade_now:
            block_reason = "BLOQUEADA POR RISK MANAGER"

        live_count = active_positions
        if execution_allowed:
            try:
                live_positions = self.position_cache.get()
                live_count, symbols_with_positions = count_active_positions(live_positions)
                if live_count >= self.settings.max_positions:
                    execution_allowed = False
                    block_reason = (
                        "SEÑAL BLOQUEADA: MAXIMO DE POSICIONES "
                        f"({live_count}/{self.settings.max_positions})"
                    )
            except RECOVERABLE_ERRORS as exc:
                self.logger.warning("position_gate_failed interval=%s err=%s", interval, exc)
                self._ops_call(
                    "record_error",
                    stage="position_gate",
                    err=exc,
                    recoverable=True,
                    api_related=True,
                )
                execution_allowed = False
                block_reason = "BLOQUEADA: ERROR VERIFICANDO POSICION"

        ops_allows_entries = True
        try:
            ops_allows_entries = self.operations.can_open_new_entries() if self.operations else True
        except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
            self.logger.debug("ops_gate_failed interval=%s err=%s", interval, exc)
            ops_allows_entries = True
        if execution_allowed and not ops_allows_entries:
            execution_allowed = False
            block_reason = "BLOQUEADA POR SUSPENSION OPERATIVA"

        self._broadcast_signal_alerts(valid_signals, interval)

        slots_available = max(0, self.settings.max_positions - live_count)
        candidates = valid_signals[: max(1, slots_available)]
        best_candidate = candidates[0]

        if not execution_allowed:
            self.trades_logger.info(
                "signal_only tf=%s reason=%s total=%d",
                interval,
                block_reason or "BLOQUEADA",
                len(valid_signals),
            )
            self._ops_call(
                "record_signal_discarded",
                reason=block_reason or "blocked",
                symbol=best_candidate.symbol,
                interval=interval,
                trace_id=str(best_candidate.payload.get("trace_id") or ""),
            )
            return

        executed = self._execute_candidate(best_candidate, interval)
        if not executed:
            return

        if len(candidates) > 1:
            self.position_cache.invalidate()
            with interval_state["lock"]:
                interval_state["last_close_ms"] = None
            threading.Thread(
                target=lambda: self._on_close(interval),
                daemon=True,
                name=f"on_close_slot2_{interval}",
            ).start()

    def _broadcast_signal_alerts(self, candidates: list[SignalCandidate], interval: str) -> None:
        for candidate in candidates:
            signal = candidate.payload
            side = signal["side"]
            trace_id = str(signal.get("trace_id") or "")
            entry = self._entry_price_with_offset(side, float(signal["price"]))
            atr_value = float(signal.get("atr") or 0.0)
            signal_risk = float(signal.get("risk_per_unit") or 0.0)
            signal_rr = max(float(signal.get("rr_target") or 0.0), 1.8)
            risk_distance = signal_risk if signal_risk > 0 else max(
                atr_value * self.settings.atr_sl_mult,
                entry * self.settings.min_sl_pct,
            )
            if risk_distance <= 0:
                continue

            if side == "BUY":
                sl = entry - risk_distance
                tp = entry + (risk_distance * signal_rr)
            else:
                sl = entry + risk_distance
                tp = entry - (risk_distance * signal_rr)
            rr_value = abs(tp - entry) / abs(entry - sl) if entry != sl else 0.0
            msg = format_signal_message(
                symbol=candidate.symbol,
                side=side,
                timeframe=signal.get("timeframe", interval.upper()),
                htf_bias=str(signal.get("htf_bias") or ("LONG" if side == "BUY" else "SHORT")),
                entry=entry,
                sl=sl,
                tp=tp,
                rr=rr_value,
                quality="A",
                volatility="Normal",
                structure=signal.get("strategy", "ob_bos").replace("_", " ").title(),
            )
            self.trades_logger.info(
                "signal_alert %s tf=%s side=%s trace=%s",
                candidate.symbol,
                interval,
                side,
                trace_id,
            )
            self._ops_call(
                "record_signal_alerted",
                symbol=candidate.symbol,
                interval=interval,
                side=side,
                trace_id=trace_id,
            )
            threading.Thread(target=self.telegram.send, args=(msg,), daemon=True).start()

    def _entry_price_with_offset(self, side: str, market_price: float) -> float:
        if side == "BUY":
            return market_price * (1 - self.settings.limit_offset_pct)
        return market_price * (1 + self.settings.limit_offset_pct)

    def _available_balance_for_entry(self) -> float:
        if self.settings.use_paper_trading:
            return max(0.0, self.risk.snapshot().equity)
        return get_available_balance(self.trade_client)

    def _execute_candidate(self, candidate: SignalCandidate, interval: str) -> bool:
        symbol = candidate.symbol
        signal = candidate.payload
        side = signal["side"]
        trace_id = str(signal.get("trace_id") or f"{symbol}-{interval}-{int(time.time() * 1000)}")
        signal["trace_id"] = trace_id
        self._ops_call(
            "record_entry_attempt",
            symbol=symbol,
            side=side,
            interval=interval,
            trace_id=trace_id,
        )
        entry_price = self._entry_price_with_offset(side, float(signal["price"]))
        signal_risk = float(signal.get("risk_per_unit") or 0.0)

        atr_value = float(signal.get("atr") or 0.0)
        if atr_value <= 0:
            atr_value = atr_last(self.stream.get_dataframe(symbol, interval), self.settings.atr_period)
        if atr_value <= 0:
            self.trades_logger.info("skip %s reason=atr_invalid", symbol)
            self._mark_entry_failed(symbol, "pre_entry", "atr_invalid", trace_id=trace_id)
            return False

        signal_rr = max(float(signal.get("rr_target") or 0.0), 1.8)
        risk_distance = signal_risk if signal_risk > 0 else max(
            atr_value * self.settings.atr_sl_mult,
            entry_price * self.settings.min_sl_pct,
        )
        reward_distance = risk_distance * signal_rr
        if side == "BUY":
            tp = entry_price + reward_distance
            sl = entry_price - risk_distance
        else:
            tp = entry_price - reward_distance
            sl = entry_price + risk_distance

        try:
            available_balance = self._available_balance_for_entry()
        except RECOVERABLE_ERRORS as exc:
            self.logger.warning("balance_fetch_failed symbol=%s err=%s", symbol, exc)
            self.trades_logger.info("skip %s reason=balance_fetch_failed", symbol)
            self._ops_call(
                "record_error",
                stage="balance_fetch",
                err=exc,
                symbol=symbol,
                recoverable=True,
                api_related=True,
                trace_id=trace_id,
            )
            self._mark_entry_failed(symbol, "pre_entry", "balance_fetch_failed", trace_id=trace_id)
            return False

        executor: FuturesExecutor = self.get_executor(symbol)
        min_qty = executor.get_min_qty()
        min_notional = executor.get_min_notional()

        entry_df = self.stream.get_dataframe(symbol, interval)
        if entry_df.empty or len(entry_df) < 12:
            self.trades_logger.info("skip %s reason=entry_df_insufficient", symbol)
            self._mark_entry_failed(symbol, "pre_entry", "entry_df_insufficient", trace_id=trace_id)
            return False

        swing_window = entry_df.iloc[-10:-1]
        swing_low = float(swing_window["low"].min())
        swing_high = float(swing_window["high"].max())
        if side == "BUY":
            sl_swing = swing_low
            sl_atr = entry_price - (self.settings.stop_atr_mult * atr_value)
            sl_common = min(sl_swing, sl_atr)
        else:
            sl_swing = swing_high
            sl_atr = entry_price + (self.settings.stop_atr_mult * atr_value)
            sl_common = max(sl_swing, sl_atr)

        margin_to_use = self.sizer.margin_to_use(
            SizingInputs(
                available_balance=available_balance,
                entry_price=entry_price,
                stop_price=sl_common,
                leverage=self.settings.leverage,
                fixed_margin_per_trade_usdt=self.settings.fixed_margin_per_trade_usdt,
                margin_utilization=self.settings.margin_utilization,
                risk_per_trade_pct=self.settings.risk_per_trade_pct,
            )
        )
        if margin_to_use <= 0:
            self.trades_logger.info(
                "skip %s reason=margin_to_use_invalid mode=%s pct=%.4f avail=%.4f",
                symbol,
                self.settings.sizing_mode,
                self.settings.risk_per_trade_pct,
                available_balance,
            )
            self._mark_entry_failed(symbol, "sizing", "margin_to_use_invalid", trace_id=trace_id)
            return False

        qty_by_margin = executor.calc_qty(margin_to_use, entry_price)
        if qty_by_margin <= 0:
            self.trades_logger.info("skip %s reason=qty_by_margin_invalid", symbol)
            self._mark_entry_failed(symbol, "sizing", "qty_by_margin_invalid", trace_id=trace_id)
            return False
        qty_l1 = executor.round_qty(qty_by_margin)

        # Keep liquidation-distance guard consistent with the effective sizing source:
        # - fixed_margin mode: use configured fixed margin
        # - other modes: use the actual computed margin_to_use for this entry
        margin_initial_ref = (
            float(self.settings.fixed_margin_per_trade_usdt)
            if self.settings.sizing_mode == SIZING_MODE_FIXED_MARGIN
            else float(margin_to_use)
        )
        if margin_initial_ref > 0 and qty_l1 > 0:
            min_sl_distance_for_rebuy = (margin_initial_ref * 1.5) / qty_l1
            if side == "BUY":
                sl_required = entry_price - min_sl_distance_for_rebuy
                if sl_common > sl_required:
                    self.trades_logger.info(
                        "skip %s reason=sl_inside_liquidation_zone sl_common=%.4f "
                        "sl_required=%.4f entry=%.4f",
                        symbol,
                        sl_common,
                        sl_required,
                        entry_price,
                    )
                    self._mark_entry_failed(
                        symbol,
                        "sl_validation",
                        "sl_inside_liquidation_zone",
                        trace_id=trace_id,
                    )
                    return False
            else:
                sl_required = entry_price + min_sl_distance_for_rebuy
                if sl_common < sl_required:
                    self.trades_logger.info(
                        "skip %s reason=sl_inside_liquidation_zone sl_common=%.4f "
                        "sl_required=%.4f entry=%.4f",
                        symbol,
                        sl_common,
                        sl_required,
                        entry_price,
                    )
                    self._mark_entry_failed(
                        symbol,
                        "sl_validation",
                        "sl_inside_liquidation_zone",
                        trace_id=trace_id,
                    )
                    return False

        if not is_entry_size_valid(qty_l1, entry_price, min_qty, min_notional):
            self.trades_logger.info(
                "skip %s reason=entry_notional_invalid side=%s qty=%.6f entry=%.6f "
                "min_qty=%.6f min_notional=%.4f",
                symbol,
                side,
                qty_l1,
                entry_price,
                min_qty,
                min_notional,
            )
            self._mark_entry_failed(symbol, "validation", "entry_notional_invalid", trace_id=trace_id)
            return False

        self.trades_logger.info(
            "entry_attempt %s tf=%s side=%s qty=%.6f entry=%.6f margin=%.4f trace=%s",
            symbol,
            interval,
            side,
            qty_l1,
            entry_price,
            margin_to_use,
            trace_id,
        )
        if not self._entry_lock.acquire(blocking=False):
            self.trades_logger.info("signal_only tf=%s reason=entry_lock_busy sym=%s", interval, symbol)
            self._mark_entry_failed(symbol, "entry_lock", "entry_lock_busy", trace_id=trace_id)
            return False

        filled_qty = 0.0
        avg_price = 0.0
        exec_type = "UNKNOWN"
        try:
            if self._is_position_gate_blocked(symbol):
                self._mark_entry_failed(
                    symbol,
                    "position_gate_recheck",
                    "blocked",
                    trace_id=trace_id,
                )
                return False

            try:
                self.trade_client.futures_cancel_all_open_orders(symbol=symbol)
            except RECOVERABLE_ERRORS as exc:
                self.logger.warning("pre_entry_cleanup_failed symbol=%s err=%s", symbol, exc)
                self._ops_call(
                    "record_error",
                    stage="pre_entry_cleanup",
                    err=exc,
                    symbol=symbol,
                    recoverable=True,
                    api_related=True,
                    trace_id=trace_id,
                )

            try:
                self._ops_call(
                    "record_event",
                    kind="order_submit",
                    detail={
                        "symbol": symbol,
                        "side": side,
                        "qty": qty_l1,
                        "entry_price": entry_price,
                    },
                    trace_id=trace_id,
                )
                filled_qty, avg_price, exec_type = self._place_entry(
                    executor=executor,
                    side=side,
                    entry_price=entry_price,
                    qty=qty_l1,
                )
            except RECOVERABLE_ERRORS as exc:
                self.logger.error(
                    "order_placement_failed symbol=%s side=%s qty=%.6f entry=%.6f err=%s",
                    symbol,
                    side,
                    qty_l1,
                    entry_price,
                    exc,
                )
                self._ops_call(
                    "record_error",
                    stage="order_placement",
                    err=exc,
                    symbol=symbol,
                    recoverable=True,
                    api_related=True,
                    trace_id=trace_id,
                )
                self.trades_logger.info(
                    "error %s stage=entry side=%s qty=%.6f entry=%.6f trace=%s msg=%s",
                    symbol,
                    side,
                    qty_l1,
                    entry_price,
                    trace_id,
                    exc,
                )
                self._mark_entry_failed(symbol, "order_placement", "exception", trace_id=trace_id)
                return False
        finally:
            self._entry_lock.release()

        if filled_qty <= 0:
            self.trades_logger.info("skip %s reason=entry_not_filled", symbol)
            self._mark_entry_failed(symbol, "order_fill", "entry_not_filled", trace_id=trace_id)
            return False

        self.position_cache.invalidate()
        entry_price = avg_price or entry_price
        risk_distance = abs(entry_price - sl_common)
        if risk_distance <= 0:
            self.trades_logger.info("skip %s reason=post_fill_risk_invalid", symbol)
            self._mark_entry_failed(symbol, "post_fill", "post_fill_risk_invalid", trace_id=trace_id)
            return False

        strategy_risk = float(signal.get("risk_per_unit") or 0.0)
        tp_risk_cap = strategy_risk if strategy_risk > 0 else risk_distance
        tp_risk_basis = min(risk_distance, tp_risk_cap)
        tp_rr_effective = max(float(self.settings.tp_rr), 1.8)
        tp = (
            entry_price + (tp_rr_effective * tp_risk_basis)
            if side == "BUY"
            else entry_price - (tp_rr_effective * tp_risk_basis)
        )
        breakeven_trigger_pct_trade = (
            max((risk_distance * 0.3) / entry_price, 0.004) if entry_price > 0 else 0.004
        )
        filled_qty = executor.round_qty(filled_qty)
        self._ops_call(
            "record_entry_executed",
            symbol=symbol,
            side=side,
            interval=interval,
            qty=filled_qty,
            entry=entry_price,
            margin=margin_to_use,
            exec_type=exec_type,
            trace_id=trace_id,
        )
        self._ops_call("record_success", stage="entry_execution")
        self.trades_logger.info(
            "entry_executed %s tf=%s side=%s qty=%.6f entry=%.6f trace=%s",
            symbol,
            interval,
            side,
            filled_qty,
            entry_price,
            trace_id,
        )

        trade_state = {
            "entry_price": entry_price,
            "qty": filled_qty,
            "sl": sl_common,
            "tp": tp,
            "risk_distance": risk_distance,
            "breakeven_trigger_pct": breakeven_trigger_pct_trade,
            "anchor_entry_price": entry_price,
            "anchor_risk_distance": risk_distance,
            "tp_risk_cap": tp_risk_cap,
            "trace_id": trace_id,
        }
        level_state = {
            "loss_l1_done": False,
            "loss_l2_done": False,
            "loss_l3_done": False,
            "loss_l1_attempts": 0,
            "loss_l2_attempts": 0,
            "loss_l3_attempts": 0,
            "loss_l1_next_try_ts": 0.0,
            "loss_l2_next_try_ts": 0.0,
            "loss_l3_next_try_ts": 0.0,
        }

        def price_fn() -> float | None:
            return safe_mark_price(self.trade_client, symbol, logger=self.logger)

        def atr_fn() -> float | None:
            symbol_df = self.stream.get_dataframe(symbol, interval)
            return atr_last(symbol_df, self.settings.atr_period)

        def on_event(kind: str, new_sl: float) -> None:
            self.trades_logger.info("%s %s new_sl=%.4f", kind, symbol, new_sl)

        monitor = PositionMonitor(
            executor=executor,
            stream=self.stream,
            settings=self.settings,
            risk=self.risk,
            trade_state=trade_state,
            level_state=level_state,
            side=side,
            symbol=symbol,
            interval=interval,
            client_id_prefix=f"{symbol}-{int(time.time() * 1000)}",
            logger=self.logger,
            trades_logger=self.trades_logger,
            price_fn=price_fn,
            atr_fn=atr_fn,
            on_event=on_event,
            pos_cache_invalidate=self.position_cache.invalidate,
            risk_updater=self.risk.update_trade,
            min_qty=min_qty,
            min_notional=min_notional,
            atr_val=atr_value,
            signal=signal,
            sl_swing=sl_swing,
            sl_atr=sl_atr,
            exec_type=exec_type,
            margin_to_use=margin_to_use,
            max_hold_candles=self.settings.max_hold_candles,
            operations=self.operations,
            trace_id=trace_id,
        )
        self._ops_call(
            "record_event",
            kind="monitor_started",
            detail={"symbol": symbol, "interval": interval, "side": side},
            trace_id=trace_id,
        )
        threading.Thread(target=monitor.run, daemon=True).start()
        return True

    def _is_position_gate_blocked(self, symbol: str) -> bool:
        try:
            positions = self.trade_client.futures_position_information()
        except RECOVERABLE_ERRORS as exc:
            self.logger.warning("entry_recheck_failed symbol=%s err=%s", symbol, exc)
            self._ops_call(
                "record_error",
                stage="entry_recheck",
                err=exc,
                symbol=symbol,
                recoverable=True,
                api_related=True,
            )
            return True

        recheck_count, recheck_symbols = count_active_positions(positions)
        if recheck_count >= self.settings.max_positions:
            self.trades_logger.info(
                "signal_only reason=concurrent_limit_recheck active=%d max=%d",
                recheck_count,
                self.settings.max_positions,
            )
            return True
        if symbol in recheck_symbols:
            self.trades_logger.info("signal_only reason=symbol_already_open sym=%s", symbol)
            return True
        return False

    def _place_entry(
        self,
        executor: FuturesExecutor,
        side: str,
        entry_price: float,
        qty: float,
    ) -> tuple[float, float, str]:
        if self.settings.use_limit_only:
            return self._place_limit_only(executor, side, entry_price, qty)
        return executor.place_limit_with_market_fallback(
            side=side,
            price=entry_price,
            qty=qty,
            timeout_sec=self.settings.limit_timeout_sec,
        )

    def _place_limit_only(
        self,
        executor: FuturesExecutor,
        side: str,
        entry_price: float,
        qty: float,
    ) -> tuple[float, float, str]:
        order = executor.place_limit_entry(side=side, price=entry_price, qty=qty)
        if order.order_id is None:
            return qty, entry_price, "MAKER"

        filled = executor.wait_for_fill(order.order_id, timeout_sec=self.settings.limit_timeout_sec)
        if not filled:
            try:
                executor.cancel_order(order.order_id)
            except RECOVERABLE_ERRORS:
                # Keep broad cancellation tolerance: order might already be terminal.
                pass
            return 0.0, 0.0, "LIMIT_TIMEOUT"

        order_payload = self.trade_client.futures_get_order(symbol=executor.symbol, orderId=order.order_id)
        filled_qty = float(order_payload.get("executedQty", qty) or qty or 0.0)
        avg_price = float(order_payload.get("avgPrice") or order_payload.get("price") or entry_price)
        return filled_qty, avg_price, "LIMIT_ONLY"

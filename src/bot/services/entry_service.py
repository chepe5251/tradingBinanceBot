"""Signal gating, entry execution, and monitor spawn flow."""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from bot.config import Settings
from bot.exchange_utils import safe_mark_price
from bot.execution import FuturesExecutor
from bot.indicators import atr_last
from bot.monitor import PositionMonitor
from bot.risk import RiskManager
from bot.services.domain_models import (
    EntryAttempt,
    EntryFillResult,
    EntryValidationResult,
    LevelState,
    MonitorLaunchContext,
    TradePlan,
    TradeState,
)
from bot.services.position_service import (
    PositionCache,
    count_active_positions,
    get_available_balance,
)
from bot.services.signal_service import SignalCandidate, evaluate_interval_signals
from bot.services.telegram_service import TelegramService, format_signal_message
from bot.sizing import (
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
        except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
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

    def _on_close(self, interval: str, _followup_depth: int = 0) -> None:
        interval_state = self._interval_states[interval]
        close_ms = self._resolve_interval_close(interval, interval_state)
        if close_ms is None:
            return

        active_positions, symbols_with_positions = self._load_active_positions(interval)
        if active_positions is None or symbols_with_positions is None:
            return
        has_open_position = active_positions >= self.settings.max_positions
        can_trade_now = self.risk.can_trade(datetime.now(timezone.utc))

        valid_signals = self._evaluate_signals(interval)
        if not valid_signals:
            return

        self._record_detected_signals(valid_signals, interval, close_ms)
        valid_signals = self._filter_signals_by_symbol_limit(
            candidates=valid_signals,
            symbols_with_positions=symbols_with_positions,
            interval=interval,
            active_positions=active_positions,
        )
        if not valid_signals:
            return

        execution_allowed, block_reason, live_count = self._resolve_execution_gate(
            interval=interval,
            can_trade_now=can_trade_now,
            has_open_position=has_open_position,
            active_positions=active_positions,
        )

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

        if not self._execute_candidate(best_candidate, interval):
            return
        self._schedule_followup_if_needed(
            candidates, interval, interval_state, depth=_followup_depth
        )

    def _resolve_interval_close(self, interval: str, interval_state: dict) -> int | None:
        anchor_symbol = self.symbols[0]
        anchor_df = self.stream.get_dataframe(anchor_symbol, interval)
        if anchor_df.empty:
            return None

        close_time = anchor_df.iloc[-1]["close_time"]
        close_ms = int(close_time.timestamp() * 1000)
        with interval_state["lock"]:
            if interval_state["last_close_ms"] == close_ms:
                return None
            interval_state["last_close_ms"] = close_ms
        return close_ms

    def _load_active_positions(self, interval: str) -> tuple[int | None, set[str] | None]:
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
            return None, None
        return count_active_positions(positions_snapshot)

    def _evaluate_signals(self, interval: str) -> list[SignalCandidate]:
        context_interval = self.context_map.get(interval)
        return evaluate_interval_signals(
            stream=self.stream,
            symbols=self.symbols,
            interval=interval,
            context_interval=context_interval,
            settings=self.settings,
            trades_logger=self.trades_logger,
            operations=self.operations,
        )

    def _record_detected_signals(
        self,
        candidates: list[SignalCandidate],
        interval: str,
        close_ms: int,
    ) -> None:
        for candidate in candidates:
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

    def _filter_signals_by_symbol_limit(
        self,
        *,
        candidates: list[SignalCandidate],
        symbols_with_positions: set[str],
        interval: str,
        active_positions: int,
    ) -> list[SignalCandidate]:
        filtered = [candidate for candidate in candidates if candidate.symbol not in symbols_with_positions]
        if filtered:
            return filtered
        self.trades_logger.info(
            "all_signals_skipped tf=%s reason=per_symbol_limit active=%d",
            interval,
            active_positions,
        )
        for candidate in candidates:
            self._ops_call(
                "record_signal_discarded",
                reason="per_symbol_limit",
                symbol=candidate.symbol,
                interval=interval,
                trace_id=str(candidate.payload.get("trace_id") or ""),
            )
        return []

    def _resolve_execution_gate(
        self,
        *,
        interval: str,
        can_trade_now: bool,
        has_open_position: bool,
        active_positions: int,
    ) -> tuple[bool, str, int]:
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
                live_count, _ = count_active_positions(live_positions)
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
        return execution_allowed, block_reason, live_count

    def _schedule_followup_if_needed(
        self,
        candidates: list[SignalCandidate],
        interval: str,
        interval_state: dict,
        depth: int = 0,
    ) -> None:
        if len(candidates) <= 1:
            return
        # Tope de seguridad: como mucho (max_positions - 1) re-evaluaciones
        # encadenadas por cierre de vela. Evita recursion ilimitada de hilos.
        if depth >= max(0, self.settings.max_positions - 1):
            return
        self.position_cache.invalidate()
        with interval_state["lock"]:
            interval_state["last_close_ms"] = None
        threading.Thread(
            target=lambda: self._on_close(interval, _followup_depth=depth + 1),
            daemon=True,
            name=f"on_close_slot{depth + 2}_{interval}",
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

        plan = self._build_trade_plan(candidate, interval, trace_id=trace_id)
        if plan is None:
            return False

        validation = self._validate_trade_plan(plan)
        if not validation.ok:
            self._log_validation_failure(plan, validation)
            self._mark_entry_failed(plan.symbol, validation.stage, validation.reason, trace_id=trace_id)
            return False

        fill = self._submit_entry(plan)
        if not fill.success:
            self._mark_entry_failed(plan.symbol, fill.stage, fill.reason, trace_id=trace_id)
            return False

        context = self._finalize_entry(plan, fill)
        if context is None:
            return False
        self._launch_monitor(context)
        return True

    def _build_trade_plan(
        self,
        candidate: SignalCandidate,
        interval: str,
        trace_id: str,
    ) -> TradePlan | None:
        symbol = candidate.symbol
        signal = candidate.payload
        side = signal["side"]
        entry_price = self._entry_price_with_offset(side, float(signal["price"]))
        signal_risk = float(signal.get("risk_per_unit") or 0.0)

        atr_value = float(signal.get("atr") or 0.0)
        if atr_value <= 0:
            atr_value = atr_last(self.stream.get_dataframe(symbol, interval), self.settings.atr_period)
        if atr_value <= 0:
            self.trades_logger.info("skip %s reason=atr_invalid", symbol)
            self._mark_entry_failed(symbol, "pre_entry", "atr_invalid", trace_id=trace_id)
            return None

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
            return None

        executor: FuturesExecutor = self.get_executor(symbol)
        min_qty = executor.get_min_qty()
        min_notional = executor.get_min_notional()

        entry_df = self.stream.get_dataframe(symbol, interval)
        if entry_df.empty or len(entry_df) < 12:
            self.trades_logger.info("skip %s reason=entry_df_insufficient", symbol)
            self._mark_entry_failed(symbol, "pre_entry", "entry_df_insufficient", trace_id=trace_id)
            return None

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
            return None

        qty_by_margin = executor.calc_qty(margin_to_use, entry_price)
        if qty_by_margin <= 0:
            self.trades_logger.info("skip %s reason=qty_by_margin_invalid", symbol)
            self._mark_entry_failed(symbol, "sizing", "qty_by_margin_invalid", trace_id=trace_id)
            return None
        qty_l1 = executor.round_qty(qty_by_margin)

        return TradePlan(
            symbol=symbol,
            interval=interval,
            side=side,
            trace_id=trace_id,
            signal=signal,
            executor=executor,
            entry_price=entry_price,
            atr_value=atr_value,
            signal_risk=signal_risk,
            signal_rr=signal_rr,
            risk_distance=risk_distance,
            reward_distance=reward_distance,
            sl=sl,
            tp=tp,
            entry_df_len=len(entry_df),
            sl_swing=sl_swing,
            sl_atr=sl_atr,
            sl_common=sl_common,
            available_balance=available_balance,
            min_qty=min_qty,
            min_notional=min_notional,
            margin_to_use=margin_to_use,
            qty_by_margin=qty_by_margin,
            qty_l1=qty_l1,
        )

    def _validate_trade_plan(self, plan: TradePlan) -> EntryValidationResult:
        # Keep liquidation-distance guard consistent with the effective sizing source:
        # - fixed_margin mode: use configured fixed margin
        # - other modes: use the actual computed margin_to_use for this entry
        margin_initial_ref = (
            float(self.settings.fixed_margin_per_trade_usdt)
            if self.settings.sizing_mode == SIZING_MODE_FIXED_MARGIN
            else float(plan.margin_to_use)
        )
        if self.settings.anti_liq_trigger_r > 0 and margin_initial_ref > 0 and plan.qty_l1 > 0:
            min_sl_distance_for_rebuy = (margin_initial_ref * self.settings.anti_liq_trigger_r) / plan.qty_l1
            if plan.side == "BUY":
                sl_required = plan.entry_price - min_sl_distance_for_rebuy
                if plan.sl_common > sl_required:
                    return EntryValidationResult(
                        ok=False,
                        stage="sl_validation",
                        reason="sl_inside_liquidation_zone",
                    )
            else:
                sl_required = plan.entry_price + min_sl_distance_for_rebuy
                if plan.sl_common < sl_required:
                    return EntryValidationResult(
                        ok=False,
                        stage="sl_validation",
                        reason="sl_inside_liquidation_zone",
                    )

        if not is_entry_size_valid(
            plan.qty_l1,
            plan.entry_price,
            plan.min_qty,
            plan.min_notional,
        ):
            return EntryValidationResult(
                ok=False,
                stage="validation",
                reason="entry_notional_invalid",
            )
        return EntryValidationResult(ok=True, stage="ok", reason="")

    def _log_validation_failure(self, plan: TradePlan, result: EntryValidationResult) -> None:
        if result.reason == "sl_inside_liquidation_zone":
            margin_initial_ref = (
                float(self.settings.fixed_margin_per_trade_usdt)
                if self.settings.sizing_mode == SIZING_MODE_FIXED_MARGIN
                else float(plan.margin_to_use)
            )
            min_sl_distance_for_rebuy = (margin_initial_ref * self.settings.anti_liq_trigger_r) / plan.qty_l1 if plan.qty_l1 > 0 else 0.0
            if plan.side == "BUY":
                sl_required = plan.entry_price - min_sl_distance_for_rebuy
            else:
                sl_required = plan.entry_price + min_sl_distance_for_rebuy
            self.trades_logger.info(
                "skip %s reason=sl_inside_liquidation_zone sl_common=%.4f "
                "sl_required=%.4f entry=%.4f",
                plan.symbol,
                plan.sl_common,
                sl_required,
                plan.entry_price,
            )
            return
        if result.reason == "entry_notional_invalid":
            self.trades_logger.info(
                "skip %s reason=entry_notional_invalid side=%s qty=%.6f entry=%.6f "
                "min_qty=%.6f min_notional=%.4f",
                plan.symbol,
                plan.side,
                plan.qty_l1,
                plan.entry_price,
                plan.min_qty,
                plan.min_notional,
            )

    def _submit_entry(self, plan: TradePlan) -> EntryFillResult:
        attempt = EntryAttempt(
            symbol=plan.symbol,
            interval=plan.interval,
            side=plan.side,
            qty=plan.qty_l1,
            entry_price=plan.entry_price,
            margin_to_use=plan.margin_to_use,
            trace_id=plan.trace_id,
        )
        self.trades_logger.info(
            "entry_attempt %s tf=%s side=%s qty=%.6f entry=%.6f margin=%.4f trace=%s",
            attempt.symbol,
            attempt.interval,
            attempt.side,
            attempt.qty,
            attempt.entry_price,
            attempt.margin_to_use,
            attempt.trace_id,
        )
        if not self._entry_lock.acquire(blocking=False):
            self.trades_logger.info(
                "signal_only tf=%s reason=entry_lock_busy sym=%s",
                plan.interval,
                plan.symbol,
            )
            return EntryFillResult(
                success=False,
                filled_qty=0.0,
                avg_price=0.0,
                exec_type="UNKNOWN",
                stage="entry_lock",
                reason="entry_lock_busy",
            )

        filled_qty = 0.0
        avg_price = 0.0
        exec_type = "UNKNOWN"
        try:
            if self._is_position_gate_blocked(plan.symbol):
                return EntryFillResult(
                    success=False,
                    filled_qty=0.0,
                    avg_price=0.0,
                    exec_type="UNKNOWN",
                    stage="position_gate_recheck",
                    reason="blocked",
                )

            try:
                self.trade_client.futures_cancel_all_open_orders(symbol=plan.symbol)
            except RECOVERABLE_ERRORS as exc:
                self.logger.warning("pre_entry_cleanup_failed symbol=%s err=%s", plan.symbol, exc)
                self._ops_call(
                    "record_error",
                    stage="pre_entry_cleanup",
                    err=exc,
                    symbol=plan.symbol,
                    recoverable=True,
                    api_related=True,
                    trace_id=plan.trace_id,
                )

            try:
                self._ops_call(
                    "record_event",
                    kind="order_submit",
                    detail={
                        "symbol": plan.symbol,
                        "side": plan.side,
                        "qty": plan.qty_l1,
                        "entry_price": plan.entry_price,
                    },
                    trace_id=plan.trace_id,
                )
                filled_qty, avg_price, exec_type = self._place_entry(
                    executor=plan.executor,
                    side=plan.side,
                    entry_price=plan.entry_price,
                    qty=plan.qty_l1,
                )
            except RECOVERABLE_ERRORS as exc:
                self.logger.error(
                    "order_placement_failed symbol=%s side=%s qty=%.6f entry=%.6f err=%s",
                    plan.symbol,
                    plan.side,
                    plan.qty_l1,
                    plan.entry_price,
                    exc,
                )
                self._ops_call(
                    "record_error",
                    stage="order_placement",
                    err=exc,
                    symbol=plan.symbol,
                    recoverable=True,
                    api_related=True,
                    trace_id=plan.trace_id,
                )
                self.trades_logger.info(
                    "error %s stage=entry side=%s qty=%.6f entry=%.6f trace=%s msg=%s",
                    plan.symbol,
                    plan.side,
                    plan.qty_l1,
                    plan.entry_price,
                    plan.trace_id,
                    exc,
                )
                return EntryFillResult(
                    success=False,
                    filled_qty=0.0,
                    avg_price=0.0,
                    exec_type="UNKNOWN",
                    stage="order_placement",
                    reason="exception",
                    error_message=str(exc),
                )
        finally:
            self._entry_lock.release()

        if filled_qty <= 0:
            self.trades_logger.info("skip %s reason=entry_not_filled", plan.symbol)
            return EntryFillResult(
                success=False,
                filled_qty=0.0,
                avg_price=0.0,
                exec_type=exec_type,
                stage="order_fill",
                reason="entry_not_filled",
            )
        return EntryFillResult(
            success=True,
            filled_qty=filled_qty,
            avg_price=avg_price,
            exec_type=exec_type,
        )

    def _finalize_entry(self, plan: TradePlan, fill: EntryFillResult) -> MonitorLaunchContext | None:
        self.position_cache.invalidate()
        entry_price = fill.avg_price or plan.entry_price
        risk_distance = abs(entry_price - plan.sl_common)
        if risk_distance <= 0:
            self.trades_logger.info("skip %s reason=post_fill_risk_invalid", plan.symbol)
            self._mark_entry_failed(
                plan.symbol,
                "post_fill",
                "post_fill_risk_invalid",
                trace_id=plan.trace_id,
            )
            return None

        strategy_risk = float(plan.signal.get("risk_per_unit") or 0.0)
        tp_risk_cap = strategy_risk if strategy_risk > 0 else risk_distance
        tp_risk_basis = min(risk_distance, tp_risk_cap)
        # Keep execution RR aligned with the original signal/trade plan RR.
        tp_rr_effective = max(float(plan.signal_rr), 1.8)
        tp = (
            entry_price + (tp_rr_effective * tp_risk_basis)
            if plan.side == "BUY"
            else entry_price - (tp_rr_effective * tp_risk_basis)
        )
        breakeven_trigger_pct_trade = (
            max((risk_distance * 0.3) / entry_price, 0.004) if entry_price > 0 else 0.004
        )
        filled_qty = plan.executor.round_qty(fill.filled_qty)
        self._ops_call(
            "record_entry_executed",
            symbol=plan.symbol,
            side=plan.side,
            interval=plan.interval,
            qty=filled_qty,
            entry=entry_price,
            margin=plan.margin_to_use,
            exec_type=fill.exec_type,
            trace_id=plan.trace_id,
        )
        self._ops_call("record_success", stage="entry_execution")
        self.trades_logger.info(
            "entry_executed %s tf=%s side=%s qty=%.6f entry=%.6f trace=%s",
            plan.symbol,
            plan.interval,
            plan.side,
            filled_qty,
            entry_price,
            plan.trace_id,
        )

        trade_state = TradeState(
            entry_price=entry_price,
            qty=filled_qty,
            sl=plan.sl_common,
            tp=tp,
            risk_distance=risk_distance,
            breakeven_trigger_pct=breakeven_trigger_pct_trade,
            anchor_entry_price=entry_price,
            anchor_risk_distance=risk_distance,
            tp_risk_cap=tp_risk_cap,
            trace_id=plan.trace_id,
        )
        return MonitorLaunchContext(
            symbol=plan.symbol,
            side=plan.side,
            interval=plan.interval,
            trace_id=plan.trace_id,
            plan=plan,
            trade_state=trade_state,
            level_state=LevelState(),
            filled_qty=filled_qty,
            entry_price=entry_price,
            exec_type=fill.exec_type,
        )

    def _launch_monitor(self, context: MonitorLaunchContext) -> None:
        plan = context.plan
        symbol = context.symbol

        def price_fn() -> float | None:
            return safe_mark_price(self.trade_client, symbol, logger=self.logger)

        def atr_fn() -> float | None:
            symbol_df = self.stream.get_dataframe(symbol, context.interval)
            return atr_last(symbol_df, self.settings.atr_period)

        def on_event(kind: str, new_sl: float) -> None:
            self.trades_logger.info("%s %s new_sl=%.4f", kind, symbol, new_sl)

        monitor = PositionMonitor(
            executor=plan.executor,
            stream=self.stream,
            settings=self.settings,
            risk=self.risk,
            trade_state=context.trade_state.to_dict(),
            level_state=context.level_state.to_dict(),
            side=context.side,
            symbol=symbol,
            interval=context.interval,
            client_id_prefix=f"{symbol}-{int(time.time() * 1000)}",
            logger=self.logger,
            trades_logger=self.trades_logger,
            price_fn=price_fn,
            atr_fn=atr_fn,
            on_event=on_event,
            pos_cache_invalidate=self.position_cache.invalidate,
            risk_updater=self.risk.update_trade,
            min_qty=plan.min_qty,
            min_notional=plan.min_notional,
            atr_val=plan.atr_value,
            signal=plan.signal,
            sl_swing=plan.sl_swing,
            sl_atr=plan.sl_atr,
            exec_type=context.exec_type,
            margin_to_use=plan.margin_to_use,
            max_hold_candles=self.settings.max_hold_candles,
            operations=self.operations,
            trace_id=context.trace_id,
        )
        self._ops_call(
            "record_event",
            kind="monitor_started",
            detail={"symbol": symbol, "interval": context.interval, "side": context.side},
            trace_id=context.trace_id,
        )
        threading.Thread(target=monitor.run, daemon=True, name=f"monitor_{symbol}_{context.interval}").start()

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



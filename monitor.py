"""Post-entry position monitoring and orphan recovery.

`PositionMonitor` encapsulates the full trade-supervision lifecycle:
  - TP/SL order placement with retry and emergency escalation
  - OCO monitoring loop (breakeven, trailing, early-exit review)
  - Loss-based scaling stages (currently disabled via scale_fn=None)
  - PnL accounting and risk-manager update on exit

For orphaned positions discovered at startup, use the static
`resume_orphan()` method, which reconstructs state from open orders
and starts a daemon monitor thread.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable, Optional

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException

from execution import FuturesExecutor, STOP_ORDER_TYPES, TP_ORDER_TYPES
from indicators import atr_last, safe_mark_price
from monitor_logic import evaluate_early_exit
from risk import RiskManager

MONITOR_ERRORS = (
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
    OSError,
    ValueError,
    TypeError,
)

if TYPE_CHECKING:
    from config import Settings
    from data_stream import MarketDataStream



class PositionMonitor:
    """Supervises an open position from TP/SL placement through to exit.

    Instantiate after a trade fills and call `run()` in a daemon thread.
    For positions opened before this process started, use `resume_orphan()`.
    """

    def __init__(
        self,
        executor: FuturesExecutor,
        stream: "MarketDataStream",
        settings: "Settings",
        risk: RiskManager,
        trade_state: dict,
        level_state: dict,
        side: str,
        symbol: str,
        interval: str,
        client_id_prefix: str,
        logger: logging.Logger,
        trades_logger: logging.Logger,
        price_fn: Callable[[], Optional[float]],
        atr_fn: Callable[[], Optional[float]],
        on_event: Callable[[str, float], None],
        pos_cache_invalidate: Callable[[], None],
        risk_updater: Callable[[float, datetime], None],
        min_qty: float = 0.0,
        min_notional: float = 0.0,
        atr_val: float = 0.0,
        signal: Optional[dict] = None,
        sl_swing: float = 0.0,
        sl_atr: float = 0.0,
        exec_type: str = "MARKET",
        margin_to_use: float = 0.0,
    ) -> None:
        self.executor = executor
        self.stream = stream
        self.settings = settings
        self.risk = risk
        self.trade_state = trade_state
        self.level_state = level_state
        self.side = side
        self.symbol = symbol
        self.interval = interval
        self.client_id_prefix = client_id_prefix
        self.logger = logger
        self.trades_logger = trades_logger
        self.price_fn = price_fn
        self.atr_fn = atr_fn
        self.on_event = on_event
        self.pos_cache_invalidate = pos_cache_invalidate
        self.risk_updater = risk_updater
        self.min_qty = min_qty
        self.min_notional = min_notional
        self.atr_val = atr_val
        self.signal = signal or {}
        self.sl_swing = sl_swing
        self.sl_atr = sl_atr
        self.exec_type = exec_type
        self.margin_to_use = margin_to_use

    # ── Internal callbacks ───────────────────────────────────────────────────

    def _review_fn(self, break_even: bool) -> tuple[bool, str]:
        """Run structure/volume/context reviews for potential early exits."""
        settings = self.settings
        df = self.stream.get_dataframe(self.symbol, self.interval)
        should_exit, reason, metrics = evaluate_early_exit(
            df=df,
            side=self.side,
            ema_fast_period=settings.ema_fast,
            ema_mid_period=settings.ema_mid,
            ema_trend_period=settings.ema_trend,
            volume_avg_window=settings.volume_avg_window,
            trend_slope_min=settings.trend_slope_min,
            break_even=break_even,
            context_df=df,
        )

        tp_ok, sl_ok = self.executor.protection_status(
            self.side, client_id_prefix=self.client_id_prefix
        )
        self.trades_logger.info(
            "monitor %s tp_ok=%s sl_ok=%s ctx_dir=%s ctx_slope=%.6f struct_break=%s vol_strong=%s",
            self.symbol,
            tp_ok,
            sl_ok,
            metrics.get("ctx_dir") or "NONE",
            float(metrics.get("ctx_slope") or 0.0),
            bool(metrics.get("struct_break")),
            bool(metrics.get("vol_strong")),
        )
        return should_exit, reason

    def _scale_fn(self, state: dict) -> dict | None:
        """Evaluate and execute loss-based DCA scaling stages for a losing position.

        What it does:
            Adds margin to the position in 3 progressive levels when floating
            loss exceeds 50%, 100%, and 200% of the initial margin respectively.
            Level 3 can add up to 4× the initial margin to an already-losing trade.

        Why it is disabled:
            This is an unvalidated martingale-style strategy.  In live conditions
            it amplifies losses on directional moves against the position.
            Do not enable until a full backtest with realistic slippage confirms
            positive expectancy.

        How to activate:
            Set ENABLE_LOSS_SCALING=true in .env **only** after exhaustive backtest.
            The field ``enable_loss_scaling`` in Settings controls the guard below.

        Maximum risk:
            Level 3 adds 4× the initial margin to a position already down 200%.
            Total risk exposure per trade can be 8× the entry margin.
        """
        if not getattr(self.settings, "enable_loss_scaling", False):
            return None

        now_ts = time.time()
        level_state = self.level_state
        trade_state = self.trade_state
        settings = self.settings

        if (
            level_state["loss_l1_done"]
            and level_state["loss_l2_done"]
            and level_state["loss_l3_done"]
        ):
            return None

        df_scale = self.stream.get_dataframe(self.symbol, self.interval)
        if df_scale.empty or len(df_scale) < max(settings.ema_mid + 2, 10):
            return None
        mark = self.price_fn()
        if mark is None:
            return None
        mark = float(mark)
        sl_ref_price = float(trade_state["sl"])

        def _defer_level(level_key: str, reason: str, exc: Exception | None = None) -> None:
            """Back off failed scale attempts and disable level after max retries."""
            attempts_key = f"{level_key}_attempts"
            next_try_key = f"{level_key}_next_try_ts"
            _attempts = int(level_state.get(attempts_key, 0)) + 1
            level_state[attempts_key] = _attempts
            if _attempts >= 5:
                level_state[f"{level_key}_done"] = True
                self.trades_logger.info(
                    "skip %s reason=loss_scale_disabled level=%s attempts=%d last_reason=%s",
                    self.symbol, level_key, _attempts, reason,
                )
                return
            delay = min(60, 2 ** _attempts)
            level_state[next_try_key] = now_ts + delay
            if exc is not None:
                self.trades_logger.info(
                    "retry %s level=%s in=%ss reason=%s msg=%s",
                    self.symbol, level_key, delay, reason, exc,
                )
            else:
                self.trades_logger.info(
                    "retry %s level=%s in=%ss reason=%s",
                    self.symbol, level_key, delay, reason,
                )

        if self.side == "BUY":
            if mark <= sl_ref_price:
                return {"close_all": True, "reason": "scale_structure_break", "exit_price": mark}
            structure_ok = float(df_scale["close"].iloc[-1]) > sl_ref_price
        else:
            if mark >= sl_ref_price:
                return {"close_all": True, "reason": "scale_structure_break", "exit_price": mark}
            structure_ok = float(df_scale["close"].iloc[-1]) < sl_ref_price

        if not structure_ok:
            return {"close_all": True, "reason": "scale_structure_break", "exit_price": mark}

        current_entry = float(state["entry_price"])
        current_qty = float(state["qty"])
        floating_pnl = (mark - current_entry) * current_qty
        if self.side == "SELL":
            floating_pnl = -floating_pnl
        floating_loss = abs(min(floating_pnl, 0.0))
        margin_initial = float(settings.fixed_margin_per_trade_usdt)
        if margin_initial <= 0:
            return None

        level_key = ""
        trigger_label = ""
        add_margin = 0.0
        if (
            not level_state["loss_l1_done"]
            and now_ts >= float(level_state.get("loss_l1_next_try_ts", 0.0))
            and floating_loss >= (0.5 * margin_initial)
        ):
            level_key = "loss_l1"
            trigger_label = "50%"
            add_margin = margin_initial
        elif (
            level_state["loss_l1_done"]
            and not level_state["loss_l2_done"]
            and now_ts >= float(level_state.get("loss_l2_next_try_ts", 0.0))
            and floating_loss >= (1.0 * margin_initial)
        ):
            level_key = "loss_l2"
            trigger_label = "100%"
            add_margin = margin_initial * 2.0
        elif (
            level_state["loss_l2_done"]
            and not level_state["loss_l3_done"]
            and now_ts >= float(level_state.get("loss_l3_next_try_ts", 0.0))
            and floating_loss >= (2.0 * margin_initial)
        ):
            level_key = "loss_l3"
            trigger_label = "200%"
            add_margin = margin_initial * 4.0
        else:
            return None

        if add_margin <= 0:
            level_state[f"{level_key}_done"] = True
            return None

        add_qty_plan = self.executor.calc_qty(add_margin, mark)
        add_qty = self.executor.round_qty(add_qty_plan)
        add_notional = add_qty * mark
        if (
            add_qty <= 0
            or (self.min_qty > 0 and add_qty < self.min_qty)
            or (self.min_notional > 0 and add_notional < self.min_notional)
        ):
            level_state[f"{level_key}_done"] = True
            self.trades_logger.info(
                "skip %s reason=loss_scale_qty_invalid level=%s margin=%.4f qty=%.6f notional=%.4f",
                self.symbol, level_key, add_margin, add_qty, add_notional,
            )
            return None

        try:
            if self.executor.paper:
                add_filled, add_avg = add_qty, mark
            else:
                add_filled, add_avg = self.executor.place_market_entry(self.side, add_qty)
        except MONITOR_ERRORS as exc:
            _defer_level(level_key, "loss_scale_market_error", exc)
            return None
        if add_filled <= 0:
            _defer_level(level_key, "loss_scale_market_no_fill")
            return None
        add_avg = float(add_avg) if add_avg and add_avg > 0 else mark
        self.trades_logger.info(
            "loss_scale %s level=%s trigger=%s floating_loss=%.4f add_margin=%.2f add_qty=%.6f mark=%.6f",
            self.symbol, level_key, trigger_label, floating_loss, add_margin, add_filled, mark,
        )

        prev_qty = float(state["qty"])
        prev_entry = float(state["entry_price"])
        new_qty = self.executor.round_qty(prev_qty + add_filled)
        if new_qty <= 0:
            return None
        new_entry = ((prev_entry * prev_qty) + (float(add_avg) * float(add_filled))) / new_qty
        new_risk = abs(new_entry - sl_ref_price)
        if new_risk <= 0:
            return None
        tp_rr_eff = max(float(settings.tp_rr), 1.8)
        new_tp = (
            new_entry + (tp_rr_eff * new_risk)
            if self.side == "BUY"
            else new_entry - (tp_rr_eff * new_risk)
        )

        new_tp_ref = None
        new_sl_ref = None
        replace_exc = None
        for replace_attempt in range(1, 4):
            try:
                new_tp_ref, new_sl_ref = self.executor.replace_tp_sl(
                    self.side,
                    new_tp,
                    sl_ref_price,
                    new_qty,
                    client_id_prefix=self.client_id_prefix,
                )
                break
            except MONITOR_ERRORS as exc:
                replace_exc = exc
                time.sleep(min(replace_attempt, 2))
        if not new_tp_ref or not new_sl_ref:
            self.trades_logger.info(
                "critical %s reason=loss_scale_protection_fail level=%s msg=%s",
                self.symbol, level_key, replace_exc,
            )
            return {"close_all": True, "reason": "loss_scale_protection_fail", "exit_price": mark}

        level_state[f"{level_key}_done"] = True
        level_state[f"{level_key}_next_try_ts"] = 0.0

        trade_state["entry_price"] = new_entry
        trade_state["qty"] = new_qty
        trade_state["tp"] = new_tp
        trade_state["risk_distance"] = new_risk

        return {
            "entry_price": new_entry,
            "qty": new_qty,
            "tp_price": new_tp,
            "sl_price": sl_ref_price,
            "breakeven_trigger_pct": float(
                state.get("breakeven_trigger_pct", trade_state.get("breakeven_trigger_pct", 0.005))
            ),
            "tp_ref": new_tp_ref,
            "sl_ref": new_sl_ref,
        }

    # ── Public interface ─────────────────────────────────────────────────────

    def run(self) -> None:
        """Place TP/SL protections and supervise the trade to completion.

        Designed to run in a daemon thread.  Exits when the OCO monitor
        returns a definitive result (TP / SL / EARLY / TIMEOUT / ERROR).
        """
        trade_state = self.trade_state
        attempts = 0
        emergency = False
        tp_ref = sl_ref = None
        position_wait_deadline = time.time() + 8.0

        # ── wait for position to appear, then place protections ───────────
        while True:
            if not self.executor.paper:
                try:
                    has_pos = self.executor.has_open_position()
                except MONITOR_ERRORS:
                    has_pos = False
                if not has_pos:
                    if time.time() < position_wait_deadline:
                        time.sleep(0.25)
                        continue
                    self.trades_logger.info(
                        "critical %s reason=position_closed_no_protection", self.symbol
                    )
                    return
            try:
                existing_tp, existing_sl = self.executor.get_protection_refs(
                    self.side, client_id_prefix=self.client_id_prefix
                )
                if existing_tp and existing_sl:
                    tp_ref, sl_ref = existing_tp, existing_sl
                    break
                tp_ref, sl_ref = self.executor.place_tp_sl(
                    self.side,
                    float(trade_state["tp"]),
                    float(trade_state["sl"]),
                    float(trade_state["qty"]),
                    client_id_prefix=self.client_id_prefix,
                )
                if tp_ref and sl_ref:
                    break
            except MONITOR_ERRORS as exc:
                self.logger.error("TP/SL placement failed %s: %s", self.symbol, exc)
                self.trades_logger.info("error %s stage=tp_sl msg=%s", self.symbol, exc)

            attempts += 1
            if attempts >= 10 and not emergency:
                emergency = True
                self.trades_logger.info("critical %s reason=tp_sl_emergency", self.symbol)
            time.sleep(2 if emergency else 1)

        # ── log confirmed entry ───────────────────────────────────────────
        risk_dist = float(trade_state["risk_distance"])
        self.trades_logger.info(
            "ENTRY %s tf=%s side=%s entry=%.4f sl_strategy=%.4f sl_swing=%.4f "
            "sl_atr=%.4f sl_final=%.4f tp=%.4f risk=%.4f rr_real=%.3f "
            "atr=%.4f score=%.2f exec=%s qty=%.6f margin=%.2f",
            self.symbol, self.interval, self.side,
            float(trade_state["entry_price"]),
            float(self.signal.get("stop_price", 0)),
            self.sl_swing, self.sl_atr,
            float(trade_state["sl"]),
            float(trade_state["tp"]),
            risk_dist,
            abs(float(trade_state["tp"]) - float(trade_state["entry_price"])) / risk_dist
            if risk_dist > 0 else 0.0,
            self.atr_val,
            float(self.signal.get("score", 0)),
            self.exec_type,
            float(trade_state["qty"]),
            self.margin_to_use,
        )

        # ── OCO monitor loop (breakeven → trailing → review → exit) ──────
        result, exit_price = self.executor.monitor_oco(
            tp_ref,
            sl_ref,
            side=self.side,
            entry_price=float(trade_state["entry_price"]),
            tp_price=float(trade_state["tp"]),
            sl_price=float(trade_state["sl"]),
            qty=float(trade_state["qty"]),
            atr=self.atr_val,
            breakeven_trigger_pct=float(trade_state["breakeven_trigger_pct"]),
            trail_mult=0.8,
            trail_activation_pct=max(self.settings.trailing_activation_pct, 0.01),
            price_fn=self.price_fn,
            atr_fn=self.atr_fn,
            on_event=self.on_event,
            scale_fn=None,  # _scale_fn desactivado — ver enable_loss_scaling en Settings
            safety_check_sec=2,
            review_fn=self._review_fn,
            review_sec=7,
            client_id_prefix=self.client_id_prefix,
        )

        if result in {"TP", "SL"} or result.startswith("EARLY"):
            final_entry = float(trade_state["entry_price"])
            final_qty = float(trade_state["qty"])
            pnl = (exit_price - final_entry) * final_qty
            if self.side == "SELL":
                pnl = -pnl
            self.risk_updater(pnl, datetime.now(timezone.utc))
            self.pos_cache_invalidate()
            self.trades_logger.info("exit %s result=%s pnl=%.4f", self.symbol, result, pnl)
            return
        self.trades_logger.info("exit %s result=%s pnl=0.0", self.symbol, result)

    @staticmethod
    def resume_orphan(
        orphan: dict,
        symbols: list[str],
        stream: "MarketDataStream",
        settings: "Settings",
        get_executor: Callable[[str], FuturesExecutor],
        risk: RiskManager,
        trade_client: Client,
        pos_cache_invalidate: Callable[[], None],
        risk_updater: Callable[[float, datetime], None],
        logger: logging.Logger,
        trades_logger: logging.Logger,
    ) -> None:
        """Recover a single orphaned position and start its daemon monitor thread.

        Validates the symbol, reconstructs SL/TP from existing open orders
        (falling back to ATR estimates), then starts `_orphan_monitor` as a
        daemon thread so the position is protected for the rest of its life.
        """
        symbol = orphan.get("symbol", "")
        if not symbol or symbol not in set(symbols):
            logger.warning("Orphan recovery: symbol %s not in universe, skipping.", symbol)
            return

        try:
            qty = abs(float(orphan.get("positionAmt", 0)))
            entry_price = float(orphan.get("entryPrice", 0))
            side = "BUY" if float(orphan.get("positionAmt", 0)) > 0 else "SELL"
        except (TypeError, ValueError) as exc:
            logger.warning("Orphan recovery: could not parse position data: %s", exc)
            return

        if qty <= 0 or entry_price <= 0:
            logger.warning(
                "Orphan recovery: invalid qty=%.4f entry=%.4f, skipping.", qty, entry_price,
            )
            return

        logger.info(
            "Orphan recovery: resuming %s side=%s qty=%.4f entry=%.4f",
            symbol, side, qty, entry_price,
        )

        # Prefer existing TP/SL orders; fall back to ATR-based estimates.
        sl_price: float | None = None
        tp_price: float | None = None
        try:
            open_orders = trade_client.futures_get_open_orders(symbol=symbol)
            for o in open_orders:
                ot = o.get("type", "")
                sp = float(o.get("stopPrice", 0) or 0)
                if sp <= 0:
                    continue
                # Binance may return either variant depending on endpoint and order version
                if ot in STOP_ORDER_TYPES and sl_price is None:
                    sl_price = sp
                elif ot in TP_ORDER_TYPES and tp_price is None:
                    tp_price = sp
        except MONITOR_ERRORS as exc:
            logger.warning("Orphan recovery: could not fetch open orders: %s", exc)

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
            _df = stream.get_dataframe(symbol, settings.main_interval)
            return atr_last(_df, settings.atr_period)

        def _on_event(kind: str, new_sl: float) -> None:
            trades_logger.info("%s %s new_sl=%.4f (orphan)", kind, symbol, new_sl)

        def _orphan_monitor() -> None:
            attempts = 0
            tp_ref = sl_ref = None
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
                    return

                try:
                    existing_tp, existing_sl = executor.get_protection_refs(
                        side, client_id_prefix=client_id_prefix
                    )
                    if existing_tp and existing_sl:
                        tp_ref, sl_ref = existing_tp, existing_sl
                        break
                    tp_ref, sl_ref = executor.place_tp_sl(
                        side,
                        float(orphan_trade_state["tp"]),
                        float(orphan_trade_state["sl"]),
                        float(orphan_trade_state["qty"]),
                        client_id_prefix=client_id_prefix,
                    )
                    if tp_ref and sl_ref:
                        break
                except MONITOR_ERRORS as exc:
                    logger.error("Orphan TP/SL placement failed %s: %s", symbol, exc)

                attempts += 1
                time.sleep(2)
                if attempts >= 15:
                    trades_logger.info("critical %s reason=orphan_tp_sl_fail", symbol)
                    return

            trades_logger.info(
                "orphan_resumed %s side=%s price=%.4f qty=%.6f tp=%.4f sl=%.4f",
                symbol, side, entry_price, qty_rounded,
                float(orphan_trade_state["tp"]), float(orphan_trade_state["sl"]),
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
                trail_mult=0.8,
                trail_activation_pct=max(settings.trailing_activation_pct, 0.01),
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
            trades_logger.info("orphan_exit %s result=%s pnl=%.4f", symbol, result, pnl)

        threading.Thread(
            target=_orphan_monitor, daemon=True, name=f"orphan-{symbol}"
        ).start()

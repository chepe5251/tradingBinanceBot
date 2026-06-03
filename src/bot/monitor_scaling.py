"""Loss-scaling logic extracted from monitor runtime."""
from __future__ import annotations

import time
from typing import Any

from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException

MONITOR_ERRORS = (
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
    OSError,
    ValueError,
    TypeError,
)


def evaluate_loss_scaling(monitor: Any, state: dict) -> dict | None:
    """Evaluate and execute configured loss-scaling stages for a position."""
    if not getattr(monitor.settings, "enable_loss_scaling", False):
        return None

    now_ts = time.time()
    level_state = monitor.level_state
    trade_state = monitor.trade_state
    settings = monitor.settings

    if (
        level_state["loss_l1_done"]
        and level_state["loss_l2_done"]
        and level_state["loss_l3_done"]
    ):
        return None

    df_scale = monitor.stream.get_dataframe(monitor.symbol, monitor.interval)
    if df_scale.empty or len(df_scale) < max(settings.ema_mid + 2, 10):
        return None
    mark = monitor.price_fn()
    if mark is None:
        return None
    mark = float(mark)
    sl_ref_price = float(trade_state["sl"])

    def _defer_level(level_key: str, reason: str, exc: Exception | None = None) -> None:
        """Back off failed scale attempts and disable level after max retries."""
        attempts_key = f"{level_key}_attempts"
        next_try_key = f"{level_key}_next_try_ts"
        attempts = int(level_state.get(attempts_key, 0)) + 1
        level_state[attempts_key] = attempts
        if attempts >= 5:
            level_state[f"{level_key}_done"] = True
            monitor.trades_logger.info(
                "skip %s reason=loss_scale_disabled level=%s attempts=%d last_reason=%s",
                monitor.symbol,
                level_key,
                attempts,
                reason,
            )
            return
        delay = min(60, 2**attempts)
        level_state[next_try_key] = now_ts + delay
        if exc is not None:
            monitor.trades_logger.info(
                "retry %s level=%s in=%ss reason=%s msg=%s",
                monitor.symbol,
                level_key,
                delay,
                reason,
                exc,
            )
        else:
            monitor.trades_logger.info(
                "retry %s level=%s in=%ss reason=%s",
                monitor.symbol,
                level_key,
                delay,
                reason,
            )

    if monitor.side == "BUY":
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
    if monitor.side == "SELL":
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

    add_qty_plan = monitor.executor.calc_qty(add_margin, mark)
    add_qty = monitor.executor.round_qty(add_qty_plan)
    add_notional = add_qty * mark
    if (
        add_qty <= 0
        or (monitor.min_qty > 0 and add_qty < monitor.min_qty)
        or (monitor.min_notional > 0 and add_notional < monitor.min_notional)
    ):
        level_state[f"{level_key}_done"] = True
        monitor.trades_logger.info(
            "skip %s reason=loss_scale_qty_invalid level=%s margin=%.4f qty=%.6f notional=%.4f",
            monitor.symbol,
            level_key,
            add_margin,
            add_qty,
            add_notional,
        )
        return None

    try:
        if monitor.executor.paper:
            add_filled, add_avg = add_qty, mark
        else:
            add_filled, add_avg = monitor.executor.place_market_entry(monitor.side, add_qty)
    except MONITOR_ERRORS as exc:
        _defer_level(level_key, "loss_scale_market_error", exc)
        return None
    if add_filled <= 0:
        _defer_level(level_key, "loss_scale_market_no_fill")
        return None
    add_avg = float(add_avg) if add_avg and add_avg > 0 else mark
    monitor.trades_logger.info(
        "loss_scale %s level=%s trigger=%s floating_loss=%.4f add_margin=%.2f add_qty=%.6f mark=%.6f",
        monitor.symbol,
        level_key,
        trigger_label,
        floating_loss,
        add_margin,
        add_filled,
        mark,
    )

    prev_qty = float(state["qty"])
    prev_entry = float(state["entry_price"])
    new_qty = monitor.executor.round_qty(prev_qty + add_filled)
    if new_qty <= 0:
        return None
    new_entry = ((prev_entry * prev_qty) + (float(add_avg) * float(add_filled))) / new_qty
    new_risk = abs(new_entry - sl_ref_price)
    if new_risk <= 0:
        return None
    tp_rr_eff = max(float(settings.tp_rr), 1.8)
    new_tp = new_entry + (tp_rr_eff * new_risk) if monitor.side == "BUY" else new_entry - (tp_rr_eff * new_risk)

    new_tp_ref = None
    new_sl_ref = None
    replace_exc = None
    for replace_attempt in range(1, 4):
        try:
            new_tp_ref, new_sl_ref = monitor.executor.replace_tp_sl(
                monitor.side,
                new_tp,
                sl_ref_price,
                new_qty,
                client_id_prefix=monitor.client_id_prefix,
            )
            break
        except MONITOR_ERRORS as exc:
            replace_exc = exc
            time.sleep(min(replace_attempt, 2))
    if not new_tp_ref or not new_sl_ref:
        monitor.trades_logger.info(
            "critical %s reason=loss_scale_protection_fail level=%s msg=%s",
            monitor.symbol,
            level_key,
            replace_exc,
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


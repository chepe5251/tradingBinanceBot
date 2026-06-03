"""Decision helpers for monitor runtime."""
from __future__ import annotations

from typing import Any

from bot.monitor.logic import evaluate_early_exit


def evaluate_monitor_review(monitor: Any, break_even: bool) -> tuple[bool, str]:
    """Evaluate early-exit conditions for an active monitored position."""
    settings = monitor.settings
    df = monitor.stream.get_dataframe(monitor.symbol, monitor.interval)
    should_exit, reason, metrics = evaluate_early_exit(
        df=df,
        side=monitor.side,
        ema_fast_period=settings.ema_fast,
        ema_mid_period=settings.ema_mid,
        ema_trend_period=settings.ema_trend,
        volume_avg_window=settings.volume_avg_window,
        trend_slope_min=settings.trend_slope_min,
        break_even=break_even,
        context_df=df,
    )

    tp_ok, sl_ok = monitor.executor.protection_status(
        monitor.side, client_id_prefix=monitor.client_id_prefix
    )
    monitor.trades_logger.info(
        "monitor %s tp_ok=%s sl_ok=%s ctx_dir=%s ctx_slope=%.6f struct_break=%s vol_strong=%s",
        monitor.symbol,
        tp_ok,
        sl_ok,
        metrics.get("ctx_dir") or "NONE",
        float(metrics.get("ctx_slope") or 0.0),
        bool(metrics.get("struct_break")),
        bool(metrics.get("vol_strong")),
    )
    return should_exit, reason


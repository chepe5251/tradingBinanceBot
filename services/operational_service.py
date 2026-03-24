"""Operational metrics, suspension rules, and status reporting.

This module is intentionally strategy-agnostic:
- It does not alter leverage, sizing, TP/SL, or signal logic by itself.
- It provides observability and optional safety gates for new entries.
- Safety gates are disabled by default via `enable_operational_kill_switches=false`.
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import Counter, deque
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import logging

    from config import Settings
    from risk import RiskState
    from services.telegram_service import TelegramService


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def render_operational_markdown(snapshot: dict[str, Any]) -> str:
    """Render a compact human-readable operational summary."""
    runtime = snapshot.get("runtime", {})
    metrics = snapshot.get("metrics", {})
    counters = snapshot.get("counters", {})
    scheduler = snapshot.get("scheduler", {})
    suspensions = snapshot.get("suspensions", {})
    last_signal = snapshot.get("last_signal") or {}
    last_entry = snapshot.get("last_entry") or {}
    last_exit = snapshot.get("last_exit") or {}
    risk = snapshot.get("risk") or {}
    errors = snapshot.get("errors") or {}

    lines = [
        "# Operational Status",
        "",
        f"- generated_at_utc: `{snapshot.get('generated_at_utc', '')}`",
        f"- mode: `{runtime.get('mode', 'UNKNOWN')}`",
        f"- uptime_sec: `{int(runtime.get('uptime_sec', 0))}`",
        f"- health: `{runtime.get('health', 'unknown')}`",
        f"- paused_new_entries: `{bool(suspensions.get('active'))}`",
        "",
        "## Scheduler",
        "",
        f"- scheduler_alive: `{bool(scheduler.get('alive'))}`",
        f"- polls: `{int(counters.get('polls', 0))}`",
        f"- last_poll_age_sec: `{round(float(scheduler.get('last_poll_age_sec', 0.0)), 1)}`",
        f"- last_poll_duration_sec: `{round(float(scheduler.get('last_poll_duration_sec', 0.0)), 3)}`",
        f"- next_close_in_sec: `{round(float(scheduler.get('next_close_in_sec', 0.0)), 1)}`",
        "",
        "## Core Metrics",
        "",
        f"- signals_detected: `{int(metrics.get('signals_detected', counters.get('signals_detected', 0)))}`",
        f"- signals_alerted: `{int(metrics.get('signals_alerted', counters.get('signals_alerted', 0)))}`",
        f"- signals_rejected: `{int(metrics.get('signals_rejected', counters.get('signals_rejected', 0)))}`",
        f"- entries_attempted: `{int(metrics.get('entries_attempted', counters.get('entries_attempted', 0)))}`",
        f"- entries_executed: `{int(metrics.get('entries_executed', counters.get('entries_executed', 0)))}`",
        f"- entries_failed: `{int(metrics.get('entries_failed', counters.get('entries_failed', 0)))}`",
        f"- protection_failures: `{int(metrics.get('protection_failures', counters.get('protection_failures', 0)))}`",
        f"- order_failures: `{int(metrics.get('order_failures', counters.get('order_failures', 0)))}`",
        f"- orphan_recoveries: `{int(metrics.get('orphan_recoveries', counters.get('orphan_recoveries', 0)))}`",
        f"- heartbeat_count: `{int(metrics.get('heartbeat_count', counters.get('heartbeat_count', 0)))}`",
        f"- polling_cycles: `{int(metrics.get('polling_cycles', counters.get('polling_cycles', 0)))}`",
        f"- open_positions: `{int(counters.get('open_positions', 0))}`",
        f"- last_signal_time: `{metrics.get('last_signal_time')}`",
        f"- last_entry_time: `{metrics.get('last_entry_time')}`",
        f"- last_exit_time: `{metrics.get('last_exit_time')}`",
        "",
        "## PnL",
        "",
        f"- paper_realized_pnl: `{round(float(counters.get('paper_realized_pnl', 0.0)), 4)}`",
        f"- live_realized_pnl: `{round(float(counters.get('live_realized_pnl', 0.0)), 4)}`",
        "",
        "## Risk Snapshot",
        "",
        f"- equity: `{round(float(risk.get('equity', 0.0)), 4)}`",
        f"- day_start_equity: `{round(float(risk.get('day_start_equity', 0.0)), 4)}`",
        f"- consecutive_losses: `{int(risk.get('consecutive_losses', 0))}`",
        f"- risk_paused: `{bool(risk.get('paused'))}`",
        "",
        "## Last Events",
        "",
        f"- last_signal: `{last_signal}`",
        f"- last_entry: `{last_entry}`",
        f"- last_exit: `{last_exit}`",
        "",
        "## Suspensions",
        "",
        f"- active: `{bool(suspensions.get('active'))}`",
        f"- reasons: `{suspensions.get('reasons', [])}`",
        "",
        "## Error Counters",
        "",
        f"- by_type: `{errors.get('by_type', {})}`",
        f"- by_stage: `{errors.get('by_stage', {})}`",
    ]
    return "\n".join(lines) + "\n"


class OperationalService:
    """Collects operational telemetry and optional entry suspension state."""

    def __init__(self, settings: "Settings", logger: "logging.Logger") -> None:
        self.settings = settings
        self.logger = logger
        self._lock = threading.RLock()

        self._started_ts = time.time()
        self._runtime_mode = "UNKNOWN"
        self._telegram: TelegramService | None = None

        self._counters: Counter[str] = Counter()
        self._error_by_type: Counter[str] = Counter()
        self._error_by_stage: Counter[str] = Counter()

        max_recent = max(5, int(getattr(settings, "ops_recent_events", 25)))
        self._recent_errors: deque[dict[str, Any]] = deque(maxlen=max_recent)
        self._recent_signals: deque[dict[str, Any]] = deque(maxlen=max_recent)
        self._recent_entries: deque[dict[str, Any]] = deque(maxlen=max_recent)
        self._recent_exits: deque[dict[str, Any]] = deque(maxlen=max_recent)
        self._recent_events: deque[dict[str, Any]] = deque(maxlen=max_recent)
        self._paper_equity_events: deque[dict[str, Any]] = deque(maxlen=5_000)

        self._last_signal: dict[str, Any] | None = None
        self._last_entry: dict[str, Any] | None = None
        self._last_exit: dict[str, Any] | None = None

        self._last_stream_status: dict[str, Any] = {}
        self._last_risk_snapshot: dict[str, Any] = {}
        self._last_poll_ts: float | None = None
        self._last_poll_duration_sec: float = 0.0
        self._last_seen_event_count = 0

        self._consecutive_errors = 0
        self._consecutive_api_errors = 0
        self._consecutive_order_failures = 0
        self._consecutive_protection_failures = 0

        self._suspensions: dict[str, float | None] = {}
        self._last_alert_ts: dict[str, float] = {}
        self._last_report_ts = 0.0
        self._last_hour_bucket: str | None = None
        self._last_daily_date: str | None = None

    def bind_telegram(self, telegram: "TelegramService") -> None:
        with self._lock:
            self._telegram = telegram

    def set_runtime_mode(self, mode: str) -> None:
        with self._lock:
            self._runtime_mode = mode

    def load_state(self, path: str) -> None:
        """Load counters from disk to preserve long-running paper diagnostics."""
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError, TypeError):
            return

        with self._lock:
            for key, value in (data.get("counters") or {}).items():
                try:
                    self._counters[str(key)] = int(value)
                except (TypeError, ValueError):
                    continue
            for key, value in (data.get("error_by_type") or {}).items():
                try:
                    self._error_by_type[str(key)] = int(value)
                except (TypeError, ValueError):
                    continue
            for key, value in (data.get("error_by_stage") or {}).items():
                try:
                    self._error_by_stage[str(key)] = int(value)
                except (TypeError, ValueError):
                    continue

    def save_state(self, path: str) -> None:
        with self._lock:
            payload = {
                "saved_at_utc": _utc_now_iso(),
                "counters": dict(self._counters),
                "error_by_type": dict(self._error_by_type),
                "error_by_stage": dict(self._error_by_stage),
            }
        self._write_json(path, payload)

    def record_startup(self, symbols: int, intervals: list[str]) -> None:
        with self._lock:
            self._append_event_locked(
                kind="startup",
                detail={
                    "mode": self._runtime_mode,
                    "symbols": symbols,
                    "intervals": intervals,
                    "sizing_mode": self.settings.sizing_mode,
                    "paper": bool(self.settings.use_paper_trading),
                },
            )
            self._send_alert_locked(
                key="startup",
                title="Startup",
                detail=(
                    f"mode={self._runtime_mode} symbols={symbols} intervals={intervals} "
                    f"sizing={self.settings.sizing_mode}"
                ),
            )

    def heartbeat(self, stream_status: dict[str, Any], risk_state: "RiskState", open_positions: int) -> None:
        """Update scheduler/risk snapshots and emit periodic reports."""
        now = time.time()
        with self._lock:
            self._counters["heartbeat_count"] += 1
            self._last_stream_status = dict(stream_status or {})
            self._last_risk_snapshot = {
                "equity": float(getattr(risk_state, "equity", 0.0) or 0.0),
                "day_start_equity": float(getattr(risk_state, "day_start_equity", 0.0) or 0.0),
                "consecutive_losses": int(getattr(risk_state, "consecutive_losses", 0) or 0),
                "paused": bool(getattr(risk_state, "paused", False)),
            }
            self._counters["open_positions"] = int(max(0, open_positions))

            event_count = int((stream_status or {}).get("event_count") or 0)
            if event_count > self._last_seen_event_count:
                delta = event_count - self._last_seen_event_count
                self._counters["polls"] += delta
                self._counters["polling_cycles"] += delta
                self._last_seen_event_count = event_count
                self._last_poll_ts = now

                self._clear_suspension_locked("scheduler_stale", note="poll activity resumed")
            elif event_count < self._last_seen_event_count:
                # Scheduler reset/restart.
                self._last_seen_event_count = event_count

            self._last_poll_duration_sec = float(
                (stream_status or {}).get("last_poll_duration_sec") or 0.0
            )

            self._expire_suspensions_locked(now)
            self._evaluate_suspension_rules_locked(now)
            self._maybe_emit_periodic_summaries_locked(now)
            self._maybe_write_report_locked(now)

    def can_open_new_entries(self) -> bool:
        """Return whether new entries are allowed by operational guards."""
        with self._lock:
            if not bool(getattr(self.settings, "enable_operational_kill_switches", False)):
                return True
            now = time.time()
            self._expire_suspensions_locked(now)
            self._evaluate_suspension_rules_locked(now)
            return len(self._suspensions) == 0

    def active_suspensions(self) -> list[str]:
        with self._lock:
            self._expire_suspensions_locked(time.time())
            return sorted(self._suspensions.keys())

    def record_signal_detected(
        self,
        *,
        symbol: str,
        interval: str,
        side: str,
        score: float,
        trace_id: str = "",
    ) -> None:
        with self._lock:
            self._counters["signals_detected"] += 1
            self._last_signal = {
                "ts_utc": _utc_now_iso(),
                "symbol": symbol,
                "interval": interval,
                "side": side,
                "score": round(float(score), 4),
                "trace_id": trace_id,
            }
            self._recent_signals.append(dict(self._last_signal))

    def record_signal_discarded(
        self,
        *,
        reason: str,
        symbol: str = "",
        interval: str = "",
        trace_id: str = "",
    ) -> None:
        with self._lock:
            self._counters["signals_discarded"] += 1
            self._counters["signals_rejected"] += 1
            self._append_event_locked(
                kind="signal_discarded",
                detail={
                    "reason": reason,
                    "symbol": symbol,
                    "interval": interval,
                    "trace_id": trace_id,
                },
            )

    def record_signal_alerted(
        self,
        *,
        symbol: str,
        interval: str,
        side: str,
        trace_id: str = "",
    ) -> None:
        with self._lock:
            self._counters["signals_alerted"] += 1
            self._append_event_locked(
                kind="signal_alerted",
                detail={
                    "symbol": symbol,
                    "interval": interval,
                    "side": side,
                    "trace_id": trace_id,
                },
            )

    def record_entry_attempt(self, *, symbol: str, side: str, interval: str, trace_id: str = "") -> None:
        with self._lock:
            self._counters["entry_attempts"] += 1
            self._counters["entries_attempted"] += 1
            self._append_event_locked(
                kind="entry_attempt",
                detail={"symbol": symbol, "side": side, "interval": interval, "trace_id": trace_id},
            )

    def record_entry_executed(
        self,
        *,
        symbol: str,
        side: str,
        interval: str,
        qty: float,
        entry: float,
        margin: float,
        exec_type: str,
        trace_id: str = "",
    ) -> None:
        with self._lock:
            self._counters["entries_executed"] += 1
            self._consecutive_order_failures = 0
            self._last_entry = {
                "ts_utc": _utc_now_iso(),
                "symbol": symbol,
                "side": side,
                "interval": interval,
                "qty": round(float(qty), 8),
                "entry": round(float(entry), 8),
                "margin": round(float(margin), 8),
                "exec_type": exec_type,
                "trace_id": trace_id,
            }
            self._recent_entries.append(dict(self._last_entry))
            self._append_event_locked(kind="entry_executed", detail=dict(self._last_entry))

    def record_entry_failed(self, *, symbol: str, stage: str, reason: str, trace_id: str = "") -> None:
        with self._lock:
            self._counters["entries_failed"] += 1
            self._counters["order_failures"] += 1
            self._consecutive_order_failures += 1
            self._append_event_locked(
                kind="entry_failed",
                detail={"symbol": symbol, "stage": stage, "reason": reason, "trace_id": trace_id},
            )
            if stage in {"order_placement", "order_fill"}:
                self._send_alert_locked(
                    key=f"order_fail:{symbol}",
                    title="Order Failure",
                    detail=f"symbol={symbol} stage={stage} reason={reason} trace={trace_id}",
                )
            self._evaluate_suspension_rules_locked(time.time())

    def record_protection_result(
        self,
        *,
        symbol: str,
        ok: bool,
        stage: str,
        trace_id: str = "",
    ) -> None:
        with self._lock:
            if ok:
                self._counters["protections_ok"] += 1
                self._consecutive_protection_failures = 0
            else:
                self._counters["protections_failed"] += 1
                self._counters["protection_failures"] += 1
                self._consecutive_protection_failures += 1
                self._send_alert_locked(
                    key=f"protection_fail:{symbol}",
                    title="Protection Failure",
                    detail=f"symbol={symbol} stage={stage} trace={trace_id}",
                )
            self._append_event_locked(
                kind="protection_check",
                detail={"symbol": symbol, "ok": ok, "stage": stage, "trace_id": trace_id},
            )
            self._evaluate_suspension_rules_locked(time.time())

    def record_trade_closed(
        self,
        *,
        symbol: str,
        result: str,
        pnl: float,
        paper: bool,
        equity_after: float,
        trace_id: str = "",
    ) -> None:
        with self._lock:
            self._counters["trades_closed"] += 1
            pnl_value = float(pnl)
            if paper:
                self._counters["paper_realized_pnl"] += pnl_value
            else:
                self._counters["live_realized_pnl"] += pnl_value

            event = {
                "ts_utc": _utc_now_iso(),
                "symbol": symbol,
                "result": result,
                "pnl": round(pnl_value, 8),
                "paper": paper,
                "equity_after": round(float(equity_after), 8),
                "trace_id": trace_id,
            }
            self._last_exit = dict(event)
            self._recent_exits.append(event)

            if paper:
                self._paper_equity_events.append(
                    {"ts_utc": event["ts_utc"], "equity": event["equity_after"]}
                )

    def record_error(
        self,
        *,
        stage: str,
        err: Exception | str,
        symbol: str = "",
        recoverable: bool = True,
        api_related: bool | None = None,
        trace_id: str = "",
    ) -> None:
        with self._lock:
            err_name = type(err).__name__ if isinstance(err, Exception) else "Error"
            self._error_by_type[err_name] += 1
            self._error_by_stage[str(stage)] += 1
            self._counters["errors_total"] += 1
            self._consecutive_errors += 1
            api_hint = str(stage).lower()
            if api_related is None:
                api_related = (
                    "api" in api_hint or "exchange" in api_hint or "binance" in api_hint
                )
            if api_related:
                self._counters["api_errors"] += 1
                self._consecutive_api_errors += 1

            item = {
                "ts_utc": _utc_now_iso(),
                "stage": stage,
                "type": err_name,
                "symbol": symbol,
                "recoverable": recoverable,
                "message": str(err)[:240],
                "trace_id": trace_id,
            }
            self._recent_errors.append(item)
            self._append_event_locked(kind="error", detail=item)
            self._evaluate_suspension_rules_locked(time.time())

    def record_success(self, *, stage: str = "") -> None:
        with self._lock:
            self._consecutive_errors = 0
            if stage and ("api" in stage.lower() or "exchange" in stage.lower()):
                self._consecutive_api_errors = 0
            if stage:
                self._append_event_locked(kind="success", detail={"stage": stage})

    def record_orphan_status(
        self,
        *,
        symbol: str,
        status: str,
        detail: str = "",
        trace_id: str = "",
    ) -> None:
        with self._lock:
            self._append_event_locked(
                kind="orphan",
                detail={
                    "symbol": symbol,
                    "status": status,
                    "detail": detail[:240],
                    "trace_id": trace_id,
                },
            )
            if status == "resumed":
                self._counters["orphan_recoveries"] += 1
            if status in {"detected", "resumed", "unrecoverable"}:
                self._send_alert_locked(
                    key=f"orphan:{symbol}:{status}",
                    title="Orphan Event",
                    detail=f"symbol={symbol} status={status} detail={detail[:120]} trace={trace_id}",
                )
            if status == "unrecoverable" and bool(
                getattr(self.settings, "kill_switch_pause_on_orphan_unrecoverable", False)
            ):
                self._activate_suspension_locked(
                    reason="orphan_unrecoverable",
                    duration_sec=int(getattr(self.settings, "operational_suspend_sec", 900)),
                    detail=f"symbol={symbol} detail={detail[:120]}",
                )

    def record_event(self, *, kind: str, detail: dict[str, Any], trace_id: str = "") -> None:
        with self._lock:
            payload = dict(detail or {})
            if trace_id:
                payload["trace_id"] = trace_id
            self._append_event_locked(kind=kind, detail=payload)

    def force_report(self) -> None:
        with self._lock:
            self._write_reports_locked()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._build_snapshot_locked()

    def _evaluate_suspension_rules_locked(self, now: float) -> None:
        if not bool(getattr(self.settings, "enable_operational_kill_switches", False)):
            return

        max_errors = int(getattr(self.settings, "kill_switch_max_consecutive_errors", 0))
        max_api_errors = int(getattr(self.settings, "kill_switch_max_api_errors", 0))
        max_order_fails = int(getattr(self.settings, "kill_switch_max_order_failures", 0))
        max_protection_fails = int(getattr(self.settings, "kill_switch_max_protection_failures", 0))
        max_idle = int(getattr(self.settings, "kill_switch_max_scheduler_idle_sec", 0))
        suspend_sec = int(getattr(self.settings, "operational_suspend_sec", 900))

        if max_errors > 0 and self._consecutive_errors >= max_errors:
            self._activate_suspension_locked(
                reason="consecutive_errors",
                duration_sec=suspend_sec,
                detail=f"errors={self._consecutive_errors} threshold={max_errors}",
            )
        if max_api_errors > 0 and self._consecutive_api_errors >= max_api_errors:
            self._activate_suspension_locked(
                reason="api_degraded",
                duration_sec=suspend_sec,
                detail=f"api_errors={self._consecutive_api_errors} threshold={max_api_errors}",
            )
        if max_order_fails > 0 and self._consecutive_order_failures >= max_order_fails:
            self._activate_suspension_locked(
                reason="order_failures",
                duration_sec=suspend_sec,
                detail=f"order_failures={self._consecutive_order_failures} threshold={max_order_fails}",
            )
        if max_protection_fails > 0 and self._consecutive_protection_failures >= max_protection_fails:
            self._activate_suspension_locked(
                reason="protection_failures",
                duration_sec=suspend_sec,
                detail=(
                    f"protection_failures={self._consecutive_protection_failures} "
                    f"threshold={max_protection_fails}"
                ),
            )
        if max_idle > 0 and self._last_poll_ts is not None and (now - self._last_poll_ts) > max_idle:
            self._activate_suspension_locked(
                reason="scheduler_stale",
                duration_sec=suspend_sec,
                detail=f"idle_sec={round(now - self._last_poll_ts, 1)} threshold={max_idle}",
            )

    def _activate_suspension_locked(self, reason: str, duration_sec: int, detail: str) -> None:
        now = time.time()
        until = None if duration_sec <= 0 else now + float(duration_sec)
        existing = self._suspensions.get(reason)
        if existing is None and reason in self._suspensions:
            # Existing persistent suspension; keep it.
            return
        if existing is not None and until is not None and existing >= until:
            return

        self._suspensions[reason] = until
        self.logger.warning("ops_suspension_activated reason=%s detail=%s", reason, detail)
        self._append_event_locked(
            kind="suspension_on",
            detail={"reason": reason, "detail": detail, "until_ts": until},
        )
        self._send_alert_locked(
            key=f"suspension:{reason}",
            title="Suspension ON",
            detail=f"reason={reason} detail={detail}",
        )

    def _clear_suspension_locked(self, reason: str, note: str) -> None:
        if reason not in self._suspensions:
            return
        self._suspensions.pop(reason, None)
        # Make auto-suspensions reversible: once elapsed/cleared, reset the
        # triggering consecutive counter so entries can resume.
        if reason == "consecutive_errors":
            self._consecutive_errors = 0
        elif reason == "api_degraded":
            self._consecutive_api_errors = 0
        elif reason == "order_failures":
            self._consecutive_order_failures = 0
        elif reason == "protection_failures":
            self._consecutive_protection_failures = 0
        self.logger.info("ops_suspension_cleared reason=%s note=%s", reason, note)
        self._append_event_locked(kind="suspension_off", detail={"reason": reason, "note": note})
        self._send_alert_locked(
            key=f"suspension_clear:{reason}",
            title="Suspension OFF",
            detail=f"reason={reason} note={note}",
        )

    def _expire_suspensions_locked(self, now: float) -> None:
        for reason, until in list(self._suspensions.items()):
            if until is None:
                continue
            if now >= until:
                self._clear_suspension_locked(reason, note="duration_elapsed")

    def _append_event_locked(self, *, kind: str, detail: dict[str, Any]) -> None:
        self._recent_events.append({"ts_utc": _utc_now_iso(), "kind": kind, "detail": detail})

    def _send_alert_locked(self, *, key: str, title: str, detail: str) -> None:
        if not bool(getattr(self.settings, "enable_operational_alerts", False)):
            return
        if self._telegram is None or not self._telegram.enabled:
            return

        now = time.time()
        cooldown = max(10, int(getattr(self.settings, "operational_alert_cooldown_sec", 300)))
        last = self._last_alert_ts.get(key, 0.0)
        if now - last < cooldown:
            return
        self._last_alert_ts[key] = now

        msg = f"{title}\nmode={self._runtime_mode}\n{detail}"
        threading.Thread(target=self._telegram.send, args=(msg,), daemon=True).start()

    def _maybe_emit_periodic_summaries_locked(self, now: float) -> None:
        if bool(getattr(self.settings, "ops_hourly_summary", True)):
            hour_key = datetime.now(timezone.utc).strftime("%Y-%m-%d %H")
            if hour_key != self._last_hour_bucket:
                self._last_hour_bucket = hour_key
                self._send_alert_locked(
                    key=f"hourly:{hour_key}",
                    title="Operational Summary (hourly)",
                    detail=self._summary_line_locked(),
                )

        if bool(getattr(self.settings, "ops_daily_summary", True)):
            day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if day_key != self._last_daily_date:
                self._last_daily_date = day_key
                self._send_alert_locked(
                    key=f"daily:{day_key}",
                    title="Operational Summary (daily)",
                    detail=self._summary_line_locked(),
                )

    def _summary_line_locked(self) -> str:
        return (
            f"polls={int(self._counters.get('polls', 0))} "
            f"signals={int(self._counters.get('signals_detected', 0))} "
            f"entries={int(self._counters.get('entries_executed', 0))}/"
            f"{int(self._counters.get('entry_attempts', 0))} "
            f"errors={int(self._counters.get('errors_total', 0))} "
            f"paused={bool(self._suspensions)}"
        )

    def _maybe_write_report_locked(self, now: float) -> None:
        interval = max(10, int(getattr(self.settings, "ops_report_interval_sec", 60)))
        if now - self._last_report_ts < interval:
            return
        self._last_report_ts = now
        self._write_reports_locked()

    def _write_reports_locked(self) -> None:
        snapshot = self._build_snapshot_locked()
        json_path = str(getattr(self.settings, "ops_status_json_path", "logs/ops_status.json"))
        md_path = str(getattr(self.settings, "ops_summary_md_path", "logs/ops_summary.md"))
        self._write_json(json_path, snapshot)
        self._write_text(md_path, render_operational_markdown(snapshot))

    def _build_snapshot_locked(self) -> dict[str, Any]:
        now = time.time()
        runtime = {
            "mode": self._runtime_mode,
            "started_at_utc": datetime.fromtimestamp(self._started_ts, tz=timezone.utc).isoformat(),
            "uptime_sec": max(0.0, now - self._started_ts),
            "health": self._compute_health_locked(now),
        }

        last_poll_age_sec = 0.0 if self._last_poll_ts is None else max(0.0, now - self._last_poll_ts)
        scheduler = {
            "alive": bool(self._last_stream_status.get("scheduler_alive", False)),
            "last_poll_age_sec": last_poll_age_sec,
            "last_poll_duration_sec": self._last_poll_duration_sec,
            "next_close_in_sec": float(self._last_stream_status.get("next_close_in_sec", 0.0) or 0.0),
            "event_count": int(self._last_stream_status.get("event_count", 0) or 0),
            "last_closed_ts": self._last_stream_status.get("last_closed_ts"),
        }

        suspensions = {
            "active": bool(self._suspensions),
            "reasons": sorted(self._suspensions.keys()),
            "until": {
                reason: (
                    datetime.fromtimestamp(until, tz=timezone.utc).isoformat() if until is not None else None
                )
                for reason, until in self._suspensions.items()
            },
        }

        metrics = {
            "signals_detected": int(self._counters.get("signals_detected", 0)),
            "signals_alerted": int(self._counters.get("signals_alerted", 0)),
            "signals_rejected": int(self._counters.get("signals_rejected", self._counters.get("signals_discarded", 0))),
            "entries_attempted": int(self._counters.get("entries_attempted", self._counters.get("entry_attempts", 0))),
            "entries_executed": int(self._counters.get("entries_executed", 0)),
            "entries_failed": int(self._counters.get("entries_failed", 0)),
            "protection_failures": int(self._counters.get("protection_failures", self._counters.get("protections_failed", 0))),
            "order_failures": int(self._counters.get("order_failures", 0)),
            "orphan_recoveries": int(self._counters.get("orphan_recoveries", 0)),
            "heartbeat_count": int(self._counters.get("heartbeat_count", 0)),
            "polling_cycles": int(self._counters.get("polling_cycles", self._counters.get("polls", 0))),
            "last_signal_time": (self._last_signal or {}).get("ts_utc"),
            "last_entry_time": (self._last_entry or {}).get("ts_utc"),
            "last_exit_time": (self._last_exit or {}).get("ts_utc"),
            "uptime_sec": max(0.0, now - self._started_ts),
        }

        return {
            "generated_at_utc": _utc_now_iso(),
            "runtime": runtime,
            "scheduler": scheduler,
            "counters": dict(self._counters),
            "metrics": metrics,
            "risk": dict(self._last_risk_snapshot),
            "last_signal": self._last_signal,
            "last_entry": self._last_entry,
            "last_exit": self._last_exit,
            "suspensions": suspensions,
            "errors": {
                "by_type": dict(self._error_by_type),
                "by_stage": dict(self._error_by_stage),
                "recent": list(self._recent_errors),
            },
            "recent_signals": list(self._recent_signals),
            "recent_entries": list(self._recent_entries),
            "recent_exits": list(self._recent_exits),
            "recent_events": list(self._recent_events),
            "paper_equity_events": list(self._paper_equity_events),
        }

    def _compute_health_locked(self, now: float) -> str:
        if self._suspensions:
            return "paused"
        if not bool(self._last_stream_status.get("scheduler_alive", False)):
            return "degraded"
        max_idle = int(getattr(self.settings, "kill_switch_max_scheduler_idle_sec", 0))
        if max_idle > 0 and self._last_poll_ts is not None and (now - self._last_poll_ts) > max_idle:
            return "degraded"
        return "healthy"

    def _write_json(self, path: str, payload: dict[str, Any]) -> None:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=True, indent=2)
        except OSError as exc:
            self.logger.warning("ops_status_write_failed path=%s err=%s", path, exc)

    def _write_text(self, path: str, content: str) -> None:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(content)
        except OSError as exc:
            self.logger.warning("ops_summary_write_failed path=%s err=%s", path, exc)

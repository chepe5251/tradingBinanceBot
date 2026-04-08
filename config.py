"""Configuration model and environment loading for the trading bot."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from sizing import SIZING_MODE_PCT_BALANCE, normalize_sizing_mode


@dataclass
class Settings:
    """Runtime configuration consumed by strategy, execution, and risk layers."""

    # ── Configurable via .env ────────────────────────────────────────────────
    # Trading universe and intervals
    symbol: str = "BTCUSDT"
    symbols: list[str] = field(default_factory=list)
    extra_symbols: list[str] = field(default_factory=list)
    use_top_volume_symbols: bool = True
    top_volume_allowlist: list[str] = field(default_factory=list)
    top_volume_min_price: float = 0.0
    top_volume_min_quote_volume: float = 0.0
    main_interval: str = "15m"
    context_interval: str = "1h"

    # ── Fixed by design — not overridable via .env ───────────────────────────
    # Symbol count is intentionally hardcoded so the bot's resource budget
    # (API weight, memory, CPU) is predictable and tested at this scale.
    top_volume_symbols_count: int = 300

    # Exchange/account
    leverage: int = 20
    margin_type: str = "ISOLATED"
    use_testnet: bool = True
    data_use_testnet: bool = False
    use_paper_trading: bool = False
    paper_start_balance: float = 25.0

    # Sizing and position control
    sizing_mode: str = SIZING_MODE_PCT_BALANCE
    fixed_margin_per_trade_usdt: float = 5.0
    risk_per_trade_pct: float = 0.01  # 1% of available balance per position
    margin_utilization: float = 0.95
    max_positions: int = 2
    use_limit_only: bool = False

    # Strategy indicators
    ema_fast: int = 20
    ema_mid: int = 50
    ema_trend: int = 200
    atr_period: int = 14
    atr_avg_window: int = 30
    volume_avg_window: int = 20
    trend_slope_min: float = 0.00005
    rsi_period: int = 14
    rsi_long_min: float = 48.0
    rsi_long_max: float = 68.0
    volume_min_ratio: float = 1.05
    volume_max_ratio: float = 1.5
    pullback_tolerance_atr: float = 0.8
    min_ema_spread_atr: float = 0.15
    max_ema_spread_atr: float = 1.0
    min_body_ratio: float = 0.35
    rr_target: float = 2.0
    min_risk_atr: float = 0.5
    max_risk_atr: float = 3.0
    min_score: float = 1.5
    max_atr_avg_ratio: float = 2.5

    # Hold timeout
    max_hold_candles: int = 50

    # ATR-based management
    atr_sl_mult: float = 1.6
    atr_tp_mult: float = 1.8
    min_sl_pct: float = 0.007
    breakeven_trigger_pct: float = 0.006
    stop_atr_mult: float = 1.2
    tp_rr: float = 1.8

    # Entry/management risk
    limit_offset_pct: float = 0.0001
    limit_timeout_sec: int = 6
    cooldown_sec: int = 180
    max_consecutive_losses: int = 3
    daily_drawdown_limit: float = 0.20
    daily_drawdown_limit_usdt: float = 6.0
    risk_pause_after_losses_sec: int = 86400
    anti_liq_trigger_r: float = 1.1

    # Scaling controls (currently disabled by monitor runtime)
    enable_loss_scaling: bool = False  # DCA on losing positions — only enable after exhaustive backtest
    scale_level1_margin_usdt: float = 5.0
    scale_level2_margin_usdt: float = 0.0
    scale_level2_atr_mult: float = 0.4

    # Signal filtering
    block_sell_on_intervals: list[str] = field(default_factory=lambda: ["4h"])

    # Data/history and logs
    history_candles_main: int = 600
    history_candles_context: int = 400
    data_stream_max_workers: int = 20
    log_heartbeat_sec: int = 60
    log_candle_updates: bool = False
    log_candle_every_sec: int = 60

    # Operational observability/safety (optional, disabled by default)
    enable_operational_kill_switches: bool = False
    kill_switch_max_consecutive_errors: int = 0
    kill_switch_max_api_errors: int = 0
    kill_switch_max_order_failures: int = 0
    kill_switch_max_protection_failures: int = 0
    kill_switch_max_scheduler_idle_sec: int = 0
    kill_switch_pause_on_orphan_unrecoverable: bool = False
    operational_suspend_sec: int = 900
    enable_operational_alerts: bool = False
    operational_alert_cooldown_sec: int = 300
    ops_report_interval_sec: int = 60
    ops_recent_events: int = 25
    ops_hourly_summary: bool = True
    ops_daily_summary: bool = True
    ops_status_json_path: str = "logs/ops_status.json"
    ops_summary_md_path: str = "logs/ops_summary.md"
    ops_state_json_path: str = "logs/ops_state.json"


def load_env(path: str = ".env") -> None:
    """Populate `os.environ` from a dotenv-style file without overwriting values."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            # Strip inline comments suffix (e.g. "0.05  # note" → "0.05")
            if " #" in value:
                value = value.split(" #")[0].rstrip()
            if key and key not in os.environ:
                os.environ[key] = value


def _parse_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


def _parse_list(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _set_int(settings: Settings, field_name: str, env_name: str, minimum: int | None = None) -> None:
    raw = os.getenv(env_name)
    if raw is None or raw == "":
        return
    try:
        value = int(raw)
    except ValueError:
        return
    if minimum is not None:
        value = max(minimum, value)
    setattr(settings, field_name, value)


def _set_float(
    settings: Settings,
    field_name: str,
    env_name: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> None:
    raw = os.getenv(env_name)
    if raw is None or raw == "":
        return
    try:
        value = float(raw)
    except ValueError:
        return
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    setattr(settings, field_name, value)


def _set_bool(settings: Settings, field_name: str, env_name: str) -> None:
    raw = os.getenv(env_name)
    if raw is None:
        return
    setattr(settings, field_name, _parse_bool(raw))


def _set_str(settings: Settings, field_name: str, env_name: str) -> None:
    raw = os.getenv(env_name)
    if raw is None or raw.strip() == "":
        return
    setattr(settings, field_name, raw.strip())


def from_env() -> Settings:
    """Build a validated `Settings` instance from environment variables."""
    load_env()
    settings = Settings()

    _set_bool(settings, "use_testnet", "BINANCE_TESTNET")
    _set_bool(settings, "data_use_testnet", "BINANCE_DATA_TESTNET")
    _set_bool(settings, "use_paper_trading", "USE_PAPER_TRADING")

    _set_str(settings, "symbol", "SYMBOL")
    symbols_raw = os.getenv("SYMBOLS")
    if symbols_raw:
        settings.symbols = _parse_list(symbols_raw)
    extra_symbols_raw = os.getenv("EXTRA_SYMBOLS")
    if extra_symbols_raw:
        settings.extra_symbols = _parse_list(extra_symbols_raw)

    _set_bool(settings, "use_top_volume_symbols", "USE_TOP_VOLUME_SYMBOLS")
    # top_volume_symbols_count is fixed in code — not overridable via .env
    _set_float(settings, "top_volume_min_price", "TOP_VOLUME_MIN_PRICE", minimum=0.0)
    _set_float(
        settings,
        "top_volume_min_quote_volume",
        "TOP_VOLUME_MIN_QUOTE_VOLUME",
        minimum=0.0,
    )
    allowlist_raw = os.getenv("TOP_VOLUME_ALLOWLIST")
    if allowlist_raw:
        settings.top_volume_allowlist = _parse_list(allowlist_raw)

    _set_str(settings, "main_interval", "MAIN_INTERVAL")
    _set_str(settings, "context_interval", "CONTEXT_INTERVAL")

    _set_int(settings, "history_candles_main", "HISTORY_CANDLES_MAIN", minimum=120)
    _set_int(settings, "history_candles_context", "HISTORY_CANDLES_CONTEXT", minimum=120)
    _set_int(settings, "data_stream_max_workers", "DATA_STREAM_MAX_WORKERS", minimum=1)
    _set_int(settings, "log_heartbeat_sec", "LOG_HEARTBEAT_SEC", minimum=5)
    _set_bool(settings, "log_candle_updates", "LOG_CANDLE_UPDATES")
    _set_int(settings, "log_candle_every_sec", "LOG_CANDLE_EVERY_SEC", minimum=5)

    _set_bool(
        settings,
        "enable_operational_kill_switches",
        "ENABLE_OPERATIONAL_KILL_SWITCHES",
    )
    _set_int(
        settings,
        "kill_switch_max_consecutive_errors",
        "KILL_SWITCH_MAX_CONSECUTIVE_ERRORS",
        minimum=0,
    )
    _set_int(
        settings,
        "kill_switch_max_api_errors",
        "KILL_SWITCH_MAX_API_ERRORS",
        minimum=0,
    )
    _set_int(
        settings,
        "kill_switch_max_order_failures",
        "KILL_SWITCH_MAX_ORDER_FAILURES",
        minimum=0,
    )
    _set_int(
        settings,
        "kill_switch_max_protection_failures",
        "KILL_SWITCH_MAX_PROTECTION_FAILURES",
        minimum=0,
    )
    _set_int(
        settings,
        "kill_switch_max_scheduler_idle_sec",
        "KILL_SWITCH_MAX_SCHEDULER_IDLE_SEC",
        minimum=0,
    )
    _set_bool(
        settings,
        "kill_switch_pause_on_orphan_unrecoverable",
        "KILL_SWITCH_PAUSE_ON_ORPHAN_UNRECOVERABLE",
    )
    _set_int(settings, "operational_suspend_sec", "OPERATIONAL_SUSPEND_SEC", minimum=0)
    _set_bool(settings, "enable_operational_alerts", "ENABLE_OPERATIONAL_ALERTS")
    _set_int(
        settings,
        "operational_alert_cooldown_sec",
        "OPERATIONAL_ALERT_COOLDOWN_SEC",
        minimum=10,
    )
    _set_int(settings, "ops_report_interval_sec", "OPS_REPORT_INTERVAL_SEC", minimum=10)
    _set_int(settings, "ops_recent_events", "OPS_RECENT_EVENTS", minimum=5)
    _set_bool(settings, "ops_hourly_summary", "OPS_HOURLY_SUMMARY")
    _set_bool(settings, "ops_daily_summary", "OPS_DAILY_SUMMARY")
    _set_str(settings, "ops_status_json_path", "OPS_STATUS_JSON_PATH")
    _set_str(settings, "ops_summary_md_path", "OPS_SUMMARY_MD_PATH")
    _set_str(settings, "ops_state_json_path", "OPS_STATE_JSON_PATH")

    _set_int(settings, "leverage", "LEVERAGE", minimum=1)
    _set_str(settings, "margin_type", "MARGIN_TYPE")

    _set_float(settings, "paper_start_balance", "PAPER_START_BALANCE", minimum=1.0)
    _set_str(settings, "sizing_mode", "SIZING_MODE")
    settings.sizing_mode = normalize_sizing_mode(settings.sizing_mode)
    _set_float(
        settings,
        "fixed_margin_per_trade_usdt",
        "FIXED_MARGIN_PER_TRADE_USDT",
        minimum=1.0,
    )
    _set_float(settings, "risk_per_trade_pct", "RISK_PER_TRADE_PCT", minimum=0.01, maximum=0.20)
    _set_float(settings, "margin_utilization", "MARGIN_UTILIZATION", minimum=0.50, maximum=0.95)
    _set_int(settings, "max_positions", "MAX_POSITIONS", minimum=1)
    _set_bool(settings, "use_limit_only", "USE_LIMIT_ONLY")

    _set_int(settings, "ema_fast", "EMA_FAST", minimum=2)
    _set_int(settings, "ema_mid", "EMA_MID", minimum=2)
    _set_int(settings, "ema_trend", "EMA_TREND", minimum=20)
    _set_int(settings, "atr_period", "ATR_PERIOD", minimum=2)
    _set_int(settings, "atr_avg_window", "ATR_AVG_WINDOW", minimum=2)
    _set_int(settings, "volume_avg_window", "VOLUME_AVG_WINDOW", minimum=2)
    _set_float(settings, "trend_slope_min", "TREND_SLOPE_MIN", minimum=0.0)
    _set_int(settings, "rsi_period", "RSI_PERIOD", minimum=2)
    _set_float(settings, "rsi_long_min", "RSI_LONG_MIN", minimum=0.0, maximum=100.0)
    _set_float(settings, "rsi_long_max", "RSI_LONG_MAX", minimum=0.0, maximum=100.0)
    _set_float(settings, "volume_min_ratio", "VOLUME_MIN_RATIO", minimum=0.0)
    _set_float(settings, "volume_max_ratio", "VOLUME_MAX_RATIO", minimum=0.0)
    _set_float(settings, "pullback_tolerance_atr", "PULLBACK_TOLERANCE_ATR", minimum=0.0)
    _set_float(settings, "min_ema_spread_atr", "MIN_EMA_SPREAD_ATR", minimum=0.0)
    _set_float(settings, "max_ema_spread_atr", "MAX_EMA_SPREAD_ATR", minimum=0.0)
    _set_float(settings, "min_body_ratio", "MIN_BODY_RATIO", minimum=0.0, maximum=1.0)
    _set_float(settings, "rr_target", "RR_TARGET", minimum=0.1)
    _set_float(settings, "min_risk_atr", "MIN_RISK_ATR", minimum=0.0)
    _set_float(settings, "max_risk_atr", "MAX_RISK_ATR", minimum=0.0)
    _set_float(settings, "min_score", "MIN_SCORE", minimum=0.0)
    _set_float(settings, "max_atr_avg_ratio", "MAX_ATR_AVG_RATIO", minimum=0.1)

    _set_int(settings, "max_hold_candles", "MAX_HOLD_CANDLES", minimum=1)

    _set_float(settings, "atr_sl_mult", "ATR_SL_MULT", minimum=0.0)
    _set_float(settings, "atr_tp_mult", "ATR_TP_MULT", minimum=0.0)
    _set_float(settings, "min_sl_pct", "MIN_SL_PCT", minimum=0.0)
    _set_float(settings, "breakeven_trigger_pct", "BREAKEVEN_TRIGGER_PCT", minimum=0.0)
    _set_float(settings, "stop_atr_mult", "STOP_ATR_MULT", minimum=0.0)
    _set_float(settings, "tp_rr", "TP_RR", minimum=0.1)

    _set_float(settings, "limit_offset_pct", "LIMIT_OFFSET_PCT", minimum=0.0)
    _set_int(settings, "limit_timeout_sec", "LIMIT_TIMEOUT_SEC", minimum=1)
    _set_int(settings, "cooldown_sec", "COOLDOWN_SEC", minimum=0)
    _set_int(settings, "max_consecutive_losses", "MAX_CONSECUTIVE_LOSSES", minimum=0)
    _set_float(settings, "daily_drawdown_limit", "DAILY_DRAWDOWN_LIMIT", minimum=0.0, maximum=1.0)
    _set_float(
        settings,
        "daily_drawdown_limit_usdt",
        "DAILY_DRAWDOWN_LIMIT_USDT",
        minimum=0.0,
    )
    _set_int(
        settings,
        "risk_pause_after_losses_sec",
        "RISK_PAUSE_AFTER_LOSSES_SEC",
        minimum=60,
    )
    _set_float(settings, "anti_liq_trigger_r", "ANTI_LIQ_TRIGGER_R", minimum=0.0)

    _set_float(settings, "scale_level1_margin_usdt", "SCALE_LEVEL1_MARGIN_USDT", minimum=0.0)
    _set_float(settings, "scale_level2_margin_usdt", "SCALE_LEVEL2_MARGIN_USDT", minimum=0.0)
    _set_float(settings, "scale_level2_atr_mult", "SCALE_LEVEL2_ATR_MULT", minimum=0.0)

    _set_bool(settings, "enable_loss_scaling", "ENABLE_LOSS_SCALING")

    raw = os.getenv("BLOCK_SELL_ON_INTERVALS")
    if raw:
        settings.block_sell_on_intervals = _parse_list(raw)

    # Keep RSI bounds coherent if configured inversely.
    if settings.rsi_long_min > settings.rsi_long_max:
        settings.rsi_long_min, settings.rsi_long_max = settings.rsi_long_max, settings.rsi_long_min

    return settings

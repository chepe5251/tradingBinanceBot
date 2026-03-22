"""Configuration model and environment loading for the trading bot.

This module centralizes runtime settings so the rest of the codebase can
consume a typed `Settings` object instead of querying environment variables
directly.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    """Runtime configuration consumed by strategy, execution, and risk layers.

    Values are initialized with safe defaults and then overridden in `from_env`.
    The bot expects a single immutable snapshot of this dataclass during
    process startup.
    """

    # Trading
    symbol: str = "BTCUSDT"
    symbols: list[str] = field(default_factory=list)
    extra_symbols: list[str] = field(default_factory=list)
    use_top_volume_symbols: bool = False
    top_volume_symbols_count: int = 0
    top_volume_allowlist: list[str] = field(default_factory=list)
    top_volume_min_price: float = 0.0
    top_volume_min_quote_volume: float = 0.0
    main_interval: str = "15m"
    context_interval: str = "1h"

    leverage: int = 20
    margin_type: str = "ISOLATED"
    use_testnet: bool = True
    data_use_testnet: bool = False

    # Capital management
    margin_utilization: float = 0.50  # use 50% of available balance as margin
    paper_start_balance: float = 25.0

    # Indicators
    ema_fast: int = 20
    ema_mid: int = 50
    ema_trend: int = 200
    atr_period: int = 14
    atr_avg_window: int = 30
    volume_avg_window: int = 20
    trend_slope_min: float = 0.00005
    rsi_period: int = 14
    rsi_long_min: float = 40.0
    rsi_long_max: float = 70.0
    rsi_short_min: float = 30.0
    rsi_short_max: float = 60.0
    volume_min_ratio: float = 1.0

    # ATR-based targets
    atr_sl_mult: float = 1.6
    atr_tp_mult: float = 1.8
    atr_trail_mult: float = 1.0
    min_sl_pct: float = 0.007  # 0.7%
    breakeven_trigger_pct: float = 0.006  # 0.6%
    trailing_activation_pct: float = 0.010  # 1.0%

    # Entry/management
    limit_offset_pct: float = 0.0001  # 0.01%
    limit_timeout_sec: int = 6
    cooldown_sec: int = 180
    max_consecutive_losses: int = 3
    daily_drawdown_limit: float = 0.20  # legacy pct drawdown guard
    daily_drawdown_limit_usdt: float = 6.0
    risk_pause_after_losses_sec: int = 86400  # pause until end of UTC day
    risk_per_trade_pct: float = 0.10
    fixed_margin_per_trade_usdt: float = 5.0
    scale_level1_margin_usdt: float = 5.0
    scale_level2_margin_usdt: float = 0.0
    scale_level2_atr_mult: float = 0.4
    stop_atr_mult: float = 1.2
    tp_rr: float = 1.8
    anti_liq_trigger_r: float = 1.1

    # Position control
    max_positions: int = 1
    use_limit_only: bool = True
    use_paper_trading: bool = False
    log_heartbeat_sec: int = 60
    log_candle_updates: bool = False
    log_candle_every_sec: int = 60

    # Data
    history_candles_main: int = 600
    history_candles_context: int = 400


def load_env(path: str = ".env") -> None:
    """Populate `os.environ` from a dotenv-style file.

    Existing environment variables are preserved and never overwritten.
    The parser is intentionally small: `KEY=VALUE` lines are supported and
    comments/blank lines are ignored.
    """

    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"").strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def _parse_bool(value: str) -> bool:
    """Parse common truthy string values."""
    return value.lower() in {"1", "true", "yes", "on"}


def _parse_list(value: str) -> list[str]:
    """Parse a comma-separated symbol list and normalize it to uppercase."""
    return [v.strip().upper() for v in value.split(",") if v.strip()]


def from_env() -> Settings:
    """Build a validated `Settings` instance from environment variables.

    This function also enforces operational bounds (for example, utilization
    and risk caps) so callers always receive sane values.
    """

    load_env()
    settings = Settings()
    testnet = os.getenv("BINANCE_TESTNET", "true").lower() in {"1", "true", "yes"}
    settings.use_testnet = testnet
    data_testnet = os.getenv("BINANCE_DATA_TESTNET")
    if data_testnet is not None:
        settings.data_use_testnet = _parse_bool(data_testnet)
    else:
        settings.data_use_testnet = False

    margin_util = os.getenv("MARGIN_UTILIZATION")
    if margin_util:
        try:
            settings.margin_utilization = float(margin_util)
        except ValueError:
            pass
    if settings.margin_utilization < 0.5:
        settings.margin_utilization = 0.5
    if settings.margin_utilization > 0.95:
        settings.margin_utilization = 0.95

    risk_per_trade = os.getenv("RISK_PER_TRADE_PCT")
    if risk_per_trade:
        try:
            settings.risk_per_trade_pct = float(risk_per_trade)
        except ValueError:
            pass
    if settings.risk_per_trade_pct < 0.01:
        settings.risk_per_trade_pct = 0.01
    if settings.risk_per_trade_pct > 0.20:
        settings.risk_per_trade_pct = 0.20

    fixed_margin = os.getenv("FIXED_MARGIN_PER_TRADE_USDT")
    if fixed_margin:
        try:
            settings.fixed_margin_per_trade_usdt = float(fixed_margin)
        except ValueError:
            pass
    if settings.fixed_margin_per_trade_usdt < 1.0:
        settings.fixed_margin_per_trade_usdt = 1.0

    scale_l1_margin = os.getenv("SCALE_LEVEL1_MARGIN_USDT")
    if scale_l1_margin:
        try:
            settings.scale_level1_margin_usdt = float(scale_l1_margin)
        except ValueError:
            pass
    if settings.scale_level1_margin_usdt < 0.0:
        settings.scale_level1_margin_usdt = 0.0

    scale_l2_margin = os.getenv("SCALE_LEVEL2_MARGIN_USDT")
    if scale_l2_margin:
        try:
            settings.scale_level2_margin_usdt = float(scale_l2_margin)
        except ValueError:
            pass
    if settings.scale_level2_margin_usdt < 0.0:
        settings.scale_level2_margin_usdt = 0.0

    anti_liq_trigger = os.getenv("ANTI_LIQ_TRIGGER_R")
    if anti_liq_trigger:
        try:
            settings.anti_liq_trigger_r = float(anti_liq_trigger)
        except ValueError:
            pass

    dd_usdt = os.getenv("DAILY_DRAWDOWN_LIMIT_USDT")
    if dd_usdt:
        try:
            settings.daily_drawdown_limit_usdt = float(dd_usdt)
        except ValueError:
            pass
    if settings.daily_drawdown_limit_usdt < 1.0:
        settings.daily_drawdown_limit_usdt = 1.0

    max_consec = os.getenv("MAX_CONSECUTIVE_LOSSES")
    if max_consec:
        try:
            settings.max_consecutive_losses = max(1, int(max_consec))
        except ValueError:
            pass

    pause_sec = os.getenv("RISK_PAUSE_AFTER_LOSSES_SEC")
    if pause_sec:
        try:
            settings.risk_pause_after_losses_sec = max(60, int(pause_sec))
        except ValueError:
            pass

    paper_balance = os.getenv("PAPER_START_BALANCE")
    if paper_balance:
        try:
            settings.paper_start_balance = float(paper_balance)
        except ValueError:
            pass

    use_paper = os.getenv("USE_PAPER_TRADING")
    if use_paper is not None:
        settings.use_paper_trading = _parse_bool(use_paper)

    log_updates = os.getenv("LOG_CANDLE_UPDATES")
    if log_updates is not None:
        settings.log_candle_updates = _parse_bool(log_updates)

    log_every = os.getenv("LOG_CANDLE_EVERY_SEC")
    if log_every:
        try:
            settings.log_candle_every_sec = int(log_every)
        except ValueError:
            pass

    symbols = os.getenv("SYMBOLS")
    if symbols:
        settings.symbols = _parse_list(symbols)

    extra_symbols = os.getenv("EXTRA_SYMBOLS")
    if extra_symbols:
        settings.extra_symbols = _parse_list(extra_symbols)

    use_top = os.getenv("USE_TOP_VOLUME_SYMBOLS")
    if use_top is not None:
        settings.use_top_volume_symbols = _parse_bool(use_top)

    top_count = os.getenv("TOP_VOLUME_SYMBOLS_COUNT")
    if top_count:
        try:
            settings.top_volume_symbols_count = max(0, int(top_count))
        except ValueError:
            pass

    allowlist = os.getenv("TOP_VOLUME_ALLOWLIST")
    if allowlist:
        settings.top_volume_allowlist = _parse_list(allowlist)

    return settings

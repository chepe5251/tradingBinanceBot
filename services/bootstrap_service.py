"""Bootstrap and wiring helpers for bot runtime objects."""
from __future__ import annotations

import logging
import logging.handlers
import os
import re
import threading
from dataclasses import dataclass
from typing import Callable

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

from config import Settings
from data_stream import MarketDataStream
from execution import FuturesExecutor
from risk import RiskManager
from services.exchange_metadata_service import ExchangeMetadataError, ExchangeMetadataService
from services.operational_service import OperationalService
from services.position_service import (
    PositionCache,
    cleanup_open_orders,
    configure_client,
    get_available_balance,
    has_any_position_or_entry_order,
)
from services.signal_service import strategy_config_from_settings

DEFAULT_HTF_MAP: dict[str, str] = {
    "1m": "5m",
    "3m": "15m",
    "5m": "15m",
    "15m": "1h",
    "15m_short": "1h",
    "30m": "2h",
    "1h": "4h",
    "2h": "8h",
    "4h": "1d",
    "6h": "1d",
    "8h": "1d",
    "12h": "1d",
}


@dataclass
class RuntimeContext:
    """Container with runtime services built during startup."""

    logger: logging.Logger
    trades_logger: logging.Logger
    trade_client: Client
    data_client: Client
    stream: MarketDataStream
    symbols: list[str]
    risk: RiskManager
    position_cache: PositionCache
    get_executor: Callable[[str], FuturesExecutor]
    evaluation_intervals: list[str]
    context_map: dict[str, str]
    operations: OperationalService
    metadata_service: ExchangeMetadataService


def setup_logging() -> tuple[logging.Logger, logging.Logger]:
    """Configure runtime loggers and return `(bot_logger, trades_logger)`."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    class _SuppressReadLoopClosed(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
            return "Read loop has been closed" not in record.getMessage()

    for name in ("binance", "binance.ws", "binance.ws.threaded_stream"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.CRITICAL)
        logger.addFilter(_SuppressReadLoopClosed())
        logger.propagate = False

    bot_logger = logging.getLogger("bot")
    trades_logger = logging.getLogger("trades")
    trades_logger.setLevel(logging.INFO)

    try:
        os.makedirs("logs", exist_ok=True)
        trades_handler = logging.handlers.RotatingFileHandler(
            "logs/trades.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
        )
        trades_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
        trades_logger.addHandler(trades_handler)
    except PermissionError:
        logging.warning("Cannot write logs/trades.log (permission denied).")

    return bot_logger, trades_logger


def _normalize_symbol_list(raw_symbols: list[str]) -> list[str]:
    pattern = re.compile(r"^[A-Z0-9]{2,20}USDT$")
    unique = {symbol.strip().upper() for symbol in raw_symbols if symbol.strip()}
    return sorted(symbol for symbol in unique if pattern.match(symbol))


def _load_all_tradable_usdt_perp_symbols(client: Client, logger: logging.Logger) -> list[str]:
    try:
        exchange_info = client.futures_exchange_info()
    except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
        logger.warning("exchange_info_load_failed err=%s", exc)
        return []

    symbols: list[str] = []
    pattern = re.compile(r"^[A-Z0-9]{2,20}USDT$")
    for item in exchange_info.get("symbols", []):
        if item.get("status") != "TRADING":
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        symbol = item.get("symbol")
        if symbol and pattern.match(symbol):
            symbols.append(symbol)
    return sorted(set(symbols))


def _ticker_map(client: Client, logger: logging.Logger) -> dict[str, dict]:
    try:
        tickers = client.futures_ticker()
    except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
        logger.warning("ticker_load_failed err=%s", exc)
        return {}
    return {item.get("symbol", ""): item for item in tickers if item.get("symbol")}


def load_symbol_universe(settings: Settings, client: Client, logger: logging.Logger) -> list[str]:
    """Return symbol universe according to explicit and top-volume settings."""
    tradable_symbols = _load_all_tradable_usdt_perp_symbols(client, logger)
    if not tradable_symbols:
        return _normalize_symbol_list(settings.symbols + settings.extra_symbols + [settings.symbol])

    tradable_set = set(tradable_symbols)
    explicit_symbols = _normalize_symbol_list(settings.symbols + settings.extra_symbols)
    if not explicit_symbols and settings.symbol:
        explicit_symbols = _normalize_symbol_list([settings.symbol])

    if explicit_symbols and not settings.use_top_volume_symbols:
        filtered = [symbol for symbol in explicit_symbols if symbol in tradable_set]
        if filtered:
            logger.info("Using %d configured symbols.", len(filtered))
            return filtered

    ticker_by_symbol = _ticker_map(client, logger)
    ranked_symbols = tradable_symbols
    if ticker_by_symbol:
        ranked_symbols = sorted(
            tradable_symbols,
            key=lambda symbol: float(ticker_by_symbol.get(symbol, {}).get("quoteVolume", 0.0) or 0.0),
            reverse=True,
        )

    if settings.use_top_volume_symbols:
        min_price = max(0.0, settings.top_volume_min_price)
        min_quote_volume = max(0.0, settings.top_volume_min_quote_volume)
        filtered_ranked: list[str] = []
        for symbol in ranked_symbols:
            ticker = ticker_by_symbol.get(symbol, {})
            try:
                price = float(ticker.get("lastPrice", 0.0) or 0.0)
                quote_volume = float(ticker.get("quoteVolume", 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
            if price < min_price or quote_volume < min_quote_volume:
                continue
            filtered_ranked.append(symbol)
        ranked_symbols = filtered_ranked or ranked_symbols

        top_count = settings.top_volume_symbols_count
        if top_count > 0:
            ranked_symbols = ranked_symbols[:top_count]
        logger.info("Loaded %d symbols by top-volume filter.", len(ranked_symbols))
    else:
        logger.info("Loaded all tradable symbols (%d).", len(ranked_symbols))

    for allow_symbol in _normalize_symbol_list(settings.top_volume_allowlist):
        if allow_symbol in tradable_set and allow_symbol not in ranked_symbols:
            ranked_symbols.append(allow_symbol)

    for symbol in explicit_symbols:
        if symbol in tradable_set and symbol not in ranked_symbols:
            ranked_symbols.append(symbol)

    return ranked_symbols


def build_interval_plan(settings: Settings) -> tuple[list[str], dict[str, str]]:
    """Build evaluation intervals and their context mapping."""
    main_interval = settings.main_interval
    context_interval = settings.context_interval
    higher_context = DEFAULT_HTF_MAP.get(context_interval)

    intervals: list[str] = [main_interval]
    if context_interval not in intervals:
        intervals.append(context_interval)
    if higher_context and higher_context not in intervals:
        intervals.append(higher_context)

    context_map: dict[str, str] = {main_interval: context_interval}
    if higher_context:
        context_map[context_interval] = higher_context
    return intervals, context_map


def _build_stream(
    client: Client,
    symbols: list[str],
    settings: Settings,
    evaluation_intervals: list[str],
) -> MarketDataStream:
    cfg = strategy_config_from_settings(settings)
    warmup_min = max(
        cfg.ema_trend + 3,
        cfg.volume_avg_window + 3,
        cfg.atr_avg_window + 3,
        cfg.rsi_period + 3,
    )
    warmup_with_buffer = max(120, warmup_min + 40)
    history_by_interval: dict[str, int] = {
        "15m": max(warmup_with_buffer, 320),
        "1h": max(warmup_with_buffer, 280),
        "4h": max(warmup_with_buffer, 260),
        "1d": max(warmup_with_buffer, 240),
    }

    main_interval = evaluation_intervals[0]  # "15m"
    main_limit = max(120, history_by_interval.get(main_interval, settings.history_candles_main))

    extra_intervals: dict[str, int] = {}
    for interval in evaluation_intervals[1:]:
        limit = history_by_interval.get(interval, settings.history_candles_context)
        extra_intervals[interval] = max(120, limit)

    return MarketDataStream(
        client=client,
        symbols=symbols,
        main_interval=main_interval,
        main_limit=main_limit,
        testnet=settings.data_use_testnet,
        extra_intervals=extra_intervals,
        max_workers=settings.data_stream_max_workers,
    )


def _build_risk_manager(settings: Settings) -> RiskManager:
    return RiskManager(
        cooldown_sec=settings.cooldown_sec,
        max_consecutive_losses=settings.max_consecutive_losses,
        daily_drawdown_limit=settings.daily_drawdown_limit,
        daily_drawdown_limit_usdt=settings.daily_drawdown_limit_usdt,
        loss_pause_sec=settings.risk_pause_after_losses_sec,
        volatility_pause=False,
        volatility_threshold=0.0,
    )


def bootstrap_runtime(settings: Settings, api_key: str, api_secret: str) -> RuntimeContext:
    """Create runtime clients/services while preserving current bot behavior."""
    logger, trades_logger = setup_logging()
    operations = OperationalService(settings=settings, logger=logger)
    operations.load_state(settings.ops_state_json_path)

    trade_client = configure_client(api_key, api_secret, settings.use_testnet)
    data_client = configure_client("", "", settings.data_use_testnet)
    position_cache = PositionCache(trade_client)
    metadata_service = ExchangeMetadataService(trade_client, logger=logger)
    try:
        metadata_service.load()
    except ExchangeMetadataError as exc:
        logger.warning("metadata_load_failed err=%s", exc)

    symbols = load_symbol_universe(settings, data_client, logger)
    if not symbols:
        fallback = _normalize_symbol_list(settings.symbols + settings.extra_symbols + [settings.symbol])
        symbols = fallback or ["BTCUSDT"]
        logger.warning("Falling back to configured symbols (%d).", len(symbols))

    evaluation_intervals, context_map = build_interval_plan(settings)
    stream = _build_stream(data_client, symbols, settings, evaluation_intervals)
    stream.load_initial()

    executors: dict[str, FuturesExecutor] = {}
    executors_lock = threading.Lock()

    def get_executor(symbol: str) -> FuturesExecutor:
        with executors_lock:
            if symbol not in executors:
                executor = FuturesExecutor(
                    client=trade_client,
                    symbol=symbol,
                    leverage=settings.leverage,
                    margin_type=settings.margin_type,
                    paper=settings.use_paper_trading,
                    metadata_service=metadata_service,
                )
                executor.setup()
                executors[symbol] = executor
            return executors[symbol]

    risk = _build_risk_manager(settings)
    risk.load("logs/risk_state.json")

    mode = "PAPER" if settings.use_paper_trading else ("TESTNET" if settings.use_testnet else "LIVE")
    operations.set_runtime_mode(mode)
    logger.info(
        "Startup | mode=%s leverage=%dx sizing=%s pct=%.0f%% "
        "max_pos=%d symbols=%d intervals=%s hold=%d candles",
        mode,
        settings.leverage,
        settings.sizing_mode,
        settings.risk_per_trade_pct * 100,
        settings.max_positions,
        len(symbols),
        evaluation_intervals,
        settings.max_hold_candles,
    )

    cfg = strategy_config_from_settings(settings)
    logger.info(
        "Strategy config | ema=%d/%d/%d atr=%d rsi=%.0f-%.0f "
        "vol=%.2f-%.2fx body>=%.2f score>=%.1f rr=%.1f",
        cfg.ema_fast, cfg.ema_mid, cfg.ema_trend,
        cfg.atr_period,
        cfg.rsi_long_min, cfg.rsi_long_max,
        cfg.volume_min_ratio, cfg.volume_max_ratio,
        cfg.min_body_ratio,
        cfg.min_score,
        cfg.rr_target,
    )
    if settings.use_paper_trading:
        risk.init_equity(settings.paper_start_balance)
    else:
        try:
            available = get_available_balance(trade_client)
        except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
            logger.warning("balance_fetch_failed err=%s", exc)
            available = 0.0
        risk.init_equity(available)

        try:
            if not has_any_position_or_entry_order(trade_client):
                cleanup_open_orders(trade_client, symbols, logger)
            else:
                logger.info("Open position detected; orphan recovery will run after startup.")
        except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
            logger.warning("open_order_cleanup_failed err=%s", exc)

    return RuntimeContext(
        logger=logger,
        trades_logger=trades_logger,
        trade_client=trade_client,
        data_client=data_client,
        stream=stream,
        symbols=symbols,
        risk=risk,
        position_cache=position_cache,
        get_executor=get_executor,
        evaluation_intervals=evaluation_intervals,
        context_map=context_map,
        operations=operations,
        metadata_service=metadata_service,
    )


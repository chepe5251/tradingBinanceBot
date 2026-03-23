"""Position-related exchange helpers and cache utilities."""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Callable

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

from execution import FuturesExecutor
from monitor import PositionMonitor
from risk import RiskManager

if TYPE_CHECKING:
    from config import Settings
    from data_stream import MarketDataStream


# Binance may return either variant depending on the endpoint and order version
PROTECTION_ORDER_TYPES = {"TAKE_PROFIT_MARKET", "STOP_MARKET", "TAKE_PROFIT", "STOP", "TRAILING_STOP_MARKET"}


def configure_client(api_key: str, api_secret: str, testnet: bool) -> Client:
    """Build a Binance client and normalize futures endpoint selection."""
    client = Client(api_key, api_secret, testnet=testnet)
    if testnet:
        client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
    return client


def get_available_balance(client: Client) -> float:
    """Return current available USDT balance from futures account."""
    balances = client.futures_account_balance()
    for balance in balances:
        if balance.get("asset") == "USDT":
            return float(balance.get("availableBalance", 0.0) or 0.0)
    return 0.0


def has_any_position_or_entry_order(client: Client) -> bool:
    """Return True when there is an open position or a pending entry order."""
    try:
        positions = client.futures_position_information()
        if count_active_positions(positions)[0] > 0:
            return True
    except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError):
        return True

    try:
        open_orders = client.futures_get_open_orders()
        for order in open_orders:
            if order.get("type") not in PROTECTION_ORDER_TYPES:
                return True
    except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError):
        return True
    return False


def cleanup_open_orders(client: Client, symbols: list[str], logger: logging.Logger) -> None:
    """Cancel open orders for a target symbol universe."""
    allowed_symbols = set(symbols)
    try:
        open_orders = client.futures_get_open_orders()
    except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
        logger.warning("cleanup_open_orders_failed err=%s", exc)
        return

    open_counts: dict[str, int] = {}
    for order in open_orders:
        symbol = order.get("symbol")
        if not symbol:
            continue
        if allowed_symbols and symbol not in allowed_symbols:
            continue
        open_counts[symbol] = open_counts.get(symbol, 0) + 1

    if not open_counts:
        logger.info("No open orders found for cleanup.")
        return

    for symbol, count in sorted(open_counts.items()):
        try:
            client.futures_cancel_all_open_orders(symbol=symbol)
            logger.info("Canceled %d open orders for %s", count, symbol)
        except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
            logger.warning("cleanup_cancel_failed symbol=%s err=%s", symbol, exc)


def count_active_positions(positions_snapshot: list[dict]) -> tuple[int, set[str]]:
    """Return active position count and symbol set from Binance snapshot."""
    active_symbols: set[str] = set()
    for position in positions_snapshot:
        symbol = position.get("symbol")
        if not symbol:
            continue
        try:
            amount = float(position.get("positionAmt", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if abs(amount) > 0:
            active_symbols.add(symbol)
    return len(active_symbols), active_symbols


class PositionCache:
    """Thread-safe cache for `futures_position_information` with a short TTL."""

    _TTL_SEC = 2.0

    def __init__(self, client: Client) -> None:
        self._client = client
        self._data: list[dict] = []
        self._ts: float = 0.0
        self._lock = threading.Lock()

    def get(self) -> list[dict]:
        """Return cached snapshot when fresh, otherwise refresh from exchange."""
        now = time.monotonic()
        with self._lock:
            if now - self._ts < self._TTL_SEC:
                return self._data
        data = self._client.futures_position_information()
        with self._lock:
            self._data = data
            self._ts = time.monotonic()
        return data

    def invalidate(self) -> None:
        with self._lock:
            self._ts = 0.0


def resume_orphaned_positions(
    trade_client: Client,
    symbols: list[str],
    stream: "MarketDataStream",
    settings: "Settings",
    get_executor: Callable[[str], FuturesExecutor],
    risk: RiskManager,
    pos_cache_invalidate: Callable[[], None],
    risk_updater: Callable[[float, datetime], None],
    logger: logging.Logger,
    trades_logger: logging.Logger,
) -> None:
    """Recover orphaned open positions and attach monitors."""
    try:
        positions = trade_client.futures_position_information()
    except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
        logger.warning("orphan_recovery_fetch_failed err=%s", exc)
        return

    orphans: list[dict] = []
    for position in positions:
        try:
            amount = float(position.get("positionAmt", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if abs(amount) > 0:
            orphans.append(position)

    for orphan in orphans:
        PositionMonitor.resume_orphan(
            orphan=orphan,
            symbols=symbols,
            stream=stream,
            settings=settings,
            get_executor=get_executor,
            risk=risk,
            trade_client=trade_client,
            pos_cache_invalidate=pos_cache_invalidate,
            risk_updater=risk_updater,
            logger=logger,
            trades_logger=trades_logger,
        )

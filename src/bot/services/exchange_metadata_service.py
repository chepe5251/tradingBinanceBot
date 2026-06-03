"""Centralized Binance exchange metadata cache.

This service loads `futures_exchange_info` once and exposes normalized,
typed accessors for symbol trading filters.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

from binance.exceptions import BinanceAPIException, BinanceRequestException

if TYPE_CHECKING:
    import logging

    from binance import Client


EXCHANGE_METADATA_ERRORS = (
    BinanceAPIException,
    BinanceRequestException,
    OSError,
    ValueError,
    TypeError,
)


class ExchangeMetadataError(RuntimeError):
    """Raised when exchange metadata cannot be loaded or parsed."""


class SymbolMetadataNotFoundError(KeyError):
    """Raised when requesting metadata for a symbol not present in cache."""


@dataclass(frozen=True)
class SymbolMetadata:
    """Normalized subset of exchange filters for one symbol."""

    symbol: str
    raw_symbol_info: dict
    step_size: float
    tick_size: float
    min_qty: float
    min_notional: float
    min_price: float
    max_price: float


class ExchangeMetadataService:
    """Thread-safe cache of futures symbol metadata."""

    def __init__(self, client: "Client", logger: "logging.Logger | None" = None) -> None:
        self._client = client
        self._logger = logger
        self._lock = threading.RLock()
        self._loaded = False
        self._symbols: dict[str, SymbolMetadata] = {}

    def load(self, force: bool = False) -> None:
        """Load metadata from exchange and populate local cache."""
        with self._lock:
            if self._loaded and not force:
                return
            try:
                payload = self._client.futures_exchange_info()
            except EXCHANGE_METADATA_ERRORS as exc:
                raise ExchangeMetadataError(f"futures_exchange_info failed: {exc}") from exc

            symbols_map: dict[str, SymbolMetadata] = {}
            for raw in payload.get("symbols", []):
                symbol = str(raw.get("symbol") or "").upper()
                if not symbol:
                    continue
                try:
                    symbols_map[symbol] = self._parse_symbol(raw)
                except ExchangeMetadataError as exc:
                    if self._logger:
                        self._logger.warning("metadata_parse_skip symbol=%s err=%s", symbol, exc)
                    continue

            self._symbols = symbols_map
            self._loaded = True
            if self._logger:
                self._logger.info("Exchange metadata loaded symbols=%d", len(self._symbols))

    def get_symbol_info(self, symbol: str) -> dict:
        """Return raw exchange symbol info."""
        return dict(self._get_symbol(symbol).raw_symbol_info)

    def get_step_size(self, symbol: str) -> float:
        return self._get_symbol(symbol).step_size

    def get_tick_size(self, symbol: str) -> float:
        return self._get_symbol(symbol).tick_size

    def get_min_qty(self, symbol: str) -> float:
        return self._get_symbol(symbol).min_qty

    def get_min_notional(self, symbol: str) -> float:
        return self._get_symbol(symbol).min_notional

    def get_price_limits(self, symbol: str) -> tuple[float, float]:
        meta = self._get_symbol(symbol)
        return meta.min_price, meta.max_price

    def known_symbols(self) -> list[str]:
        with self._lock:
            self._ensure_loaded_locked()
            return sorted(self._symbols.keys())

    def _get_symbol(self, symbol: str) -> SymbolMetadata:
        lookup = symbol.upper()
        with self._lock:
            self._ensure_loaded_locked()
            item = self._symbols.get(lookup)
            if item is None:
                raise SymbolMetadataNotFoundError(lookup)
            return item

    def _ensure_loaded_locked(self) -> None:
        if not self._loaded:
            # Avoid re-entrant deadlock by deferring to public loader while lock is held.
            # RLock allows this call pattern safely.
            self.load()

    @staticmethod
    def _parse_symbol(raw: dict) -> SymbolMetadata:
        filters = {f.get("filterType"): f for f in raw.get("filters", []) if f.get("filterType")}
        lot = filters.get("LOT_SIZE")
        px = filters.get("PRICE_FILTER")
        if lot is None:
            raise ExchangeMetadataError("LOT_SIZE filter missing")
        if px is None:
            raise ExchangeMetadataError("PRICE_FILTER filter missing")

        step_size = float(lot.get("stepSize", 0.0) or 0.0)
        min_qty = float(lot.get("minQty", 0.0) or 0.0)
        tick_size = float(px.get("tickSize", 0.0) or 0.0)
        min_price = float(px.get("minPrice", 0.0) or 0.0)
        max_price = float(px.get("maxPrice", 0.0) or 0.0)

        min_notional = 0.0
        min_notional_filter = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL")
        if min_notional_filter:
            raw_notional = (
                min_notional_filter.get("notional")
                or min_notional_filter.get("minNotional")
                or 0.0
            )
            min_notional = float(raw_notional or 0.0)

        return SymbolMetadata(
            symbol=str(raw.get("symbol") or "").upper(),
            raw_symbol_info=dict(raw),
            step_size=step_size,
            tick_size=tick_size,
            min_qty=min_qty,
            min_notional=min_notional,
            min_price=min_price,
            max_price=max_price,
        )

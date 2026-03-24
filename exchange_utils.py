"""Exchange-specific helpers that depend on the Binance client.

Keep this module separate from indicators.py so pure indicator math stays
free of exchange/API dependencies and can be tested without mocking Binance.
"""
from __future__ import annotations

import logging
from typing import Optional

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException


def safe_mark_price(
    client: Client,
    symbol: str,
    logger: logging.Logger | None = None,
) -> Optional[float]:
    """Fetch mark price and return None on recoverable API/network errors."""
    try:
        payload = client.futures_mark_price(symbol=symbol)
        mark = payload.get("markPrice")
        return float(mark) if mark is not None else None
    except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
        if logger is not None:
            logger.debug("mark_price_failed symbol=%s err=%s", symbol, exc)
        return None

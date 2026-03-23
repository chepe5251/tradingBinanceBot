"""Telegram transport and message templates."""
from __future__ import annotations

import logging
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def format_signal_message(
    symbol: str,
    side: str,
    timeframe: str,
    htf_bias: str,
    entry: float,
    sl: float,
    tp: float,
    rr: float,
    quality: str,
    volatility: str,
    structure: str,
) -> str:
    """Render a standard signal alert."""
    is_long = side == "BUY"
    direction = "LONG" if is_long else "SHORT"
    htf_txt = "Bullish" if htf_bias == "LONG" else "Bearish"
    return (
        f"SIGNAL CONFIRMED\n"
        f"{direction} | {symbol} | {timeframe}\n"
        "------------------\n"
        f"HTF bias: {htf_txt}\n"
        f"Structure: {structure}\n"
        f"Quality: {quality}\n"
        f"Volatility: {volatility}\n"
        "\n"
        f"Entry: {entry:.6f}\n"
        f"Stop Loss: {sl:.6f}\n"
        f"Take Profit: {tp:.6f}\n"
        f"R:R: 1:{rr:.2f}"
    )


def format_trade_event_message(symbol: str, title: str, detail: str) -> str:
    """Build compact messages for entry/exit/rebuy events."""
    return f"{title}\n{symbol}\n{detail}"


class TelegramService:
    """Rate-limited Telegram sender with retries."""

    def __init__(
        self,
        token: str,
        chat_id: str,
        logger: logging.Logger,
        min_interval_sec: float = 1.2,
    ) -> None:
        self._token = token.strip()
        self._chat_id = chat_id.strip()
        self._logger = logger
        self._min_interval_sec = min_interval_sec
        self._send_lock = threading.Lock()
        self._last_send_ts = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    def send(self, message: str) -> None:
        """Best-effort send. Errors are logged and never propagated."""
        if not self.enabled:
            return
        try:
            self._send_with_retry(message)
        except (HTTPError, URLError, OSError, TimeoutError, ValueError) as exc:
            self._logger.warning("telegram_send_failed err=%s", exc)

    def _send_with_retry(self, message: str) -> None:
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = urlencode({"chat_id": self._chat_id, "text": message})
        last_exc: Exception | None = None

        for attempt in range(1, 6):
            try:
                with self._send_lock:
                    now = time.time()
                    wait_sec = self._min_interval_sec - (now - self._last_send_ts)
                    if wait_sec > 0:
                        time.sleep(wait_sec)
                    req = Request(
                        url,
                        data=payload.encode("utf-8"),
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        method="POST",
                    )
                    with urlopen(req, timeout=10):
                        self._last_send_ts = time.time()
                        return
            except HTTPError as exc:
                last_exc = exc
                if exc.code == 429:
                    retry_after = 5.0
                    try:
                        header_val = exc.headers.get("Retry-After")
                        if header_val:
                            retry_after = max(1.0, float(header_val))
                    except (TypeError, ValueError):
                        retry_after = 5.0
                    time.sleep(retry_after)
                    continue
                if attempt < 5:
                    time.sleep(min(float(attempt), 5.0))
            except (URLError, OSError, TimeoutError, ValueError) as exc:
                last_exc = exc
                if attempt < 5:
                    time.sleep(min(float(attempt), 5.0))

        if last_exc is not None:
            raise last_exc

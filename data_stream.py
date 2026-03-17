"""WebSocket market-data ingestion and in-memory candle storage.

`MarketDataStream` is responsible for:
- Bootstrapping historical klines for all tracked intervals/symbols.
- Maintaining live candle updates from Binance websocket streams.
- Providing thread-safe dataframe snapshots to strategy/execution code.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Callable, Dict, Optional

import pandas as pd
from binance import Client
from binance import ThreadedWebsocketManager

logger = logging.getLogger(__name__)


def _kline_to_row(k: dict) -> dict:
    """Normalize a websocket kline payload into the internal row schema."""
    return {
        "open_time": int(k["t"]),
        "open": float(k["o"]),
        "high": float(k["h"]),
        "low": float(k["l"]),
        "close": float(k["c"]),
        "volume": float(k["v"]),
        "close_time": int(k["T"]),
    }


class MarketDataStream:
    """Thread-safe wrapper around Binance threaded websocket klines.

    The class keeps a bounded in-memory deque per `(interval, symbol)` pair.
    """

    def __init__(
        self,
        client: Client,
        symbols: list[str],
        main_interval: str,
        main_limit: int,
        api_key: str,
        api_secret: str,
        testnet: bool,
        context_interval: str | None = None,
        context_limit: int | None = None,
        extra_intervals: dict[str, int] | None = None,
    ) -> None:
        """Initialize stream configuration and caches without opening sockets."""
        self.client = client
        self.symbols = [s.upper() for s in symbols]
        self.main_interval = main_interval
        self.main_limit = main_limit
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet

        self._lock = threading.Lock()
        self._candles: Dict[str, Dict[str, deque]] = {
            main_interval: {s: deque(maxlen=main_limit) for s in self.symbols},
        }
        if context_interval and context_limit:
            self._candles[context_interval] = {s: deque(maxlen=context_limit) for s in self.symbols}
        if extra_intervals:
            for interval, limit in extra_intervals.items():
                if not interval or limit <= 0:
                    continue
                if interval in self._candles:
                    continue
                self._candles[interval] = {s: deque(maxlen=limit) for s in self.symbols}

        self._twm: Optional[ThreadedWebsocketManager] = None
        self._on_main_close: Optional[Callable[[str], None]] = None
        self._last_event_ts: Optional[int] = None
        self._last_closed_ts: Optional[int] = None
        self._event_count: int = 0
        self._last_event_symbol: Optional[str] = None
        self._last_close_symbol: Optional[str] = None
        self._restart_lock = threading.Lock()
        self._restarting = False
        self._last_error_log = 0.0
        self._last_event_wall: Optional[float] = None
        # WS stability tuning for large universes (full USDT-M symbol list).
        self._stream_chunk_size: int = 50
        self._chunk_start_delay_sec: float = 0.5

    @staticmethod
    def _chunks(items: list[str], size: int) -> list[list[str]]:
        """Split a list into fixed-size chunks."""
        if size <= 0:
            size = 1
        return [items[i:i + size] for i in range(0, len(items), size)]

    def load_initial(self) -> None:
        """Hydrate each cache with historical klines before streaming starts."""
        for interval, sym_map in self._candles.items():
            for symbol in self.symbols:
                klines = None
                delay = 1.0
                for attempt in range(1, 4):
                    try:
                        klines = self.client.futures_klines(
                            symbol=symbol, interval=interval, limit=sym_map[symbol].maxlen
                        )
                        break
                    except Exception as exc:
                        if attempt == 3:
                            logger.warning(
                                "Initial klines failed %s %s after %s attempts: %s",
                                symbol,
                                interval,
                                attempt,
                                exc,
                            )
                        else:
                            logger.warning(
                                "Initial klines retry %s/%s for %s %s: %s",
                                attempt,
                                3,
                                symbol,
                                interval,
                                exc,
                            )
                            time.sleep(delay)
                            delay = min(delay * 2.0, 5.0)
                if not klines:
                    continue
                rows = [
                    {
                        "open_time": int(k[0]),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                        "close_time": int(k[6]),
                    }
                    for k in klines
                ]
                with self._lock:
                    self._candles[interval][symbol].clear()
                    self._candles[interval][symbol].extend(rows)

    def _schedule_restart(self, reason: str) -> None:
        """Request an asynchronous websocket restart with bounded retries."""
        with self._restart_lock:
            if self._restarting:
                return
            self._restarting = True

        def _restart() -> None:
            """Stop and recreate websocket subscriptions with exponential delays."""
            try:
                logger.warning("WS restarting: %s", reason)
                attempts = 0
                delay = 5
                while attempts < 3:
                    self.stop()
                    time.sleep(delay)
                    try:
                        self._start_stream()
                        return
                    except Exception as exc:
                        attempts += 1
                        logger.error("WS restart failed: %s", exc)
                        delay = min(delay * 2, 30)
            except Exception as exc:
                logger.error("WS restart failed: %s", exc)
            finally:
                with self._restart_lock:
                    self._restarting = False

        threading.Thread(target=_restart, daemon=True).start()

    def _handle_kline(self, msg: dict) -> None:
        """Handle raw websocket messages and update internal candle caches."""
        # Handle combined stream payloads.
        if "data" in msg and isinstance(msg["data"], dict):
            msg = msg["data"]
        if msg.get("e") == "error":
            now = time.time()
            err_type = str(msg.get("type") or "")
            err_msg = str(msg.get("m") or "")
            err_msg_l = err_msg.lower()
            restartable = (
                err_type in {"ReadLoopClosed", "ConnectionClosedError"}
                or "keepalive ping timeout" in err_msg_l
                or "connection closed" in err_msg_l
            )
            if restartable:
                # Ignore transient WS errors while events are still flowing.
                if self._last_event_wall is not None and now - self._last_event_wall < 20:
                    return
                if now - self._last_error_log >= 30:
                    logger.warning("WS error restart | type=%s msg=%s", err_type, err_msg)
                    self._last_error_log = now
                self._schedule_restart(err_type or err_msg or "ws_error")
            elif now - self._last_error_log >= 30:
                logger.error("WS error: %s", msg)
                self._last_error_log = now
            return
        if msg.get("e") != "kline":
            return
        k = msg.get("k", {})
        interval = k.get("i")
        if interval not in self._candles:
            return
        symbol = k.get("s")
        if symbol not in self._candles[interval]:
            return
        self._event_count += 1
        self._last_event_wall = time.time()
        self._last_event_ts = int(k.get("T", 0)) if k.get("T") else None
        self._last_event_symbol = symbol

        # Only update cache and trigger callbacks on closed candles.
        # Intra-candle updates are unused by strategy and monitor logic.
        if not k.get("x"):
            return

        row = _kline_to_row(k)
        with self._lock:
            series = self._candles[interval][symbol]
            if series and series[-1]["open_time"] == row["open_time"]:
                series[-1] = row
            else:
                series.append(row)
        self._last_closed_ts = int(k.get("T", 0)) if k.get("T") else None
        self._last_close_symbol = symbol

        if interval == self.main_interval and self._on_main_close:
            self._on_main_close(symbol)

    def _start_stream(self) -> None:
        """Open websocket subscriptions for all configured intervals/symbols."""
        if self._twm is not None:
            try:
                self._twm.stop()
            except Exception:
                pass
            self._twm = None
        self._twm = ThreadedWebsocketManager(
            api_key=None,
            api_secret=None,
            testnet=self.testnet,
        )
        self._twm.start()
        symbols_lower = [s.lower() for s in self.symbols]
        preview = ",".join(self.symbols[:20])
        if len(self.symbols) > 20:
            preview += f",...(+{len(self.symbols)-20})"
        logger.info(
            "WS init | testnet=%s symbols_count=%s symbols=%s interval=%s",
            self.testnet,
            len(self.symbols),
            preview,
            self.main_interval,
        )
        if hasattr(self._twm, "start_futures_multiplex_socket"):
            logger.info("Using start_futures_multiplex_socket")
            streams = []
            for interval in self._candles.keys():
                streams.extend([f"{sym}@kline_{interval}" for sym in symbols_lower])
            # Avoid HTTP 414 (Request-URI Too Large) by splitting stream lists.
            for idx, chunk in enumerate(self._chunks(streams, self._stream_chunk_size), start=1):
                self._twm.start_futures_multiplex_socket(streams=chunk, callback=self._handle_kline)
                logger.info("Started futures multiplex chunk %s with %s streams", idx, len(chunk))
                time.sleep(self._chunk_start_delay_sec)
        elif hasattr(self._twm, "start_kline_futures_socket"):
            logger.info("Using start_kline_futures_socket")
            for interval in self._candles.keys():
                for sym in symbols_lower:
                    self._twm.start_kline_futures_socket(
                        symbol=sym,
                        interval=interval,
                        callback=self._handle_kline,
                    )
        elif hasattr(self._twm, "start_futures_kline_socket"):
            logger.info("Using start_futures_kline_socket")
            for interval in self._candles.keys():
                for sym in symbols_lower:
                    self._twm.start_futures_kline_socket(
                        symbol=sym,
                        interval=interval,
                        callback=self._handle_kline,
                    )
        elif hasattr(self._twm, "start_kline_socket"):
            logger.warning("Futures kline socket not available; falling back to spot kline socket.")
            for interval in self._candles.keys():
                for sym in symbols_lower:
                    self._twm.start_kline_socket(
                        symbol=sym,
                        interval=interval,
                        callback=self._handle_kline,
                    )
        elif hasattr(self._twm, "start_multiplex_socket"):
            logger.warning("Using spot multiplex socket as fallback.")
            streams = []
            for interval in self._candles.keys():
                streams.extend([f"{sym}@kline_{interval}" for sym in symbols_lower])
            for idx, chunk in enumerate(self._chunks(streams, self._stream_chunk_size), start=1):
                self._twm.start_multiplex_socket(streams=chunk, callback=self._handle_kline)
                logger.info("Started spot multiplex chunk %s with %s streams", idx, len(chunk))
                time.sleep(self._chunk_start_delay_sec)
        else:
            raise RuntimeError("No kline socket method available in ThreadedWebsocketManager")

    def start(self, on_main_close: Callable[[str], None]) -> None:
        """Start websocket streaming and register main-interval close callback."""
        self._on_main_close = on_main_close
        self._start_stream()

    def stop(self) -> None:
        """Stop websocket streaming and release the manager instance."""
        if self._twm:
            try:
                self._twm.stop()
            finally:
                self._twm = None

    def restart_if_stale(self, max_idle_sec: int) -> None:
        """Restart stream when no events were received for `max_idle_sec`."""
        if self._last_event_wall is None:
            return
        if time.time() - self._last_event_wall >= max_idle_sec:
            self._schedule_restart(f"stale>{max_idle_sec}s")

    def get_dataframe(self, symbol: str, interval: str) -> pd.DataFrame:
        """Return a pandas snapshot for `(symbol, interval)` in UTC timestamps."""
        with self._lock:
            data = list(self._candles.get(interval, {}).get(symbol, []))
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        return df

    def status(self) -> dict:
        """Expose heartbeat-friendly stream diagnostics."""
        return {
            "event_count": self._event_count,
            "last_event_ts": self._last_event_ts,
            "last_closed_ts": self._last_closed_ts,
            "last_event_symbol": self._last_event_symbol,
            "last_close_symbol": self._last_close_symbol,
            "last_event_wall": self._last_event_wall,
        }

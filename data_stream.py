"""REST-scheduler market-data ingestion and in-memory candle storage.

`MarketDataStream` is responsible for:
- Bootstrapping historical klines for all tracked intervals/symbols.
- Polling closed candles at each M15 boundary via Binance REST API.
- Providing thread-safe dataframe snapshots to strategy/execution code.

Replacing the previous WebSocket multiplex approach with a REST scheduler
reduces open connections from 22 streams to zero while maintaining correct
behavior, since strategy evaluation only needs closed candles (fired at
fixed :00/:15/:30/:45 boundaries for M15).
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Callable, Dict, Optional

import pandas as pd
from binance import Client
from requests.adapters import HTTPAdapter

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

    _INTERVAL_SECONDS: dict[str, int] = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900,
        "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
        "6h": 21600, "8h": 28800, "12h": 43200, "1d": 86400,
    }

    def __init__(
        self,
        client: Client,
        symbols: list[str],
        main_interval: str,
        main_limit: int,
        api_key: str = "",
        api_secret: str = "",
        testnet: bool = False,
        context_interval: str | None = None,
        context_limit: int | None = None,
        extra_intervals: dict[str, int] | None = None,
    ) -> None:
        """Initialize stream configuration and caches without opening connections."""
        self.client = client
        # Expand connection pool to match the polling semaphore (20 concurrent
        # requests). Default pool_maxsize=10 causes "pool is full" warnings.
        _adapter = HTTPAdapter(pool_connections=25, pool_maxsize=25)
        self.client.session.mount("https://", _adapter)
        self.client.session.mount("http://", _adapter)
        self.symbols = [s.upper() for s in symbols]
        self.main_interval = main_interval
        self.main_limit = main_limit

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

        self._on_close_callbacks: Dict[str, Optional[Callable[[str], None]]] = {}
        self._last_closed_ts: Optional[int] = None
        self._last_closed_wall: Optional[float] = None
        self._event_count: int = 0
        self._stop_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None

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

    def _seconds_to_next_close(self) -> float:
        """Return seconds until the next main-interval candle close + 2 s buffer."""
        period = self._INTERVAL_SECONDS.get(self.main_interval, 900)
        elapsed = time.time() % period
        return (period - elapsed) + 2.0

    def _fetch_and_update(self, symbol: str, interval: str, limit: int = 3) -> None:
        """Fetch the latest `limit` closed klines and update the in-memory cache."""
        try:
            klines = self.client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        except Exception as exc:
            logger.debug("REST fetch failed %s %s: %s", symbol, interval, exc)
            return
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
            series = self._candles.get(interval, {}).get(symbol)
            if series is None:
                return
            for row in rows:
                if series and series[-1]["open_time"] == row["open_time"]:
                    series[-1] = row
                else:
                    series.append(row)

    def _refresh_all(self) -> None:
        """Fetch latest candles for every symbol/interval in parallel (max 20 concurrent)."""
        semaphore = threading.Semaphore(20)
        threads: list[threading.Thread] = []

        for interval in list(self._candles.keys()):
            for sym in self.symbols:
                def _task(s: str = sym, iv: str = interval) -> None:
                    with semaphore:
                        self._fetch_and_update(s, iv)
                t = threading.Thread(target=_task, daemon=True)
                t.start()
                threads.append(t)

        for t in threads:
            t.join(timeout=30)

    def _scheduler_loop(self) -> None:
        """Core loop: sleep until next M15 boundary, refresh REST data, fire callback."""
        logger.info(
            "Scheduler init | symbols=%d intervals=%s",
            len(self.symbols),
            list(self._candles.keys()),
        )
        while not self._stop_event.is_set():
            wait_sec = self._seconds_to_next_close()
            logger.debug("Scheduler: next close in %.1f s", wait_sec)
            self._stop_event.wait(timeout=wait_sec)
            if self._stop_event.is_set():
                break

            logger.info("Scheduler: polling %d symbols × %d intervals via REST",
                        len(self.symbols), len(self._candles))
            self._refresh_all()

            self._event_count += 1
            self._last_closed_ts = int(time.time() * 1000)
            self._last_closed_wall = time.time()

            now_wall = time.time()
            for iv, callback in list(self._on_close_callbacks.items()):
                if callback is None:
                    continue
                period = self._INTERVAL_SECONDS.get(iv, 0)
                if period <= 0:
                    continue
                # Fire callback only when this interval's boundary just passed.
                # The scheduler wakes ~2 s after M15 close; refresh takes ~17 s,
                # so elapsed ≤ 30 s reliably covers all newly-closed intervals.
                if now_wall % period <= 30.0 and self.symbols:
                    try:
                        callback(self.symbols[0])
                    except Exception as exc:
                        logger.error("on_close[%s] raised: %s", iv, exc)

    def start_scheduler(self, on_close_callbacks: Dict[str, Callable[[str], None]]) -> None:
        """Start the REST-based scheduler and register per-interval close callbacks."""
        self._on_close_callbacks = dict(on_close_callbacks)
        self._stop_event.clear()
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            daemon=True,
            name="rest-scheduler",
        )
        self._scheduler_thread.start()

    def stop(self) -> None:
        """Stop the scheduler."""
        self._stop_event.set()

    def restart_if_stale(self, max_idle_sec: int) -> None:
        """No-op kept for API compatibility; scheduler is self-correcting."""

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
        """Expose heartbeat-friendly scheduler diagnostics."""
        next_close_sec = self._seconds_to_next_close()
        return {
            "event_count": self._event_count,
            "last_closed_ts": self._last_closed_ts,
            "last_closed_wall": self._last_closed_wall,
            "next_close_in_sec": round(next_close_sec, 1),
            "scheduler_alive": (
                self._scheduler_thread is not None and self._scheduler_thread.is_alive()
            ),
        }

"""REST-scheduler market-data ingestion and in-memory candle storage.

`MarketDataStream` is responsible for:
- Bootstrapping historical klines for all tracked intervals/symbols.
- Polling closed candles on scheduler boundaries via Binance REST API.
- Providing thread-safe dataframe snapshots to strategy/execution code.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Callable, Dict, Optional

import pandas as pd
from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

DATA_ERRORS = (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError)


class MarketDataStream:
    """Thread-safe REST poller with bounded in-memory candle caches."""

    _INTERVAL_SECONDS: dict[str, int] = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "8h": 28800,
        "12h": 43200,
        "1d": 86400,
    }

    def __init__(
        self,
        client: Client,
        symbols: list[str],
        main_interval: str,
        main_limit: int,
        # Legacy parameters kept for backward-compatible constructor calls.
        api_key: str = "",
        api_secret: str = "",
        testnet: bool = False,
        context_interval: str | None = None,
        context_limit: int | None = None,
        extra_intervals: dict[str, int] | None = None,
        max_workers: int = 20,
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
        self.max_workers = max(1, int(max_workers))

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

        # Snapshot cache: key=(symbol, interval)
        self._df_cache: dict[tuple[str, str], pd.DataFrame] = {}
        self._df_dirty: set[tuple[str, str]] = set()

        self._on_close_callbacks: Dict[str, Optional[Callable[[], None]]] = {}
        self._last_closed_ts: Optional[int] = None
        self._last_closed_wall: Optional[float] = None
        self._event_count: int = 0
        self._last_poll_duration_sec: float = 0.0
        self._stop_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._pool_lock = threading.Lock()
        self._pool: ThreadPoolExecutor | None = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="md-poll",
        )

    @staticmethod
    def _rows_from_klines(klines: list[list[object]]) -> list[dict[str, float | int]]:
        return [
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

    @staticmethod
    def _cache_key(symbol: str, interval: str) -> tuple[str, str]:
        return (symbol.upper(), interval)

    def _mark_df_dirty_locked(self, symbol: str, interval: str) -> None:
        self._df_dirty.add(self._cache_key(symbol, interval))

    def _load_initial_one(self, symbol: str, interval: str, limit: int) -> None:
        klines = None
        delay = 1.0
        for attempt in range(1, 4):
            try:
                klines = self.client.futures_klines(symbol=symbol, interval=interval, limit=limit)
                break
            except DATA_ERRORS as exc:
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
            return

        rows = self._rows_from_klines(klines)
        with self._lock:
            series = self._candles.get(interval, {}).get(symbol)
            if series is None:
                return
            series.clear()
            series.extend(rows)
            self._mark_df_dirty_locked(symbol, interval)

    def load_initial(self) -> None:
        """Hydrate each cache with historical klines before streaming starts."""
        tasks: list[tuple[str, str, int]] = [
            (symbol, interval, sym_map[symbol].maxlen)
            for interval, sym_map in self._candles.items()
            for symbol in self.symbols
        ]
        if not tasks:
            return

        pool = self._ensure_pool()
        futures: list[Future[None]] = [
            pool.submit(self._load_initial_one, symbol, interval, limit)
            for symbol, interval, limit in tasks
        ]
        done, pending = wait(futures, timeout=max(60, len(tasks) // 2))
        for future in done:
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001
                logger.debug("initial_worker_failed err=%s", exc)
        if pending:
            logger.warning("initial_worker_timeout pending=%d", len(pending))
            for future in pending:
                future.cancel()

    def _seconds_to_next_close(self) -> float:
        """Return seconds until the next main-interval candle close + 2 s buffer."""
        period = self._INTERVAL_SECONDS.get(self.main_interval, 900)
        elapsed = time.time() % period
        return (period - elapsed) + 2.0

    def _fetch_and_update(self, symbol: str, interval: str, limit: int = 3) -> None:
        """Fetch the latest `limit` closed klines and update the in-memory cache."""
        try:
            klines = self.client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        except DATA_ERRORS as exc:
            logger.debug("REST fetch failed %s %s: %s", symbol, interval, exc)
            return

        rows = self._rows_from_klines(klines)
        with self._lock:
            series = self._candles.get(interval, {}).get(symbol)
            if series is None:
                return
            for row in rows:
                if series and series[-1]["open_time"] == row["open_time"]:
                    # Replacement can change final OHLCV values.
                    if series[-1] != row:
                        series[-1] = row
                        self._mark_df_dirty_locked(symbol, interval)
                else:
                    series.append(row)
                    self._mark_df_dirty_locked(symbol, interval)

    def _refresh_intervals(self, intervals: list[str]) -> None:
        """Fetch latest candles for selected intervals via fixed worker pool."""
        active_intervals = [interval for interval in intervals if interval in self._candles]
        if not active_intervals:
            return

        tasks: list[tuple[str, str]] = [
            (symbol, interval)
            for interval in active_intervals
            for symbol in self.symbols
        ]
        if not tasks:
            return

        pool = self._ensure_pool()
        futures: list[Future[None]] = [
            pool.submit(self._fetch_and_update, symbol, interval)
            for symbol, interval in tasks
        ]
        done, pending = wait(futures, timeout=35)
        for future in done:
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001
                # Keep broad catch: worker exceptions should not abort scheduler loop.
                logger.debug("poll_worker_failed err=%s", exc)
        if pending:
            logger.warning("poll_worker_timeout pending=%d", len(pending))
            for future in pending:
                future.cancel()

    def _refresh_all(self) -> None:
        """Backward-compatible full refresh used by existing tests."""
        self._refresh_intervals(list(self._candles.keys()))

    def _due_intervals(self, now_wall: float, window_sec: float = 30.0) -> list[str]:
        due = [
            interval
            for interval in self._candles.keys()
            if self._INTERVAL_SECONDS.get(interval, 0) > 0
            and now_wall % self._INTERVAL_SECONDS[interval] <= window_sec
        ]
        if self.main_interval in self._candles and self.main_interval not in due:
            due.append(self.main_interval)
        return due

    def _ensure_pool(self) -> ThreadPoolExecutor:
        with self._pool_lock:
            if self._pool is None:
                self._pool = ThreadPoolExecutor(
                    max_workers=self.max_workers,
                    thread_name_prefix="md-poll",
                )
            return self._pool

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

            now_wall = time.time()
            due_intervals = self._due_intervals(now_wall)
            due_set = set(due_intervals)
            logger.info(
                "Scheduler: polling %d symbols x %d intervals via REST (%s)",
                len(self.symbols),
                len(due_intervals),
                due_intervals,
            )
            poll_started = time.perf_counter()
            self._refresh_intervals(due_intervals)
            self._last_poll_duration_sec = max(0.0, time.perf_counter() - poll_started)

            self._event_count += 1
            self._last_closed_ts = int(time.time() * 1000)
            self._last_closed_wall = time.time()

            for iv, callback in list(self._on_close_callbacks.items()):
                if callback is None:
                    continue
                if iv not in due_set:
                    continue
                if not self.symbols:
                    continue
                try:
                    callback()
                except Exception as exc:
                    # Keep broad catch: callbacks are caller-provided and must not stop scheduler.
                    logger.error("on_close[%s] raised: %s", iv, exc)

    def start_scheduler(self, on_close_callbacks: Dict[str, Callable[[], None]]) -> None:
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
        """Stop the scheduler and release worker resources."""
        self._stop_event.set()
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=5)
        with self._pool_lock:
            if self._pool is not None:
                self._pool.shutdown(wait=False, cancel_futures=True)
                self._pool = None

    def restart_if_stale(self, max_idle_sec: int) -> None:
        """Legacy no-op kept for API compatibility.

        The REST scheduler is timer-driven and does not require stale restarts.
        """

    def get_dataframe(self, symbol: str, interval: str) -> pd.DataFrame:
        """Return a pandas snapshot for `(symbol, interval)` in UTC timestamps."""
        symbol = symbol.upper()
        key = self._cache_key(symbol, interval)
        with self._lock:
            cached = self._df_cache.get(key)
            if cached is not None and key not in self._df_dirty:
                return cached
            data = list(self._candles.get(interval, {}).get(symbol, []))

        if not data:
            with self._lock:
                self._df_cache.pop(key, None)
                self._df_dirty.discard(key)
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        with self._lock:
            self._df_cache[key] = df
            self._df_dirty.discard(key)
        return df

    def status(self) -> dict:
        """Expose heartbeat-friendly scheduler diagnostics."""
        next_close_sec = self._seconds_to_next_close()
        return {
            "event_count": self._event_count,
            "last_closed_ts": self._last_closed_ts,
            "last_closed_wall": self._last_closed_wall,
            "next_close_in_sec": round(next_close_sec, 1),
            "last_poll_duration_sec": round(self._last_poll_duration_sec, 4),
            "scheduler_alive": (
                self._scheduler_thread is not None and self._scheduler_thread.is_alive()
            ),
        }

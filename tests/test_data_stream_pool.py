from __future__ import annotations

import threading
import unittest

import pytest

from data_stream import MarketDataStream


class _DummySession:
    def mount(self, *_args, **_kwargs) -> None:
        return None


class _KlineClient:
    def __init__(self) -> None:
        self.session = _DummySession()

    def futures_klines(self, symbol: str, interval: str, limit: int = 3) -> list[list[str | int]]:
        base = 100.0 if symbol == "BTCUSDT" else 200.0
        return [
            [
                1_700_000_000_000 + i * 60_000,
                f"{base + i:.4f}",
                f"{base + i + 1:.4f}",
                f"{base + i - 1:.4f}",
                f"{base + i + 0.5:.4f}",
                "100.0",
                1_700_000_000_000 + (i + 1) * 60_000 - 1,
            ]
            for i in range(limit)
        ]


class _MutableKlineClient:
    def __init__(self) -> None:
        self.session = _DummySession()
        self._calls = 0

    def futures_klines(self, symbol: str, interval: str, limit: int = 3) -> list[list[str | int]]:
        del symbol, interval, limit
        responses = [
            [[1_700_000_000_000, "100.0", "101.0", "99.0", "100.5", "100.0", 1_700_000_059_999]],
            [[1_700_000_000_000, "100.0", "101.2", "99.0", "101.0", "120.0", 1_700_000_059_999]],
            [[1_700_000_060_000, "101.0", "102.0", "100.0", "101.5", "110.0", 1_700_000_119_999]],
        ]
        idx = min(self._calls, len(responses) - 1)
        self._calls += 1
        return responses[idx]


@pytest.mark.unit
class DataStreamPoolTests(unittest.TestCase):
    def test_refresh_uses_fixed_pool_instance(self) -> None:
        stream = MarketDataStream(
            client=_KlineClient(),
            symbols=["BTCUSDT", "ETHUSDT"],
            main_interval="15m",
            main_limit=10,
            extra_intervals={"1h": 10},
            max_workers=2,
        )

        stream._refresh_all()  # noqa: SLF001
        first_pool = stream._pool  # noqa: SLF001
        assert first_pool is not None

        stream._refresh_all()  # noqa: SLF001
        assert stream._pool is first_pool  # noqa: SLF001

        poll_threads = [
            thread for thread in threading.enumerate() if thread.name.startswith("md-poll")
        ]
        assert len(poll_threads) <= 2

        stream.stop()

    def test_dataframe_cache_invalidates_on_replace_and_append(self) -> None:
        stream = MarketDataStream(
            client=_MutableKlineClient(),
            symbols=["BTCUSDT"],
            main_interval="15m",
            main_limit=10,
            max_workers=1,
        )
        try:
            stream._fetch_and_update("BTCUSDT", "15m", limit=1)  # noqa: SLF001
            first = stream.get_dataframe("BTCUSDT", "15m")
            first_again = stream.get_dataframe("BTCUSDT", "15m")
            assert first is first_again
            assert float(first.iloc[-1]["close"]) == 100.5

            # Same open_time but changed OHLCV must invalidate cache.
            stream._fetch_and_update("BTCUSDT", "15m", limit=1)  # noqa: SLF001
            replaced = stream.get_dataframe("BTCUSDT", "15m")
            assert replaced is not first
            assert float(replaced.iloc[-1]["close"]) == 101.0

            # New open_time append must invalidate cache and increase length.
            stream._fetch_and_update("BTCUSDT", "15m", limit=1)  # noqa: SLF001
            appended = stream.get_dataframe("BTCUSDT", "15m")
            assert appended is not replaced
            assert len(appended) == 2
        finally:
            stream.stop()

    def test_due_intervals_selects_only_closed_boundaries(self) -> None:
        stream = MarketDataStream(
            client=_KlineClient(),
            symbols=["BTCUSDT"],
            main_interval="15m",
            main_limit=10,
            extra_intervals={"1h": 10, "4h": 10},
            max_workers=1,
        )
        try:
            due = stream._due_intervals(3602.0)  # noqa: SLF001
            assert "15m" in due
            assert "1h" in due
            assert "4h" not in due
        finally:
            stream.stop()


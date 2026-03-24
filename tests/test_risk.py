from __future__ import annotations

import os
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone

import pytest

from risk import RiskManager


@pytest.mark.unit
class RiskManagerTests(unittest.TestCase):
    def _build_risk(self) -> RiskManager:
        risk = RiskManager(
            cooldown_sec=180,
            max_consecutive_losses=2,
            daily_drawdown_limit=0.20,
            daily_drawdown_limit_usdt=5.0,
            loss_pause_sec=3600,
            volatility_pause=False,
            volatility_threshold=0.0,
        )
        risk.init_equity(100.0)
        return risk

    def test_cooldown_blocks_until_elapsed(self) -> None:
        risk = self._build_risk()
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertTrue(risk.can_trade(start))
        risk.update_trade(-1.0, start)

        self.assertFalse(risk.can_trade(start + timedelta(seconds=60)))
        self.assertTrue(risk.can_trade(start + timedelta(seconds=181)))

    def test_consecutive_loss_pause(self) -> None:
        risk = self._build_risk()
        t0 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        risk.update_trade(-1.0, t0)
        risk.update_trade(-1.0, t0 + timedelta(seconds=181))

        self.assertFalse(risk.can_trade(t0 + timedelta(seconds=400)))
        self.assertFalse(risk.can_trade(t0 + timedelta(seconds=1000)))
        self.assertTrue(risk.can_trade(t0 + timedelta(seconds=4000)))

    def test_daily_drawdown_blocks_trading(self) -> None:
        risk = self._build_risk()
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        risk.update_trade(-6.0, t0)

        self.assertFalse(risk.can_trade(t0 + timedelta(seconds=181)))
        self.assertTrue(risk.snapshot().paused)

    def test_thread_safe_update_trade(self) -> None:
        risk = self._build_risk()
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        workers: list[threading.Thread] = []

        def worker(offset: int) -> None:
            risk.update_trade(-1.0, base + timedelta(seconds=offset))

        for i in range(25):
            thread = threading.Thread(target=worker, args=(i,))
            workers.append(thread)
            thread.start()
        for thread in workers:
            thread.join()

        snapshot = risk.snapshot()
        self.assertEqual(snapshot.equity, 75.0)

    def test_daily_reset_clears_consecutive_losses(self) -> None:
        """Day rollover must reset consecutive_losses so paused is cleared."""
        risk = self._build_risk()
        day1 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        risk.update_trade(-1.0, day1)
        risk.update_trade(-1.0, day1 + timedelta(seconds=181))
        # On day1 the manager is loss-paused (consecutive losses reached max)
        self.assertFalse(risk.can_trade(day1 + timedelta(seconds=400)))

        # Day2 well past any loss_pause_until — consecutive_losses and paused reset
        day2 = datetime(2026, 1, 2, 14, 0, tzinfo=timezone.utc)
        self.assertTrue(risk.can_trade(day2))
        self.assertEqual(risk.snapshot().consecutive_losses, 0)

    def test_win_resets_consecutive_losses(self) -> None:
        """A profitable trade must clear the consecutive loss counter."""
        risk = self._build_risk()
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        risk.update_trade(-1.0, t0)
        self.assertEqual(risk.snapshot().consecutive_losses, 1)

        risk.update_trade(+2.0, t0 + timedelta(seconds=181))
        self.assertEqual(risk.snapshot().consecutive_losses, 0)

    def test_persistence_round_trip(self) -> None:
        """save() then load() must faithfully restore state."""
        risk = self._build_risk()
        t0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        risk.update_trade(-3.0, t0)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "state.json")
            risk.save(path)

            risk2 = self._build_risk()
            risk2.load(path)

            s = risk2.snapshot()
            self.assertAlmostEqual(s.equity, 97.0)
            self.assertEqual(s.consecutive_losses, 1)
            self.assertEqual(s.last_trade_time, t0)

    def test_load_missing_file_is_no_op(self) -> None:
        """load() must not raise when the file does not exist."""
        risk = self._build_risk()
        risk.load("/nonexistent/path/state.json")  # must not raise
        self.assertAlmostEqual(risk.snapshot().equity, 100.0)

    def test_load_corrupt_file_falls_back_to_fresh_state(self) -> None:
        """load() with corrupt JSON must silently fall back to the current state."""
        risk = self._build_risk()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bad.json")
            with open(path, "w") as fh:
                fh.write("{not valid json")
            risk.load(path)
        # State unchanged from init_equity
        self.assertAlmostEqual(risk.snapshot().equity, 100.0)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import threading
import unittest
from datetime import datetime, timedelta, timezone

from risk import RiskManager


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


if __name__ == "__main__":
    unittest.main()

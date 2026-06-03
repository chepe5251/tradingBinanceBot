from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pytest
from bot.execution import FuturesExecutor


def _client_with_filters() -> MagicMock:
    client = MagicMock()
    client.futures_exchange_info.return_value = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "0.10", "maxPrice": "1000000"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                ],
            }
        ]
    }
    return client


@pytest.mark.unit
class ExecutionFallbackTests(unittest.TestCase):
    def test_timeout_falls_back_to_market_taker(self) -> None:
        client = _client_with_filters()
        client.futures_create_order.side_effect = [
            {"orderId": 1001, "price": "100.0"},  # limit entry
            {"avgPrice": "100.5", "executedQty": "1.000"},  # market fallback
        ]
        client.futures_cancel_order.return_value = {}

        executor = FuturesExecutor(client, "BTCUSDT", leverage=20, margin_type="ISOLATED", paper=False)
        qty, avg_price, exec_type = executor.place_limit_with_market_fallback(
            side="BUY",
            price=100.0,
            qty=1.0,
            timeout_sec=0,
        )

        self.assertEqual(exec_type, "TAKER")
        self.assertAlmostEqual(qty, 1.0, places=6)
        self.assertAlmostEqual(avg_price, 100.5, places=6)
        client.futures_cancel_order.assert_called_once()

    def test_partial_fill_results_in_hybrid_execution(self) -> None:
        client = _client_with_filters()
        client.futures_create_order.side_effect = [
            {"orderId": 2001, "price": "100.0"},  # limit
            {"avgPrice": "101.0", "executedQty": "0.600"},  # market for remaining
        ]
        client.futures_get_order.return_value = {"status": "NEW", "executedQty": "0.4", "avgPrice": "99.0"}
        client.futures_cancel_order.return_value = {}

        executor = FuturesExecutor(client, "BTCUSDT", leverage=20, margin_type="ISOLATED", paper=False)
        with (
            patch("bot.execution.time.time", side_effect=[0.0, 0.0, 1.0]),
            patch("bot.execution.time.sleep", return_value=None),
        ):
            qty, avg_price, exec_type = executor.place_limit_with_market_fallback(
                side="BUY",
                price=100.0,
                qty=1.0,
                timeout_sec=0.5,
            )

        self.assertEqual(exec_type, "HYBRID")
        self.assertAlmostEqual(qty, 1.0, places=6)
        # weighted avg uses maker anchor price (100.0) + taker remainder (101.0).
        self.assertAlmostEqual(avg_price, 100.6, places=4)

from __future__ import annotations

import unittest

from execution import FuturesExecutor
from sizing import is_entry_size_valid


class _FakeClient:
    def futures_exchange_info(self) -> dict:
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.01"},
                        {
                            "filterType": "PRICE_FILTER",
                            "tickSize": "0.10",
                            "minPrice": "0.10",
                            "maxPrice": "1000000",
                        },
                        {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                    ],
                }
            ]
        }


class ExecutionRoundingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.executor = FuturesExecutor(
            client=_FakeClient(),  # type: ignore[arg-type]
            symbol="BTCUSDT",
            leverage=20,
            margin_type="ISOLATED",
            paper=True,
        )

    def test_round_qty(self) -> None:
        self.assertEqual(self.executor.round_qty(1.23456), 1.234)
        self.assertEqual(self.executor.round_qty(0.0009), 0.0)

    def test_round_price(self) -> None:
        self.assertEqual(self.executor._round_price(123.456), 123.4)
        self.assertEqual(self.executor._round_price(0.05), 0.1)

    def test_min_qty_and_notional_validation(self) -> None:
        min_qty = self.executor.get_min_qty()
        min_notional = self.executor.get_min_notional()

        self.assertFalse(is_entry_size_valid(0.005, 100.0, min_qty, min_notional))
        self.assertFalse(is_entry_size_valid(0.02, 100.0, min_qty, 10.0))
        self.assertTrue(is_entry_size_valid(0.1, 100.0, min_qty, min_notional))


if __name__ == "__main__":
    unittest.main()

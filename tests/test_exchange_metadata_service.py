from __future__ import annotations

import unittest

import pytest
from bot.execution import FuturesExecutor
from bot.services.exchange_metadata_service import (
    ExchangeMetadataService,
    SymbolMetadataNotFoundError,
)


class _MetaClient:
    def futures_exchange_info(self) -> dict:
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        {
                            "filterType": "PRICE_FILTER",
                            "tickSize": "0.10",
                            "minPrice": "0.10",
                            "maxPrice": "1000000",
                        },
                        {"filterType": "NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }


@pytest.mark.unit
class ExchangeMetadataServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = _MetaClient()
        self.meta = ExchangeMetadataService(self.client)
        self.meta.load()

    def test_getters_return_expected_values(self) -> None:
        self.assertEqual(self.meta.get_step_size("BTCUSDT"), 0.001)
        self.assertEqual(self.meta.get_tick_size("BTCUSDT"), 0.10)
        self.assertEqual(self.meta.get_min_qty("BTCUSDT"), 0.001)
        self.assertEqual(self.meta.get_min_notional("BTCUSDT"), 5.0)
        self.assertEqual(self.meta.get_price_limits("BTCUSDT"), (0.10, 1_000_000.0))

    def test_missing_symbol_raises(self) -> None:
        with self.assertRaises(SymbolMetadataNotFoundError):
            self.meta.get_min_qty("ETHUSDT")

    def test_executor_uses_metadata_service_and_fails_for_unknown_symbol(self) -> None:
        executor = FuturesExecutor(
            client=self.client,  # should not be queried for filters when meta service is injected
            symbol="ETHUSDT",
            leverage=20,
            margin_type="ISOLATED",
            paper=True,
            metadata_service=self.meta,
        )
        with self.assertRaises(SymbolMetadataNotFoundError):
            executor.get_min_qty()


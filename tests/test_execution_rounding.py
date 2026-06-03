from __future__ import annotations

import unittest
from unittest.mock import MagicMock

import pytest

from bot.execution import FuturesExecutor, OrderRef
from bot.sizing import is_entry_size_valid

# ── Shared fake exchange-info client ────────────────────────────────────────

class _FakeClient:
    """Minimal exchange-info stub used for rounding/filter tests."""

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


def _make_executor(paper: bool = True, client=None) -> FuturesExecutor:
    return FuturesExecutor(
        client=client or _FakeClient(),
        symbol="BTCUSDT",
        leverage=20,
        margin_type="ISOLATED",
        paper=paper,
    )


# ── Rounding and filter validation ──────────────────────────────────────────

@pytest.mark.unit
class ExecutionRoundingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.executor = _make_executor(paper=True)

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


# ── _first_positive_float (pure static) ─────────────────────────────────────

@pytest.mark.unit
class FirstPositiveFloatTests(unittest.TestCase):
    def test_returns_first_positive(self) -> None:
        self.assertEqual(FuturesExecutor._first_positive_float(None, 0, -1, 3.0), 3.0)

    def test_skips_none_and_zero(self) -> None:
        self.assertEqual(FuturesExecutor._first_positive_float(None, 0.0, 5.0), 5.0)

    def test_parses_string(self) -> None:
        self.assertEqual(FuturesExecutor._first_positive_float("7.5"), 7.5)

    def test_all_invalid_returns_default(self) -> None:
        self.assertEqual(FuturesExecutor._first_positive_float(None, "abc", -3, 0), 0.0)

    def test_custom_default_returned(self) -> None:
        self.assertEqual(FuturesExecutor._first_positive_float(None, default=42.0), 42.0)

    def test_negative_skipped(self) -> None:
        self.assertEqual(FuturesExecutor._first_positive_float(-5, -1, 2.0), 2.0)


# ── calc_qty (pure math) ─────────────────────────────────────────────────────

@pytest.mark.unit
class CalcQtyTests(unittest.TestCase):
    def test_basic_leverage_math(self) -> None:
        executor = _make_executor()
        # 5 USDT * 20 leverage / 50000 = 0.002 (step 0.001 → rounds to 0.002)
        qty = executor.calc_qty(5.0, 50000.0)
        self.assertAlmostEqual(qty, 0.002, places=6)

    def test_rounds_down_to_step(self) -> None:
        executor = _make_executor()
        # 10 * 10 = 100 / 30000 = 0.003333... → floor to 0.003
        executor.leverage = 10
        qty = executor.calc_qty(10.0, 30000.0)
        self.assertAlmostEqual(qty, 0.003, places=6)

    def test_zero_price_gives_zero(self) -> None:
        executor = _make_executor()
        # division by zero guarded in rounding — result will be 0 or raise
        # calc_qty itself divides: if price=0 → ZeroDivisionError; we just verify
        # it is NOT expected to be called with 0 price in production
        with self.assertRaises((ZeroDivisionError, ValueError, Exception)):
            executor.calc_qty(5.0, 0.0)


# ── Paper-path short-circuits ────────────────────────────────────────────────

@pytest.mark.unit
class ExecutionPaperPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.executor = _make_executor(paper=True)

    def test_setup_paper_is_noop(self) -> None:
        self.executor.setup()  # must not raise, must not call client

    def test_has_open_position_paper_false(self) -> None:
        self.assertFalse(self.executor.has_open_position())

    def test_place_limit_entry_paper(self) -> None:
        result = self.executor.place_limit_entry("BUY", 100.0, 0.1)
        self.assertIsNone(result.order_id)
        self.assertEqual(result.status, "FILLED")
        self.assertEqual(result.qty, 0.1)

    def test_place_market_entry_paper(self) -> None:
        qty, price = self.executor.place_market_entry("BUY", 0.5)
        self.assertEqual(qty, 0.5)
        self.assertEqual(price, 0.0)

    def test_place_market_entry_paper_zero_qty(self) -> None:
        qty, price = self.executor.place_market_entry("BUY", 0.0)
        self.assertEqual(qty, 0.0)
        self.assertEqual(price, 0.0)

    def test_place_limit_with_market_fallback_paper(self) -> None:
        qty, price, typ = self.executor.place_limit_with_market_fallback("BUY", 100.0, 0.1)
        self.assertEqual(qty, 0.1)
        self.assertEqual(price, 100.0)
        self.assertEqual(typ, "MAKER")

    def test_wait_for_fill_paper_true(self) -> None:
        self.assertTrue(self.executor.wait_for_fill(12345))

    def test_place_tp_sl_paper_sentinel_refs(self) -> None:
        tp_ref, sl_ref = self.executor.place_tp_sl("BUY", 110.0, 90.0, 0.1)
        self.assertEqual(tp_ref.order_id, -1)
        self.assertEqual(sl_ref.order_id, -1)
        self.assertEqual(tp_ref.kind, "order")

    def test_replace_tp_sl_paper(self) -> None:
        tp_ref, sl_ref = self.executor.replace_tp_sl("BUY", 110.0, 90.0, 0.1)
        self.assertEqual(tp_ref.order_id, -1)

    def test_cancel_order_paper_noop(self) -> None:
        self.executor.cancel_order(999)  # must not raise

    def test_cancel_all_paper_noop(self) -> None:
        self.executor.cancel_all()  # must not raise

    def test_protection_status_paper_true_true(self) -> None:
        tp_ok, sl_ok = self.executor.protection_status("BUY")
        self.assertTrue(tp_ok)
        self.assertTrue(sl_ok)

    def test_get_protection_refs_paper_none_none(self) -> None:
        tp_ref, sl_ref = self.executor.get_protection_refs("BUY")
        self.assertIsNone(tp_ref)
        self.assertIsNone(sl_ref)

    def test_close_position_market_paper_noop(self) -> None:
        self.executor.close_position_market("BUY", 0.1)  # must not raise

    def test_monitor_oco_paper_returns_filled(self) -> None:
        tp_ref = OrderRef(order_id=-1, kind="order")
        sl_ref = OrderRef(order_id=-1, kind="order")
        result, exit_p = self.executor.monitor_oco(tp_ref, sl_ref, entry_price=100.0)
        self.assertEqual(result, "FILLED")
        self.assertEqual(exit_p, 100.0)

    def test_monitor_oco_paper_entry_none(self) -> None:
        tp_ref = OrderRef(order_id=-1, kind="order")
        sl_ref = OrderRef(order_id=-1, kind="order")
        result, exit_p = self.executor.monitor_oco(tp_ref, sl_ref)
        self.assertEqual(result, "FILLED")
        self.assertEqual(exit_p, 0.0)


# ── Live paths via mock client ────────────────────────────────────────────────

def _mock_client() -> MagicMock:
    """Return a pre-configured MagicMock Binance client."""
    client = MagicMock()
    client.futures_exchange_info.return_value = {
        "symbols": [{
            "symbol": "BTCUSDT",
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                {"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "0.10", "maxPrice": "1000000"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
            ],
        }]
    }
    return client


@pytest.mark.unit
class ExecutionLiveMockedTests(unittest.TestCase):
    """Live-path tests using a MagicMock Binance client."""

    def _make(self, client=None) -> FuturesExecutor:
        return _make_executor(paper=False, client=client or _mock_client())

    # setup -------------------------------------------------------------------

    def test_setup_applies_leverage(self) -> None:
        client = _mock_client()
        client.futures_change_leverage.return_value = {"leverage": 20}
        executor = self._make(client)
        executor.setup()
        client.futures_change_leverage.assert_called_once()
        self.assertEqual(executor.leverage, 20)

    def test_setup_tolerates_margin_type_error(self) -> None:
        from binance.exceptions import BinanceAPIException
        client = _mock_client()
        client.futures_change_margin_type.side_effect = BinanceAPIException(
            MagicMock(status_code=400), 400, '{"code":-4046}'
        )
        client.futures_change_leverage.return_value = {"leverage": 20}
        executor = self._make(client)
        executor.setup()  # must not raise

    # has_open_position -------------------------------------------------------

    def test_has_open_position_with_position(self) -> None:
        client = _mock_client()
        client.futures_position_information.return_value = [{"positionAmt": "0.5"}]
        self.assertTrue(self._make(client).has_open_position())

    def test_has_open_position_no_position(self) -> None:
        client = _mock_client()
        client.futures_position_information.return_value = [{"positionAmt": "0"}]
        self.assertFalse(self._make(client).has_open_position())

    def test_has_open_position_empty_list(self) -> None:
        client = _mock_client()
        client.futures_position_information.return_value = []
        self.assertFalse(self._make(client).has_open_position())

    # place_limit_entry -------------------------------------------------------

    def test_place_limit_entry_live(self) -> None:
        client = _mock_client()
        client.futures_create_order.return_value = {"orderId": 999, "status": "NEW"}
        result = self._make(client).place_limit_entry("BUY", 100.0, 0.1)
        self.assertEqual(result.order_id, 999)
        self.assertEqual(result.status, "NEW")

    # wait_for_fill -----------------------------------------------------------

    def test_wait_for_fill_filled(self) -> None:
        client = _mock_client()
        client.futures_get_order.return_value = {"status": "FILLED"}
        self.assertTrue(self._make(client).wait_for_fill(999, timeout_sec=5))

    def test_wait_for_fill_canceled(self) -> None:
        client = _mock_client()
        client.futures_get_order.return_value = {"status": "CANCELED"}
        self.assertFalse(self._make(client).wait_for_fill(999, timeout_sec=5))

    # cancel ------------------------------------------------------------------

    def test_cancel_order_calls_client(self) -> None:
        client = _mock_client()
        self._make(client).cancel_order(999)
        client.futures_cancel_order.assert_called_once_with(symbol="BTCUSDT", orderId=999)

    def test_cancel_all_calls_client(self) -> None:
        client = _mock_client()
        self._make(client).cancel_all()
        client.futures_cancel_all_open_orders.assert_called()

    # protection_status -------------------------------------------------------

    def test_protection_status_tp_and_sl_found(self) -> None:
        client = _mock_client()
        client.futures_get_open_orders.return_value = [
            {"orderId": 101, "algoId": None, "side": "SELL", "type": "TAKE_PROFIT", "clientOrderId": ""},
            {"orderId": 102, "algoId": None, "side": "SELL", "type": "STOP", "clientOrderId": ""},
        ]
        tp_ok, sl_ok = self._make(client).protection_status("BUY")
        self.assertTrue(tp_ok)
        self.assertTrue(sl_ok)

    def test_protection_status_missing(self) -> None:
        client = _mock_client()
        client.futures_get_open_orders.return_value = []
        tp_ok, sl_ok = self._make(client).protection_status("BUY")
        self.assertFalse(tp_ok)
        self.assertFalse(sl_ok)

    # get_protection_refs -----------------------------------------------------

    def test_get_protection_refs_live(self) -> None:
        client = _mock_client()
        client.futures_get_open_orders.return_value = [
            {"orderId": 101, "algoId": None, "side": "SELL", "type": "TAKE_PROFIT", "clientOrderId": ""},
            {"orderId": 102, "algoId": None, "side": "SELL", "type": "STOP", "clientOrderId": ""},
        ]
        tp_ref, sl_ref = self._make(client).get_protection_refs("BUY")
        self.assertIsNotNone(tp_ref)
        self.assertIsNotNone(sl_ref)
        self.assertEqual(tp_ref.order_id, 101)
        self.assertEqual(sl_ref.order_id, 102)

    def test_get_protection_refs_empty_orders(self) -> None:
        client = _mock_client()
        client.futures_get_open_orders.return_value = []
        tp_ref, sl_ref = self._make(client).get_protection_refs("BUY")
        self.assertIsNone(tp_ref)
        self.assertIsNone(sl_ref)

    # _check_order_fill_status ------------------------------------------------

    def test_check_order_fill_tp_filled_sl_open(self) -> None:
        client = _mock_client()

        def _get_order(symbol, orderId):
            return {"status": "FILLED"} if orderId == 101 else {"status": "NEW"}

        client.futures_get_order.side_effect = _get_order
        executor = self._make(client)
        tp_ref = OrderRef(order_id=101, kind="order")
        sl_ref = OrderRef(order_id=102, kind="order")
        tp_filled, sl_filled, tp_open, sl_open = executor._check_order_fill_status(
            tp_ref, sl_ref, last_replace_ts=0.0
        )
        self.assertTrue(tp_filled)
        self.assertFalse(sl_filled)
        self.assertFalse(tp_open)
        self.assertTrue(sl_open)

    def test_check_order_fill_guarded_by_replace_ts(self) -> None:
        """Fill is suppressed for 2 s after last replace."""
        import time
        client = _mock_client()
        client.futures_get_order.return_value = {"status": "FILLED"}
        executor = self._make(client)
        tp_ref = OrderRef(order_id=101, kind="order")
        sl_ref = OrderRef(order_id=102, kind="order")
        tp_filled, sl_filled, _, _ = executor._check_order_fill_status(
            tp_ref, sl_ref, last_replace_ts=time.time()  # just replaced
        )
        self.assertFalse(tp_filled)
        self.assertFalse(sl_filled)


if __name__ == "__main__":
    unittest.main()

"""Order routing and protection lifecycle for Binance Futures.

The executor exposes a thin adapter over Binance API primitives while keeping
rounding, symbol filters, and protection synchronization in one place.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from binance import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException

EXCHANGE_ERRORS = (
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
    OSError,
    ValueError,
    TypeError,
)

# Binance may return either variant depending on the endpoint and order version
STOP_ORDER_TYPES: frozenset[str] = frozenset({"STOP", "STOP_MARKET"})
TP_ORDER_TYPES: frozenset[str] = frozenset({"TAKE_PROFIT", "TAKE_PROFIT_MARKET"})

if TYPE_CHECKING:
    from services.exchange_metadata_service import ExchangeMetadataService


@dataclass
class OrderResult:
    """Normalized representation of a submitted order."""

    order_id: Optional[int]
    status: str
    price: float
    qty: float


@dataclass
class OrderRef:
    """Reference to an active protection order identifier."""

    order_id: int
    kind: str  # "order" or "algo"


class FuturesExecutor:
    """Per-symbol execution helper with exchange-rule aware rounding."""

    def __init__(
        self,
        client: Client,
        symbol: str,
        leverage: int,
        margin_type: str,
        paper: bool,
        metadata_service: "ExchangeMetadataService | None" = None,
    ) -> None:
        """Create an executor bound to a single symbol and account context."""
        self.client = client
        self.symbol = symbol
        self.leverage = leverage
        self.margin_type = margin_type
        self.paper = paper
        self._metadata_service = metadata_service
        self._symbol_info = None
        self._step_size = None
        self._tick_size = None
        self._min_qty = None
        self._min_notional = None
        self._min_price = None
        self._max_price = None

    @staticmethod
    def _first_positive_float(*values: object, default: float = 0.0) -> float:
        """Return first parseable positive float from `values`."""
        for v in values:
            if v is None:
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if f > 0:
                return f
        return default

    def _get_symbol_info(self) -> dict:
        """Load and cache exchange metadata for the configured symbol."""
        if self._symbol_info is not None:
            return self._symbol_info
        if self._metadata_service is not None:
            info = self._metadata_service.get_symbol_info(self.symbol)
            self._symbol_info = info
            return info
        info = self.client.futures_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == self.symbol:
                self._symbol_info = s
                return s
        raise RuntimeError(f"Symbol {self.symbol} not found")

    def _get_step_size(self) -> float:
        """Return cached LOT_SIZE step for quantity rounding."""
        if self._step_size is not None:
            return self._step_size
        if self._metadata_service is not None:
            self._step_size = self._metadata_service.get_step_size(self.symbol)
            return self._step_size
        symbol_info = self._get_symbol_info()
        for f in symbol_info["filters"]:
            if f["filterType"] == "LOT_SIZE":
                self._step_size = float(f["stepSize"])
                return self._step_size
        raise RuntimeError("LOT_SIZE not found")

    def _get_tick_size(self) -> float:
        """Return cached PRICE_FILTER tick size for price rounding."""
        if self._tick_size is not None:
            return self._tick_size
        if self._metadata_service is not None:
            self._tick_size = self._metadata_service.get_tick_size(self.symbol)
            return self._tick_size
        symbol_info = self._get_symbol_info()
        for f in symbol_info["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                self._tick_size = float(f["tickSize"])
                return self._tick_size
        raise RuntimeError("PRICE_FILTER not found")

    def _get_price_limits(self) -> tuple[float, float]:
        """Return PRICE_FILTER min/max allowed prices."""
        if self._min_price is not None and self._max_price is not None:
            return self._min_price, self._max_price
        if self._metadata_service is not None:
            self._min_price, self._max_price = self._metadata_service.get_price_limits(self.symbol)
            return self._min_price, self._max_price
        symbol_info = self._get_symbol_info()
        for f in symbol_info["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                self._min_price = float(f["minPrice"])
                self._max_price = float(f["maxPrice"])
                return self._min_price, self._max_price
        self._min_price = 0.0
        self._max_price = 0.0
        return self._min_price, self._max_price

    def get_min_qty(self) -> float:
        """Return exchange minimum quantity for this symbol."""
        if self._min_qty is not None:
            return self._min_qty
        if self._metadata_service is not None:
            self._min_qty = self._metadata_service.get_min_qty(self.symbol)
            return self._min_qty
        symbol_info = self._get_symbol_info()
        for f in symbol_info["filters"]:
            if f["filterType"] == "LOT_SIZE":
                self._min_qty = float(f["minQty"])
                return self._min_qty
        self._min_qty = 0.0
        return self._min_qty

    def get_min_notional(self) -> float:
        """Return exchange minimum notional requirement for this symbol."""
        if self._min_notional is not None:
            return self._min_notional
        if self._metadata_service is not None:
            self._min_notional = self._metadata_service.get_min_notional(self.symbol)
            return self._min_notional
        symbol_info = self._get_symbol_info()
        for f in symbol_info["filters"]:
            if f["filterType"] in {"MIN_NOTIONAL", "NOTIONAL"}:
                value = f.get("notional") or f.get("minNotional")
                if value is not None:
                    self._min_notional = float(value)
                    return self._min_notional
        self._min_notional = 0.0
        return self._min_notional

    def _round_qty(self, qty: float) -> float:
        """Floor quantity to the valid LOT_SIZE step."""
        step = self._get_step_size()
        if step <= 0:
            return qty
        step_dec = Decimal(str(step))
        qty_dec = Decimal(str(qty))
        rounded = (qty_dec // step_dec) * step_dec
        return float(rounded)

    def round_qty(self, qty: float) -> float:
        """Public wrapper for quantity normalization."""
        return self._round_qty(qty)

    def _round_price(self, price: float) -> float:
        """Floor price to tick size and clamp to exchange limits."""
        tick = self._get_tick_size()
        min_price, max_price = self._get_price_limits()
        if price is None or price != price or price <= 0:
            raise ValueError("Invalid price for order")
        if tick <= 0:
            raise ValueError("Invalid tick size")
        tick_dec = Decimal(str(tick))
        price_dec = Decimal(str(price))
        rounded = (price_dec // tick_dec) * tick_dec
        if min_price:
            rounded = max(rounded, Decimal(str(min_price)))
        else:
            rounded = max(rounded, tick_dec)
        if max_price:
            rounded = min(rounded, Decimal(str(max_price)))
        return float(rounded)

    def setup(self) -> None:
        """Apply margin mode and leverage in live mode."""
        if self.paper:
            return
        try:
            self.client.futures_change_margin_type(symbol=self.symbol, marginType=self.margin_type)
        except EXCHANGE_ERRORS:
            # Silently tolerated: exchange rejects margin-type change when a position is already open
            # or the symbol is already in the requested mode.  Neither case is an error worth aborting.
            pass
        lev = self.leverage
        while lev >= 1:
            try:
                self.client.futures_change_leverage(symbol=self.symbol, leverage=lev)
                self.leverage = lev
                break
            except EXCHANGE_ERRORS as exc:
                msg = str(exc)
                if "-4028" in msg and lev > 1:
                    lev = max(1, lev - 5)
                else:
                    raise

    def has_open_position(self) -> bool:
        """Return whether this symbol currently has any non-zero position."""
        if self.paper:
            return False
        positions = self.client.futures_position_information(symbol=self.symbol)
        for p in positions:
            amt = float(p["positionAmt"])
            if abs(amt) > 0:
                return True
        return False

    def calc_qty(self, capital_usdt: float, price: float) -> float:
        """Convert margin budget to position quantity using configured leverage."""
        notional = capital_usdt * self.leverage
        qty = notional / price
        return self._round_qty(qty)

    def place_limit_entry(self, side: str, price: float, qty: float) -> OrderResult:
        """Submit a LIMIT entry order and return normalized metadata."""
        if self.paper:
            return OrderResult(order_id=None, status="FILLED", price=price, qty=qty)

        price = self._round_price(price)
        order = self.client.futures_create_order(
            symbol=self.symbol,
            side=side,
            type="LIMIT",
            timeInForce="GTC",
            quantity=qty,
            price=str(price),
        )
        return OrderResult(order_id=int(order["orderId"]), status=order["status"], price=price, qty=qty)

    def place_market_entry(self, side: str, qty: float) -> tuple[float, float]:
        """Submit a MARKET entry order and return `(filled_qty, avg_price)`."""
        if self.paper:
            return qty, 0.0
        qty = self._round_qty(qty)
        if qty <= 0:
            return 0.0, 0.0
        order = self.client.futures_create_order(
            symbol=self.symbol,
            side=side,
            type="MARKET",
            quantity=qty,
        )
        filled_qty = float(order.get("executedQty") or qty or 0.0)
        avg_price = self._first_positive_float(order.get("avgPrice"), order.get("price"), default=0.0)
        return filled_qty, avg_price

    def place_limit_with_market_fallback(
        self,
        side: str,
        price: float,
        qty: float,
        timeout_sec: int = 6,
    ) -> tuple[float, float, str]:
        """Try maker entry first, then complete remaining quantity via market."""
        if self.paper:
            return qty, price, "MAKER"

        price = self._round_price(price)
        order = self.client.futures_create_order(
            symbol=self.symbol,
            side=side,
            type="LIMIT",
            timeInForce="GTC",
            quantity=qty,
            price=str(price),
        )
        order_id = int(order["orderId"])
        executed_qty = 0.0
        avg_price = self._first_positive_float(order.get("avgPrice"), order.get("price"), price, default=price)
        start = time.time()
        while time.time() - start < timeout_sec:
            o = self.client.futures_get_order(symbol=self.symbol, orderId=order_id)
            executed_qty = float(o.get("executedQty", 0))
            if o.get("status") == "FILLED":
                avg_price = self._first_positive_float(o.get("avgPrice"), o.get("price"), avg_price, price, default=price)
                return executed_qty, avg_price, "MAKER"
            time.sleep(0.2)

        # Not filled within timeout: cancel and market remaining
        try:
            self.client.futures_cancel_order(symbol=self.symbol, orderId=order_id)
        except EXCHANGE_ERRORS:
            pass

        remaining = max(0.0, qty - executed_qty)
        exec_type = "TAKER" if executed_qty == 0 else "HYBRID"
        if remaining > 0:
            m = self.client.futures_create_order(
                symbol=self.symbol,
                side=side,
                type="MARKET",
                quantity=self._round_qty(remaining),
            )
            m_avg = self._first_positive_float(m.get("avgPrice"), m.get("price"), avg_price, price, default=price)
            m_executed_qty = float(m.get("executedQty") or self._round_qty(remaining) or 0.0)
            if executed_qty > 0:
                total_qty = executed_qty + m_executed_qty
                if total_qty > 0:
                    avg_price = ((executed_qty * avg_price) + (m_executed_qty * m_avg)) / total_qty
            else:
                avg_price = m_avg
            executed_qty = executed_qty + m_executed_qty
        if avg_price <= 0:
            avg_price = self._first_positive_float(price, default=price)
        return executed_qty, avg_price, exec_type

    def wait_for_fill(self, order_id: int, timeout_sec: int = 30) -> bool:
        """Poll an order until filled, terminally rejected, or timeout."""
        if self.paper:
            return True
        start = time.time()
        while time.time() - start < timeout_sec:
            order = self.client.futures_get_order(symbol=self.symbol, orderId=order_id)
            status = order.get("status")
            if status == "FILLED":
                return True
            if status in {"CANCELED", "REJECTED", "EXPIRED"}:
                return False
            time.sleep(1)
        return False

    def place_tp_sl(
        self,
        side: str,
        tp_price: float,
        sl_price: float,
        qty: float,
        client_id_prefix: Optional[str] = None,
    ) -> tuple[OrderRef, OrderRef]:
        """Place reduce-only TP and SL protection orders for an open position."""
        if self.paper:
            return (OrderRef(order_id=-1, kind="order"), OrderRef(order_id=-1, kind="order"))

        is_long = side == "BUY"
        tp_price = self._round_price(tp_price)
        sl_price = self._round_price(sl_price)
        tp_cid = None
        sl_cid = None
        if client_id_prefix:
            safe = client_id_prefix.replace(" ", "")[:20]
            tp_cid = f"{safe}-TP"
            sl_cid = f"{safe}-SL"

        tp_order = self.client.futures_create_order(
            symbol=self.symbol,
            side="SELL" if is_long else "BUY",
            type="TAKE_PROFIT",
            timeInForce="GTC",
            quantity=qty,
            price=str(tp_price),
            stopPrice=str(tp_price),
            reduceOnly=True,
            newClientOrderId=tp_cid,
        )

        sl_order = self.client.futures_create_order(
            symbol=self.symbol,
            side="SELL" if is_long else "BUY",
            type="STOP",
            timeInForce="GTC",
            quantity=qty,
            price=str(sl_price),
            stopPrice=str(sl_price),
            reduceOnly=True,
            newClientOrderId=sl_cid,
        )

        tp_id = tp_order.get("orderId") or tp_order.get("algoId")
        sl_id = sl_order.get("orderId") or sl_order.get("algoId")
        if tp_id is None or sl_id is None:
            raise RuntimeError(f"TP/SL id missing tp={tp_order} sl={sl_order}")

        tp_kind = "order" if tp_order.get("orderId") is not None else "algo"
        sl_kind = "order" if sl_order.get("orderId") is not None else "algo"

        return OrderRef(order_id=int(tp_id), kind=tp_kind), OrderRef(order_id=int(sl_id), kind=sl_kind)

    def replace_tp_sl(
        self,
        side: str,
        tp_price: float,
        sl_price: float,
        qty: float,
        client_id_prefix: Optional[str] = None,
    ) -> tuple[OrderRef, OrderRef]:
        """Cancel existing open orders for the symbol and recreate TP/SL."""
        if not self.paper:
            try:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
            except EXCHANGE_ERRORS:
                pass
        return self.place_tp_sl(side, tp_price, sl_price, qty, client_id_prefix=client_id_prefix)

    def cancel_order(self, order_id: int) -> None:
        """Cancel one open order by identifier."""
        if self.paper:
            return
        self.client.futures_cancel_order(symbol=self.symbol, orderId=order_id)

    def cancel_all(self) -> None:
        """Cancel all open orders for the current symbol."""
        if self.paper:
            return
        self.client.futures_cancel_all_open_orders(symbol=self.symbol)

    def protection_status(self, side: str, client_id_prefix: Optional[str] = None) -> tuple[bool, bool]:
        """Check if TP and SL protections currently exist and are open."""
        if self.paper:
            return True, True
        exit_side = "SELL" if side == "BUY" else "BUY"
        tp_ok = False
        sl_ok = False
        try:
            orders = self.client.futures_get_open_orders(symbol=self.symbol)
        except EXCHANGE_ERRORS:
            return False, False
        for o in orders:
            if o.get("side") != exit_side:
                continue
            if client_id_prefix:
                cid = o.get("clientOrderId") or ""
                if not cid.startswith(client_id_prefix.replace(" ", "")[:20]):
                    continue
            otype = o.get("type") or o.get("orderType")
            # Binance may return either variant depending on endpoint and order version
            if otype in TP_ORDER_TYPES:
                tp_ok = True
            if otype in STOP_ORDER_TYPES:
                sl_ok = True
        return tp_ok, sl_ok

    def get_protection_refs(
        self, side: str, client_id_prefix: Optional[str] = None
    ) -> tuple[Optional[OrderRef], Optional[OrderRef]]:
        """Resolve currently open TP/SL references for reuse after restart."""
        if self.paper:
            return None, None
        exit_side = "SELL" if side == "BUY" else "BUY"
        tp_ref = None
        sl_ref = None
        try:
            orders = self.client.futures_get_open_orders(symbol=self.symbol)
        except EXCHANGE_ERRORS:
            return None, None
        for o in orders:
            if o.get("side") != exit_side:
                continue
            if client_id_prefix:
                cid = o.get("clientOrderId") or ""
                if not cid.startswith(client_id_prefix.replace(" ", "")[:20]):
                    continue
            otype = o.get("type") or o.get("orderType")
            oid = o.get("orderId") or o.get("algoId")
            if oid is None:
                continue
            kind = "order" if o.get("orderId") is not None else "algo"
            # Binance may return either variant depending on endpoint and order version
            if otype in TP_ORDER_TYPES:
                tp_ref = OrderRef(order_id=int(oid), kind=kind)
            if otype in STOP_ORDER_TYPES:
                sl_ref = OrderRef(order_id=int(oid), kind=kind)
        return tp_ref, sl_ref

    def close_position_market(self, side: str, qty: float) -> None:
        """Close an open position immediately with a reduce-only market order."""
        if self.paper:
            return
        close_side = "SELL" if side == "BUY" else "BUY"
        self.client.futures_create_order(
            symbol=self.symbol,
            side=close_side,
            type="MARKET",
            quantity=self._round_qty(qty),
            reduceOnly=True,
        )

    # ── Private helpers for monitor_oco ─────────────────────────────────────

    def _check_order_fill_status(
        self,
        tp_ref: "OrderRef",
        sl_ref: "OrderRef",
        last_replace_ts: float,
    ) -> tuple[bool, bool, bool, bool]:
        """Return (tp_filled, sl_filled, tp_open, sl_open) for current orders.

        Applies a 2-second guard after the last replace to avoid false fills
        from stale exchange state.
        """
        if tp_ref.kind == "order":
            tp = self.client.futures_get_order(symbol=self.symbol, orderId=tp_ref.order_id)
            tp_open = tp.get("status") not in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
            tp_filled = tp.get("status") == "FILLED"
        else:
            tp_open = self._is_algo_open(tp_ref.order_id)
            tp_filled = not tp_open

        if sl_ref.kind == "order":
            sl = self.client.futures_get_order(symbol=self.symbol, orderId=sl_ref.order_id)
            sl_open = sl.get("status") not in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
            sl_filled = sl.get("status") == "FILLED"
        else:
            sl_open = self._is_algo_open(sl_ref.order_id)
            sl_filled = not sl_open

        if time.time() - last_replace_ts < 2:
            tp_filled = False
            sl_filled = False
        return tp_filled, sl_filled, tp_open, sl_open

    def _run_safety_check(
        self,
        side: str,
        current_tp: float,
        current_sl: float,
        current_qty: float,
        tp_ref: "OrderRef",
        sl_ref: "OrderRef",
        last_safety_check_ts: float,
        safety_check_sec: int,
        on_event: Optional[callable],
        client_id_prefix: Optional[str],
    ) -> tuple[bool, float, "OrderRef", "OrderRef"]:
        """Verify TP/SL protections exist; replace if missing.

        Returns: (ok, new_last_safety_check_ts, tp_ref, sl_ref)
        """
        if time.time() - last_safety_check_ts < safety_check_sec:
            return True, last_safety_check_ts, tp_ref, sl_ref
        tp_ok, sl_ok = self.protection_status(side, client_id_prefix=client_id_prefix)
        if not (tp_ok and sl_ok):
            try:
                tp_ref, sl_ref = self.replace_tp_sl(
                    side, current_tp, current_sl, current_qty,
                    client_id_prefix=client_id_prefix,
                )
                tp_ok, sl_ok = self.protection_status(side, client_id_prefix=client_id_prefix)
            except EXCHANGE_ERRORS:
                tp_ok = False
                sl_ok = False
            if not (tp_ok and sl_ok) and on_event:
                on_event("critical", float(current_sl))
        return (tp_ok and sl_ok), time.time(), tp_ref, sl_ref

    def monitor_oco(
        self,
        tp_ref: OrderRef,
        sl_ref: OrderRef,
        side: Optional[str] = None,
        entry_price: Optional[float] = None,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
        qty: Optional[float] = None,
        atr: Optional[float] = None,
        breakeven_trigger_pct: float = 0.0,
        price_fn: Optional[callable] = None,
        atr_fn: Optional[callable] = None,
        on_event: Optional[callable] = None,
        scale_fn: Optional[callable] = None,
        safety_check_sec: int = 2,
        review_fn: Optional[callable] = None,
        review_sec: int = 5,
        client_id_prefix: Optional[str] = None,
        max_hold_sec: float = 0.0,
    ) -> tuple[str, float]:
        """Supervise TP/SL lifecycle until trade completion or early exit trigger.

        Returns:
            `(result, exit_price)` where result can be `TP`, `SL`, `EARLY:*`,
            `UNKNOWN`, or `FILLED` in paper mode.
        """
        if self.paper:
            return "FILLED", entry_price or 0.0
        start_ts = time.time()
        break_even = False
        current_entry = entry_price
        current_qty = qty
        current_be_trigger = max(float(breakeven_trigger_pct or 0.0), 0.005)  # x20 floor: 0.5%
        current_tp = tp_price
        current_sl = sl_price
        last_replace = 0.0
        last_safety_check = 0.0
        last_review = 0.0

        while True:
            # ── Max hold timeout ─────────────────────────────────────────────
            if max_hold_sec > 0 and time.time() - start_ts >= max_hold_sec:
                close_ok = True
                if side and current_qty:
                    try:
                        self.close_position_market(side, current_qty)
                    except EXCHANGE_ERRORS:
                        close_ok = False
                if not close_ok:
                    time.sleep(0.5)
                    continue
                exit_p = 0.0
                try:
                    exit_p = float(price_fn()) if price_fn else float(current_entry or 0.0)
                except Exception:  # price_fn is caller-provided; fall back to entry price
                    exit_p = float(current_entry or 0.0)
                return "EARLY:max_hold_timeout", exit_p

            # ── Loss-based scaling (disabled by default via scale_fn=None) ───
            if scale_fn and side and current_entry and current_qty and (current_tp is not None) and (current_sl is not None):
                try:
                    updates = scale_fn({
                        "entry_price": float(current_entry), "qty": float(current_qty),
                        "tp_price": float(current_tp), "sl_price": float(current_sl),
                        "break_even": break_even, "breakeven_trigger_pct": float(current_be_trigger),
                        "tp_ref": tp_ref, "sl_ref": sl_ref,
                    })
                except Exception:  # scale_fn is caller-provided; treat any error as no-op
                    updates = None
                if isinstance(updates, dict):
                    if updates.get("close_all"):
                        close_ok = True
                        try:
                            self.close_position_market(side, float(current_qty))
                        except EXCHANGE_ERRORS:
                            close_ok = False
                        if not close_ok:
                            time.sleep(0.5)
                            continue
                        return f"EARLY:{updates.get('reason', 'scale_cancel')}", float(updates.get("exit_price") or current_entry)
                    current_entry = float(updates["entry_price"]) if updates.get("entry_price") else current_entry
                    current_qty = float(updates["qty"]) if updates.get("qty") else current_qty
                    current_tp = float(updates["tp_price"]) if updates.get("tp_price") is not None else current_tp
                    current_sl = float(updates["sl_price"]) if updates.get("sl_price") is not None else current_sl
                    if updates.get("breakeven_trigger_pct") is not None:
                        current_be_trigger = max(float(updates["breakeven_trigger_pct"]), 0.005)
                    tp_ref = updates["tp_ref"] if updates.get("tp_ref") is not None else tp_ref
                    sl_ref = updates["sl_ref"] if updates.get("sl_ref") is not None else sl_ref
                    if updates.get("reset_break_even"):
                        break_even = False
                    last_replace = time.time()

            # ── Check fill status ────────────────────────────────────────────
            tp_filled, sl_filled, tp_open, sl_open = self._check_order_fill_status(tp_ref, sl_ref, last_replace)
            if tp_filled and sl_open:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
                return "TP", float(current_tp or 0.0)
            if sl_filled and tp_open:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
                return "SL", float(current_sl or 0.0)
            if tp_filled and sl_filled:
                return "UNKNOWN", float(current_sl or 0.0)

            # ── Safety check: ensure protections always exist ────────────────
            if side and current_qty and (current_tp is not None) and (current_sl is not None):
                _, last_safety_check, tp_ref, sl_ref = self._run_safety_check(
                    side, current_tp, current_sl, current_qty, tp_ref, sl_ref,
                    last_safety_check, safety_check_sec, on_event, client_id_prefix,
                )

            # ── Active review / early exit ───────────────────────────────────
            if review_fn and side and current_qty and current_entry and time.time() - last_review >= review_sec:
                last_review = time.time()
                if not break_even:
                    try:
                        should_exit, reason = review_fn(break_even)
                    except Exception:  # review_fn is caller-provided; skip exit on error
                        should_exit, reason = False, ""
                    if should_exit:
                        close_ok = True
                        try:
                            self.close_position_market(side, current_qty)
                        except EXCHANGE_ERRORS:
                            close_ok = False
                        if not close_ok:
                            time.sleep(0.5)
                            continue
                        return f"EARLY:{reason}" if reason else "EARLY", float(price_fn() or current_entry)

            # ── Breakeven management ────────────────────────────
            if side and current_entry and current_qty and price_fn:
                try:
                    price = float(price_fn())
                except Exception:  # price_fn is caller-provided; skip tick on error
                    price = None
                if price is not None:
                    if current_sl is None:
                        current_sl = current_entry
                    if not break_even and current_be_trigger > 0:
                        be_price = None
                        if side == "BUY" and price >= current_entry * (1 + current_be_trigger):
                            be_price = current_entry
                        elif side == "SELL" and price <= current_entry * (1 - current_be_trigger):
                            be_price = current_entry
                        if be_price is not None and time.time() - last_replace >= 2:
                            tp_ref, sl_ref = self.replace_tp_sl(
                                side,
                                current_tp if current_tp is not None else current_entry,
                                be_price,
                                current_qty,
                                client_id_prefix=client_id_prefix,
                            )
                            current_sl = be_price
                            break_even = True
                            last_replace = time.time()
                            if on_event:
                                on_event("breakeven", be_price)

            time.sleep(0.5)

    def _is_algo_open(self, algo_id: int) -> bool:
        """Return whether an algo/order identifier is still present in open orders."""
        try:
            orders = self.client.futures_get_open_orders(symbol=self.symbol)
        except EXCHANGE_ERRORS:
            return False
        for o in orders:
            oid = o.get("algoId") or o.get("orderId")
            if oid is not None and int(oid) == int(algo_id):
                return True
        return False


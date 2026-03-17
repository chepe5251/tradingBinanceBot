"""Application entrypoint for the Binance Futures scalping bot.

This module wires together configuration loading, market-data streaming,
signal evaluation, order execution, and lifecycle monitoring.
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from binance import Client

from config import from_env
from data_stream import MarketDataStream
from strategy import evaluate_signal
from risk import RiskManager
from execution import FuturesExecutor

_TELEGRAM_SEND_LOCK = threading.Lock()
_TELEGRAM_LAST_SEND_TS = 0.0
_TELEGRAM_MIN_INTERVAL_SEC = 1.2


def _configure_client(api_key: str, api_secret: str, testnet: bool) -> Client:
    """Build a Binance client and normalize futures endpoint selection."""
    client = Client(api_key, api_secret, testnet=testnet)
    if testnet:
        client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
    return client


def _get_available_balance(client: Client) -> float:
    """Return current available USDT balance from futures account."""
    balances = client.futures_account_balance()
    for b in balances:
        if b.get("asset") == "USDT":
            return float(b.get("availableBalance", 0))
    return 0.0


def _has_any_position(client: Client) -> bool:
    """Return True if there is any open position or a pending entry order.

    TP/SL orders (TAKE_PROFIT_MARKET, STOP_MARKET) are ignored because they
    are always present while a position is monitored.  Only unfilled entry
    orders (LIMIT, MARKET) count as a blocker.
    Defaults to True (block) on any API error.
    """
    _PROTECTION_TYPES = {"TAKE_PROFIT_MARKET", "STOP_MARKET", "TRAILING_STOP_MARKET"}
    try:
        positions = client.futures_position_information()
        for p in positions:
            if abs(float(p.get("positionAmt", 0))) > 0:
                return True
    except Exception:
        return True
    try:
        open_orders = client.futures_get_open_orders()
        for o in open_orders:
            if o.get("type") not in _PROTECTION_TYPES:
                return True  # pending entry order
    except Exception:
        return True
    return False


def _cleanup_open_orders(client: Client, symbols: list[str], logger: logging.Logger) -> None:
    """Cancel open orders for a target symbol universe."""
    allowed_symbols = set(symbols)
    try:
        open_orders = client.futures_get_open_orders()
    except Exception as exc:
        logger.warning("Failed to fetch open orders for cleanup: %s", exc)
        return

    open_counts: dict[str, int] = {}
    for order in open_orders:
        sym = order.get("symbol")
        if not sym:
            continue
        if allowed_symbols and sym not in allowed_symbols:
            continue
        open_counts[sym] = open_counts.get(sym, 0) + 1

    if not open_counts:
        logger.info("No open orders found for cleanup.")
        return

    for sym, count in sorted(open_counts.items()):
        try:
            client.futures_cancel_all_open_orders(symbol=sym)
            logger.info("Canceled %d open orders for %s", count, sym)
        except Exception as exc:
            logger.warning("Failed to cancel open orders for %s: %s", sym, exc)


def _load_all_usdt_perp_symbols(client: Client, logger: logging.Logger, limit: int | None = None) -> list[str]:
    """Load tradable USDT perpetual symbols, optionally ranked by quote volume."""
    try:
        info = client.futures_exchange_info()
    except Exception as exc:
        logger.warning("Failed to load exchange info for symbols: %s", exc)
        return []
    symbols: list[str] = []
    pattern = re.compile(r"^[A-Z0-9]{2,20}USDT$")
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("contractType") != "PERPETUAL":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        sym = s.get("symbol")
        if sym and pattern.match(sym):
            symbols.append(sym)
    symbols = sorted(set(symbols))
    if not symbols:
        return symbols

    # Keep only top symbols by 24h quote volume if a limit is provided.
    if limit is not None and limit > 0 and len(symbols) > limit:
        try:
            ticker_24h = client.futures_ticker()
            vol_map: dict[str, float] = {}
            for item in ticker_24h:
                sym = item.get("symbol")
                if not sym:
                    continue
                try:
                    vol_map[sym] = float(item.get("quoteVolume", 0.0) or 0.0)
                except Exception:
                    vol_map[sym] = 0.0
            symbols = sorted(symbols, key=lambda s: vol_map.get(s, 0.0), reverse=True)[:limit]
            logger.info("Loaded top %d USDT perpetual symbols by 24h quote volume", len(symbols))
        except Exception as exc:
            logger.warning("Failed to load 24h volume ranking, using first %d symbols: %s", limit, exc)
            symbols = symbols[:limit]
    else:
        logger.info("Loaded %d USDT perpetual symbols", len(symbols))

    return symbols


def _ema(series, period: int):
    """Compute exponential moving average for a series."""
    return series.ewm(span=period, adjust=False).mean()


def _context_direction(df, ema_period: int) -> str | None:
    """Infer directional bias from close price versus EMA."""
    if df.empty or len(df) < ema_period:
        return None
    ema = _ema(df["close"], ema_period)
    if ema.isna().iloc[-1]:
        return None
    return "LONG" if df["close"].iloc[-1] > ema.iloc[-1] else "SHORT"


def _context_slope(df, ema_period: int) -> float:
    """Compute normalized EMA slope as trend-strength proxy."""
    if df.empty or len(df) < ema_period + 2:
        return 0.0
    ema = _ema(df["close"], ema_period)
    last = ema.iloc[-1]
    prev = ema.iloc[-2]
    if prev == 0 or prev != prev:
        return 0.0
    return (last - prev) / prev


def _calc_atr(df, period: int) -> float:
    """Compute the latest ATR value using EWMA true range."""
    if df.empty or len(df) < period + 2:
        return 0.0
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = (
        (high - low).abs()
        .to_frame("hl")
        .join((high - prev_close).abs().to_frame("hc"))
        .join((low - prev_close).abs().to_frame("lc"))
        .max(axis=1)
    )
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    return float(atr.iloc[-1]) if not atr.isna().iloc[-1] else 0.0


def _calc_atr_avg(df, period: int, window: int) -> float:
    """Compute rolling ATR average for volatility regime checks."""
    if df.empty or len(df) < period + window + 2:
        return 0.0
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = (
        (high - low).abs()
        .to_frame("hl")
        .join((high - prev_close).abs().to_frame("hc"))
        .join((low - prev_close).abs().to_frame("lc"))
        .max(axis=1)
    )
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    atr_avg = atr.rolling(window=window).mean()
    return float(atr_avg.iloc[-1]) if not atr_avg.isna().iloc[-1] else 0.0


def _calc_rsi(df, period: int) -> float:
    """Compute latest RSI value using EWMA gains/losses."""
    if df.empty or len(df) < period + 2:
        return 0.0
    close = df["close"]
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    last = rsi.iloc[-1]
    return float(last) if last == last else 0.0


def _get_mark_price(client: Client, symbol: str) -> float | None:
    """Fetch mark price for one symbol; return None on API error."""
    try:
        data = client.futures_mark_price(symbol=symbol)
        return float(data.get("markPrice"))
    except Exception:
        return None


def _send_telegram_message(token: str, chat_id: str, message: str) -> None:
    """Send Telegram alert with retry and bounded backoff."""
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = urlencode({"chat_id": chat_id, "text": message})
    last_exc: Exception | None = None
    for attempt in range(1, 6):
        try:
            with _TELEGRAM_SEND_LOCK:
                global _TELEGRAM_LAST_SEND_TS
                now = time.time()
                wait_sec = _TELEGRAM_MIN_INTERVAL_SEC - (now - _TELEGRAM_LAST_SEND_TS)
                if wait_sec > 0:
                    time.sleep(wait_sec)
                req = Request(
                    url,
                    data=payload.encode("utf-8"),
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    method="POST",
                )
                with urlopen(req, timeout=10):
                    _TELEGRAM_LAST_SEND_TS = time.time()
                    return
        except HTTPError as exc:
            last_exc = exc
            if exc.code == 429:
                retry_after = 5.0
                try:
                    header_val = exc.headers.get("Retry-After")
                    if header_val:
                        retry_after = max(1.0, float(header_val))
                except Exception:
                    retry_after = 5.0
                time.sleep(retry_after)
                continue
            if attempt < 5:
                time.sleep(min(float(attempt), 5.0))
        except Exception as exc:
            last_exc = exc
            if attempt < 5:
                time.sleep(min(float(attempt), 5.0))
    if last_exc is not None:
        raise last_exc


def _fmt_side(side: str) -> str:
    """Map exchange side to operator-facing direction label."""
    if side == "BUY":
        return "LONG"
    if side == "SELL":
        return "SHORT"
    return side


def _macd_ok(df, side: str) -> bool:
    """Validate directional MACD alignment for final momentum gating."""
    if df.empty or len(df) < 35:
        return False
    close = df["close"]
    ema_fast = close.ewm(span=12, adjust=False).mean()
    ema_slow = close.ewm(span=26, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    hist = macd_line - signal_line
    macd_last = float(macd_line.iloc[-1])
    signal_last = float(signal_line.iloc[-1])
    hist_last = float(hist.iloc[-1])
    if side == "BUY":
        return macd_last > signal_last and hist_last > 0
    if side == "SELL":
        return macd_last < signal_last and hist_last < 0
    return False


def _layered_signal_check(
    signal: dict | None,
    df,
    side: str | None,
    ctx_dir: str | None,
    trend_clear: bool,
    ema_fast_period: int,
    atr_val: float,
    volume_ok: bool,
    rsi_last: float,
) -> tuple[bool, str]:
    """Apply layered hard filters before allowing a signal for execution."""
    if not signal or not side:
        return False, "sin_senal"

    # CAPA 1: contexto HTF alineado (veto obligatorio)
    if side == "BUY":
        layer_1_ok = ctx_dir == "LONG" and trend_clear
    elif side == "SELL":
        layer_1_ok = ctx_dir == "SHORT" and trend_clear
    else:
        layer_1_ok = False
    if not layer_1_ok:
        return False, "contexto_htf_desalineado"

    # CAPA 2: estructura clara (veto obligatorio)
    estructura_valida = bool(signal.get("estructura_valida") or signal.get("structure_ok"))
    retroceso_valido = bool(signal.get("retroceso_valido"))
    layer_2_ok = estructura_valida and retroceso_valido and trend_clear
    if not layer_2_ok:
        return False, "estructura_no_valida"

    # CAPA 3: no sobreextension / no FOMO (veto obligatorio)
    if df.empty or len(df) < max(ema_fast_period, 2):
        return False, "datos_insuficientes"
    price = float(signal.get("price") or 0.0)
    if price <= 0 or atr_val <= 0:
        return False, "atr_o_precio_invalido"
    ema_fast_series = _ema(df["close"], ema_fast_period)
    ema_fast_last = float(ema_fast_series.iloc[-1])
    distance_fast_atr = abs(price - ema_fast_last) / atr_val if atr_val > 0 else 999.0
    last = df.iloc[-1]
    entry_body = abs(float(last["close"] - last["open"]))
    layer_3_ok = distance_fast_atr <= 1.8 and entry_body <= (2.0 * atr_val)
    if not layer_3_ok:
        return False, "sobreextension_o_fomo"

    # CAPA 4: momentum real (veto obligatorio)
    volumen_confirmado = bool(signal.get("volumen_confirmado") or signal.get("volume_ok"))
    if side == "BUY":
        rsi_ok = 40.0 <= rsi_last <= 70.0
    else:
        rsi_ok = 30.0 <= rsi_last <= 60.0
    macd_ok = _macd_ok(df, side)
    layer_4_ok = volume_ok and volumen_confirmado and rsi_ok and macd_ok
    if not layer_4_ok:
        return False, "momentum_insuficiente"

    return True, "ok"


def _fmt_validation_reason(reason: str) -> str:
    """Translate internal reject codes to readable Spanish labels."""
    mapping = {
        "contexto_htf_desalineado": "Contexto HTF desalineado",
        "estructura_no_valida": "Estructura de pullback no valida",
        "datos_insuficientes": "Datos insuficientes",
        "atr_o_precio_invalido": "ATR o precio invalido",
        "sobreextension_o_fomo": "Precio sobreextendido o vela FOMO",
        "momentum_insuficiente": "Momentum sin confirmacion",
        "sin_senal": "No hay setup operativo",
    }
    return mapping.get(reason, reason)


def _format_signal_message(
    symbol: str,
    side: str,
    timeframe: str,
    htf_bias: str,
    entry: float,
    sl: float,
    tp: float,
    rr: float,
    quality: str,
    volatility: str,
    structure: str,
) -> str:
    """Render the canonical Telegram signal message template."""
    is_long = side == "BUY"
    direction = "🟢 LONG" if is_long else "🔴 SHORT"
    htf_txt = "📈 Alcista" if htf_bias == "LONG" else "📉 Bajista"
    warning = "⚠️ No perseguir precio. Cancelar si se extiende."
    head = "🚀 SEÑAL CONFIRMADA" if is_long else "📉 SEÑAL CONFIRMADA"
    return (
        f"{head}\n"
        f"{direction} | {symbol} | {timeframe}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🧠 Sesgo HTF: {htf_txt}\n"
        f"🏗 Estructura: {structure}\n"
        f"📊 Calidad: {quality}\n"
        f"🌪 Volatilidad: {volatility}\n"
        "\n"
        f"🎯 Entrada: {entry:.6f}\n"
        f"🛑 Stop Loss: {sl:.6f}\n"
        f"💰 Take Profit: {tp:.6f}\n"
        f"📐 R:R: 1:{rr:.2f}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"{warning}"
    )


def _format_trade_event_message(symbol: str, title: str, detail: str) -> str:
    """Build compact Telegram messages for entry/exit/rebuy events."""
    return f"{title}\n{symbol}\n{detail}"


def main() -> None:
    """Bootstrap services and run the bot heartbeat loop."""
    settings = from_env()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    class _SuppressReadLoopClosed(logging.Filter):
        """Drop noisy websocket close records already handled by auto-restart logic."""

        def filter(self, record: logging.LogRecord) -> bool:
            """Return `False` for expected read-loop-close messages."""
            msg = record.getMessage()
            return "Read loop has been closed" not in msg

    for name in ("binance", "binance.ws", "binance.ws.threaded_stream"):
        lg = logging.getLogger(name)
        lg.setLevel(logging.CRITICAL)
        lg.addFilter(_SuppressReadLoopClosed())
        lg.propagate = False

    logger = logging.getLogger("bot")
    trades_logger = logging.getLogger("trades")
    trades_logger.setLevel(logging.INFO)
    if not os.path.exists("logs"):
        os.makedirs("logs", exist_ok=True)
    trades_handler = logging.FileHandler("logs/trades.log")
    trades_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
    trades_logger.addHandler(trades_handler)

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    telegram_enabled = bool(telegram_token and telegram_chat_id)
    if not settings.use_paper_trading and (not api_key or not api_secret):
        raise RuntimeError("Missing BINANCE_API_KEY or BINANCE_API_SECRET in .env")

    trade_client = _configure_client(api_key or "", api_secret or "", settings.use_testnet)
    data_client = _configure_client("", "", settings.data_use_testnet)

    symbols = _load_all_usdt_perp_symbols(data_client, logger, limit=None)
    if not symbols:
        symbols = settings.symbols or [settings.symbol]
        logger.warning("Falling back to configured symbols (%d)", len(symbols))

    stream = MarketDataStream(
        client=data_client,
        symbols=symbols,
        main_interval=settings.main_interval,
        main_limit=settings.history_candles_main,
        api_key=api_key or "",
        api_secret=api_secret or "",
        testnet=settings.data_use_testnet,
        context_interval=settings.context_interval,
        context_limit=settings.history_candles_context,
    )
    stream.load_initial()

    executors: dict[str, FuturesExecutor] = {}

    def get_executor(sym: str) -> FuturesExecutor:
        """Lazily create and cache one executor per symbol."""
        if sym not in executors:
            ex = FuturesExecutor(
                client=trade_client,
                symbol=sym,
                leverage=settings.leverage,
                margin_type=settings.margin_type,
                paper=settings.use_paper_trading,
            )
            ex.setup()
            executors[sym] = ex
        return executors[sym]

    risk = RiskManager(
        cooldown_sec=settings.cooldown_sec,
        max_consecutive_losses=settings.max_consecutive_losses,
        daily_drawdown_limit=settings.daily_drawdown_limit,
        daily_drawdown_limit_usdt=settings.daily_drawdown_limit_usdt,
        loss_pause_sec=settings.risk_pause_after_losses_sec,
        volatility_pause=False,
        volatility_threshold=0.0,
    )
    if settings.use_paper_trading:
        risk.init_equity(settings.paper_start_balance)
    else:
        try:
            available = _get_available_balance(trade_client)
        except Exception as exc:
            logger.warning("Failed to fetch available balance: %s", exc)
            available = 0.0
        risk.init_equity(available)

        try:
            if not _has_any_position(trade_client):
                _cleanup_open_orders(trade_client, symbols, logger)
            else:
                logger.info("Open position detected; skipping open-order cleanup.")
        except Exception as exc:
            logger.warning("Open-order cleanup failed: %s", exc)

    last_eval_close_ms: int | None = None
    eval_close_lock = threading.Lock()

    def tg_send(message: str) -> None:
        """Safe Telegram wrapper that logs failures without stopping the bot."""
        if not telegram_enabled:
            return
        try:
            _send_telegram_message(telegram_token, telegram_chat_id, message)
        except Exception as exc:
            logger.warning("Telegram send failed: %s", exc)

    def on_main_close(symbol: str) -> None:
        """Main evaluation callback executed on each closed main-interval candle."""
        nonlocal last_eval_close_ms
        now = datetime.now(timezone.utc)
        main_df = stream.get_dataframe(symbol, settings.main_interval)
        if main_df.empty:
            return

        close_time = main_df.iloc[-1]["close_time"]
        close_ms = int(close_time.timestamp() * 1000)
        with eval_close_lock:
            if last_eval_close_ms == close_ms:
                return
            last_eval_close_ms = close_ms

        can_trade_now = risk.can_trade(now)

        try:
            positions_snapshot = trade_client.futures_position_information()
        except Exception as exc:
            logger.warning("Failed to fetch positions for entry gate: %s", exc)
            return
        active_positions = 0
        for p in positions_snapshot:
            try:
                amt = float(p.get("positionAmt", 0))
            except Exception:
                amt = 0.0
            if abs(amt) > 0:
                active_positions += 1
        has_open_position = active_positions > 0

        valid_signals: list[tuple[str, dict]] = []

        for sym in symbols:
            df = stream.get_dataframe(sym, settings.main_interval)
            if df.empty:
                continue
            ctx = stream.get_dataframe(sym, settings.context_interval)
            if ctx.empty:
                continue

            signal = evaluate_signal(
                df,
                ctx,
                settings.ema_trend,
                settings.ema_fast,
                settings.ema_mid,
                settings.atr_period,
                settings.atr_avg_window,
                settings.volume_avg_window,
                settings.rsi_period,
                settings.rsi_long_min,
                settings.rsi_long_max,
                settings.rsi_short_min,
                settings.rsi_short_max,
                settings.volume_min_ratio,
            )
            if not signal:
                continue

            side = signal.get("side") or "NONE"
            trades_logger.info(
                "signal %s side=%s m15=%s breakout=%s",
                sym,
                side,
                signal.get("confirm_m15", ""),
                signal.get("breakout_time", ""),
            )
            candidate = (sym, signal)
            valid_signals.append(candidate)

        if not valid_signals:
            return

        valid_signals.sort(
            key=lambda item: float((item[1] or {}).get("score") or 0.0),
            reverse=True,
        )
        best = valid_signals[0]

        execution_allowed = can_trade_now and not has_open_position
        block_reason = ""
        if has_open_position:
            block_reason = "SEÑAL BLOQUEADA POR OPERACIÓN ACTIVA"
        elif not can_trade_now:
            block_reason = "BLOQUEADA POR RISK MANAGER"
        if execution_allowed:
            try:
                if _has_any_position(trade_client):
                    execution_allowed = False
                    block_reason = "SEÑAL BLOQUEADA POR OPERACIÓN ACTIVA"
            except Exception as exc:
                logger.warning("Failed position gate before execution: %s", exc)
                execution_allowed = False
                block_reason = "BLOQUEADA: ERROR VERIFICANDO POSICIÓN"

        for sig_symbol, sig_signal in valid_signals:
            sig_side = sig_signal["side"]
            if sig_side == "BUY":
                sig_entry = sig_signal["price"] * (1 - settings.limit_offset_pct)
            else:
                sig_entry = sig_signal["price"] * (1 + settings.limit_offset_pct)

            sig_atr_val = float(sig_signal.get("atr") or 0.0)
            sig_atr_avg = float(sig_signal.get("atr_avg") or 0.0)
            sig_atr_ratio = (sig_atr_val / sig_atr_avg) if sig_atr_avg > 0 else 0.0

            sig_risk = float(sig_signal.get("risk_per_unit") or 0.0)
            sig_rr = max(float(sig_signal.get("rr_target") or 0.0), 1.8)
            sig_risk_distance = (
                sig_risk
                if sig_risk > 0
                else max(sig_atr_val * settings.atr_sl_mult, sig_entry * settings.min_sl_pct)
            )
            if sig_risk_distance <= 0:
                trades_logger.info("skip %s reason=signal_message_risk_invalid", sig_symbol)
                continue

            if sig_side == "BUY":
                sig_sl = sig_entry - sig_risk_distance
                sig_tp = sig_entry + (sig_risk_distance * sig_rr)
            else:
                sig_sl = sig_entry + sig_risk_distance
                sig_tp = sig_entry - (sig_risk_distance * sig_rr)
            sig_rr_value = abs(sig_tp - sig_entry) / abs(sig_entry - sig_sl) if sig_entry != sig_sl else 0.0

            sig_quality = (
                "A+"
                if bool(sig_signal.get("retroceso_valido"))
                and bool(sig_signal.get("volumen_confirmado"))
                and sig_atr_ratio >= 1.2
                else "A"
            )
            sig_volatility = "Alta" if sig_atr_ratio >= 1.2 else "Normal"
            sig_structure = "Pullback limpio + Continuacion"
            sig_htf_bias = str(sig_signal.get("htf_bias") or ("LONG" if sig_side == "BUY" else "SHORT"))
            tg_send(
                _format_signal_message(
                    symbol=sig_symbol,
                    side=sig_side,
                    timeframe=settings.main_interval.upper(),
                    htf_bias=sig_htf_bias,
                    entry=sig_entry,
                    sl=sig_sl,
                    tp=sig_tp,
                    rr=sig_rr_value,
                    quality=sig_quality,
                    volatility=sig_volatility,
                    structure=sig_structure,
                )
            )

        symbol, signal = best
        side = signal["side"]

        if not execution_allowed:
            trades_logger.info(
                "signal_only %s side=%s reason=%s total_valid=%d",
                symbol,
                side,
                block_reason or "BLOQUEADA",
                len(valid_signals),
            )
            return

        if side == "BUY":
            entry_price = signal["price"] * (1 - settings.limit_offset_pct)
        else:
            entry_price = signal["price"] * (1 + settings.limit_offset_pct)

        signal_risk = float(signal.get("risk_per_unit") or 0.0)
        atr_val = float(signal.get("atr") or 0.0)
        if atr_val <= 0:
            atr_val = _calc_atr(stream.get_dataframe(symbol, settings.main_interval), settings.atr_period)
        if atr_val <= 0:
            trades_logger.info("skip %s reason=atr_invalid", symbol)
            return
        signal_rr = max(float(signal.get("rr_target") or 0.0), 1.8)
        risk_distance = signal_risk if signal_risk > 0 else max(atr_val * settings.atr_sl_mult, entry_price * settings.min_sl_pct)
        reward_distance = risk_distance * signal_rr
        if side == "BUY":
            tp = entry_price + reward_distance
            sl = entry_price - risk_distance
        else:
            tp = entry_price - reward_distance
            sl = entry_price + risk_distance


        try:
            available_balance = _get_available_balance(trade_client)
        except Exception as exc:
            logger.warning("Failed to fetch available balance: %s", exc)
            trades_logger.info("skip %s reason=balance_fetch_failed", symbol)
            return

        margin_to_use = min(settings.fixed_margin_per_trade_usdt, available_balance)
        if margin_to_use <= 0:
            trades_logger.info("skip %s reason=available_balance_low", symbol)
            return

        executor = get_executor(symbol)
        min_qty = executor.get_min_qty()
        min_notional = executor.get_min_notional()

        df_entry = stream.get_dataframe(symbol, settings.main_interval)
        if df_entry.empty or len(df_entry) < 12:
            trades_logger.info("skip %s reason=entry_df_insufficient", symbol)
            return
        swing_window = df_entry.iloc[-10:-1]
        swing_low = float(swing_window["low"].min())
        swing_high = float(swing_window["high"].max())
        if side == "BUY":
            sl_swing = swing_low
            sl_atr = entry_price - (settings.stop_atr_mult * atr_val)
            sl_common = min(sl_swing, sl_atr)
        else:
            sl_swing = swing_high
            sl_atr = entry_price + (settings.stop_atr_mult * atr_val)
            sl_common = max(sl_swing, sl_atr)
        # Keep a structural SL reference for TP sizing; SL may be widened later for loss-based scaling.
        sl_tp_ref = float(sl_common)

        qty_by_margin = executor.calc_qty(margin_to_use, entry_price)
        if qty_by_margin <= 0:
            trades_logger.info("skip %s reason=qty_by_margin_invalid", symbol)
            return
        qty_l1 = executor.round_qty(qty_by_margin)

        # Ensure SL is beyond the 150% floating-loss threshold before forced stop.
        margin_initial_ref = float(settings.fixed_margin_per_trade_usdt)
        if margin_initial_ref > 0 and qty_l1 > 0:
            min_sl_distance_for_rebuy = (margin_initial_ref * 1.5) / qty_l1
            if side == "BUY":
                sl_required = entry_price - min_sl_distance_for_rebuy
                if sl_common > sl_required:
                    sl_common = sl_required
            else:
                sl_required = entry_price + min_sl_distance_for_rebuy
                if sl_common < sl_required:
                    sl_common = sl_required

        risk_distance_unit = abs(entry_price - sl_common)
        if risk_distance_unit <= 0:
            trades_logger.info("skip %s reason=risk_distance_invalid", symbol)
            return

        notional = qty_l1 * entry_price

        if (
            qty_l1 <= 0
            or (min_qty > 0 and qty_l1 < min_qty)
            or (min_notional > 0 and notional < min_notional)
        ):
            trades_logger.info(
                "skip %s reason=entry_notional_invalid avail=%.4f margin=%.4f qty_l1=%.6f notional=%.4f min_notional=%.4f",
                symbol,
                available_balance,
                margin_to_use,
                qty_l1,
                notional,
                min_notional,
            )
            return

        try:
            trade_client.futures_cancel_all_open_orders(symbol=symbol)
        except Exception as exc:
            logger.warning("Pre-entry open-order cleanup failed for %s: %s", symbol, exc)

        try:
            filled_qty, avg_price, exec_type = executor.place_limit_with_market_fallback(
                side=side,
                price=entry_price,
                qty=qty_l1,
                timeout_sec=settings.limit_timeout_sec,
            )
        except Exception as exc:
            logger.error("Order placement failed %s: %s", symbol, exc)
            trades_logger.info("error %s stage=entry_l1 msg=%s", symbol, exc)
            return

        if filled_qty <= 0:
            trades_logger.info("skip %s reason=entry_l1_not_filled", symbol)
            return

        entry_price = avg_price
        risk_distance = abs(entry_price - sl_common)
        if risk_distance <= 0:
            trades_logger.info("skip %s reason=post_fill_risk_invalid", symbol)
            return
        tp_risk_cap = abs(entry_price - sl_tp_ref)
        if tp_risk_cap <= 0:
            tp_risk_cap = risk_distance
        tp_risk_basis = min(risk_distance, tp_risk_cap)
        tp_rr_effective = max(float(settings.tp_rr), 1.8)
        tp = (
            entry_price + (tp_rr_effective * tp_risk_basis)
            if side == "BUY"
            else entry_price - (tp_rr_effective * tp_risk_basis)
        )
        breakeven_trigger_pct_trade = (
            max((risk_distance / entry_price), 0.005) if entry_price > 0 else 0.005
        )
        filled_qty = executor.round_qty(filled_qty)

        trade_state = {
            "entry_price": entry_price,
            "qty": filled_qty,
            "sl": sl_common,
            "tp": tp,
            "risk_distance": risk_distance,
            "breakeven_trigger_pct": breakeven_trigger_pct_trade,
            # Anchor scale triggers to the first 5 USDT block.
            "anchor_entry_price": entry_price,
            "anchor_risk_distance": risk_distance,
            # TP should not be inflated by the widened emergency SL.
            "tp_risk_cap": tp_risk_cap,
        }


        level_state = {
            "loss_l1_done": False,
            "loss_l2_done": False,
            "loss_l3_done": False,
            "loss_l4_done": False,
            "loss_l5_done": False,
            "loss_l1_attempts": 0,
            "loss_l2_attempts": 0,
            "loss_l3_attempts": 0,
            "loss_l4_attempts": 0,
            "loss_l5_attempts": 0,
            "loss_l1_next_try_ts": 0.0,
            "loss_l2_next_try_ts": 0.0,
            "loss_l3_next_try_ts": 0.0,
            "loss_l4_next_try_ts": 0.0,
            "loss_l5_next_try_ts": 0.0,
        }
        def price_fn() -> float | None:
            """Provide live mark price to the protection monitor."""
            return _get_mark_price(trade_client, symbol)

        def atr_fn() -> float | None:
            """Provide updated ATR snapshots to trailing logic."""
            df = stream.get_dataframe(symbol, settings.main_interval)
            return _calc_atr(df, settings.atr_period)

        def on_event(kind: str, new_sl: float) -> None:
            """Capture protection events in the trade log."""
            trades_logger.info("%s %s new_sl=%.4f", kind, symbol, new_sl)

        def protect_and_monitor() -> None:
            """Place protections and run continuous trade supervision thread."""
            attempts = 0
            emergency = False
            tp_ref = sl_ref = None
            client_id_prefix = f"{symbol}-{int(time.time() * 1000)}"
            position_wait_deadline = time.time() + 8.0

            def scale_fn(state: dict) -> dict | None:
                """Evaluate and execute loss-based scaling stages while preserving protections."""
                now_ts = time.time()
                if (
                    level_state["loss_l1_done"]
                    and level_state["loss_l2_done"]
                    and level_state["loss_l3_done"]
                    and level_state["loss_l4_done"]
                    and level_state["loss_l5_done"]
                ):
                    return None
                df_scale = stream.get_dataframe(symbol, settings.main_interval)
                if df_scale.empty or len(df_scale) < max(settings.ema_mid + 2, 10):
                    return None
                mark = price_fn()
                if mark is None:
                    return None
                mark = float(mark)
                sl_ref_price = float(trade_state["sl"])
                entry_ref = float(trade_state.get("anchor_entry_price", trade_state["entry_price"]))
                risk_ref = max(0.0, float(trade_state.get("anchor_risk_distance", abs(entry_ref - sl_ref_price))))

                def _defer_level(level_key: str, reason: str, exc: Exception | None = None) -> None:
                    """Back off failed scale attempts and disable level after max retries."""
                    attempts_key = f"{level_key}_attempts"
                    next_try_key = f"{level_key}_next_try_ts"
                    attempts = int(level_state.get(attempts_key, 0)) + 1
                    level_state[attempts_key] = attempts
                    if attempts >= 5:
                        level_state[f"{level_key}_done"] = True
                        trades_logger.info(
                            "skip %s reason=loss_scale_disabled level=%s attempts=%d last_reason=%s",
                            symbol,
                            level_key,
                            attempts,
                            reason,
                        )
                        return
                    delay = min(60, 2 ** attempts)
                    level_state[next_try_key] = now_ts + delay
                    if exc is not None:
                        trades_logger.info(
                            "retry %s level=%s in=%ss reason=%s msg=%s",
                            symbol,
                            level_key,
                            delay,
                            reason,
                            exc,
                        )
                    else:
                        trades_logger.info(
                            "retry %s level=%s in=%ss reason=%s",
                            symbol,
                            level_key,
                            delay,
                            reason,
                        )


                if side == "BUY":
                    if mark <= sl_ref_price:
                        return {"close_all": True, "reason": "scale_structure_break", "exit_price": mark}
                    structure_ok = float(df_scale["close"].iloc[-1]) > sl_ref_price
                else:
                    if mark >= sl_ref_price:
                        return {"close_all": True, "reason": "scale_structure_break", "exit_price": mark}
                    structure_ok = float(df_scale["close"].iloc[-1]) < sl_ref_price

                if not structure_ok:
                    return {"close_all": True, "reason": "scale_structure_break", "exit_price": mark}

                # Floating loss (unrealized) from current average entry.
                current_entry = float(state["entry_price"])
                current_qty = float(state["qty"])
                floating_pnl = (mark - current_entry) * current_qty
                if side == "SELL":
                    floating_pnl = -floating_pnl
                floating_loss = abs(min(floating_pnl, 0.0))
                margin_initial = float(settings.fixed_margin_per_trade_usdt)
                if margin_initial <= 0:
                    return None

                level_key = ""
                trigger_label = ""
                add_margin = 0.0
                if (
                    not level_state["loss_l1_done"]
                    and now_ts >= float(level_state.get("loss_l1_next_try_ts", 0.0))
                    and floating_loss >= (0.5 * margin_initial)
                ):
                    level_key = "loss_l1"
                    trigger_label = "50%"
                    add_margin = margin_initial
                elif (
                    level_state["loss_l1_done"]
                    and not level_state["loss_l2_done"]
                    and now_ts >= float(level_state.get("loss_l2_next_try_ts", 0.0))
                    and floating_loss >= (1.0 * margin_initial)
                ):
                    level_key = "loss_l2"
                    trigger_label = "100%"
                    add_margin = margin_initial * 2.0
                elif (
                    level_state["loss_l2_done"]
                    and not level_state["loss_l3_done"]
                    and now_ts >= float(level_state.get("loss_l3_next_try_ts", 0.0))
                    and floating_loss >= (2.0 * margin_initial)
                ):
                    level_key = "loss_l3"
                    trigger_label = "200%"
                    add_margin = margin_initial * 4.0
                elif (
                    level_state["loss_l3_done"]
                    and not level_state["loss_l4_done"]
                    and now_ts >= float(level_state.get("loss_l4_next_try_ts", 0.0))
                    and floating_loss >= (4.0 * margin_initial)
                ):
                    level_key = "loss_l4"
                    trigger_label = "400%"
                    add_margin = margin_initial * 8.0
                elif (
                    level_state["loss_l4_done"]
                    and not level_state["loss_l5_done"]
                    and now_ts >= float(level_state.get("loss_l5_next_try_ts", 0.0))
                    and floating_loss >= (8.0 * margin_initial)
                ):
                    level_key = "loss_l5"
                    trigger_label = "800%"
                    add_margin = margin_initial * 16.0
                else:
                    return None

                if add_margin <= 0:
                    level_state[f"{level_key}_done"] = True
                    return None

                add_qty_plan = executor.calc_qty(add_margin, mark)
                add_qty = executor.round_qty(add_qty_plan)
                add_notional = add_qty * mark
                if (
                    add_qty <= 0
                    or (min_qty > 0 and add_qty < min_qty)
                    or (min_notional > 0 and add_notional < min_notional)
                ):
                    level_state[f"{level_key}_done"] = True
                    trades_logger.info(
                        "skip %s reason=loss_scale_qty_invalid level=%s margin=%.4f qty=%.6f notional=%.4f",
                        symbol,
                        level_key,
                        add_margin,
                        add_qty,
                        add_notional,
                    )
                    return None
                try:
                    if executor.paper:
                        add_filled, add_avg = add_qty, mark
                    else:
                        add_filled, add_avg = executor.place_market_entry(side, add_qty)
                except Exception as exc:
                    _defer_level(level_key, "loss_scale_market_error", exc)
                    return None
                if add_filled <= 0:
                    _defer_level(level_key, "loss_scale_market_no_fill")
                    return None
                add_avg = float(add_avg) if add_avg and add_avg > 0 else mark
                trades_logger.info(
                    "loss_scale %s level=%s trigger=%s floating_loss=%.4f add_margin=%.2f add_qty=%.6f mark=%.6f",
                    symbol,
                    level_key,
                    trigger_label,
                    floating_loss,
                    add_margin,
                    add_filled,
                    mark,
                )

                prev_qty = float(state["qty"])
                prev_entry = float(state["entry_price"])
                new_qty = executor.round_qty(prev_qty + add_filled)
                if new_qty <= 0:
                    return None
                new_entry = ((prev_entry * prev_qty) + (float(add_avg) * float(add_filled))) / new_qty
                new_risk = abs(new_entry - sl_ref_price)
                if new_risk <= 0:
                    return None
                tp_risk_cap_state = float(trade_state.get("tp_risk_cap", 0.0) or 0.0)
                tp_risk_basis = min(new_risk, tp_risk_cap_state) if tp_risk_cap_state > 0 else new_risk
                tp_rr_effective = max(float(settings.tp_rr), 1.8)
                new_tp = (
                    new_entry + (tp_rr_effective * tp_risk_basis)
                    if side == "BUY"
                    else new_entry - (tp_rr_effective * tp_risk_basis)
                )

                new_tp_ref = None
                new_sl_ref = None
                replace_exc = None
                for replace_attempt in range(1, 4):
                    try:
                        new_tp_ref, new_sl_ref = executor.replace_tp_sl(
                            side,
                            new_tp,
                            sl_ref_price,
                            new_qty,
                            client_id_prefix=client_id_prefix,
                        )
                        break
                    except Exception as exc:
                        replace_exc = exc
                        time.sleep(min(replace_attempt, 2))
                if not new_tp_ref or not new_sl_ref:
                    trades_logger.info(
                        "critical %s reason=loss_scale_protection_fail level=%s msg=%s",
                        symbol,
                        level_key,
                        replace_exc,
                    )
                    return {"close_all": True, "reason": "loss_scale_protection_fail", "exit_price": mark}

                level_state[f"{level_key}_done"] = True
                level_state[f"{level_key}_next_try_ts"] = 0.0

                trade_state["entry_price"] = new_entry
                trade_state["qty"] = new_qty
                trade_state["tp"] = new_tp
                trade_state["risk_distance"] = new_risk
                # Keep existing break-even logic; do not force break-even movement after repurchase.

                return {
                    "entry_price": new_entry,
                    "qty": new_qty,
                    "tp_price": new_tp,
                    "sl_price": sl_ref_price,
                    "breakeven_trigger_pct": float(state.get("breakeven_trigger_pct", trade_state.get("breakeven_trigger_pct", 0.005))),
                    "tp_ref": new_tp_ref,
                    "sl_ref": new_sl_ref,
                }

            while True:
                if not executor.paper:
                    try:
                        has_pos = executor.has_open_position()
                    except Exception:
                        has_pos = False
                    if not has_pos:
                        if time.time() < position_wait_deadline:
                            time.sleep(0.25)
                            continue
                        trades_logger.info("critical %s reason=position_closed_no_protection", symbol)
                        return
                try:
                    existing_tp, existing_sl = executor.get_protection_refs(
                        side, client_id_prefix=client_id_prefix
                    )
                    if existing_tp and existing_sl:
                        tp_ref, sl_ref = existing_tp, existing_sl
                        break
                    tp_ref, sl_ref = executor.place_tp_sl(
                        side,
                        float(trade_state["tp"]),
                        float(trade_state["sl"]),
                        float(trade_state["qty"]),
                        client_id_prefix=client_id_prefix,
                    )
                    if tp_ref and sl_ref:
                        break
                except Exception as exc:
                    logger.error("TP/SL placement failed %s: %s", symbol, exc)
                    trades_logger.info("error %s stage=tp_sl msg=%s", symbol, exc)

                attempts += 1
                if attempts >= 10 and not emergency:
                    emergency = True
                    trades_logger.info("critical %s reason=tp_sl_emergency", symbol)
                time.sleep(2 if emergency else 1)

            trades_logger.info(
                "entry %s side=%s exec=%s price=%.4f qty=%.6f atr=%.6f tp=%.4f sl=%.4f",
                symbol,
                side,
                exec_type,
                float(trade_state["entry_price"]),
                float(trade_state["qty"]),
                atr_val,
                float(trade_state["tp"]),
                float(trade_state["sl"]),
            )
            def review_fn(break_even: bool) -> tuple[bool, str]:
                """Run structure/volume/context reviews for potential early exits."""
                df = stream.get_dataframe(symbol, settings.main_interval)
                ctx = stream.get_dataframe(symbol, settings.context_interval)
                if df.empty or len(df) < max(settings.ema_mid, settings.ema_fast, settings.volume_avg_window + 2, 4):
                    return False, "no_data"

                ema20 = _ema(df["close"], settings.ema_fast)
                ema50 = _ema(df["close"], settings.ema_mid)
                last = df.iloc[-1]
                prev1 = df.iloc[-2]
                prev2 = df.iloc[-3]
                ema20_last = ema20.iloc[-1]
                ema20_prev = ema20.iloc[-2]
                ema50_last = ema50.iloc[-1]
                ema50_prev = ema50.iloc[-2]

                candle_range = last["high"] - last["low"]
                body = abs(last["close"] - last["open"])
                strong_body = candle_range > 0 and (body / candle_range) >= 0.6

                if side == "BUY":
                    cross_against = ema20_prev >= ema50_prev and ema20_last < ema50_last
                    close_against = last["close"] < ema50_last
                    struct_break = cross_against and close_against and strong_body
                    vol_against = last["close"] < last["open"]
                else:
                    cross_against = ema20_prev <= ema50_prev and ema20_last > ema50_last
                    close_against = last["close"] > ema50_last
                    struct_break = cross_against and close_against and strong_body
                    vol_against = last["close"] > last["open"]

                vol_avg = df["volume"].iloc[-(settings.volume_avg_window + 1):-1].mean()
                vol_strong = bool(vol_avg) and last["volume"] >= 2 * vol_avg
                volume_break = vol_strong and vol_against and struct_break

                ctx_dir = _context_direction(ctx, settings.ema_trend)
                ctx_slope = _context_slope(ctx, settings.ema_trend)
                ctx_changed = (
                    ctx_dir == "SHORT" if side == "BUY" else ctx_dir == "LONG"
                ) and abs(ctx_slope) >= settings.trend_slope_min

                tp_ok, sl_ok = executor.protection_status(side, client_id_prefix=client_id_prefix)
                trades_logger.info(
                    "monitor %s tp_ok=%s sl_ok=%s ctx_dir=%s ctx_slope=%.6f struct_break=%s vol_strong=%s",
                    symbol,
                    tp_ok,
                    sl_ok,
                    ctx_dir or "NONE",
                    ctx_slope,
                    struct_break,
                    vol_strong,
                )

                if break_even:
                    return False, "break_even_active"
                if volume_break:
                    return True, "volume_break"
                if struct_break:
                    return True, "structure_break"
                if ctx_changed:
                    return True, "ctx_flip"
                return False, ""

            result, exit_price = executor.monitor_oco(
                tp_ref,
                sl_ref,
                side=side,
                entry_price=float(trade_state["entry_price"]),
                tp_price=float(trade_state["tp"]),
                sl_price=float(trade_state["sl"]),
                qty=float(trade_state["qty"]),
                atr=atr_val,
                breakeven_trigger_pct=float(trade_state["breakeven_trigger_pct"]),
                trail_mult=0.8,
                trail_activation_pct=max(settings.trailing_activation_pct, 0.01),
                price_fn=price_fn,
                atr_fn=atr_fn,
                on_event=on_event,
                scale_fn=scale_fn,
                safety_check_sec=2,
                review_fn=review_fn,
                review_sec=7,
                client_id_prefix=client_id_prefix,
            )
            if result in {"TP", "SL"} or result.startswith("EARLY"):
                final_entry = float(trade_state["entry_price"])
                final_qty = float(trade_state["qty"])
                pnl = (exit_price - final_entry) * final_qty
                if side == "SELL":
                    pnl = -pnl
                risk.update_trade(pnl, datetime.now(timezone.utc))
                trades_logger.info("exit %s result=%s pnl=%.4f", symbol, result, pnl)
                return
            trades_logger.info("exit %s result=%s pnl=0.0", symbol, result)
            return
        threading.Thread(target=protect_and_monitor, daemon=True).start()

    logger.info("Starting WebSocket stream...")
    stream.start(on_main_close)

    try:
        last_heartbeat = time.time()
        while True:
            time.sleep(1)
            if time.time() - last_heartbeat >= settings.log_heartbeat_sec:
                st = stream.status()
                logger.info(
                    "Heartbeat: bot alive | events=%s last_event=%s last_close=%s last_event_symbol=%s last_close_symbol=%s",
                    st.get("event_count"),
                    st.get("last_event_ts"),
                    st.get("last_closed_ts"),
                    st.get("last_event_symbol"),
                    st.get("last_close_symbol"),
                )
                stream.restart_if_stale(max_idle_sec=30)
                last_heartbeat = time.time()
    except KeyboardInterrupt:
        logger.info("Stopping...")
        stream.stop()


if __name__ == "__main__":
    main()



